package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Dedicated SERVICE body9 signer. Its output is not Source apply or physical release authority. */
public final class TargetResourceReclaimSigner implements AutoCloseable {
    public static final long CONFIGURATION_WINDOW_BYTES=8L*(256*1024+3*1024)+4096;
    private static final int KEY_FILE_BYTES=1024;
    private static final int CONFIGURATION_BYTES=256*1024;
    private static final byte[] KEY_MATCH=Bytes.utf8("nereus-delay-native-reclaim-key-match\0");
    private final AuthorIdentity service;
    private final int version;
    private final byte[] root;
    private final PublicKey publicKey;
    private volatile PrivateKey privateKey;
    private final long notBefore,notAfter,maximumWidth;
    private final Runnable signingOwner;

    /** Key material is loaded and retained by the original bounded deployment owner, separately from body8 signing. */
    public TargetResourceReclaimSigner(AuthorIdentity service,int version,byte[] deploymentRoot,PublicKey publicKey,
            PrivateKey privateKey,long notBefore,long notAfter,long maximumWidth,Runnable signingOwner){
        this.service=Objects.requireNonNull(service);this.version=version;
        Bytes.requireLength(deploymentRoot,32,"Native reclaim deployment Root");
        if(service.kind()!=AuthorIdentity.Kind.SERVICE||service.canonicalBytes().length>1024||version==0
                ||Arrays.equals(deploymentRoot,new byte[32])||notBefore<0||notAfter<=notBefore||maximumWidth<1||maximumWidth>60000)
            throw new IllegalArgumentException("Native reclaim requires explicit SERVICE/purpose/Root/key/time bounds");
        this.root=Bytes.copy(deploymentRoot);this.publicKey=Objects.requireNonNull(publicKey);
        if(publicKey.getEncoded()==null||publicKey.getEncoded().length!=44)throw new IllegalArgumentException("Native reclaim requires public Ed25519 key");
        this.privateKey=Objects.requireNonNull(privateKey);this.notBefore=notBefore;this.notAfter=notAfter;
        this.maximumWidth=maximumWidth;this.signingOwner=Objects.requireNonNull(signingOwner);
    }

    /** Explicit independent purpose; never reuses body8 configuration, a current role, or embedded proof keys. */
    public static TargetResourceReclaimSigner loadConfigured(java.nio.file.Path configuration,byte[] actualRoot,
            BoundedReadBudget reads,Runnable originalOwner)throws java.io.IOException{
        Objects.requireNonNull(reads);Objects.requireNonNull(originalOwner);
        if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("finite Native signing-file read budget required");
        beforeFile(configuration,CONFIGURATION_BYTES,reads,originalOwner);
        final var fields=TargetWorkerRuntimeConfigLoader.loadFields(configuration);
        afterFile(reads,originalOwner);
        if(!fields.text("signing.purpose").equals("TARGET_RESOURCE_RECLAIM"))throw new IllegalArgumentException("independent Native reclaim signing purpose required");
        final byte[] root=fields.hex("signing.deploymentRootDigest",32,32);
        if(!Arrays.equals(root,actualRoot))throw new IllegalArgumentException("Native signing configuration changed original deployment Root");
        final long version=fields.unsigned64("signing.keyVersion");
        if(version==0||Long.compareUnsigned(version,0xffff_ffffL)>0)throw new IllegalArgumentException("Native signing key version is unassigned or out of range");
        final var serviceFile=fields.absolutePath("signing.serviceIdentityFile");
        final var publicFile=fields.absolutePath("signing.publicKeyFile");
        final var privateFile=fields.absolutePath("signing.privateKeyFile");
        final long before=fields.number("signing.notBeforeMs"),after=fields.number("signing.notAfterMs"),width=fields.number("signing.maximumDecisionWidthMs");
        fields.requireComplete();
        final var service=AuthorIdentity.decode(readKeyFile(serviceFile,reads,originalOwner));
        if(service.kind()!=AuthorIdentity.Kind.SERVICE||before<0||after<=before||width<1||width>60000)
            throw new IllegalArgumentException("Native signing SERVICE or finite key/time window is invalid");
        final byte[] publicBytes=readKeyFile(publicFile,reads,originalOwner);
        beforeFile(privateFile,KEY_FILE_BYTES,reads,originalOwner);
        final byte[] privateBytes=TargetWorkerRuntimeConfigLoader.loadBoundedImage(privateFile,KEY_FILE_BYTES);
        try{
            afterFile(reads,originalOwner);
            if(publicBytes.length!=44)throw new IllegalArgumentException("Native signing requires exact Ed25519 public key");
            final var factory=java.security.KeyFactory.getInstance("Ed25519");
            final var publicKey=factory.generatePublic(new java.security.spec.X509EncodedKeySpec(publicBytes));
            final var privateKey=factory.generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(privateBytes));
            final byte[] challenge=Bytes.sha256(KEY_MATCH,root,service.canonicalBytes(),Bytes.u32beBits((int)version));
            final var signer=java.security.Signature.getInstance("Ed25519");signer.initSign(privateKey);signer.update(challenge);
            final var verifier=java.security.Signature.getInstance("Ed25519");verifier.initVerify(publicKey);verifier.update(challenge);
            if(!verifier.verify(signer.sign()))throw new IllegalArgumentException("Native private/public signing keys differ");
            afterFile(reads,originalOwner);
            return new TargetResourceReclaimSigner(service,(int)version,root,publicKey,privateKey,before,after,width,originalOwner);
        }catch(java.security.GeneralSecurityException invalid){throw new IllegalArgumentException("invalid original Native signing key",invalid);}
        finally{Arrays.fill(privateBytes,(byte)0);}
    }
    private static byte[] readKeyFile(java.nio.file.Path path,BoundedReadBudget reads,Runnable current)throws java.io.IOException{
        beforeFile(path,KEY_FILE_BYTES,reads,current);
        final byte[] bytes=TargetWorkerRuntimeConfigLoader.loadBoundedImage(path,KEY_FILE_BYTES);
        afterFile(reads,current);return bytes;
    }
    private static void beforeFile(java.nio.file.Path path,int maximum,BoundedReadBudget reads,Runnable current){
        current.run();
        if(!reads.beforeRead()||!reads.tryCharge(Bytes.utf8(path.toAbsolutePath().normalize().toString()).length,maximum))throw reads.incomplete();
    }
    private static void afterFile(BoundedReadBudget reads,Runnable current){current.run();if(!reads.beforeTimedWork())throw reads.incomplete();}

    /**
     * Runs on the original bounded I/O owner, outside Worker/Store monitors. The caller's original hold
     * protects future-reader/Pin/image/Source admission and all inputs until the exact Source first and
     * handoff action have settled, or the original quarantined preparation alone has actually exited.
     * That local exit never drops durable recovery protections or decides an UNKNOWN append.
     * Signing an in-memory image does not publish it: no request escapes
     * this method until the real Catalog/images and actual Native writer closure have been verified.
     */
    Prepared prepare(TargetSourceApplyRuntime.ReclaimPreparationRead source,List<TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget recoveryReads,BoundedReadBudget nativeReads,java.util.function.Supplier<TrustedUtcIntervalEvidence> time,long retryUntil,
            long maximumInputBytes,Runnable originalHeld){
        Objects.requireNonNull(originalHeld);Objects.requireNonNull(source);Objects.requireNonNull(time);
        if(maximumInputBytes<1||maximumInputBytes==Long.MAX_VALUE||recoveryReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
            throw new IllegalArgumentException("Native signing requires finite original input/read reservations");
        final long window=Math.addExact(OxiaSyncCheckpointPublicationBackend.recoveryProtectionInputWindowBytes(images),
                4L*(TargetResourceReclaimBody.MAX_CANONICAL_BYTES+4096L));
        if(window>maximumInputBytes)throw new IllegalStateException("Native signing/full authentication exceeds original reserved input window");
        final Runnable current=()->{source.requireCurrent();originalHeld.run();};
        current.run();requireSigningCurrent(time,retryUntil);
        final var draft=source.selectBody(root,retryUntil,recoveryReads,current);
        final var body=draft.inlinePayloadRelease()?source.catalog().freezeInlinePayloadSourceFacts(draft,images,recoveryReads,maximumInputBytes,current):draft;
        final var key=privateKey;
        if(key==null)throw new IllegalStateException("original Native signing key closed");
        final var input=SystemMutation.signed(body.shard(),SystemMutationType.TARGET_RESOURCE_RECLAIM,retryUntil,
                body.logicalOperationIdentity(),body.canonicalBytes(),service.canonicalBytes(),version,key);
        if(!input.verifySignature(publicKey))throw new IllegalArgumentException("original Native private/public signing keys differ");
        current.run();requireSigningCurrent(time,retryUntil);
        final var recovery=source.catalog().captureNewNativeRecoveryProtection(input,images,recoveryReads,maximumInputBytes,current);
        source.requireClosedDraft(input,recovery,nativeReads,current);
        recovery.requireCurrent(recoveryReads,current);
        final var prepared=new Prepared(source,input,recovery,originalHeld);
        if(prepared.retainedBytes()>maximumInputBytes)throw new IllegalStateException("complete Native signing inputs exceed original retained window");
        current.run();requireSigningCurrent(time,retryUntil);
        if(!recoveryReads.beforeTimedWork()||!nativeReads.beforeTimedWork())throw recoveryReads.incomplete();
        prepared.requireCurrent();return prepared;
    }
    private void requireSigningCurrent(java.util.function.Supplier<TrustedUtcIntervalEvidence> source,long retryUntil){
        signingOwner.run();final var time=Objects.requireNonNull(source.get(),"original signing time provider");time.requireWidthAtMost(maximumWidth);
        if(privateKey==null||time.earliestEpochMs()<notBefore||time.latestEpochMs()>=notAfter||retryUntil<time.latestEpochMs())
            throw new IllegalStateException("original Native SERVICE signing owner/key/retry window is unavailable");
        signingOwner.run();
    }
    /** Only after the original signing I/O users have exited; exact signed Source frames keep their own lifetime. */
    @Override public void close(){privateKey=null;}

    public static final class Prepared {
        private final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        private final SystemMutation input;
        private final OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery;
        private final Runnable held;
        private Prepared(TargetSourceApplyRuntime.ReclaimPreparationRead source,SystemMutation input,
                OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,Runnable held){
            this.source=source;this.input=input;this.recovery=recovery;this.held=held;
        }
        void requireRuntime(TargetSourceApplyRuntime actual){source.requireRuntime(actual);}
        /** Immutable data remains inspectable for retained-first reconciliation after a preparation hold expires. */
        public SystemMutation mutation(){return input;}
        /** Local held checks only; Source will independently collect its own historical apply protections. */
        public void requireCurrent(){source.requireCurrent();held.run();recovery.requireHeldFor(source.catalog(),input);source.requireCurrent();}
        public long retainedBytes(){return Math.addExact(input.canonicalEnvelope().length+4096L,recovery.retainedBytes());}
    }
}
