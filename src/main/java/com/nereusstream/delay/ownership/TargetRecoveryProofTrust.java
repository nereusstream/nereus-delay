package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Original deployment approval of recovery-witness issuers/SDKs and retained historical material configurations. */
public final class TargetRecoveryProofTrust {
    /** Presence here explicitly approves RECOVERY_PROOF signing, not arbitrary SERVICE keys. */
    public record Issuer(AuthorIdentity service,int keyVersion,PublicKey publicKey,long notBeforeMs,long notAfterMs){
        public Issuer{
            if(Objects.requireNonNull(service).kind()!=AuthorIdentity.Kind.SERVICE||keyVersion==0||notBeforeMs<0||notAfterMs<=notBeforeMs)
                throw invalid();
            Objects.requireNonNull(publicKey);
        }
        String id(){return Bytes.hex(service.canonicalBytes())+":"+Integer.toUnsignedString(keyVersion);}
    }
    public record Sdk(String sourceCommit,String jarSha256){
        public Sdk{if(sourceCommit==null||!sourceCommit.matches("[0-9a-f]{40}")||jarSha256==null||!jarSha256.matches("[0-9a-f]{64}"))throw invalid();}
    }
    private final byte[] deployment;
    private final String prefix;
    private final Map<String,Issuer> issuers;
    private final Set<Sdk> sdks;
    private final Map<String,TargetRecoveryCredentialAuthorization.Trust> materialHistory;
    private final long maximumDecisionWidthMs;
    private volatile boolean active=true;
    public TargetRecoveryProofTrust(byte[] deployment,String catalogPrefix,List<Issuer> proofIssuers,Set<Sdk> sdks,
            List<TargetRecoveryCredentialAuthorization.Trust> materialHistory,long maximumDecisionWidthMs){
        Bytes.requireLength(deployment,32,"original recovery deployment");this.deployment=Bytes.copy(deployment);
        prefix=Objects.requireNonNull(catalogPrefix);
        if(prefix.isBlank()||prefix.endsWith("/")||prefix.indexOf('\0')>=0||Bytes.utf8(prefix).length>1024
                ||proofIssuers.isEmpty()||proofIssuers.size()>1024||sdks.isEmpty()||sdks.size()>128
                ||materialHistory.isEmpty()||materialHistory.size()>128||maximumDecisionWidthMs<1||maximumDecisionWidthMs>60000)throw invalid();
        final var keys=new HashMap<String,Issuer>();for(var key:proofIssuers)if(keys.put(key.id(),key)!=null)throw invalid();
        issuers=Map.copyOf(keys);this.sdks=Set.copyOf(sdks);this.maximumDecisionWidthMs=maximumDecisionWidthMs;
        final var history=new HashMap<String,TargetRecoveryCredentialAuthorization.Trust>();
        for(var original:materialHistory)if(history.put(original.historyScope(),original)!=null)throw invalid();
        this.materialHistory=Map.copyOf(history);
    }
    public void invalidate(){active=false;}
    public String objectKey(byte[] digest){Bytes.requireLength(digest,32,"recovery proof digest");return prefix+"/target-kafka-recovery-proof/"+Bytes.hex(digest);}
    public void requireCurrentIssuer(AuthorIdentity service,int keyVersion,TrustedUtcIntervalEvidence now){
        if(!active)throw invalid();requireIssuer(service,keyVersion,now);if(!active)throw invalid();
    }
    private Issuer requireIssuer(AuthorIdentity service,int version,TrustedUtcIntervalEvidence at){
        at.requireWidthAtMost(maximumDecisionWidthMs);
        final var key=issuers.get(Bytes.hex(service.canonicalBytes())+":"+Integer.toUnsignedString(version));
        if(key==null||at.earliestEpochMs()<key.notBeforeMs()||at.latestEpochMs()>=key.notAfterMs())throw invalid();return key;
    }
    /** No current Owner/session/credential/active requirement: this authenticates the original signed observation only. */
    public Verified verify(String exactKey,byte[] signedValue){
        try{
            final var proof=TargetRecoveryProof.decode(signedValue);
            if(!objectKey(proof.digest()).equals(exactKey)||!Arrays.equals(deployment,proof.observer().deploymentId())
                    ||!sdks.contains(new Sdk(proof.sourceCommit(),Bytes.hex(proof.sdkJar()))))throw invalid();
            final var key=requireIssuer(proof.issuer(),proof.signingKeyVersion(),proof.observedAt());
            final var signature=Signature.getInstance("Ed25519");signature.initVerify(key.publicKey());signature.update(proof.signingDigest());
            if(!signature.verify(proof.signature()))throw invalid();
            final var material=QueryCodecSupport.read(proof.publicMaterial(),"original recovery public material");
            final var authorization=TargetRecoveryCredentialAuthorization.decode(QueryCodecSupport.nested(material.get(1),2));
            final var original=Objects.requireNonNull(materialHistory.get(authorization.historyScope()),"original recovery material configuration");
            LocalBrokerMtlsResolver.verifyRecoveryArchiveInputs(proof.publicMaterial(),original,proof.mapping(),proof.observer(),proof.tenantScope(),proof.observedAt());
            return new Verified(proof.reference(exactKey),proof);
        }catch(Exception failure){throw invalid();}
    }
    /** Private authenticated raw object snapshot, safe for later Source exact-ref checks without live SDK closures. */
    public static final class Verified{
        private final TargetRecoveryProofRef reference;
        private final TargetRecoveryProof proof;
        private Verified(TargetRecoveryProofRef reference,TargetRecoveryProof proof){this.reference=reference;this.proof=proof;}
        public TargetRecoveryProofRef reference(){return reference;}
        public TargetRecoveryProof proof(){return proof;}
        public void requireExact(TargetRecoveryProofRef candidate){if(!reference.equals(candidate))throw invalid();}
    }
    private static IllegalStateException invalid(){return new IllegalStateException("recovery proof issuer, original public inputs or scope unavailable");}
}
