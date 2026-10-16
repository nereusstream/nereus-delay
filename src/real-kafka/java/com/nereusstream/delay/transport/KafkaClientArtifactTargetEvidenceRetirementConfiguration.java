package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore;
import java.io.IOException;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Objects;

/** Dedicated Root signing inputs. These files do not grant Source apply, protection-release or delete authority. */
final class KafkaClientArtifactTargetEvidenceRetirementConfiguration {
    private static final int FILE_BYTES = 1024;
    private static final byte[] KEY_MATCH = Bytes.utf8("NEREUS_DELAY_ROOT_RETIREMENT_KEY_MATCH_V1\0");
    private KafkaClientArtifactTargetEvidenceRetirementConfiguration() {}

    record Inputs(byte[] deploymentRoot, Path service, int version, Path publicKey, Path privateKey,
            long notBefore, long notAfter, long maximumWidth, long maximumRetainedBytes) {
        Inputs { deploymentRoot = Bytes.copy(deploymentRoot); }
        @Override public byte[] deploymentRoot() { return Bytes.copy(deploymentRoot); }
    }

    static Inputs prepare(Path path, byte[] actualRoot) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        if (!f.text("signing.purpose").equals("TARGET_EXTERNAL_EVIDENCE_RETIREMENT"))
            throw new IllegalArgumentException("explicit retirement signing purpose required");
        final byte[] root = f.hex("signing.deploymentRootDigest", 32, 32);
        if (!Arrays.equals(root, actualRoot)) throw new IllegalArgumentException("retirement signer changed original Root");
        final var service = f.absolutePath("signing.serviceIdentityFile");
        final long version = f.unsigned64("signing.keyVersion");
        final var publicKey = f.absolutePath("signing.publicKeyFile");
        final var privateKey = f.absolutePath("signing.privateKeyFile");
        final long before = f.number("signing.notBeforeMs"), after = f.number("signing.notAfterMs");
        final long width = f.number("signing.maximumDecisionWidthMs"), bytes = f.number("signing.maximumInputBytes");
        f.requireComplete();
        final long paths = Math.addExact(Bytes.utf8(service.toString()).length,
                Math.addExact(Bytes.utf8(publicKey.toString()).length, Bytes.utf8(privateKey.toString()).length));
        final long minimum = Math.addExact(32L * FILE_BYTES, Math.multiplyExact(2, paths));
        if (version == 0 || Long.compareUnsigned(version, 0xffff_ffffL) > 0 || after <= before
                || width < 1 || width > 60000 || bytes < minimum || bytes > 1024 * 1024)
            throw new IllegalArgumentException("retirement signing requires finite explicit key/input/time bounds");
        return new Inputs(root, service, (int) version, publicKey, privateKey, before, after, width, bytes);
    }

    /** Called only after the original Root has reserved all input bytes, on its existing finite I/O pool. */
    static Loaded load(Inputs input, Runnable current) throws IOException {
        current.run();
        final var service = AuthorIdentity.decode(TargetWorkerRuntimeConfigLoader.loadBoundedImage(input.service(), FILE_BYTES));
        if (service.kind() != AuthorIdentity.Kind.SERVICE) throw new IllegalArgumentException("retirement signer must be SERVICE");
        current.run();
        final byte[] publicBytes = TargetWorkerRuntimeConfigLoader.loadBoundedImage(input.publicKey(), FILE_BYTES);
        final byte[] privateBytes = TargetWorkerRuntimeConfigLoader.loadBoundedImage(input.privateKey(), FILE_BYTES);
        try {
            current.run();
            if (publicBytes.length != 44) throw new IllegalArgumentException("retirement key must be Ed25519");
            final var factory = KeyFactory.getInstance("Ed25519");
            final var publicKey = factory.generatePublic(new X509EncodedKeySpec(publicBytes));
            final var privateKey = factory.generatePrivate(new PKCS8EncodedKeySpec(privateBytes));
            final byte[] challenge = Bytes.sha256(KEY_MATCH, input.deploymentRoot(), service.canonicalBytes(), Bytes.u32beBits(input.version()));
            final var signer = Signature.getInstance("Ed25519"); signer.initSign(privateKey); signer.update(challenge);
            final var verifier = Signature.getInstance("Ed25519"); verifier.initVerify(publicKey); verifier.update(challenge);
            if (!verifier.verify(signer.sign())) throw new IllegalArgumentException("retirement private/public key mismatch");
            current.run(); return new Loaded(input, service, publicKey, privateKey);
        } catch (java.security.GeneralSecurityException invalid) {
            throw new IllegalArgumentException("invalid original retirement signing key", invalid);
        } finally { Arrays.fill(privateBytes, (byte) 0); }
    }

    static final class Loaded {
        private final Inputs input;
        private final AuthorIdentity service;
        private final PublicKey publicKey;
        private volatile PrivateKey privateKey;
        private volatile boolean stopped;
        Loaded(Inputs input, AuthorIdentity service, PublicKey publicKey, PrivateKey privateKey) {
            this.input = input; this.service = service; this.publicKey = publicKey; this.privateKey = privateKey;
        }
        /** Four bounded encoding/signing images; this is an explicit reservation, not an RSS measurement. */
        long signingWindowBytes() {
            return Math.multiplyExact(4L, TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES
                    + (long) TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES
                    + TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES + FILE_BYTES + 2048L);
        }
        void requireCurrent(TrustedUtcIntervalEvidence time) {
            Objects.requireNonNull(time).requireWidthAtMost(input.maximumWidth());
            if (stopped || privateKey == null || time.earliestEpochMs() < input.notBefore() || time.latestEpochMs() >= input.notAfter())
                throw new IllegalStateException("original retirement signing owner/window unavailable");
        }
        SystemMutation sign(TargetRecoveryEvidenceStore.ClosedUses uses,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticated,
                ResourceRetireIntentBody.ProtectionSet protections, long retryUntil,
                TrustedUtcIntervalEvidence time, Runnable current) {
            current.run();requireCurrent(time);
            final var copy=authenticated.image();
            return sign(uses.resource(),uses.retirementContext(copy.digest(),copy.canonicalBytes().length),
                    authenticated,protections,retryUntil,time,current);
        }
        SystemMutation sign(com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses uses,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticated,
                ResourceRetireIntentBody.ProtectionSet protections,long retryUntil,
                TrustedUtcIntervalEvidence time,Runnable current){
            current.run();requireCurrent(time);
            final var copy=authenticated.image();
            return sign(uses.resource(),uses.retirementContext(copy.digest(),copy.canonicalBytes().length),
                    authenticated,protections,retryUntil,time,current);
        }
        SystemMutation sign(com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses uses,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticated,
                ResourceRetireIntentBody.ProtectionSet protections,long retryUntil,
                TrustedUtcIntervalEvidence time,Runnable current){
            current.run();requireCurrent(time);
            final var copy=authenticated.image();
            return sign(copy.resource(),uses.retirementContext(copy),authenticated,protections,retryUntil,time,current);
        }
        private SystemMutation sign(TargetExternalEvidenceObjectResource resource,TargetEvidenceRetirementContext context,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticated,
                ResourceRetireIntentBody.ProtectionSet protections,long retryUntil,TrustedUtcIntervalEvidence time,Runnable current){
            current.run(); requireCurrent(time);
            final var copy = authenticated.image();
            if (!Arrays.equals(input.deploymentRoot(), copy.resource().deploymentRootDigest())
                    || !Arrays.equals(copy.resource().canonicalBytes(), resource.canonicalBytes())
                    || retryUntil < time.latestEpochMs())
                throw new IllegalArgumentException("retirement signature changed Root/Native object or live retry window");
            copy.requireContext(context);
            final var body = new TargetEvidenceRetireIntentBody(copy.resource().shard(), retryUntil,
                    copy.resource(), 0, protections, context);
            final var original = privateKey;
            if (original == null) throw new IllegalStateException("original retirement key closed");
            final var signed = SystemMutation.signed(body.shard(), SystemMutationType.RESOURCE_RETIRE_INTENT,
                    retryUntil, body.logicalOperationIdentity(), body.canonicalBytes(), service.canonicalBytes(), input.version(), original);
            current.run(); requireCurrent(time); return signed;
        }
        void requireSigned(SystemMutation intent) {
            if (intent.type() != SystemMutationType.RESOURCE_RETIRE_INTENT || intent.signingKeyVersion() != input.version()
                    || !Arrays.equals(intent.authorIdentity(), service.canonicalBytes()) || !intent.verifySignature(publicKey))
                throw new IllegalArgumentException("Source input differs from original Root retirement signature");
        }
        void requestStop() { stopped = true; }
        void close() { requestStop(); privateKey = null; }
    }
}
