package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory;
import com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetRecoveryProofTrust;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Root-only signing inputs. File labels never create recovery material, Source apply or SERVICE authority. */
final class KafkaClientArtifactTargetRecoveryArchiveConfiguration {
    private static final int FILE_BYTES = 1024;
    private static final byte[] KEY_MATCH_DOMAIN = Bytes.utf8("NEREUS_DELAY_ROOT_RECOVERY_KEY_MATCH_V1\0");

    private KafkaClientArtifactTargetRecoveryArchiveConfiguration() {}

    private record IssuerInput(Path service, int version, Path publicKey, long notBefore, long notAfter) {}

    static final class Inputs {
        private final byte[] deployment;
        private final String catalogPrefix;
        private final List<IssuerInput> issuers;
        private final Set<TargetRecoveryProofTrust.Sdk> sdks;
        private final int signingIssuer;
        private final Path proofPrivateKey;
        private final IssuerInput source;
        private final Path sourcePrivateKey;
        private final List<TargetRecoveryCredentialAuthorization.Trust> materialHistory;
        private final long maximumInputBytes;
        private final long maximumDecisionWidthMs;

        private Inputs(
                byte[] deployment,
                String catalogPrefix,
                List<IssuerInput> issuers,
                Set<TargetRecoveryProofTrust.Sdk> sdks,
                int signingIssuer,
                Path proofPrivateKey,
                IssuerInput source,
                Path sourcePrivateKey,
                List<TargetRecoveryCredentialAuthorization.Trust> materialHistory,
                long maximumInputBytes,
                long maximumDecisionWidthMs) {
            this.deployment = Bytes.copy(deployment);
            this.catalogPrefix = catalogPrefix;
            this.issuers = List.copyOf(issuers);
            this.sdks = Set.copyOf(sdks);
            this.signingIssuer = signingIssuer;
            this.proofPrivateKey = proofPrivateKey;
            this.source = source;
            this.sourcePrivateKey = sourcePrivateKey;
            this.materialHistory = List.copyOf(materialHistory);
            this.maximumInputBytes = maximumInputBytes;
            this.maximumDecisionWidthMs = maximumDecisionWidthMs;
        }

        long maximumRetainedBytes() {
            return maximumInputBytes;
        }
    }

    /** Only the bounded manifest is read here; all referenced key/identity files wait for the management reservation. */
    static Inputs prepare(
            Path path,
            String actualCatalogPrefix,
            List<TargetRecoveryCredentialAuthorization.Trust> originalMaterialHistory)
            throws IOException {
        final var history =
                List.copyOf(Objects.requireNonNull(originalMaterialHistory, "original recovery material history"));
        if (history.isEmpty() || history.size() > 128) {
            throw new IllegalArgumentException("bounded actual recovery material history is required");
        }
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final byte[] deployment = f.hex("recovery.deploymentId", 32, 32);
        final String prefix = f.text("recovery.catalogPrefix");
        if (!prefix.equals(actualCatalogPrefix)) {
            throw new IllegalArgumentException("recovery signing changed the original catalog");
        }
        final long maximumBytes = f.number("recovery.maximumInputBytes");
        final long width = f.number("recovery.maximumDecisionWidthMs");
        if (maximumBytes < 1 || maximumBytes > 16L * 1024 * 1024 || width < 1 || width > 60_000) {
            throw new IllegalArgumentException("recovery signing requires finite input/time bounds");
        }
        final int count = f.integer("proofIssuers.count");
        if (count < 1 || count > 64) {
            throw new IllegalArgumentException("recovery proof issuer inventory exceeds its bound");
        }
        final var issuers = new ArrayList<IssuerInput>();
        for (int i = 0; i < count; i++) {
            issuers.add(issuer(f, "proofIssuers." + i + "."));
        }
        final int selected = f.integer("proofSigner.issuer");
        if (selected >= count) {
            throw new IllegalArgumentException("proof signing key lacks explicit purpose approval");
        }
        final var proofKey = f.absolutePath("proofSigner.privateKeyFile");
        final var source = issuer(f, "sourceSigner.");
        final var sourceKey = f.absolutePath("sourceSigner.privateKeyFile");
        final int sdkCount = f.integer("sdks.count");
        if (sdkCount < 1 || sdkCount > 32) {
            throw new IllegalArgumentException("recovery SDK inventory exceeds its bound");
        }
        final var sdks = new HashSet<TargetRecoveryProofTrust.Sdk>();
        for (int i = 0; i < sdkCount; i++) {
            final String key = "sdks." + i + ".";
            if (!sdks.add(new TargetRecoveryProofTrust.Sdk(f.text(key + "sourceCommit"), f.text(key + "jarSha256")))) {
                throw new IllegalArgumentException("duplicate original recovery SDK approval");
            }
        }
        f.requireComplete();
        long paths = Math.addExact(Bytes.utf8(proofKey.toString()).length, Bytes.utf8(sourceKey.toString()).length);
        final var all = new ArrayList<>(issuers);
        all.add(source);
        for (var issuer : all) {
            paths = Math.addExact(
                    paths,
                    Math.addExact(
                            Bytes.utf8(issuer.service().toString()).length,
                            Bytes.utf8(issuer.publicKey().toString()).length));
        }
        // Cover bounded encoded images, parsing/key-spec copies, and retained manifest/index inputs.
        // Original material authorities are borrowed; this owner does not recreate or acquire their live permissions.
        final long files = Math.addExact(Math.multiplyExact(all.size(), 2L), 2);
        final long minimum = Math.addExact(
                Math.multiplyExact(files, 8L * FILE_BYTES),
                Math.addExact(Math.multiplyExact(paths, 2), 16384L + sdkCount * 512L + history.size() * 1024L));
        if (maximumBytes < minimum) {
            throw new IllegalArgumentException(
                    "recovery input window omits bounded key/identity parsing and retention");
        }
        return new Inputs(
                deployment, prefix, issuers, sdks, selected, proofKey, source, sourceKey, history, maximumBytes, width);
    }

    private static IssuerInput issuer(TargetWorkerRuntimeConfigLoader.Fields f, String prefix) {
        final var service = f.absolutePath(prefix + "serviceIdentityFile");
        final long version = f.unsigned64(prefix + "keyVersion");
        if (version == 0 || Long.compareUnsigned(version, 0xffff_ffffL) > 0) {
            throw new IllegalArgumentException("recovery signing key requires a nonzero uint32 version");
        }
        final var publicKey = f.absolutePath(prefix + "publicKeyFile");
        final long before = f.number(prefix + "notBeforeMs"), after = f.number(prefix + "notAfterMs");
        if (after <= before) {
            throw new IllegalArgumentException("recovery signing key window is empty");
        }
        return new IssuerInput(service, (int) version, publicKey, before, after);
    }

    /** Runs only on the registered management owner's finite I/O pool after its complete cost is reserved. */
    static Loaded load(Inputs inputs, Runnable guard) throws IOException {
        final var issuers = new ArrayList<TargetRecoveryProofTrust.Issuer>();
        for (var original : inputs.issuers) {
            issuers.add(loadIssuer(original, guard));
        }
        final var source = loadIssuer(inputs.source, guard);
        final var selected = issuers.get(inputs.signingIssuer);
        final var proofKey = privateKey(inputs.proofPrivateKey, selected, guard);
        final var sourceKey = privateKey(inputs.sourcePrivateKey, source, guard);
        final var trust = new TargetRecoveryProofTrust(
                inputs.deployment,
                inputs.catalogPrefix,
                issuers,
                inputs.sdks,
                inputs.materialHistory,
                inputs.maximumDecisionWidthMs);
        guard.run();
        return new Loaded(selected, proofKey, trust, source, sourceKey, inputs.maximumDecisionWidthMs);
    }

    private static TargetRecoveryProofTrust.Issuer loadIssuer(IssuerInput input, Runnable guard) throws IOException {
        final var service = AuthorIdentity.decode(image(input.service(), guard));
        final PublicKey key;
        try {
            key = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(image(input.publicKey(), guard)));
        } catch (GeneralSecurityException invalid) {
            throw new IllegalArgumentException("invalid configured recovery public key", invalid);
        }
        guard.run();
        return new TargetRecoveryProofTrust.Issuer(service, input.version(), key, input.notBefore(), input.notAfter());
    }

    private static byte[] image(Path path, Runnable guard) throws IOException {
        guard.run();
        final byte[] bytes = TargetWorkerRuntimeConfigLoader.loadBoundedImage(path, FILE_BYTES);
        guard.run();
        return bytes;
    }

    private static PrivateKey privateKey(Path path, TargetRecoveryProofTrust.Issuer issuer, Runnable guard)
            throws IOException {
        guard.run();
        final byte[] encoded = TargetWorkerRuntimeConfigLoader.loadBoundedImage(path, FILE_BYTES);
        try {
            guard.run();
            final var key = KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(encoded));
            final byte[] challenge = Bytes.sha256(
                    KEY_MATCH_DOMAIN, issuer.service().canonicalBytes(), Bytes.u32beBits(issuer.keyVersion()));
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(key);
            signer.update(challenge);
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(issuer.publicKey());
            verifier.update(challenge);
            if (!verifier.verify(signer.sign())) {
                throw new IllegalArgumentException("configured recovery private/public key mismatch");
            }
            guard.run();
            return key;
        } catch (GeneralSecurityException invalid) {
            throw new IllegalArgumentException("invalid configured recovery signing key", invalid);
        } finally {
            Arrays.fill(encoded, (byte) 0);
        }
    }

    /** Retained privately by the existing management owner, never returned as Worker signing authority. */
    static final class Loaded {
        final KafkaClientArtifactTargetRecoveryProofIssuer issuer;
        final TargetRecoveryProofTrust trust;
        final TargetPublishOutcomeMutationFactory sourceSigner;
        private final TargetRecoveryProofTrust.Issuer source;
        private final long maximumDecisionWidthMs;
        private volatile boolean stopped;

        private Loaded(
                TargetRecoveryProofTrust.Issuer proof,
                PrivateKey proofKey,
                TargetRecoveryProofTrust trust,
                TargetRecoveryProofTrust.Issuer source,
                PrivateKey sourceKey,
                long width) {
            this.trust = trust;
            this.source = source;
            maximumDecisionWidthMs = width;
            issuer = new KafkaClientArtifactTargetRecoveryProofIssuer(
                    proof.service(), proof.keyVersion(), proofKey, trust);
            sourceSigner =
                    new TargetPublishOutcomeMutationFactory(source.keyVersion(), sourceKey, this::requireSourceSigning);
        }

        private void requireActive() {
            if (stopped) {
                throw new IllegalStateException("original Root recovery signing owner stopped");
            }
        }

        private void requireSourceSigning(TrustedUtcIntervalEvidence at) {
            requireActive();
            at.requireWidthAtMost(maximumDecisionWidthMs);
            if (at.earliestEpochMs() < source.notBeforeMs() || at.latestEpochMs() >= source.notAfterMs()) {
                throw new IllegalStateException("original Source SERVICE signing key window unavailable");
            }
            requireActive();
        }

        KafkaClientArtifactTargetRecoveryRuntime.ResolutionContext contexts(
                KafkaClientArtifactTargetRecoveryRuntime.ResolutionContext original) {
            Objects.requireNonNull(original, "actual Source retry/charge/decision provider");
            return (unknown, evidence, at) -> {
                requireSourceSigning(at);
                final var decision = Objects.requireNonNull(original.prepare(unknown, evidence, at));
                if (!Arrays.equals(
                        decision.service().canonicalBytes(), source.service().canonicalBytes())) {
                    throw new IllegalArgumentException(
                            "resolution context changed the configured Source SERVICE author");
                }
                requireSourceSigning(at);
                return decision;
            };
        }

        void requestStop() {
            stopped = true;
            trust.invalidate();
        }

        void close() {
            requestStop();
            issuer.invalidate();
            sourceSigner.invalidate();
        }
    }
}
