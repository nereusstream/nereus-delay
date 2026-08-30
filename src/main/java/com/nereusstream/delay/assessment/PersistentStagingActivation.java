package com.nereusstream.delay.assessment;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import com.nereusstream.delay.semantic.OxiaSyncHandoffPolicyAuthority;
import com.nereusstream.delay.semantic.OxiaSyncHandoffPolicyTrustStore;
import io.oxia.client.api.exceptions.OxiaException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Fail-closed loader for the local staging Gate C plus SHADOW authority.
 * Nothing is inferred from process state: all receipts, payload hashes,
 * signatures, source locks and the binary DataResetManifest must agree before
 * a persistent physical-send gate is returned.
 */
public final class PersistentStagingActivation {
    public static final String CLASSIFICATION_ENV = "NEREUS_DELAY_ENVIRONMENT_CLASSIFICATION";
    public static final String GATE_C_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_GATE_C_RECEIPT";
    public static final String SHADOW_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_SHADOW_RECEIPT";
    public static final String POLICY_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_POLICY";
    public static final String TRUSTED_PUBLIC_KEY_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_TRUSTED_PUBLIC_KEY";
    public static final String TRUSTED_KEY_GENERATION_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_ISSUER_KEY_GENERATION";
    public static final String OXIA_ENDPOINT_ENV = "NEREUS_DELAY_OXIA_ENDPOINT";
    public static final String OXIA_NAMESPACE_ENV = "NEREUS_DELAY_OXIA_NAMESPACE";
    public static final String POLICY_KEY_PREFIX_ENV = "NEREUS_DELAY_PERSISTENT_STAGING_POLICY_KEY_PREFIX";
    private static final String EXPECTED_PACKAGE_DIGEST =
            "13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b";
    private static final String EXPECTED_P1_LOCK = "0a2536484cd3932801a98dc88ff112b2df88a1c7";
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    private PersistentStagingActivation() {}

    /** Loads the complete enabled authority or throws; callers must fail closed. */
    public static Loaded loadFromEnvironment() throws IOException {
        final String classification = requiredEnv(CLASSIFICATION_ENV);
        if (!"STAGING".equals(classification)) {
            throw new IOException("persistent staging activation requires STAGING classification");
        }
        final int trustedKeyGeneration = requiredPositiveIntEnv(TRUSTED_KEY_GENERATION_ENV);
        final PublicKey trustedPublicKey =
                PersistentStagingEvidence.decodePublicKey(readRegular(Path.of(requiredEnv(TRUSTED_PUBLIC_KEY_ENV))));
        final PersistentStagingEvidence.Verified gateEnvelope = PersistentStagingEvidence.readVerified(
                Path.of(requiredEnv(GATE_C_ENV)), trustedPublicKey, trustedKeyGeneration);
        final JsonObject gate = gateEnvelope.payloadJson();
        require(gate, "gateCSchema", "nereus-delay.gate-c");
        require(gate, "gateCStatus", "PASS");
        require(gate, "environmentClassification", "STAGING");
        final String environmentId = requiredField(gate, "environmentId");
        final String candidateCommit = requiredField(gate, "candidateCommit");
        if (!COMMIT.matcher(candidateCommit).matches()) {
            throw new IOException("Gate C candidate commit is not a lowercase 40-hex commit");
        }
        final String runningCommit = RunningArtifactIdentity.requireCleanSourceCommit();
        require(runningCommit, candidateCommit, "running artifact source commit");
        require(gate, "ndipPackageDigest", EXPECTED_PACKAGE_DIGEST);
        require(gate, "p1SourceLock", EXPECTED_P1_LOCK);
        final String gateClassification = requiredField(gate, "environmentClassification");
        final String resolution = requiredField(gate, "resolution");
        final GateCAuthorization.Resolution gateResolution;
        try {
            gateResolution = GateCAuthorization.Resolution.valueOf(resolution);
        } catch (IllegalArgumentException failure) {
            throw new IOException("Gate C resolution is not a closed value", failure);
        }

        final Path assessmentEnvelopePath = Path.of(requiredField(gate, "assessmentEnvelopePath"));
        final String assessmentEnvelopeSha256 = requiredField(gate, "assessmentEnvelopeSha256");
        final PersistentStagingEvidence.Verified assessmentEnvelope =
                PersistentStagingEvidence.readVerified(assessmentEnvelopePath, trustedPublicKey, trustedKeyGeneration);
        requireDigest(assessmentEnvelope.envelopeDigest(), assessmentEnvelopeSha256, "assessment envelope");
        requireSameKey(gateEnvelope.publicKey(), assessmentEnvelope.publicKey(), "assessment envelope");
        final JsonObject assessment = assessmentEnvelope.payloadJson();
        require(assessment, "assessmentSchema", DataResetAssessmentReceipt.SCHEMA);
        require(assessment, "outcome", "PASS_DIRECT_REPLACE", "PASS_RETAIN");
        require(assessment, "ndipPackageDigest", EXPECTED_PACKAGE_DIGEST);
        require(assessment, "sourceBaselineCommit", candidateCommit);
        final JsonObject assessmentScope = assessment.getAsJsonObject("scope");
        require(assessmentScope, "environmentId", environmentId);
        final String assessmentScopeDigest = Bytes.hex(scopeDigest(assessmentScope));
        require(assessmentScopeDigest, requiredField(gate, "assessmentScopeDigest"), "assessment scope digest");
        final Path assessmentReceiptPath = Path.of(requiredField(gate, "assessmentReceiptPath"));
        final byte[] assessmentReceipt = readRegular(assessmentReceiptPath);
        requireDigest(
                Bytes.sha256(assessmentReceipt), requiredField(gate, "assessmentReceiptSha256"), "assessment receipt");
        final byte[] expectedAssessmentPayload = stripSingleTrailingNewline(assessmentReceipt);
        if (!Bytes.constantTimeEquals(expectedAssessmentPayload, assessmentEnvelope.payload())) {
            throw new IOException("assessment receipt and signed assessment envelope differ");
        }

        final Path manifestPath = Path.of(requiredField(gate, "manifestPath"));
        final byte[] manifestBytes = readRegular(manifestPath);
        requireDigest(Bytes.sha256(manifestBytes), requiredField(gate, "manifestSha256"), "DataResetManifest");
        final DataResetManifest manifest;
        try {
            manifest = DataResetManifest.decode(manifestBytes);
        } catch (RuntimeException failure) {
            throw new IOException("Gate C DataResetManifest is not canonical", failure);
        }
        final byte[] publicKeyDer = decodeBase64(requiredField(gate, "manifestPublicKeyDerBase64"));
        final PublicKey publicKey = PersistentStagingEvidence.decodePublicKey(publicKeyDer);
        requireSameKey(gateEnvelope.publicKey(), publicKey, "DataResetManifest");
        if (!manifest.verifySignature(publicKey)) {
            throw new IOException("DataResetManifest signature is invalid");
        }
        if (!manifest.scope().environmentId().equals(environmentId)
                || !manifest.sourceBaselineCommit().equals(candidateCommit)) {
            throw new IOException("DataResetManifest scope or source commit differs from Gate C");
        }
        if (!Bytes.hex(manifest.manifestDigest()).equals(requiredField(gate, "manifestDigest"))) {
            throw new IOException("DataResetManifest logical digest differs from Gate C");
        }
        if (!Bytes.constantTimeEquals(manifest.artifacts().p1SourceLockDigest(), PulsarSourceLock.digest())) {
            throw new IOException("DataResetManifest P1 source lock differs from the accepted lock");
        }
        if (!manifest.isCurrentGeneration()) {
            throw new IOException("DataResetManifest is not current generation");
        }
        manifest.requireWithinWindow(System.currentTimeMillis());

        final PersistentStagingEvidence.Verified shadowEnvelope = PersistentStagingEvidence.readVerified(
                Path.of(requiredEnv(SHADOW_ENV)), trustedPublicKey, trustedKeyGeneration);
        requireSameKey(gateEnvelope.publicKey(), shadowEnvelope.publicKey(), "SHADOW envelope");
        final JsonObject shadow = shadowEnvelope.payloadJson();
        require(shadow, "shadowSchema", "nereus-delay.shadow-certification");
        require(shadow, "shadowStatus", "PASS");
        require(shadow, "environmentId", environmentId);
        require(shadow, "candidateCommit", candidateCommit);
        require(shadow, "gateCEnvelopeSha256", Bytes.hex(gateEnvelope.envelopeDigest()));
        require(shadow, "nativeAdmission", "0");
        require(shadow, "nativeSend", "0");
        require(shadow, "handedOff", "0");
        require(shadow, "unresolvedPublishing", "false");
        require(shadow, "unresolvedUncertain", "false");
        require(shadow, "attemptJournalLeak", "false");
        require(shadow, "generationIncarnationMix", "false");

        final PersistentStagingEvidence.Verified policyEnvelope = PersistentStagingEvidence.readVerified(
                Path.of(requiredEnv(POLICY_ENV)), trustedPublicKey, trustedKeyGeneration);
        requireSameKey(gateEnvelope.publicKey(), policyEnvelope.publicKey(), "staging policy envelope");
        final JsonObject policy = policyEnvelope.payloadJson();
        require(policy, "policySchema", "nereus-delay.handoff-policy-publication");
        require(policy, "policyStatus", "ENABLED");
        require(policy, "environmentId", environmentId);
        require(policy, "candidateCommit", candidateCommit);
        require(policy, "gateCEnvelopeSha256", Bytes.hex(gateEnvelope.envelopeDigest()));
        require(policy, "shadowEnvelopeSha256", Bytes.hex(shadowEnvelope.envelopeDigest()));
        require(policy, "issuerKeyGeneration", Integer.toString(trustedKeyGeneration));
        require(policy, "issuerPublicKeySha256", Bytes.hex(Bytes.sha256(trustedPublicKey.getEncoded())));
        require(policy, "artifactSetDigest", Bytes.hex(manifest.artifacts().setDigest()));

        final byte[] policyScopeDigest = decodeDigest(requiredField(policy, "policyScopeDigest"), "policy scope");
        final long policyOxiaVersion = requiredPositiveLong(policy, "policyOxiaVersion");
        final long policyGeneration = requiredNonZeroUnsignedLong(policy, "policyGeneration");
        final OxiaSyncHandoffPolicyAuthority.ClientHandle policyHandle;
        try {
            policyHandle = OxiaSyncHandoffPolicyAuthority.connect(
                    requiredEnv(OXIA_ENDPOINT_ENV),
                    requiredEnv(OXIA_NAMESPACE_ENV),
                    "nereus-delay-persistent-activation-"
                            + ProcessHandle.current().pid(),
                    Duration.ofSeconds(5),
                    requiredEnv(POLICY_KEY_PREFIX_ENV));
        } catch (OxiaException | RuntimeException failure) {
            throw new IOException("cannot connect to the current Oxia handoff policy authority", failure);
        }
        final HandoffPolicyAuthority.Publication currentPolicy;
        try {
            currentPolicy = policyHandle.authority().requireCurrent(policyScopeDigest);
            requireCurrentEnabledPolicy(
                    currentPolicy,
                    policyOxiaVersion,
                    policyGeneration,
                    policy,
                    trustedPublicKey,
                    trustedKeyGeneration,
                    manifest.artifacts(),
                    System.currentTimeMillis());
        } catch (RuntimeException | IOException failure) {
            policyHandle.close();
            if (failure instanceof IOException ioFailure) {
                throw ioFailure;
            }
            throw new IOException("current Oxia handoff policy does not authorize activation", failure);
        }

        final GateCAuthorization gateC = new GateCAuthorization(
                environmentId,
                EnvironmentClassification.valueOf(gateClassification),
                gateResolution,
                Bytes.hexToBytes(assessmentScopeDigest),
                Bytes.sha256(assessmentReceipt),
                gateEnvelope.envelopeDigest());
        final ArtifactGenerationSet artifacts = manifest.artifacts();
        final DataResetActivationGate manifestGate =
                new DataResetActivationGate(manifest, publicKey, environmentId, artifacts);
        final PhysicalSendActivationGate physicalGate = PhysicalSendActivationGate.persistentEnabled(
                DeploymentSafetyGate.GateBStatus.PASS,
                EnvironmentClassification.STAGING,
                gateC,
                DeploymentSafetyGate.ShadowReadiness.REQUIREMENTS_PASS,
                manifestGate,
                runningCommit,
                (candidateArtifacts, trustedNowEpochMs) -> requireFrozenPolicy(
                        currentPolicy.head().snapshot(),
                        currentPolicy.head().snapshot(),
                        trustedPublicKey,
                        manifest.artifacts(),
                        candidateArtifacts,
                        trustedNowEpochMs));
        return new Loaded(
                environmentId,
                candidateCommit,
                gateC,
                manifest,
                artifacts,
                manifestGate,
                physicalGate,
                gateEnvelope.envelopeDigest(),
                shadowEnvelope.envelopeDigest(),
                policyEnvelope.envelopeDigest(),
                policyScopeDigest,
                currentPolicy,
                trustedPublicKey,
                trustedKeyGeneration,
                requiredEnv(POLICY_KEY_PREFIX_ENV),
                policyHandle);
    }

    /** Returns no authority for a normal SHADOW/managed Worker process. */
    public static Loaded loadIfConfigured() throws IOException {
        final String classification = System.getenv(CLASSIFICATION_ENV);
        if (!"STAGING".equals(classification)) {
            return null;
        }
        final String gate = System.getenv(GATE_C_ENV);
        final String shadow = System.getenv(SHADOW_ENV);
        final boolean requested =
                "true".equalsIgnoreCase(System.getenv("NEREUS_DELAY_PERSISTENT_STAGING_REQUIRE_AUTHORITY"));
        if (!requested && (gate == null || gate.isBlank()) && (shadow == null || shadow.isBlank())) {
            return null;
        }
        return loadFromEnvironment();
    }

    private static void requireFrozenPolicy(
            final HandoffPolicySnapshot expected,
            final HandoffPolicySnapshot candidate,
            final PublicKey trustedPublicKey,
            final ArtifactGenerationSet expectedArtifacts,
            final ArtifactGenerationSet candidateArtifacts,
            final long trustedNowEpochMs) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(candidateArtifacts, "candidateArtifacts");
        if (!Bytes.constantTimeEquals(expected.canonicalBytes(), candidate.canonicalBytes())
                || !expectedArtifacts.equals(candidateArtifacts)
                || !Bytes.constantTimeEquals(expectedArtifacts.setDigest(), candidateArtifacts.setDigest())
                || !candidate.verifySignature(trustedPublicKey)
                || !Bytes.constantTimeEquals(candidate.artifactGenerationSetDigest(), candidateArtifacts.setDigest())
                || candidate.mode() != HandoffPolicyMode.ENABLED
                || trustedNowEpochMs < candidate.validFromEpochMs()
                || trustedNowEpochMs >= candidate.validUntilEpochMs()) {
            throw new IllegalStateException("frozen handoff policy is stale, untrusted, or artifact-mismatched");
        }
    }

    private static String requiredEnv(final String name) throws IOException {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IOException("required persistent staging authority environment is missing: " + name);
        }
        return value;
    }

    private static int requiredPositiveIntEnv(final String name) throws IOException {
        final String value = requiredEnv(name);
        try {
            final int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new NumberFormatException("non-positive");
            }
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IOException("required persistent staging authority is not a positive integer: " + name, failure);
        }
    }

    private static String requiredField(final JsonObject object, final String name) throws IOException {
        if (object == null || !object.has(name) || !object.get(name).isJsonPrimitive()) {
            throw new IOException("receipt field is missing: " + name);
        }
        final String value = object.get(name).getAsString();
        if (value.isBlank()) {
            throw new IOException("receipt field is blank: " + name);
        }
        return value;
    }

    private static void require(final JsonObject object, final String name, final String... expected)
            throws IOException {
        final String actual = requiredField(object, name);
        for (String value : expected) {
            if (value.equals(actual)) {
                return;
            }
        }
        throw new IOException("receipt field mismatch: " + name + "=" + actual);
    }

    private static void require(final String actual, final String expected, final String name) throws IOException {
        if (!expected.equals(actual)) {
            throw new IOException(name + " mismatch");
        }
    }

    private static void requireDigest(final byte[] actual, final String expected, final String name)
            throws IOException {
        if (!Bytes.hex(actual).equals(expected)) {
            throw new IOException(name + " digest mismatch");
        }
    }

    private static void requireSameKey(final PublicKey left, final PublicKey right, final String name)
            throws IOException {
        if (!Bytes.constantTimeEquals(left.getEncoded(), right.getEncoded())) {
            throw new IOException(name + " is signed by another Ed25519 key");
        }
    }

    private static long requiredPositiveLong(final JsonObject object, final String name) throws IOException {
        final long value = requiredLong(object, name);
        if (value <= 0) {
            throw new IOException("receipt field must be positive: " + name);
        }
        return value;
    }

    private static long requiredNonZeroUnsignedLong(final JsonObject object, final String name) throws IOException {
        final String value = requiredField(object, name);
        try {
            final long parsed = Long.parseUnsignedLong(value);
            if (parsed == 0) {
                throw new NumberFormatException("zero");
            }
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IOException("receipt field is not a non-zero unsigned long: " + name, failure);
        }
    }

    private static long requiredLong(final JsonObject object, final String name) throws IOException {
        try {
            return Long.parseLong(requiredField(object, name));
        } catch (NumberFormatException failure) {
            throw new IOException("receipt field is not a long: " + name, failure);
        }
    }

    private static byte[] decodeDigest(final String value, final String name) throws IOException {
        try {
            final byte[] digest = Bytes.hexToBytes(value);
            Bytes.requireLength(digest, 32, name);
            return digest;
        } catch (IllegalArgumentException failure) {
            throw new IOException(name + " is not a canonical SHA-256 digest", failure);
        }
    }

    private static void requireCurrentEnabledPolicy(
            final HandoffPolicyAuthority.Publication publication,
            final long expectedOxiaVersion,
            final long expectedGeneration,
            final JsonObject receipt,
            final PublicKey trustedPublicKey,
            final int trustedKeyGeneration,
            final ArtifactGenerationSet artifacts,
            final long trustedNowEpochMs)
            throws IOException {
        Objects.requireNonNull(publication, "publication");
        Objects.requireNonNull(artifacts, "artifacts");
        if (publication.oxiaVersion() != expectedOxiaVersion) {
            throw new IOException("current handoff policy Oxia version differs from the publication receipt");
        }
        final HandoffPolicyHead head = publication.head();
        final HandoffPolicySnapshot snapshot = head.snapshot();
        if (head.mode() != HandoffPolicyMode.ENABLED
                || head.generation() != expectedGeneration
                || snapshot.issuerKeyGeneration() != trustedKeyGeneration
                || !snapshot.verifySignature(trustedPublicKey)
                || !Bytes.constantTimeEquals(snapshot.artifactGenerationSetDigest(), artifacts.setDigest())) {
            throw new IOException("current handoff policy head is not an enabled trusted artifact-bound lease");
        }
        require(Bytes.hex(head.headDigest()), requiredField(receipt, "policyHeadDigest"), "policy head digest");
        require(
                Bytes.hex(snapshot.snapshotDigest()),
                requiredField(receipt, "policySnapshotDigest"),
                "policy snapshot digest");
        require(
                Long.toString(snapshot.validFromEpochMs()),
                requiredField(receipt, "validFromEpochMs"),
                "policy validFrom");
        require(
                Long.toString(snapshot.validUntilEpochMs()),
                requiredField(receipt, "validUntilEpochMs"),
                "policy validUntil");
        if (trustedNowEpochMs < snapshot.validFromEpochMs() || trustedNowEpochMs >= snapshot.validUntilEpochMs()) {
            throw new IOException("current handoff policy lease is not active at trusted time");
        }
    }

    private static byte[] readRegular(final Path path) throws IOException {
        final Path normalized =
                Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        if (normalized.startsWith(Path.of("/tmp"))
                || normalized.startsWith(Path.of("/var/tmp"))
                || Files.isSymbolicLink(normalized)
                || !Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("authority receipt must name a persistent regular file: " + normalized);
        }
        return Files.readAllBytes(normalized);
    }

    private static byte[] stripSingleTrailingNewline(final byte[] value) {
        if (value.length > 0 && value[value.length - 1] == '\n') {
            return java.util.Arrays.copyOf(value, value.length - 1);
        }
        return value.clone();
    }

    private static byte[] decodeBase64(final String value) throws IOException {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException failure) {
            throw new IOException("receipt public key is not base64", failure);
        }
    }

    private static byte[] scopeDigest(final JsonObject value) throws IOException {
        try {
            final List<DataResetAssessmentScope.ResourceRef> resources = new ArrayList<>();
            for (JsonElement element : value.getAsJsonArray("resources")) {
                final JsonObject resource = element.getAsJsonObject();
                resources.add(new DataResetAssessmentScope.ResourceRef(
                        DataResetAssessmentScope.ResourceKind.valueOf(
                                resource.get("kind").getAsString()),
                        resource.get("identity").getAsString()));
            }
            return new DataResetAssessmentScope(
                            value.get("environmentId").getAsString(),
                            EnvironmentClassification.valueOf(
                                    value.get("environmentClassification").getAsString()),
                            value.get("deploymentId").getAsString(),
                            strings(value.getAsJsonArray("tenantIds")),
                            strings(value.getAsJsonArray("routeIds")),
                            strings(value.getAsJsonArray("shardIds")),
                            resources,
                            strings(value.getAsJsonArray("eligibleWorkerIds")))
                    .scopeDigest();
        } catch (RuntimeException failure) {
            throw new IOException("assessment scope is not a valid canonical scope", failure);
        }
    }

    private static List<String> strings(final JsonArray value) {
        final List<String> result = new ArrayList<>();
        for (JsonElement element : value) {
            result.add(element.getAsString());
        }
        return result;
    }

    public record Loaded(
            String environmentId,
            String candidateCommit,
            GateCAuthorization gateC,
            DataResetManifest manifest,
            ArtifactGenerationSet artifacts,
            DataResetActivationGate manifestGate,
            PhysicalSendActivationGate physicalGate,
            byte[] gateCEnvelopeDigest,
            byte[] shadowEnvelopeDigest,
            byte[] policyEnvelopeDigest,
            byte[] policyScopeDigest,
            HandoffPolicyAuthority.Publication policyPublication,
            PublicKey trustedPublicKey,
            int trustedKeyGeneration,
            String policyKeyPrefix,
            OxiaSyncHandoffPolicyAuthority.ClientHandle policyHandle)
            implements AutoCloseable {
        public Loaded {
            Objects.requireNonNull(environmentId, "environmentId");
            Objects.requireNonNull(candidateCommit, "candidateCommit");
            Objects.requireNonNull(gateC, "gateC");
            Objects.requireNonNull(manifest, "manifest");
            Objects.requireNonNull(artifacts, "artifacts");
            Objects.requireNonNull(manifestGate, "manifestGate");
            Objects.requireNonNull(physicalGate, "physicalGate");
            gateCEnvelopeDigest = Bytes.copy(gateCEnvelopeDigest);
            shadowEnvelopeDigest = Bytes.copy(shadowEnvelopeDigest);
            policyEnvelopeDigest = Bytes.copy(policyEnvelopeDigest);
            Bytes.requireLength(gateCEnvelopeDigest, 32, "gateCEnvelopeDigest");
            Bytes.requireLength(shadowEnvelopeDigest, 32, "shadowEnvelopeDigest");
            Bytes.requireLength(policyEnvelopeDigest, 32, "policyEnvelopeDigest");
            policyScopeDigest = Bytes.copy(policyScopeDigest);
            Bytes.requireLength(policyScopeDigest, 32, "policyScopeDigest");
            Objects.requireNonNull(policyPublication, "policyPublication");
            Objects.requireNonNull(trustedPublicKey, "trustedPublicKey");
            if (trustedKeyGeneration <= 0) {
                throw new IllegalArgumentException("trustedKeyGeneration must be positive");
            }
            policyKeyPrefix = Objects.requireNonNull(policyKeyPrefix, "policyKeyPrefix");
            Objects.requireNonNull(policyHandle, "policyHandle");
        }

        @Override
        public byte[] gateCEnvelopeDigest() {
            return Bytes.copy(gateCEnvelopeDigest);
        }

        @Override
        public byte[] shadowEnvelopeDigest() {
            return Bytes.copy(shadowEnvelopeDigest);
        }

        @Override
        public byte[] policyEnvelopeDigest() {
            return Bytes.copy(policyEnvelopeDigest);
        }

        @Override
        public byte[] policyScopeDigest() {
            return Bytes.copy(policyScopeDigest);
        }

        /** Verifies the exact frozen policy without treating a later DISABLED head as retroactive revocation. */
        public void requireFrozenHandoffPolicy(
                final HandoffPolicySnapshot snapshot,
                final ArtifactGenerationSet candidateArtifacts,
                final long trustedNowEpochMs) {
            requireFrozenPolicy(
                    policyPublication.head().snapshot(),
                    snapshot,
                    trustedPublicKey,
                    artifacts,
                    candidateArtifacts,
                    trustedNowEpochMs);
        }

        /** Returns the same current-head authority verified during activation. */
        public HandoffPolicyAuthority handoffPolicyAuthority() {
            return policyHandle.authority();
        }

        /** Returns the immutable source-position trust store sharing the verified Oxia session. */
        public OxiaSyncHandoffPolicyTrustStore handoffPolicyTrustStore() {
            return new OxiaSyncHandoffPolicyTrustStore(policyHandle.client(), policyKeyPrefix);
        }

        @Override
        public void close() throws IOException {
            policyHandle.close();
        }
    }
}
