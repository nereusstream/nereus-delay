package com.nereusstream.delay.assessment;

import com.google.gson.JsonObject;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.PublicKey;
import java.util.Base64;
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
        final PersistentStagingEvidence.Verified gateEnvelope =
                PersistentStagingEvidence.readVerified(Path.of(requiredEnv(GATE_C_ENV)));
        final JsonObject gate = gateEnvelope.payloadJson();
        require(gate, "gateCSchema", "nereus-delay.gate-c");
        require(gate, "gateCStatus", "PASS");
        require(gate, "environmentClassification", "STAGING");
        final String environmentId = requiredField(gate, "environmentId");
        final String candidateCommit = requiredField(gate, "candidateCommit");
        if (!COMMIT.matcher(candidateCommit).matches()) {
            throw new IOException("Gate C candidate commit is not a lowercase 40-hex commit");
        }
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
                PersistentStagingEvidence.readVerified(assessmentEnvelopePath);
        requireDigest(assessmentEnvelope.envelopeDigest(), assessmentEnvelopeSha256, "assessment envelope");
        requireSameKey(gateEnvelope.publicKey(), assessmentEnvelope.publicKey(), "assessment envelope");
        final JsonObject assessment = assessmentEnvelope.payloadJson();
        require(assessment, "assessmentSchema", DataResetAssessmentReceipt.SCHEMA);
        require(assessment, "outcome", "PASS_DIRECT_REPLACE", "PASS_RETAIN");
        require(assessment, "ndipPackageDigest", EXPECTED_PACKAGE_DIGEST);
        require(assessment, "sourceBaselineCommit", candidateCommit);
        require(assessment.getAsJsonObject("scope"), "environmentId", environmentId);
        final String assessmentScopeDigest = requiredField(assessment.getAsJsonObject("scope"), "scopeDigest");
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
        if (!Bytes.hex(manifest.artifacts().p1SourceLockDigest()).equals(EXPECTED_P1_LOCK)) {
            throw new IOException("DataResetManifest P1 source lock differs from the accepted lock");
        }
        if (!manifest.isCurrentGeneration()) {
            throw new IOException("DataResetManifest is not current generation");
        }
        manifest.requireWithinWindow(System.currentTimeMillis());

        final PersistentStagingEvidence.Verified shadowEnvelope =
                PersistentStagingEvidence.readVerified(Path.of(requiredEnv(SHADOW_ENV)));
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
                candidateCommit);
        return new Loaded(
                environmentId,
                candidateCommit,
                gateC,
                manifest,
                artifacts,
                manifestGate,
                physicalGate,
                gateEnvelope.envelopeDigest(),
                shadowEnvelope.envelopeDigest());
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

    private static String requiredEnv(final String name) throws IOException {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IOException("required persistent staging authority environment is missing: " + name);
        }
        return value;
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

    public record Loaded(
            String environmentId,
            String candidateCommit,
            GateCAuthorization gateC,
            DataResetManifest manifest,
            ArtifactGenerationSet artifacts,
            DataResetActivationGate manifestGate,
            PhysicalSendActivationGate physicalGate,
            byte[] gateCEnvelopeDigest,
            byte[] shadowEnvelopeDigest) {
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
            Bytes.requireLength(gateCEnvelopeDigest, 32, "gateCEnvelopeDigest");
            Bytes.requireLength(shadowEnvelopeDigest, 32, "shadowEnvelopeDigest");
        }

        @Override
        public byte[] gateCEnvelopeDigest() {
            return Bytes.copy(gateCEnvelopeDigest);
        }

        @Override
        public byte[] shadowEnvelopeDigest() {
            return Bytes.copy(shadowEnvelopeDigest);
        }
    }
}
