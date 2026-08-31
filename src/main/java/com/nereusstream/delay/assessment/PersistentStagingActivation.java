package com.nereusstream.delay.assessment;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.PublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
    private static final Pattern SAFE_OPERATOR = Pattern.compile("[A-Za-z0-9._@-]+");

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
        requireInt(gate, "gateCSchemaGeneration", 1);
        require(gate, "gateCStatus", "PASS");
        require(gate, "environmentClassification", "STAGING");
        requireBoolean(gate, "productionAuthority", false);
        requireInt(gate, "applicableChecks", 41);
        requireInt(gate, "passedChecks", 41);
        for (String field : List.of(
                "startupAssignmentGate",
                "noOldGeneration",
                "noUnresolvedPublishing",
                "noUnresolvedUncertain",
                "freshness")) {
            requireBoolean(gate, field, true);
        }
        final JsonObject gateEvidence = requiredObject(gate, "evidence");
        for (String field : List.of(
                "realOxia",
                "realMinio",
                "realPulsarP1",
                "realGateway",
                "realWorker",
                "oxiaAdminReady",
                "oxiaCoordinatorRestart",
                "brokerFailover",
                "workerOwnershipTransfer",
                "responseLossRecovery")) {
            requireBoolean(gateEvidence, field, true);
        }
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
        requireInt(assessment, "assessmentSchemaGeneration", DataResetAssessmentReceipt.SCHEMA_GENERATION);
        require(assessment, "outcome", "PASS_DIRECT_REPLACE", "PASS_RETAIN");
        require(assessment, "ndipPackageDigest", EXPECTED_PACKAGE_DIGEST);
        require(assessment, "sourceBaselineCommit", candidateCommit);
        final JsonObject assessmentScope = assessment.getAsJsonObject("scope");
        require(assessmentScope, "environmentId", environmentId);
        require(assessmentScope, "environmentClassification", gateClassification);
        requireResolutionBinding(gateResolution, requiredField(assessment, "outcome"));
        final DataResetAssessmentScope canonicalAssessmentScope = decodeScope(assessmentScope);
        final String assessmentScopeDigest = Bytes.hex(scopeDigest(assessmentScope));
        require(assessmentScopeDigest, requiredField(gate, "assessmentScopeDigest"), "assessment scope digest");
        final Path assessmentReceiptPath = Path.of(requiredField(gate, "assessmentReceiptPath"));
        final byte[] assessmentReceipt = readRegular(assessmentReceiptPath);
        requireDigest(
                Bytes.sha256(assessmentReceipt), requiredField(gate, "assessmentReceiptSha256"), "assessment receipt");
        requireExactAssessmentBinding(assessmentReceipt, assessmentEnvelope.payload());

        final Path dispositionPath = Path.of(requiredField(gate, "dataDispositionPath"));
        final PersistentStagingEvidence.Verified dispositionEnvelope =
                PersistentStagingEvidence.readVerified(dispositionPath, trustedPublicKey, trustedKeyGeneration);
        requireDigest(
                dispositionEnvelope.envelopeDigest(),
                requiredField(gate, "dataDispositionSha256"),
                "data disposition envelope");
        requireSameKey(gateEnvelope.publicKey(), dispositionEnvelope.publicKey(), "data disposition envelope");
        final JsonObject disposition = dispositionEnvelope.payloadJson();
        require(disposition, "schema", "nereus-delay.ndip1-staging-data-disposition");
        requireInt(disposition, "schemaGeneration", 1);
        require(disposition, "environmentId", environmentId);
        require(disposition, "environmentClassification", gateClassification);
        require(disposition, "candidateCommit", candidateCommit);
        requireBoolean(disposition, "productionAuthority", false);
        requireBoolean(disposition, "destructiveOperationsAuthorized", false);
        require(disposition, "operatorAuthorization", "EXPLICIT_ENVIRONMENT_INPUT");
        final String operator = requiredField(disposition, "operator");
        if (!SAFE_OPERATOR.matcher(operator).matches()) {
            throw new IOException("data disposition operator is not a closed safe identifier");
        }
        requireDispositionBinding(gateResolution, disposition);
        final Path candidateScopePath = requireReferencedDigest(
                disposition, "candidateScope", "candidateScopeSha256", "candidate deployment scope");
        final JsonObject candidateScopeDocument = readJsonRegular(candidateScopePath, "candidate deployment scope");
        final JsonObject candidateScopeJson = requiredObject(candidateScopeDocument, "scope");
        final DataResetAssessmentScope candidateScope = decodeScope(candidateScopeJson);
        require(candidateScopeJson, "environmentId", environmentId);
        require(candidateScopeJson, "environmentClassification", gateClassification);
        if (candidateScope.deploymentId().equals(canonicalAssessmentScope.deploymentId())
                && "RESET_INTERNAL_ONLY".equals(requiredField(disposition, "decision"))) {
            throw new IOException("RESET candidate scope reuses the assessed deployment identity");
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
        if (!manifest.scope().deploymentId().equals(candidateScope.deploymentId())) {
            throw new IOException("DataResetManifest deployment differs from the signed candidate scope");
        }
        requireManifestResources(manifest, candidateScope);
        if (!Bytes.hex(manifest.manifestDigest()).equals(requiredField(gate, "manifestDigest"))) {
            throw new IOException("DataResetManifest logical digest differs from Gate C");
        }
        if (!Bytes.constantTimeEquals(manifest.artifacts().p1SourceLockDigest(), PulsarSourceLock.digest())) {
            throw new IOException("DataResetManifest P1 source lock differs from the accepted lock");
        }
        if (!manifest.isCurrentGeneration()) {
            throw new IOException("DataResetManifest is not current generation");
        }
        if (!manifest.obligationZeroProof().isZero()) {
            throw new IOException("DataResetManifest retains a non-zero obligation");
        }
        manifest.requireWithinWindow(System.currentTimeMillis());

        final Path manifestReadbackPath = requireReferencedDigest(
                gate, "manifestReadbackPath", "manifestReadbackSha256", "manifest operation readback");
        final JsonObject manifestReadback = readJsonRegular(manifestReadbackPath, "manifest operation readback");
        require(manifestReadback, "schema", "nereus-delay.ndip1-manifest-operation-readback");
        requireInt(manifestReadback, "schemaGeneration", 1);
        require(manifestReadback, "environmentId", environmentId);
        require(manifestReadback, "candidateCommit", candidateCommit);
        final Path readbackScopePath =
                requireReferencedDigest(manifestReadback, "scope", "scopeSha256", "manifest candidate scope");
        if (!readbackScopePath.equals(candidateScopePath)) {
            throw new IOException("manifest readback names another candidate scope");
        }
        final Path intentPath =
                requireReferencedDigest(manifestReadback, "intent", "intentSha256", "manifest operation intent");
        final JsonObject intent = readJsonRegular(intentPath, "manifest operation intent");
        requireBoolean(intent, "exactScope", true);
        requireEmptyArray(intent, "destructiveOperations");
        final Path resourceReadbackPath = requireReferencedDigest(
                manifestReadback, "resourceReadback", "resourceReadbackSha256", "manifest resource readback");
        final JsonArray resourceReadback = readJsonArrayRegular(resourceReadbackPath, "manifest resource readback");
        requireExactResourceReadback(resourceReadback, candidateScope);
        requireEmptyArray(manifestReadback, "destructiveOperations");
        final JsonObject operations = requiredObject(manifestReadback, "operations");
        requireInt(operations, "resourceReadbackCount", DataResetAssessmentScope.ResourceKind.values().length);
        for (String field : List.of(
                "exactScope",
                "allFresh",
                "topicsCreatedAndReadBack",
                "evidenceCursorCreatedAndReadBack",
                "oxiaIncarnationsCreatedAndReadBack",
                "minioPayloadMarkerCreatedAndReadBack",
                "rocksdbIncarnationCreatedAndReadBack")) {
            requireBoolean(operations, field, true);
        }

        final Path g0Path = requireReferencedDigest(gate, "g0SnapshotPath", "g0SnapshotSha256", "G0 snapshot");
        final JsonObject g0 = readJsonRegular(g0Path, "G0 snapshot");
        require(g0, "schema", "nereus-delay.ndip1-g0-data-reset-snapshot");
        requireInt(g0, "schemaGeneration", 1);
        require(g0, "environmentId", environmentId);
        require(g0, "classification", gateClassification);
        require(g0, "candidateCommit", candidateCommit);
        require(g0, "acceptedPackageDigest", EXPECTED_PACKAGE_DIGEST);
        require(g0, "p1SourceLock", EXPECTED_P1_LOCK);
        requireBoolean(g0, "unresolvedPublishingOrUncertain", false);
        final JsonObject assessedDeployment = requiredObject(g0, "assessedDeployment");
        require(
                requiredField(assessedDeployment, "dataDispositionEnvelope"),
                dispositionPath.toAbsolutePath().normalize().toString(),
                "G0 data disposition path");
        require(
                requiredField(assessedDeployment, "dataDispositionEnvelopeSha256"),
                Bytes.hex(dispositionEnvelope.envelopeDigest()),
                "G0 data disposition digest");
        final Path observationsPath =
                requireReferencedDigest(gate, "g0ObservationsPath", "g0ObservationsSha256", "G0 resource observations");
        final JsonArray observations = readJsonArrayRegular(observationsPath, "G0 resource observations");
        requireAssessmentObservations(assessment, observations, canonicalAssessmentScope, gateResolution);

        final Path skipAuditPath =
                requireReferencedDigest(gate, "skipAuditPath", "skipAuditSha256", "Gate C skip audit");
        requireSkipAudit(readJsonRegular(skipAuditPath, "Gate C skip audit"));

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

    private static void requireBoolean(final JsonObject object, final String name, final boolean expected)
            throws IOException {
        if (object == null
                || !object.has(name)
                || !object.get(name).isJsonPrimitive()
                || !object.getAsJsonPrimitive(name).isBoolean()
                || object.get(name).getAsBoolean() != expected) {
            throw new IOException("receipt boolean field mismatch: " + name);
        }
    }

    private static void requireInt(final JsonObject object, final String name, final int expected) throws IOException {
        if (object == null
                || !object.has(name)
                || !object.get(name).isJsonPrimitive()
                || !object.getAsJsonPrimitive(name).isNumber()) {
            throw new IOException("receipt integer field is missing: " + name);
        }
        try {
            if (object.get(name).getAsInt() != expected) {
                throw new IOException("receipt integer field mismatch: " + name);
            }
        } catch (NumberFormatException failure) {
            throw new IOException("receipt integer field is not canonical: " + name, failure);
        }
    }

    private static JsonObject requiredObject(final JsonObject object, final String name) throws IOException {
        if (object == null || !object.has(name) || !object.get(name).isJsonObject()) {
            throw new IOException("receipt object field is missing: " + name);
        }
        return object.getAsJsonObject(name);
    }

    private static void requireEmptyArray(final JsonObject object, final String name) throws IOException {
        if (object == null
                || !object.has(name)
                || !object.get(name).isJsonArray()
                || !object.getAsJsonArray(name).isEmpty()) {
            throw new IOException("receipt array is missing or non-empty: " + name);
        }
    }

    static void requireResolutionBinding(final GateCAuthorization.Resolution resolution, final String assessmentOutcome)
            throws IOException {
        final GateCAuthorization.Resolution expected =
                switch (assessmentOutcome) {
                    case "PASS_DIRECT_REPLACE" -> GateCAuthorization.Resolution.RESET;
                    case "PASS_RETAIN" -> GateCAuthorization.Resolution.RETAIN;
                    default -> throw new IOException("assessment outcome is not decision-ready");
                };
        if (resolution != expected) {
            throw new IOException("Gate C resolution differs from the assessment outcome");
        }
    }

    static void requireDispositionBinding(final GateCAuthorization.Resolution resolution, final JsonObject disposition)
            throws IOException {
        requireBoolean(disposition, "destructiveOperationsAuthorized", false);
        switch (resolution) {
            case RESET -> {
                require(disposition, "decision", "RESET_INTERNAL_ONLY", "CREATE_NEW_INTERNAL_ONLY");
                requireBoolean(disposition, "externalUserDataPresent", false);
                requireBoolean(disposition, "existingResourcesAreInternalStagingOnly", true);
                require(disposition, "replacementDisposition", "REINCARNATE");
            }
            case RETAIN -> {
                require(disposition, "decision", "RETAIN_EXISTING");
                require(disposition, "replacementDisposition", "RETAIN");
            }
            case MIGRATED -> throw new IOException("MIGRATED requires a separate accepted migration proof");
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

    private static Path requireReferencedDigest(
            final JsonObject value, final String pathField, final String digestField, final String label)
            throws IOException {
        final Path path =
                Path.of(requiredField(value, pathField)).toAbsolutePath().normalize();
        requireDigest(Bytes.sha256(readRegular(path)), requiredField(value, digestField), label);
        return path;
    }

    private static JsonObject readJsonRegular(final Path path, final String label) throws IOException {
        try {
            final JsonElement value = JsonParser.parseString(new String(readRegular(path), StandardCharsets.UTF_8));
            if (!value.isJsonObject()) {
                throw new IOException(label + " is not a JSON object");
            }
            return value.getAsJsonObject();
        } catch (RuntimeException failure) {
            throw new IOException(label + " is not canonical JSON", failure);
        }
    }

    private static JsonArray readJsonArrayRegular(final Path path, final String label) throws IOException {
        try {
            final JsonElement value = JsonParser.parseString(new String(readRegular(path), StandardCharsets.UTF_8));
            if (!value.isJsonArray()) {
                throw new IOException(label + " is not a JSON array");
            }
            return value.getAsJsonArray();
        } catch (RuntimeException failure) {
            throw new IOException(label + " is not canonical JSON", failure);
        }
    }

    private static void requireManifestResources(
            final DataResetManifest manifest, final DataResetAssessmentScope candidateScope) throws IOException {
        final Set<String> expected = resourceSubjects(candidateScope);
        final Set<String> actual = new HashSet<>();
        for (DataResetManifest.ResourceIncarnation resource : manifest.resources()) {
            final String subject = resource.kind() + '\0' + resource.identity();
            if (!resource.fresh() || !actual.add(subject)) {
                throw new IOException("DataResetManifest resource set is stale or duplicated");
            }
        }
        if (!actual.equals(expected)) {
            throw new IOException("DataResetManifest resource set differs from the signed candidate scope");
        }
    }

    static void requireExactResourceReadback(final JsonArray rows, final DataResetAssessmentScope candidateScope)
            throws IOException {
        final Set<String> expected = resourceSubjects(candidateScope);
        final Set<String> actual = new HashSet<>();
        if (rows.size() != expected.size()) {
            throw new IOException("manifest resource readback cardinality differs from the candidate scope");
        }
        for (JsonElement element : rows) {
            if (!element.isJsonObject()) {
                throw new IOException("manifest resource readback row is not an object");
            }
            final JsonObject row = element.getAsJsonObject();
            final DataResetAssessmentScope.ResourceKind kind;
            try {
                kind = DataResetAssessmentScope.ResourceKind.valueOf(requiredField(row, "kind"));
            } catch (IllegalArgumentException failure) {
                throw new IOException("manifest resource readback has an unknown kind", failure);
            }
            require(row, "status", "PASS");
            final String subject = kind.name() + '\0' + requiredField(row, "identity");
            if (!actual.add(subject)) {
                throw new IOException("manifest resource readback contains a duplicate resource");
            }
            requireReferencedDigest(row, "evidence", "evidenceSha256", "manifest resource evidence");
        }
        if (!actual.equals(expected)) {
            throw new IOException("manifest resource readback differs from the signed candidate scope");
        }
    }

    private static void requireAssessmentObservations(
            final JsonObject assessment,
            final JsonArray observations,
            final DataResetAssessmentScope assessmentScope,
            final GateCAuthorization.Resolution resolution)
            throws IOException {
        final JsonObject inventory = requiredObject(assessment, "inventory");
        final JsonArray signedObservations;
        if (!inventory.has("resourceObservations")
                || !inventory.get("resourceObservations").isJsonArray()) {
            throw new IOException("assessment has no signed resource observations");
        }
        signedObservations = inventory.getAsJsonArray("resourceObservations");
        if (!signedObservations.equals(observations)) {
            throw new IOException("G0 observations differ from the signed assessment inventory");
        }
        final Set<String> expected = resourceSubjects(assessmentScope);
        final Set<String> actual = new HashSet<>();
        if (observations.size() != expected.size()) {
            throw new IOException("G0 observations do not cover the exact assessed scope");
        }
        for (JsonElement element : observations) {
            if (!element.isJsonObject()) {
                throw new IOException("G0 observation row is not an object");
            }
            final JsonObject row = element.getAsJsonObject();
            require(row, "accessStatus", "COMPLETE");
            if (resolution == GateCAuthorization.Resolution.RESET) {
                require(row, "externalRetention", "NONE");
                require(row, "replacementDisposition", "REINCARNATE", "DISCARDABLE");
            } else if (resolution == GateCAuthorization.Resolution.RETAIN) {
                require(row, "replacementDisposition", "RETAIN_COMPATIBLE");
            } else {
                throw new IOException("MIGRATED observations require a separate migration proof");
            }
            decodeDigest(requiredField(row, "evidenceSha256"), "G0 observation evidence");
            final String subject = requiredField(row, "kind") + '\0' + requiredField(row, "identity");
            if (!actual.add(subject)) {
                throw new IOException("G0 observations contain a duplicate resource");
            }
        }
        if (!actual.equals(expected)) {
            throw new IOException("G0 observations differ from the exact assessed scope");
        }
    }

    static void requireSkipAudit(final JsonObject audit) throws IOException {
        require(audit, "schema", "nereus-delay.ndip1-staging-skip-audit");
        requireInt(audit, "schemaGeneration", 1);
        requireInt(audit, "expectedConditionalSkips", 41);
        final JsonObject counts = requiredObject(audit, "counts");
        requireInt(counts, "pass", 41);
        requireInt(counts, "failed", 0);
        requireInt(counts, "skipped", 0);
        requireInt(counts, "notExecuted", 0);
        if (!audit.has("rows")
                || !audit.get("rows").isJsonArray()
                || audit.getAsJsonArray("rows").size() != 41) {
            throw new IOException("Gate C skip audit does not contain exactly 41 rows");
        }
        for (JsonElement element : audit.getAsJsonArray("rows")) {
            final JsonObject row = element.getAsJsonObject();
            require(row, "baselineStatus", "CONDITIONAL_SKIP");
            require(row, "applicability", "REQUIRED_STAGING");
            require(row, "status", "PASS");
            if (requiredNonNegativeInt(row, "effectiveRuns") < 1) {
                throw new IOException("Gate C skip audit contains a case without an effective passing run");
            }
        }
    }

    private static int requiredNonNegativeInt(final JsonObject object, final String name) throws IOException {
        if (object == null
                || !object.has(name)
                || !object.get(name).isJsonPrimitive()
                || !object.getAsJsonPrimitive(name).isNumber()) {
            throw new IOException("receipt integer field is missing: " + name);
        }
        try {
            final int value = object.get(name).getAsInt();
            if (value < 0) {
                throw new IOException("receipt integer field is negative: " + name);
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IOException("receipt integer field is not canonical: " + name, failure);
        }
    }

    private static Set<String> resourceSubjects(final DataResetAssessmentScope scope) throws IOException {
        if (scope.resources().size() != DataResetAssessmentScope.ResourceKind.values().length) {
            throw new IOException("persistent staging scope must contain exactly one resource of each kind");
        }
        final Set<String> subjects = new HashSet<>();
        final Set<DataResetAssessmentScope.ResourceKind> kinds =
                EnumSet.noneOf(DataResetAssessmentScope.ResourceKind.class);
        for (DataResetAssessmentScope.ResourceRef resource : scope.resources()) {
            if (!kinds.add(resource.kind()) || !subjects.add(resource.kind().name() + '\0' + resource.identity())) {
                throw new IOException("persistent staging scope contains a duplicate resource kind");
            }
        }
        if (!kinds.equals(EnumSet.allOf(DataResetAssessmentScope.ResourceKind.class))) {
            throw new IOException("persistent staging scope is not a closed 13-resource set");
        }
        return subjects;
    }

    static void requireExactAssessmentBinding(final byte[] assessmentReceipt, final byte[] signedPayload)
            throws IOException {
        if (!Bytes.constantTimeEquals(assessmentReceipt, signedPayload)) {
            throw new IOException("assessment receipt and signed assessment envelope differ");
        }
    }

    private static byte[] decodeBase64(final String value) throws IOException {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException failure) {
            throw new IOException("receipt public key is not base64", failure);
        }
    }

    private static byte[] scopeDigest(final JsonObject value) throws IOException {
        return decodeScope(value).scopeDigest();
    }

    static DataResetAssessmentScope decodeScope(final JsonObject value) throws IOException {
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
                    strings(value.getAsJsonArray("eligibleWorkerIds")));
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
