package com.nereusstream.delay.assessment;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceKind;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.AccessStatus;
import com.nereusstream.delay.assessment.DataResetInventory.ExternalRetentionRequirement;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationKind;
import com.nereusstream.delay.assessment.DataResetInventory.ReplacementDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerUpgradeStatus;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Local-only authority tooling for the persistent NDIP-1 staging workflow.
 * It consumes explicit machine-collected JSON and never enumerates or mutates
 * runtime resources itself. The shell orchestration owns the read-only G0
 * snapshot and the exact resource operations; this class owns canonical
 * assessment/manifest construction and Ed25519 evidence persistence.
 */
public final class PersistentStagingAuthorityTool {
    private PersistentStagingAuthorityTool() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 2) {
            throw new IllegalArgumentException(
                    "usage: <assessment|manifest|verify-manifest|sign-json|scope-digest|verify-activation|verify-json> "
                            + "<config-or-payload.json>");
        }
        switch (arguments[0]) {
            case "assessment" -> writeAssessment(readObject(Path.of(arguments[1])));
            case "manifest" -> writeManifest(readObject(Path.of(arguments[1])));
            case "verify-manifest" -> verifyManifest(readObject(Path.of(arguments[1])));
            case "sign-json" -> signJson(readObject(Path.of(arguments[1])));
            case "scope-digest" -> printScopeDigest(readObject(Path.of(arguments[1])));
            case "verify-activation" -> verifyActivation();
            case "verify-json" -> verifyJson(readObject(Path.of(arguments[1])));
            default ->
                throw new IllegalArgumentException("unknown persistent staging authority command: " + arguments[0]);
        }
    }

    private static void printScopeDigest(final JsonObject config) {
        System.out.println("scopeDigest=" + Bytes.hex(scope(config).scopeDigest()));
    }

    private static void verifyActivation() throws Exception {
        final PersistentStagingActivation.Loaded activation = PersistentStagingActivation.loadFromEnvironment();
        System.out.println("environmentId=" + activation.environmentId());
        System.out.println("candidateCommit=" + activation.candidateCommit());
        System.out.println("gateCEnvelopeDigest=" + Bytes.hex(activation.gateCEnvelopeDigest()));
        System.out.println("shadowEnvelopeDigest=" + Bytes.hex(activation.shadowEnvelopeDigest()));
        System.out.println("policyEnvelopeDigest=" + Bytes.hex(activation.policyEnvelopeDigest()));
        System.out.println(
                "artifactSetDigest=" + Bytes.hex(activation.artifacts().setDigest()));
    }

    private static void verifyJson(final JsonObject config) throws Exception {
        final Path envelopePath = persistentPath(text(config, "signedEnvelopePath"));
        final PersistentStagingEvidence.Verified envelope = PersistentStagingEvidence.readVerified(envelopePath);
        final Path publicKeyPath = persistentPath(text(config, "publicKeyPath"));
        final PublicKey expected = PersistentStagingEvidence.decodePublicKey(readRegular(publicKeyPath));
        if (!Bytes.constantTimeEquals(
                expected.getEncoded(), envelope.publicKey().getEncoded())) {
            throw new IOException("signed JSON envelope uses another public key");
        }
        System.out.println("signedEnvelope=" + envelopePath);
        System.out.println("envelopeDigest=" + Bytes.hex(envelope.envelopeDigest()));
        System.out.println("payloadDigest=" + Bytes.hex(Bytes.sha256(envelope.payload())));
    }

    private static void writeAssessment(final JsonObject config) throws Exception {
        final DataResetAssessmentScope scope = scope(config);
        final JsonObject inventoryObject = object(config, "inventory");
        final DataResetInventory inventory = inventory(scope, inventoryObject);
        final String packageDigest = text(config, "ndipPackageDigest");
        final String sourceCommit = text(config, "sourceBaselineCommit");
        final DataResetAssessmentReceipt receipt =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, packageDigest, sourceCommit);
        final Path receiptPath = persistentPath(text(config, "receiptPath"));
        final Path envelopePath = persistentPath(text(config, "signedEnvelopePath"));
        final KeyMaterial key = keyMaterial(config);
        DataResetAssessmentReceiptWriter.writeNew(receiptPath, receipt);
        PersistentStagingEvidence.writeSignedNew(
                envelopePath, receipt.canonicalJsonBytes(), key.privateKey(), key.publicKey(), key.generation());
        System.out.println("assessmentReceipt=" + receiptPath);
        System.out.println("assessmentEnvelope=" + envelopePath);
        System.out.println("assessmentDigest=" + Bytes.hex(receipt.assessmentDigest()));
        System.out.println("assessmentScopeDigest=" + Bytes.hex(scope.scopeDigest()));
        System.out.println("assessmentOutcome=" + receipt.outcome());
    }

    private static void writeManifest(final JsonObject config) throws Exception {
        final String p1Lock = text(config, "p1SourceLockDigest");
        if (!Bytes.hex(PulsarSourceLock.digest()).equals(p1Lock)) {
            throw new IllegalArgumentException("manifest P1 source lock is not the accepted source lock");
        }
        final long resetGeneration = longValue(config, "resetGeneration");
        final byte[] schemaHash = digest(config, "canonicalSchemaBundleHash");
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.current(resetGeneration, PulsarSourceLock.digest(), schemaHash);
        final List<DataResetManifest.ResourceIncarnation> resources = manifestResources(config);
        final List<DataResetManifest.WorkerCapability> workers = manifestWorkers(config, artifacts);
        final long createdAt = longValue(config, "createdAtEpochMs");
        final TrustedUtcIntervalEvidence createdEvidence = timeEvidence(config, createdAt, createdAt);
        final DataResetManifest manifest = new DataResetManifestIssuer(
                        keyMaterial(config).privateKey(), intValue(config, "issuerKeyGeneration"))
                .issue(
                        new DataResetManifest.ManifestScope(
                                text(config, "environmentId"),
                                text(config, "deploymentId"),
                                digest(config, "tenantScopeDigest"),
                                digest(config, "routeSnapshotDigest"),
                                new ShardSubject(
                                        new RouteIncarnation(hex(config, "routeIncarnation")),
                                        intValue(config, "shardPartition"))),
                        text(config, "sourceBaselineCommit"),
                        resetGeneration,
                        artifacts,
                        resources,
                        digest(config, "freshResourceEvidenceDigest"),
                        new DataResetManifest.ObligationZeroProof(0, 0, 0, digest(config, "obligationEvidenceDigest")),
                        workers,
                        createdEvidence,
                        new DataResetManifest.ActivationWindow(
                                longValue(config, "activationValidFromEpochMs"),
                                longValue(config, "activationValidUntilEpochMs")));
        final KeyMaterial key = keyMaterial(config);
        if (key.generation() != manifest.issuerKeyGeneration()) {
            throw new IllegalArgumentException("manifest key generation differs from key material");
        }
        final Path manifestPath = persistentPath(text(config, "manifestPath"));
        PersistentStagingEvidence.writeNew(manifestPath, manifest.canonicalBytes());
        if (!manifest.verifySignature(key.publicKey())) {
            throw new IllegalStateException("new DataResetManifest did not verify with its public key");
        }
        System.out.println("manifest=" + manifestPath);
        System.out.println("manifestDigest=" + Bytes.hex(manifest.manifestDigest()));
        System.out.println("artifactSetDigest=" + Bytes.hex(manifest.artifacts().setDigest()));
        System.out.println("manifestCreatedAt=" + Instant.ofEpochMilli(createdAt));
    }

    private static void verifyManifest(final JsonObject config) throws Exception {
        final Path manifestPath = persistentPath(text(config, "manifestPath"));
        final DataResetManifest manifest = DataResetManifest.decode(readRegular(manifestPath));
        final PublicKey publicKey =
                PersistentStagingEvidence.decodePublicKey(readRegular(persistentPath(text(config, "publicKeyPath"))));
        if (!manifest.verifySignature(publicKey)) {
            throw new IOException("DataResetManifest signature verification failed");
        }
        if (!manifest.isCurrentGeneration()) {
            throw new IOException("DataResetManifest is not current generation");
        }
        System.out.println("manifest=" + manifestPath);
        System.out.println("manifestDigest=" + Bytes.hex(manifest.manifestDigest()));
        System.out.println("artifactSetDigest=" + Bytes.hex(manifest.artifacts().setDigest()));
        System.out.println("resourceCount=" + manifest.resources().size());
        System.out.println("workerCount=" + manifest.workerCapabilities().size());
    }

    private static void signJson(final JsonObject config) throws Exception {
        final Path payloadPath = persistentPath(text(config, "payloadPath"));
        final Path envelopePath = persistentPath(text(config, "signedEnvelopePath"));
        final KeyMaterial key = keyMaterial(config);
        final byte[] payload = readRegular(payloadPath);
        PersistentStagingEvidence.writeSignedNew(
                envelopePath, payload, key.privateKey(), key.publicKey(), key.generation());
        System.out.println("signedEnvelope=" + envelopePath);
        System.out.println("envelopeDigest=" + Bytes.hex(Bytes.sha256(readRegular(envelopePath))));
    }

    private static DataResetAssessmentScope scope(final JsonObject config) {
        final JsonObject value = object(config, "scope");
        return new DataResetAssessmentScope(
                text(value, "environmentId"),
                EnvironmentClassification.valueOf(text(value, "environmentClassification")),
                text(value, "deploymentId"),
                strings(value, "tenantIds"),
                strings(value, "routeIds"),
                strings(value, "shardIds"),
                resourceRefs(value),
                strings(value, "eligibleWorkerIds"));
    }

    private static DataResetInventory inventory(final DataResetAssessmentScope scope, final JsonObject value) {
        final TrustedUtcInterval time = time(value);
        final List<ResourceObservation> observations = new ArrayList<>();
        for (JsonElement element : array(value, "resourceObservations")) {
            final JsonObject observation = element.getAsJsonObject();
            final ResourceRef resource =
                    new ResourceRef(ResourceKind.valueOf(text(observation, "kind")), text(observation, "identity"));
            observations.add(new ResourceObservation(
                    resource,
                    AccessStatus.valueOf(text(observation, "accessStatus")),
                    ExternalRetentionRequirement.valueOf(text(observation, "externalRetention")),
                    ReplacementDisposition.valueOf(text(observation, "replacementDisposition")),
                    digest(observation, "evidenceSha256")));
        }
        final List<DataResetInventory.ObligationObservation> obligations = new ArrayList<>();
        for (JsonElement element : array(value, "obligations")) {
            final JsonObject obligation = element.getAsJsonObject();
            obligations.add(new DataResetInventory.ObligationObservation(
                    text(obligation, "identity"),
                    ObligationKind.valueOf(text(obligation, "kind")),
                    ObligationDisposition.valueOf(text(obligation, "disposition")),
                    digest(obligation, "evidenceSha256")));
        }
        final List<WorkerObservation> workers = new ArrayList<>();
        for (JsonElement element : array(value, "workers")) {
            final JsonObject worker = element.getAsJsonObject();
            workers.add(new WorkerObservation(
                    text(worker, "workerId"),
                    WorkerUpgradeStatus.valueOf(text(worker, "upgradeStatus")),
                    digest(worker, "evidenceSha256")));
        }
        final byte[] expectedScopeDigest = scope.scopeDigest();
        if (!Bytes.hex(expectedScopeDigest).equals(text(value, "scopeDigest"))) {
            throw new IllegalArgumentException("G0 inventory scopeDigest does not match canonical scope");
        }
        return new DataResetInventory(
                expectedScopeDigest,
                bool(value, "scopeEnumerationComplete"),
                digest(value, "scopeEvidenceSha256"),
                time,
                observations,
                bool(value, "obligationEnumerationComplete"),
                digest(value, "obligationEvidenceSha256"),
                obligations,
                bool(value, "workerEnumerationComplete"),
                digest(value, "workerEvidenceSha256"),
                workers);
    }

    private static TrustedUtcInterval time(final JsonObject value) {
        final JsonObject time = object(value, "observationTime");
        final long earliest = longValue(time, "earliestEpochMs");
        final long latest = longValue(time, "latestEpochMs");
        final TrustedUtcIntervalEvidence evidence = timeEvidence(time, earliest, latest);
        return new TrustedUtcInterval(earliest, latest, bool(time, "qualified"), evidence);
    }

    private static TrustedUtcIntervalEvidence timeEvidence(
            final JsonObject value, final long earliest, final long latest) {
        final String sourceId = value.has("sourceIdBase64")
                ? new String(base64(value, "sourceIdBase64"), StandardCharsets.UTF_8)
                : text(value, "sourceId");
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.valueOf(text(value, "source")),
                Bytes.utf8(sourceId),
                longValue(value, "sourceConfigGeneration"),
                longValue(value, "sampleSequence"),
                longValue(value, "monotonicAnchorNs"),
                digest(value, "sourceEvidenceSha256"),
                0,
                null);
    }

    private static List<ResourceRef> resourceRefs(final JsonObject value) {
        final List<ResourceRef> result = new ArrayList<>();
        for (JsonElement element : array(value, "resources")) {
            final JsonObject resource = element.getAsJsonObject();
            result.add(new ResourceRef(ResourceKind.valueOf(text(resource, "kind")), text(resource, "identity")));
        }
        return result;
    }

    private static List<DataResetManifest.ResourceIncarnation> manifestResources(final JsonObject config) {
        final List<DataResetManifest.ResourceIncarnation> result = new ArrayList<>();
        final EnumSet<ResourceKind> kinds = EnumSet.noneOf(ResourceKind.class);
        for (JsonElement element : array(config, "resourceIncarnations")) {
            final JsonObject resource = element.getAsJsonObject();
            final String kind = text(resource, "kind");
            kinds.add(ResourceKind.valueOf(kind));
            result.add(new DataResetManifest.ResourceIncarnation(
                    kind,
                    text(resource, "identity"),
                    digest(resource, "incarnationDigest"),
                    bool(resource, "fresh"),
                    digest(resource, "evidenceDigest")));
        }
        if (!kinds.equals(EnumSet.allOf(ResourceKind.class))) {
            throw new IllegalArgumentException("manifest resource incarnations do not cover all closed resource kinds");
        }
        return result;
    }

    private static List<DataResetManifest.WorkerCapability> manifestWorkers(
            final JsonObject config, final ArtifactGenerationSet artifacts) {
        if (!config.has("workers")) {
            return List.of(manifestWorker(config, artifacts));
        }
        final List<DataResetManifest.WorkerCapability> result = new ArrayList<>();
        for (JsonElement element : array(config, "workers")) {
            result.add(manifestWorker(element.getAsJsonObject(), artifacts));
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("manifest workers must be non-empty");
        }
        return List.copyOf(result);
    }

    private static DataResetManifest.WorkerCapability manifestWorker(
            final JsonObject worker, final ArtifactGenerationSet artifacts) {
        final String workerId = text(worker, "workerId");
        final byte[] workerIdentity = digest(worker, "workerIdentity");
        final byte[] sessionIdentity = digest(worker, "sessionIdentity");
        final ProtocolCapabilityDeclaration declaration = new ProtocolCapabilityDeclaration(
                workerId,
                workerIdentity,
                List.of(artifacts.clientCommandTuple(), artifacts.systemMutationTuple()),
                artifacts,
                longValue(worker, "capabilityEpoch"),
                sessionIdentity);
        return new DataResetManifest.WorkerCapability(
                workerId,
                workerIdentity,
                sessionIdentity,
                declaration,
                digest(worker, "workerCapabilityEvidenceDigest"));
    }

    private static KeyMaterial keyMaterial(final JsonObject config) throws Exception {
        final Path privatePath = persistentPath(text(config, "privateKeyPath"));
        final Path publicPath = persistentPath(text(config, "publicKeyPath"));
        final int generation = intValue(config, "issuerKeyGeneration");
        final boolean privateExists = Files.exists(privatePath, LinkOption.NOFOLLOW_LINKS);
        final boolean publicExists = Files.exists(publicPath, LinkOption.NOFOLLOW_LINKS);
        if (privateExists != publicExists) {
            throw new IOException("Ed25519 key pair is incomplete: " + privatePath + " / " + publicPath);
        }
        if (!privateExists) {
            final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
            final KeyPair generated = generator.generateKeyPair();
            PersistentStagingEvidence.writeNew(
                    privatePath, generated.getPrivate().getEncoded());
            PersistentStagingEvidence.writeNew(publicPath, generated.getPublic().getEncoded());
            return new KeyMaterial(generated.getPrivate(), generated.getPublic(), generation);
        }
        final PrivateKey privateKey = PersistentStagingEvidence.decodePrivateKey(readRegular(privatePath));
        final PublicKey publicKey = PersistentStagingEvidence.decodePublicKey(readRegular(publicPath));
        return new KeyMaterial(privateKey, publicKey, generation);
    }

    private static JsonObject readObject(final Path path) throws IOException {
        final Path normalized = persistentPath(path.toString());
        return JsonParser.parseString(new String(readRegular(normalized), StandardCharsets.UTF_8))
                .getAsJsonObject();
    }

    private static Path persistentPath(final String value) {
        final Path path = Path.of(value).toAbsolutePath().normalize();
        if (path.startsWith(Path.of("/tmp")) || path.startsWith(Path.of("/var/tmp"))) {
            throw new IllegalArgumentException("persistent staging evidence may not be written under a temporary path");
        }
        return path;
    }

    private static byte[] readRegular(final Path path) throws IOException {
        final Path normalized = path.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalized) || !Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("expected regular non-symlink file: " + normalized);
        }
        return Files.readAllBytes(normalized);
    }

    private static JsonObject object(final JsonObject parent, final String name) {
        return parent.getAsJsonObject(name);
    }

    private static JsonArray array(final JsonObject parent, final String name) {
        return parent.getAsJsonArray(name);
    }

    private static List<String> strings(final JsonObject parent, final String name) {
        final List<String> result = new ArrayList<>();
        for (JsonElement element : array(parent, name)) {
            result.add(element.getAsString());
        }
        return result;
    }

    private static String text(final JsonObject parent, final String name) {
        final JsonElement value = parent.get(name);
        if (value == null || !value.isJsonPrimitive() || value.getAsString().isBlank()) {
            throw new IllegalArgumentException("missing or blank JSON field: " + name);
        }
        return value.getAsString();
    }

    private static boolean bool(final JsonObject parent, final String name) {
        return Boolean.parseBoolean(text(parent, name));
    }

    private static long longValue(final JsonObject parent, final String name) {
        try {
            return Long.parseLong(text(parent, name));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("JSON field is not a long: " + name, failure);
        }
    }

    private static int intValue(final JsonObject parent, final String name) {
        try {
            return Integer.parseInt(text(parent, name));
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("JSON field is not an int: " + name, failure);
        }
    }

    private static byte[] digest(final JsonObject parent, final String name) {
        final byte[] result = hex(parent, name);
        Bytes.requireLength(result, 32, name);
        for (byte element : result) {
            if (element != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static byte[] hex(final JsonObject parent, final String name) {
        final String value = text(parent, name);
        try {
            return Bytes.hexToBytes(value);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("JSON field is not hex: " + name, failure);
        }
    }

    private static byte[] base64(final JsonObject parent, final String name) {
        try {
            return Base64.getDecoder().decode(text(parent, name));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("JSON field is not base64: " + name, failure);
        }
    }

    private record KeyMaterial(PrivateKey privateKey, PublicKey publicKey, int generation) {
        private KeyMaterial {
            Objects.requireNonNull(privateKey, "privateKey");
            Objects.requireNonNull(publicKey, "publicKey");
            if (generation <= 0) {
                throw new IllegalArgumentException("key generation must be positive");
            }
        }
    }
}
