package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Signed, cutover-window proof for one exact environment generation.
 *
 * <p>This manifest is intentionally independent from the read-only G0
 * assessment receipt. An assessment can describe an environment, but only a
 * fresh manifest can prove the resource incarnations, zero obligations and
 * exact Worker barrier at the moment a generation is activated.</p>
 */
public final class DataResetManifest {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final int MAX_RESOURCES = 4096;
    private static final int MAX_WORKERS = 4096;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-data-reset-manifest\0");
    private static final byte[] SIGNATURE_DOMAIN = Bytes.utf8("nereus-delay-data-reset-manifest-signature\0");
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    private final ManifestScope scope;
    private final String sourceBaselineCommit;
    private final long resetGeneration;
    private final ArtifactGenerationSet artifacts;
    private final List<ResourceIncarnation> resources;
    private final byte[] freshResourceEvidenceDigest;
    private final ObligationZeroProof obligationZeroProof;
    private final List<WorkerCapability> workerCapabilities;
    private final TrustedUtcIntervalEvidence createdAt;
    private final ActivationWindow activationWindow;
    private final int issuerKeyGeneration;
    private final byte[] manifestDigest;
    private final byte[] signature;

    private DataResetManifest(
            final ManifestScope scope,
            final String sourceBaselineCommit,
            final long resetGeneration,
            final ArtifactGenerationSet artifacts,
            final List<ResourceIncarnation> resources,
            final byte[] freshResourceEvidenceDigest,
            final ObligationZeroProof obligationZeroProof,
            final List<WorkerCapability> workerCapabilities,
            final TrustedUtcIntervalEvidence createdAt,
            final ActivationWindow activationWindow,
            final int issuerKeyGeneration,
            final byte[] manifestDigest,
            final byte[] signature) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.sourceBaselineCommit = commit(sourceBaselineCommit);
        if (resetGeneration == 0) {
            throw new IllegalArgumentException("resetGeneration must be non-zero");
        }
        this.resetGeneration = resetGeneration;
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        if (resetGeneration != artifacts.environmentResetGeneration()) {
            throw new IllegalArgumentException("reset generation does not match ArtifactGenerationSet");
        }
        this.resources = sortedResources(resources);
        for (ResourceIncarnation resource : this.resources) {
            if (!resource.fresh()) {
                throw new IllegalArgumentException("DataResetManifest requires fresh resources");
            }
        }
        this.freshResourceEvidenceDigest = nonZero(freshResourceEvidenceDigest, "freshResourceEvidenceDigest");
        this.obligationZeroProof = Objects.requireNonNull(obligationZeroProof, "obligationZeroProof");
        if (!obligationZeroProof.isZero()) {
            throw new IllegalArgumentException("DataResetManifest requires a zero-obligation proof");
        }
        this.workerCapabilities = sortedWorkers(workerCapabilities);
        for (WorkerCapability worker : this.workerCapabilities) {
            if (!worker.declaration().isCurrentGeneration()
                    || !worker.declaration().supports(artifacts)) {
                throw new IllegalArgumentException(
                        "every manifest Worker must declare the exact current ArtifactGenerationSet");
            }
        }
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt");
        this.activationWindow = Objects.requireNonNull(activationWindow, "activationWindow");
        if (createdAt.latestEpochMs() >= activationWindow.validFromEpochMs()) {
            throw new IllegalArgumentException("manifest createdAt must precede the activation window");
        }
        if (issuerKeyGeneration <= 0) {
            throw new IllegalArgumentException("issuerKeyGeneration must be positive");
        }
        this.issuerKeyGeneration = issuerKeyGeneration;
        final byte[] expectedDigest = computeDigest();
        Bytes.requireLength(manifestDigest, HASH_LENGTH, "manifestDigest");
        if (!Bytes.constantTimeEquals(expectedDigest, manifestDigest)) {
            throw new IllegalArgumentException("DataResetManifest digest mismatch");
        }
        this.manifestDigest = Bytes.copy(manifestDigest);
        Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
        this.signature = Bytes.copy(signature);
    }

    /** Creates and signs a manifest from fresh cutover evidence. */
    public static DataResetManifest create(
            final ManifestScope scope,
            final String sourceBaselineCommit,
            final long resetGeneration,
            final ArtifactGenerationSet artifacts,
            final List<ResourceIncarnation> resources,
            final byte[] freshResourceEvidenceDigest,
            final ObligationZeroProof obligationZeroProof,
            final List<WorkerCapability> workerCapabilities,
            final TrustedUtcIntervalEvidence createdAt,
            final ActivationWindow activationWindow,
            final int issuerKeyGeneration,
            final PrivateKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        final byte[] fields = canonicalFields(
                scope,
                sourceBaselineCommit,
                resetGeneration,
                artifacts,
                resources,
                freshResourceEvidenceDigest,
                obligationZeroProof,
                workerCapabilities,
                createdAt,
                activationWindow,
                issuerKeyGeneration);
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, fields);
        return new DataResetManifest(
                scope,
                sourceBaselineCommit,
                resetGeneration,
                artifacts,
                resources,
                freshResourceEvidenceDigest,
                obligationZeroProof,
                workerCapabilities,
                createdAt,
                activationWindow,
                issuerKeyGeneration,
                digest,
                sign(digest, issuerKeyGeneration, issuerKey));
    }

    public ManifestScope scope() {
        return scope;
    }

    public String sourceBaselineCommit() {
        return sourceBaselineCommit;
    }

    public long resetGeneration() {
        return resetGeneration;
    }

    public ArtifactGenerationSet artifacts() {
        return artifacts;
    }

    public List<ResourceIncarnation> resources() {
        return resources;
    }

    public byte[] freshResourceEvidenceDigest() {
        return Bytes.copy(freshResourceEvidenceDigest);
    }

    public ObligationZeroProof obligationZeroProof() {
        return obligationZeroProof;
    }

    public List<WorkerCapability> workerCapabilities() {
        return workerCapabilities;
    }

    public TrustedUtcIntervalEvidence createdAt() {
        return createdAt;
    }

    public ActivationWindow activationWindow() {
        return activationWindow;
    }

    public int issuerKeyGeneration() {
        return issuerKeyGeneration;
    }

    public byte[] manifestDigest() {
        return Bytes.copy(manifestDigest);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    public boolean isCurrentGeneration() {
        return resetGeneration == artifacts.environmentResetGeneration()
                && artifacts.clientCommandTuple().bodyVersion() == 2
                && artifacts.systemMutationTuple().bodyVersion() == 2;
    }

    /** Requires a trusted wall-clock sample inside the half-open cutover window. */
    public void requireWithinWindow(final long trustedNowEpochMs) {
        if (trustedNowEpochMs < activationWindow.validFromEpochMs()
                || trustedNowEpochMs >= activationWindow.validUntilEpochMs()) {
            throw new IllegalStateException("DataResetManifest is outside its activation window");
        }
    }

    /** Requires an exact scope projection, including the route and shard identity. */
    public void requireExactScope(
            final String environmentId,
            final String deploymentId,
            final byte[] tenantScopeDigest,
            final byte[] routeSnapshotDigest,
            final ShardSubject shard) {
        final ManifestScope expected =
                new ManifestScope(environmentId, deploymentId, tenantScopeDigest, routeSnapshotDigest, shard);
        if (!scope.equals(expected)) {
            throw new IllegalStateException("DataResetManifest scope mismatch");
        }
    }

    /** Returns the exact canonical outer representation, including digest and signature. */
    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            output.writeBytes(canonicalFields(
                    scope,
                    sourceBaselineCommit,
                    resetGeneration,
                    artifacts,
                    resources,
                    freshResourceEvidenceDigest,
                    obligationZeroProof,
                    workerCapabilities,
                    createdAt,
                    activationWindow,
                    issuerKeyGeneration));
            CanonicalProtobuf.bytes(output, 13, manifestDigest);
            CanonicalProtobuf.bytes(output, 14, signature);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("DataResetManifest is too large");
        }
        return encoded;
    }

    public boolean verifySignature(final PublicKey issuerKey) {
        Objects.requireNonNull(issuerKey, "issuerKey");
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(issuerKey);
            verifier.update(signatureInput(manifestDigest, issuerKeyGeneration));
            return verifier.verify(signature);
        } catch (GeneralSecurityException error) {
            throw new IllegalArgumentException("cannot verify DataResetManifest signature", error);
        }
    }

    public static DataResetManifest decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid DataResetManifest length");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "DataResetManifest", true);
        if (fields.size() < 14
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || fields.get(4).number() != 5) {
            throw new IllegalArgumentException("DataResetManifest required fields are incomplete");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported DataResetManifest schema generation");
        }
        final ManifestScope scope = ManifestScope.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final String baseline = text(QueryCodecSupport.bytes(fields.get(2), 3), "sourceBaselineCommit");
        final long resetGeneration = QueryCodecSupport.uint64Bits(fields.get(3), 4);
        final ArtifactGenerationSet artifacts =
                ArtifactGenerationSet.decode(QueryCodecSupport.nested(fields.get(4), 5));
        int index = 5;
        final List<ResourceIncarnation> resources = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 6) {
            resources.add(ResourceIncarnation.decode(QueryCodecSupport.nested(fields.get(index++), 6)));
        }
        if (index + 1 >= fields.size() || fields.get(index).number() != 7) {
            throw new IllegalArgumentException("DataResetManifest resource evidence is missing");
        }
        final byte[] freshEvidence = QueryCodecSupport.fixed(fields.get(index++), 7, HASH_LENGTH);
        final ObligationZeroProof obligations =
                ObligationZeroProof.decode(QueryCodecSupport.nested(fields.get(index++), 8));
        final List<WorkerCapability> workers = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 9) {
            workers.add(WorkerCapability.decode(QueryCodecSupport.nested(fields.get(index++), 9)));
        }
        if (index + 5 != fields.size()
                || fields.get(index).number() != 10
                || fields.get(index + 1).number() != 11
                || fields.get(index + 2).number() != 12
                || fields.get(index + 3).number() != 13
                || fields.get(index + 4).number() != 14) {
            throw new IllegalArgumentException("DataResetManifest trailing fields are incomplete or out of order");
        }
        final TrustedUtcIntervalEvidence createdAt =
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index), 10));
        final ActivationWindow window = ActivationWindow.decode(QueryCodecSupport.nested(fields.get(index + 1), 11));
        final int keyGeneration = QueryCodecSupport.uint32(fields.get(index + 2), 12);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index + 3), 13, HASH_LENGTH);
        final byte[] signature = QueryCodecSupport.fixed(fields.get(index + 4), 14, SIGNATURE_LENGTH);
        final DataResetManifest result = new DataResetManifest(
                scope,
                baseline,
                resetGeneration,
                artifacts,
                resources,
                freshEvidence,
                obligations,
                workers,
                createdAt,
                window,
                keyGeneration,
                digest,
                signature);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DataResetManifest");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DataResetManifest that
                && scope.equals(that.scope)
                && sourceBaselineCommit.equals(that.sourceBaselineCommit)
                && resetGeneration == that.resetGeneration
                && artifacts.equals(that.artifacts)
                && resources.equals(that.resources)
                && Arrays.equals(freshResourceEvidenceDigest, that.freshResourceEvidenceDigest)
                && obligationZeroProof.equals(that.obligationZeroProof)
                && workerCapabilities.equals(that.workerCapabilities)
                && createdAt.equals(that.createdAt)
                && activationWindow.equals(that.activationWindow)
                && issuerKeyGeneration == that.issuerKeyGeneration
                && Arrays.equals(manifestDigest, that.manifestDigest)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                scope,
                sourceBaselineCommit,
                resetGeneration,
                artifacts,
                resources,
                Arrays.hashCode(freshResourceEvidenceDigest),
                obligationZeroProof,
                workerCapabilities,
                createdAt,
                activationWindow,
                issuerKeyGeneration,
                Arrays.hashCode(manifestDigest),
                Arrays.hashCode(signature));
    }

    private byte[] computeDigest() {
        return Bytes.sha256(
                DIGEST_DOMAIN,
                canonicalFields(
                        scope,
                        sourceBaselineCommit,
                        resetGeneration,
                        artifacts,
                        resources,
                        freshResourceEvidenceDigest,
                        obligationZeroProof,
                        workerCapabilities,
                        createdAt,
                        activationWindow,
                        issuerKeyGeneration));
    }

    private static byte[] canonicalFields(
            final ManifestScope scope,
            final String baseline,
            final long resetGeneration,
            final ArtifactGenerationSet artifacts,
            final List<ResourceIncarnation> resources,
            final byte[] freshEvidence,
            final ObligationZeroProof obligations,
            final List<WorkerCapability> workers,
            final TrustedUtcIntervalEvidence createdAt,
            final ActivationWindow window,
            final int keyGeneration) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, baseline.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.uint64Bits(output, 4, resetGeneration);
            CanonicalProtobuf.bytes(output, 5, artifacts.canonicalBytes());
            for (ResourceIncarnation resource : sortedResources(resources)) {
                CanonicalProtobuf.bytes(output, 6, resource.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 7, freshEvidence);
            CanonicalProtobuf.bytes(output, 8, obligations.canonicalBytes());
            for (WorkerCapability worker : sortedWorkers(workers)) {
                CanonicalProtobuf.bytes(output, 9, worker.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 10, createdAt.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, window.canonicalBytes());
            CanonicalProtobuf.uint32(output, 12, keyGeneration);
        });
    }

    private static byte[] signatureInput(final byte[] digest, final int keyGeneration) {
        return Bytes.concat(SIGNATURE_DOMAIN, digest, Bytes.u32be(keyGeneration));
    }

    private static byte[] sign(final byte[] digest, final int keyGeneration, final PrivateKey issuerKey) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(issuerKey);
            signer.update(signatureInput(digest, keyGeneration));
            final byte[] signature = signer.sign();
            Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
            return signature;
        } catch (GeneralSecurityException error) {
            throw new IllegalArgumentException("cannot sign DataResetManifest", error);
        }
    }

    private static List<ResourceIncarnation> sortedResources(final List<ResourceIncarnation> values) {
        Objects.requireNonNull(values, "resources");
        if (values.isEmpty() || values.size() > MAX_RESOURCES) {
            throw new IllegalArgumentException("resources must be non-empty and bounded");
        }
        final List<ResourceIncarnation> result = new ArrayList<>(values);
        result.forEach(value -> Objects.requireNonNull(value, "resource"));
        result.sort(Comparator.comparing(ResourceIncarnation::canonicalBytes, DataResetManifest::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("duplicate manifest resource");
            }
        }
        return List.copyOf(result);
    }

    private static List<WorkerCapability> sortedWorkers(final List<WorkerCapability> values) {
        Objects.requireNonNull(values, "workerCapabilities");
        if (values.isEmpty() || values.size() > MAX_WORKERS) {
            throw new IllegalArgumentException("workerCapabilities must be non-empty and bounded");
        }
        final List<WorkerCapability> result = new ArrayList<>(values);
        result.forEach(value -> Objects.requireNonNull(value, "workerCapability"));
        result.sort(Comparator.comparing(
                worker -> worker.workerId().getBytes(StandardCharsets.UTF_8), DataResetManifest::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).workerId().equals(result.get(index).workerId())) {
                throw new IllegalArgumentException("duplicate manifest Worker");
            }
        }
        return List.copyOf(result);
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static String commit(final String value) {
        final String result = canonicalText(value, "sourceBaselineCommit");
        if (!COMMIT.matcher(result).matches()) {
            throw new IllegalArgumentException("sourceBaselineCommit must be a lowercase 40-hex commit");
        }
        return result;
    }

    private static String text(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return canonicalText(result, name);
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        final byte[] result = Bytes.copy(value);
        for (byte element : result) {
            if (element != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }

    /** Exact deployment, tenant, route and shard scope of this manifest. */
    public record ManifestScope(
            String environmentId,
            String deploymentId,
            byte[] tenantScopeDigest,
            byte[] routeSnapshotDigest,
            ShardSubject shard) {
        public ManifestScope {
            environmentId = canonicalText(environmentId, "environmentId");
            deploymentId = canonicalText(deploymentId, "deploymentId");
            tenantScopeDigest = nonZero(tenantScopeDigest, "tenantScopeDigest");
            routeSnapshotDigest = nonZero(routeSnapshotDigest, "routeSnapshotDigest");
            Objects.requireNonNull(shard, "shard");
        }

        @Override
        public byte[] tenantScopeDigest() {
            return Bytes.copy(tenantScopeDigest);
        }

        @Override
        public byte[] routeSnapshotDigest() {
            return Bytes.copy(routeSnapshotDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, environmentId.getBytes(StandardCharsets.UTF_8));
                CanonicalProtobuf.bytes(output, 2, deploymentId.getBytes(StandardCharsets.UTF_8));
                CanonicalProtobuf.bytes(output, 3, tenantScopeDigest);
                CanonicalProtobuf.bytes(output, 4, routeSnapshotDigest);
                CanonicalProtobuf.bytes(output, 5, shard.canonicalBytes());
            });
        }

        public static ManifestScope decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "DataResetManifest.ManifestScope");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "DataResetManifest.ManifestScope");
            final ManifestScope result = new ManifestScope(
                    text(QueryCodecSupport.bytes(fields.get(0), 1), "environmentId"),
                    text(QueryCodecSupport.bytes(fields.get(1), 2), "deploymentId"),
                    QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                    QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                    ShardSubject.decode(QueryCodecSupport.nested(fields.get(4), 5)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DataResetManifest.ManifestScope");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ManifestScope that
                    && environmentId.equals(that.environmentId)
                    && deploymentId.equals(that.deploymentId)
                    && Arrays.equals(tenantScopeDigest, that.tenantScopeDigest)
                    && Arrays.equals(routeSnapshotDigest, that.routeSnapshotDigest)
                    && shard.equals(that.shard);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    environmentId,
                    deploymentId,
                    Arrays.hashCode(tenantScopeDigest),
                    Arrays.hashCode(routeSnapshotDigest),
                    shard);
        }
    }

    /** Resource identity and incarnation freshly observed for this cutover. */
    public record ResourceIncarnation(
            String kind, String identity, byte[] incarnationDigest, boolean fresh, byte[] evidenceDigest) {
        public ResourceIncarnation {
            kind = canonicalText(kind, "resource kind");
            identity = canonicalText(identity, "resource identity");
            incarnationDigest = nonZero(incarnationDigest, "incarnationDigest");
            evidenceDigest = nonZero(evidenceDigest, "resource evidenceDigest");
        }

        @Override
        public byte[] incarnationDigest() {
            return Bytes.copy(incarnationDigest);
        }

        @Override
        public byte[] evidenceDigest() {
            return Bytes.copy(evidenceDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, kind.getBytes(StandardCharsets.UTF_8));
                CanonicalProtobuf.bytes(output, 2, identity.getBytes(StandardCharsets.UTF_8));
                CanonicalProtobuf.bytes(output, 3, incarnationDigest);
                CanonicalProtobuf.uint32(output, 4, fresh ? 1 : 0);
                CanonicalProtobuf.bytes(output, 5, evidenceDigest);
            });
        }

        public static ResourceIncarnation decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "DataResetManifest.ResourceIncarnation");
            QueryCodecSupport.requireNumbers(
                    fields, new int[] {1, 2, 3, 4, 5}, "DataResetManifest.ResourceIncarnation");
            final ResourceIncarnation result = new ResourceIncarnation(
                    text(QueryCodecSupport.bytes(fields.get(0), 1), "resource kind"),
                    text(QueryCodecSupport.bytes(fields.get(1), 2), "resource identity"),
                    QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                    QueryCodecSupport.bool(fields.get(3), 4),
                    QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH));
            QueryCodecSupport.requireCanonical(
                    encoded, result.canonicalBytes(), "DataResetManifest.ResourceIncarnation");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ResourceIncarnation that
                    && kind.equals(that.kind)
                    && identity.equals(that.identity)
                    && Arrays.equals(incarnationDigest, that.incarnationDigest)
                    && fresh == that.fresh
                    && Arrays.equals(evidenceDigest, that.evidenceDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    kind, identity, Arrays.hashCode(incarnationDigest), fresh, Arrays.hashCode(evidenceDigest));
        }
    }

    /** Counts proving no external retention, publishing or uncertain obligation remains. */
    public record ObligationZeroProof(
            long externalRetentionCount, long publishingCount, long uncertainCount, byte[] evidenceDigest) {
        public ObligationZeroProof {
            if (externalRetentionCount < 0 || publishingCount < 0 || uncertainCount < 0) {
                throw new IllegalArgumentException("obligation counts must be non-negative");
            }
            evidenceDigest = nonZero(evidenceDigest, "obligation evidenceDigest");
        }

        @Override
        public byte[] evidenceDigest() {
            return Bytes.copy(evidenceDigest);
        }

        public boolean isZero() {
            return externalRetentionCount == 0 && publishingCount == 0 && uncertainCount == 0;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint64(output, 1, externalRetentionCount);
                CanonicalProtobuf.uint64(output, 2, publishingCount);
                CanonicalProtobuf.uint64(output, 3, uncertainCount);
                CanonicalProtobuf.bytes(output, 4, evidenceDigest);
            });
        }

        public static ObligationZeroProof decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "DataResetManifest.ObligationZeroProof");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4}, "DataResetManifest.ObligationZeroProof");
            final ObligationZeroProof result = new ObligationZeroProof(
                    nonNegative(QueryCodecSupport.uint64Bits(fields.get(0), 1), "externalRetentionCount"),
                    nonNegative(QueryCodecSupport.uint64Bits(fields.get(1), 2), "publishingCount"),
                    nonNegative(QueryCodecSupport.uint64Bits(fields.get(2), 3), "uncertainCount"),
                    QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
            QueryCodecSupport.requireCanonical(
                    encoded, result.canonicalBytes(), "DataResetManifest.ObligationZeroProof");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ObligationZeroProof that
                    && externalRetentionCount == that.externalRetentionCount
                    && publishingCount == that.publishingCount
                    && uncertainCount == that.uncertainCount
                    && Arrays.equals(evidenceDigest, that.evidenceDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    externalRetentionCount, publishingCount, uncertainCount, Arrays.hashCode(evidenceDigest));
        }
    }

    /** One exact Worker session and its current-generation capability evidence. */
    public record WorkerCapability(
            String workerId,
            byte[] workerIdentity,
            byte[] sessionIdentity,
            ProtocolCapabilityDeclaration declaration,
            byte[] capabilityEvidenceDigest) {
        public WorkerCapability {
            workerId = canonicalText(workerId, "workerId");
            workerIdentity = nonZero(workerIdentity, "workerIdentity");
            sessionIdentity = nonZero(sessionIdentity, "sessionIdentity");
            declaration = Objects.requireNonNull(declaration, "declaration");
            capabilityEvidenceDigest = nonZero(capabilityEvidenceDigest, "capabilityEvidenceDigest");
            if (!workerId.equals(declaration.workerId())
                    || !Arrays.equals(workerIdentity, declaration.workerIdentity())
                    || !Arrays.equals(sessionIdentity, declaration.sessionIdentity())) {
                throw new IllegalArgumentException("manifest Worker capability identity does not match declaration");
            }
        }

        @Override
        public byte[] workerIdentity() {
            return Bytes.copy(workerIdentity);
        }

        @Override
        public byte[] sessionIdentity() {
            return Bytes.copy(sessionIdentity);
        }

        @Override
        public byte[] capabilityEvidenceDigest() {
            return Bytes.copy(capabilityEvidenceDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, workerId.getBytes(StandardCharsets.UTF_8));
                CanonicalProtobuf.bytes(output, 2, workerIdentity);
                CanonicalProtobuf.bytes(output, 3, sessionIdentity);
                CanonicalProtobuf.bytes(output, 4, declaration.canonicalBytes());
                CanonicalProtobuf.bytes(output, 5, capabilityEvidenceDigest);
            });
        }

        public static WorkerCapability decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "DataResetManifest.WorkerCapability");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "DataResetManifest.WorkerCapability");
            final WorkerCapability result = new WorkerCapability(
                    text(QueryCodecSupport.bytes(fields.get(0), 1), "workerId"),
                    QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                    QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                    ProtocolCapabilityDeclaration.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                    QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DataResetManifest.WorkerCapability");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof WorkerCapability that
                    && workerId.equals(that.workerId)
                    && Arrays.equals(workerIdentity, that.workerIdentity)
                    && Arrays.equals(sessionIdentity, that.sessionIdentity)
                    && declaration.equals(that.declaration)
                    && Arrays.equals(capabilityEvidenceDigest, that.capabilityEvidenceDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    workerId,
                    Arrays.hashCode(workerIdentity),
                    Arrays.hashCode(sessionIdentity),
                    declaration,
                    Arrays.hashCode(capabilityEvidenceDigest));
        }
    }

    /** Half-open trusted time interval in which activation may be attempted. */
    public record ActivationWindow(long validFromEpochMs, long validUntilEpochMs) {
        public ActivationWindow {
            if (validFromEpochMs < 0 || validUntilEpochMs <= validFromEpochMs) {
                throw new IllegalArgumentException("invalid DataResetManifest activation window");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.int64(output, 1, validFromEpochMs);
                CanonicalProtobuf.int64(output, 2, validUntilEpochMs);
            });
        }

        public static ActivationWindow decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "DataResetManifest.ActivationWindow");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "DataResetManifest.ActivationWindow");
            final ActivationWindow result = new ActivationWindow(
                    nonNegative(QueryCodecSupport.uint64Bits(fields.get(0), 1), "validFromEpochMs"),
                    nonNegative(QueryCodecSupport.uint64Bits(fields.get(1), 2), "validUntilEpochMs"));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DataResetManifest.ActivationWindow");
            return result;
        }
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
