package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.ProtocolVersionActivatePayload;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Verifies the external Worker capability authority at the activation edge.
 *
 * <p>This coordinator does not write the source log marker. It produces the
 * exact reader-set evidence that the marker must carry and fails closed when
 * any eligible Worker is absent, session-stale or missing the requested tuple.
 * The caller must persist the returned evidence in the source-ordered control
 * record before allowing a new writer version.</p>
 */
public final class ProtocolActivationAuthorityCoordinator {
    private static final byte[] EVIDENCE_DOMAIN = Bytes.utf8("nereus-delay-protocol-eligible-reader-set\0");

    private final ProtocolCapabilityAuthority authority;

    public ProtocolActivationAuthorityCoordinator(final ProtocolCapabilityAuthority authority) {
        this.authority = Objects.requireNonNull(authority, "authority");
    }

    public EligibleReaderSet requireEligibleReaders(final ProtocolTuple tuple, final List<String> eligibleWorkerIds) {
        Objects.requireNonNull(tuple, "tuple");
        final List<String> workerIds = sortedWorkerIds(eligibleWorkerIds);
        final List<ProtocolCapabilityAuthority.Publication> publications = new ArrayList<>();
        for (String workerId : workerIds) {
            final ProtocolCapabilityAuthority.Publication publication = authority
                    .current(workerId)
                    .orElseThrow(() -> new IllegalStateException(
                            "eligible Worker has no current protocol capability: " + workerId));
            final ProtocolCapabilityDeclaration declaration = publication.declaration();
            if (!workerId.equals(declaration.workerId())) {
                throw new IllegalStateException("protocol capability authority returned another Worker");
            }
            if (!declaration.supports(tuple)) {
                throw new IllegalStateException(
                        "eligible Worker does not support the requested protocol tuple: " + workerId);
            }
            publications.add(publication);
        }
        final byte[] evidenceHash = evidenceHash(tuple, publications);
        return new EligibleReaderSet(tuple, workerIds, publications, evidenceHash);
    }

    /**
     * Requires every eligible Worker to advertise one exact current
     * ArtifactGenerationSet before a generation-bound marker is built.
     */
    public EligibleReaderSet requireEligibleReaders(
            final ProtocolTuple tuple, final ArtifactGenerationSet artifacts, final List<String> eligibleWorkerIds) {
        Objects.requireNonNull(tuple, "tuple");
        Objects.requireNonNull(artifacts, "artifacts");
        final List<String> workerIds = sortedWorkerIds(eligibleWorkerIds);
        final List<ProtocolCapabilityAuthority.Publication> publications = new ArrayList<>();
        for (String workerId : workerIds) {
            final ProtocolCapabilityAuthority.Publication publication = authority
                    .current(workerId)
                    .orElseThrow(() -> new IllegalStateException(
                            "eligible Worker has no current protocol capability: " + workerId));
            final ProtocolCapabilityDeclaration declaration = publication.declaration();
            if (!workerId.equals(declaration.workerId())) {
                throw new IllegalStateException("protocol capability authority returned another Worker");
            }
            if (!declaration.isCurrentGeneration() || !declaration.supports(artifacts)) {
                throw new IllegalStateException(
                        "eligible Worker does not support the requested ArtifactGenerationSet: " + workerId);
            }
            if (!declaration.supports(tuple)) {
                throw new IllegalStateException(
                        "eligible Worker does not support the requested protocol tuple: " + workerId);
            }
            publications.add(publication);
        }
        final byte[] evidenceHash = evidenceHash(tuple, artifacts, publications);
        return new EligibleReaderSet(tuple, workerIds, publications, evidenceHash);
    }

    /** Verifies a control-plane activation payload against current readers. */
    public EligibleReaderSet authorize(
            final ProtocolVersionActivatePayload payload, final List<String> eligibleWorkerIds) {
        Objects.requireNonNull(payload, "payload");
        if (payload.isCurrentGeneration()) {
            return authorize(payload, payload.artifactGenerationSet(), eligibleWorkerIds);
        }
        final EligibleReaderSet result = requireEligibleReaders(payload.tuple(), eligibleWorkerIds);
        if (!Bytes.constantTimeEquals(payload.compatibleReaderSetEvidenceHash(), result.evidenceHash())) {
            throw new IllegalStateException("activation payload reader-set evidence is stale or forged");
        }
        return result;
    }

    /** Verifies a generation-bound activation payload against current readers. */
    public EligibleReaderSet authorize(
            final ProtocolVersionActivatePayload payload,
            final ArtifactGenerationSet artifacts,
            final List<String> eligibleWorkerIds) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(artifacts, "artifacts");
        if (!payload.isCurrentGeneration() || !payload.artifactGenerationSet().equals(artifacts)) {
            throw new IllegalStateException("activation payload ArtifactGenerationSet is stale or mixed");
        }
        final EligibleReaderSet result = requireEligibleReaders(payload.tuple(), artifacts, eligibleWorkerIds);
        if (!Bytes.constantTimeEquals(payload.compatibleReaderSetEvidenceHash(), result.evidenceHash())) {
            throw new IllegalStateException("activation payload reader-set evidence is stale or forged");
        }
        return result;
    }

    /** Reads one exact current declaration for an assignment barrier. */
    public ProtocolCapabilityDeclaration requireCurrentDeclaration(final String workerId) {
        final String exactWorkerId = canonicalText(workerId);
        return authority
                .current(exactWorkerId)
                .map(ProtocolCapabilityAuthority.Publication::declaration)
                .filter(declaration -> exactWorkerId.equals(declaration.workerId()))
                .orElseThrow(
                        () -> new IllegalStateException("Worker has no current protocol capability: " + exactWorkerId));
    }

    /** Caller supplies and protects the authority's complete eligible membership, as for existing activation. */
    public com.nereusstream.delay.protocol.TargetProtocolReaderSet requireTargetEligibleReaders(
            com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor descriptor,
            List<String> eligibleWorkerIds) {
        Objects.requireNonNull(descriptor);
        if (eligibleWorkerIds.size() > com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_READERS) {
            throw new IllegalArgumentException("Target eligible reader membership exceeds finite bound");
        }
        final var workerIds = sortedWorkerIds(eligibleWorkerIds);
        long bytes = 128;
        final var readers = new ArrayList<com.nereusstream.delay.protocol.TargetProtocolReaderSet.Reader>();
        for (var workerId : workerIds) {
            final var publication = authority
                    .current(workerId)
                    .orElseThrow(() -> new IllegalStateException("eligible Worker has no actual protocol capability"));
            final var declaration = publication.declaration();
            if (!workerId.equals(declaration.workerId())
                    || !declaration.isCurrentGeneration()
                    || !declaration.supports(descriptor.base())
                    || !declaration.supports(descriptor.tuple())
                    || !declaration.supports(descriptor)) {
                throw new IllegalStateException("eligible Worker lacks exact Target descriptor/base/tuple declaration");
            }
            bytes = Math.addExact(bytes, declaration.canonicalBytes().length + 32L);
            if (bytes > com.nereusstream.delay.protocol.TargetProtocolReaderSet.MAX_BYTES) {
                throw new IllegalArgumentException("complete Target reader evidence exceeds finite byte bound");
            }
            readers.add(new com.nereusstream.delay.protocol.TargetProtocolReaderSet.Reader(
                    publication.revision(), publication.declaration()));
        }
        final var evidence = new com.nereusstream.delay.protocol.TargetProtocolReaderSet(descriptor, readers);
        evidence.requireMembership(workerIds);
        evidence.canonicalBytes();
        return evidence;
    }

    /** Pre-A data-reader proof. Declaration of summary schema2 is not a Target Source activation34. */
    public com.nereusstream.delay.protocol.TargetProtocolReaderSet requireMigrationA2EligibleReaders(
            com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor importReader,
            List<String> completeEligibleWorkers) {
        if (!importReader.supportsPayloadObligationSummary()) {
            throw new IllegalArgumentException("A2 import needs the explicit Native32v2 reader matrix");
        }
        final var evidence = requireTargetEligibleReaders(importReader, completeEligibleWorkers);
        for (var reader : evidence.readers()) {
            if (!reader.declaration().supports(ProtocolTuple.targetMigrationActivateRootIssuer())) {
                throw new IllegalStateException("eligible A2 reader lacks tuple35");
            }
        }
        return evidence;
    }

    public com.nereusstream.delay.protocol.TargetProtocolReaderSet authorize(
            com.nereusstream.delay.protocol.TargetProtocolActivatePayload payload, List<String> eligibleWorkerIds) {
        final var evidence = requireTargetEligibleReaders(payload.descriptor(), eligibleWorkerIds);
        if (!Bytes.constantTimeEquals(payload.readerEvidenceHash(), evidence.digest())) {
            throw new IllegalStateException("Target activation payload reader evidence is stale or mismatched");
        }
        return evidence;
    }

    /**
     * Actual eligible-reader snapshot plus one original Root-issuer signature, for the existing Control registration
     * path.
     */
    public com.nereusstream.delay.protocol.TargetProtocolActivatePayload authorizeTargetRootTuple(
            com.nereusstream.delay.protocol.TargetInitialControlSnapshot originalRoot,
            com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor descriptor,
            List<String> completeEligibleWorkers,
            java.security.PrivateKey originalSnapshotIssuer) {
        final var readers = requireTargetEligibleReaders(descriptor, completeEligibleWorkers);
        return com.nereusstream.delay.protocol.TargetProtocolActivatePayload.forRoot(
                originalRoot, descriptor, readers.digest(), originalSnapshotIssuer);
    }

    public com.nereusstream.delay.protocol.TargetProtocolActivatePayload authorizeMigrationRootTuple(
            com.nereusstream.delay.protocol.TargetMigrationRoot originalRoot,
            com.nereusstream.delay.protocol.TargetProtocolActivationDescriptor descriptor,
            List<String> completeEligibleWorkers,
            int originalManagementIssuerVersion,
            java.security.PrivateKey originalManagementIssuer) {
        final var readers = requireTargetEligibleReaders(descriptor, completeEligibleWorkers);
        return com.nereusstream.delay.protocol.TargetProtocolActivatePayload.forMigrationRoot(
                originalRoot, descriptor, readers.digest(), originalManagementIssuerVersion, originalManagementIssuer);
    }

    /** Reuses the actual initial snapshot issuer purpose, exact original Prepared/frame and its held history. */
    public static com.nereusstream.delay.runtime.TargetProtocolActivationStore.RootIssuerAuthority originalRootIssuer(
            com.nereusstream.delay.runtime.TargetStoreBootstrap.InitialControlAuthority originalIssuer,
            com.nereusstream.delay.protocol.PreparedControlOperation originalPrepared,
            com.nereusstream.delay.protocol.SystemMutation originalFrame,
            Runnable originalProtection) {
        Objects.requireNonNull(originalIssuer);
        Objects.requireNonNull(originalPrepared);
        Objects.requireNonNull(originalFrame);
        Objects.requireNonNull(originalProtection);
        return (evidence, authorization, activationSource) -> {
            originalProtection.run();
            if (evidence.migrated()) {
                throw new IllegalArgumentException("ordinary initial issuer cannot authorize a migration Root");
            }
            final var initial = evidence.initial();
            if (!authorization.root().equals(initial.root())
                    || !Arrays.equals(authorization.initialSnapshotDigest(), initial.snapshotDigest())) {
                throw new IllegalStateException("tuple issuer changed original immutable Root");
            }
            final var body =
                    com.nereusstream.delay.protocol.TargetQuotaGrantControlBody.decode(originalFrame.canonicalBody());
            if (!Arrays.equals(initial.mutation().mutationDigest(), Bytes.sha256(originalFrame.canonicalEnvelope()))
                    || !Arrays.equals(initial.initialGrant().systemMutationId(), originalFrame.systemMutationId())
                    || !Arrays.equals(
                            initial.initialGrant().request().canonicalBytes(),
                            body.request().canonicalBytes())
                    || !initial.initialGrant().controlRef().equals(body.controlRef())
                    || !Arrays.equals(initial.authorIdentity(), originalFrame.authorIdentity())
                    || initial.mutation().source().compareTo(activationSource) >= 0) {
                throw new IllegalStateException(
                        "tuple issuer history changed original accepted initial grant/Source/frame");
            }
            com.nereusstream.delay.protocol.ControlTargetMutationBinding.validate(
                    originalPrepared, originalPrepared.targets().getFirst(), originalFrame);
            final var key = originalIssuer.authenticate(
                    initial.input(),
                    originalPrepared,
                    originalFrame,
                    initial.mutation().source());
            originalProtection.run();
            if (key == null || !initial.input().verifySignature(key) || !authorization.verifySignature(key)) {
                throw new IllegalStateException("tuple authorization lacks actual original snapshot issuer pin");
            }
            return key;
        };
    }

    private static byte[] evidenceHash(
            final ProtocolTuple tuple, final List<ProtocolCapabilityAuthority.Publication> publications) {
        return evidenceHash(tuple, null, publications);
    }

    private static byte[] evidenceHash(
            final ProtocolTuple tuple,
            final ArtifactGenerationSet artifacts,
            final List<ProtocolCapabilityAuthority.Publication> publications) {
        final byte[] canonical = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            if (artifacts != null) {
                CanonicalProtobuf.bytes(output, 2, artifacts.canonicalBytes());
            }
            for (ProtocolCapabilityAuthority.Publication publication : publications) {
                final ProtocolCapabilityDeclaration declaration = publication.declaration();
                final byte[] worker = CanonicalProtobuf.message(workerOutput -> {
                    CanonicalProtobuf.bytes(
                            workerOutput, 1, declaration.workerId().getBytes(StandardCharsets.UTF_8));
                    CanonicalProtobuf.uint64Bits(workerOutput, 2, publication.revision());
                    CanonicalProtobuf.bytes(workerOutput, 3, declaration.declarationDigest());
                    CanonicalProtobuf.bytes(workerOutput, 4, declaration.sessionIdentity());
                });
                CanonicalProtobuf.bytes(output, artifacts == null ? 2 : 3, worker);
            }
        });
        return Bytes.sha256(EVIDENCE_DOMAIN, canonical);
    }

    private static List<String> sortedWorkerIds(final List<String> values) {
        Objects.requireNonNull(values, "eligibleWorkerIds");
        if (values.isEmpty()) {
            throw new IllegalArgumentException("eligibleWorkerIds must be non-empty");
        }
        final List<String> result = new ArrayList<>(values.size());
        for (String value : values) {
            result.add(canonicalText(value));
        }
        result.sort(Comparator.naturalOrder());
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("eligibleWorkerIds contains a duplicate Worker");
            }
        }
        return List.copyOf(result);
    }

    private static String canonicalText(final String value) {
        Objects.requireNonNull(value, "workerId");
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("workerId must be nonblank NFC UTF-8");
        }
        return value;
    }

    public record EligibleReaderSet(
            ProtocolTuple tuple,
            List<String> workerIds,
            List<ProtocolCapabilityAuthority.Publication> publications,
            byte[] evidenceHash) {
        public EligibleReaderSet {
            Objects.requireNonNull(tuple, "tuple");
            workerIds = List.copyOf(Objects.requireNonNull(workerIds, "workerIds"));
            publications = List.copyOf(Objects.requireNonNull(publications, "publications"));
            Bytes.requireLength(evidenceHash, 32, "evidenceHash");
            evidenceHash = Bytes.copy(evidenceHash);
            if (workerIds.size() != publications.size()) {
                throw new IllegalArgumentException("reader Worker and capability counts differ");
            }
        }

        @Override
        public byte[] evidenceHash() {
            return Bytes.copy(evidenceHash);
        }
    }
}
