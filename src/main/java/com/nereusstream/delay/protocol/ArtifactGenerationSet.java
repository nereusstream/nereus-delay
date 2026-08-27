package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * One immutable binding for all protocol, store, environment and P1
 * generations used by a worker session.
 */
public final class ArtifactGenerationSet {
    public static final int SCHEMA_GENERATION = 1;
    public static final int DESTINATION_PROFILE_GENERATION = 2;
    public static final int DESCRIPTOR_GENERATION = 2;
    public static final int ADAPTER_ENCODING_GENERATION = 2;
    public static final int NATIVE_PREPARED_GENERATION = 2;
    public static final int ATTEMPT_JOURNAL_GENERATION = 2;
    public static final int PULSAR_RECORD_GENERATION = 1;
    public static final int HANDOFF_SNAPSHOT_GENERATION = 1;
    public static final int EVIDENCE_GENERATION = 2;
    public static final int STORE_VALUE_GENERATION = 5;
    public static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-artifact-generation-set\0";

    private final ProtocolTuple clientCommandTuple;
    private final ProtocolTuple systemMutationTuple;
    private final int destinationProfileGeneration;
    private final int descriptorGeneration;
    private final int adapterEncodingGeneration;
    private final int nativePreparedGeneration;
    private final int attemptJournalGeneration;
    private final int pulsarRecordGeneration;
    private final int handoffSnapshotGeneration;
    private final int evidenceGeneration;
    private final int storeValueGeneration;
    private final long environmentResetGeneration;
    private final byte[] p1SourceLockDigest;
    private final byte[] canonicalSchemaBundleHash;
    private final byte[] setDigest;

    public ArtifactGenerationSet(
            final ProtocolTuple clientCommandTuple,
            final ProtocolTuple systemMutationTuple,
            final int destinationProfileGeneration,
            final int descriptorGeneration,
            final int adapterEncodingGeneration,
            final int nativePreparedGeneration,
            final int attemptJournalGeneration,
            final int pulsarRecordGeneration,
            final int handoffSnapshotGeneration,
            final int evidenceGeneration,
            final int storeValueGeneration,
            final long environmentResetGeneration,
            final byte[] p1SourceLockDigest,
            final byte[] canonicalSchemaBundleHash) {
        this(
                clientCommandTuple,
                systemMutationTuple,
                destinationProfileGeneration,
                descriptorGeneration,
                adapterEncodingGeneration,
                nativePreparedGeneration,
                attemptJournalGeneration,
                pulsarRecordGeneration,
                handoffSnapshotGeneration,
                evidenceGeneration,
                storeValueGeneration,
                environmentResetGeneration,
                p1SourceLockDigest,
                canonicalSchemaBundleHash,
                null);
    }

    private ArtifactGenerationSet(
            final ProtocolTuple clientCommandTuple,
            final ProtocolTuple systemMutationTuple,
            final int destinationProfileGeneration,
            final int descriptorGeneration,
            final int adapterEncodingGeneration,
            final int nativePreparedGeneration,
            final int attemptJournalGeneration,
            final int pulsarRecordGeneration,
            final int handoffSnapshotGeneration,
            final int evidenceGeneration,
            final int storeValueGeneration,
            final long environmentResetGeneration,
            final byte[] p1SourceLockDigest,
            final byte[] canonicalSchemaBundleHash,
            final byte[] suppliedDigest) {
        this.clientCommandTuple = requireTuple(clientCommandTuple, ProtocolTuple.CLIENT_COMMAND, "clientCommandTuple");
        this.systemMutationTuple =
                requireTuple(systemMutationTuple, ProtocolTuple.SYSTEM_MUTATION, "systemMutationTuple");
        if (this.clientCommandTuple.bodyVersion() != 2 || this.systemMutationTuple.bodyVersion() != 2) {
            throw new IllegalArgumentException("ArtifactGenerationSet requires body generation 2 tuples");
        }
        this.destinationProfileGeneration =
                exact(destinationProfileGeneration, DESTINATION_PROFILE_GENERATION, "destinationProfileGeneration");
        this.descriptorGeneration = exact(descriptorGeneration, DESCRIPTOR_GENERATION, "descriptorGeneration");
        this.adapterEncodingGeneration =
                exact(adapterEncodingGeneration, ADAPTER_ENCODING_GENERATION, "adapterEncodingGeneration");
        this.nativePreparedGeneration =
                exact(nativePreparedGeneration, NATIVE_PREPARED_GENERATION, "nativePreparedGeneration");
        this.attemptJournalGeneration =
                exact(attemptJournalGeneration, ATTEMPT_JOURNAL_GENERATION, "attemptJournalGeneration");
        this.pulsarRecordGeneration = exact(pulsarRecordGeneration, PULSAR_RECORD_GENERATION, "pulsarRecordGeneration");
        this.handoffSnapshotGeneration =
                exact(handoffSnapshotGeneration, HANDOFF_SNAPSHOT_GENERATION, "handoffSnapshotGeneration");
        this.evidenceGeneration = exact(evidenceGeneration, EVIDENCE_GENERATION, "evidenceGeneration");
        this.storeValueGeneration = exact(storeValueGeneration, STORE_VALUE_GENERATION, "storeValueGeneration");
        if (environmentResetGeneration == 0) {
            throw new IllegalArgumentException("environmentResetGeneration must be non-zero");
        }
        this.environmentResetGeneration = environmentResetGeneration;
        this.p1SourceLockDigest = fixed(p1SourceLockDigest, "p1SourceLockDigest");
        PulsarSourceLock.requireExact(this.p1SourceLockDigest);
        this.canonicalSchemaBundleHash = fixed(canonicalSchemaBundleHash, "canonicalSchemaBundleHash");
        this.setDigest = computeDigest();
        if (suppliedDigest != null) {
            Bytes.requireLength(suppliedDigest, HASH_LENGTH, "setDigest");
            if (!Bytes.constantTimeEquals(this.setDigest, suppliedDigest)) {
                throw new IllegalArgumentException("ArtifactGenerationSet digest mismatch");
            }
        }
    }

    /** Creates the exact current generation tuple with caller-supplied environment and source locks. */
    public static ArtifactGenerationSet current(
            final long environmentResetGeneration,
            final byte[] p1SourceLockDigest,
            final byte[] canonicalSchemaBundleHash) {
        return new ArtifactGenerationSet(
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 2),
                new ProtocolTuple(1, 1, ProtocolTuple.SYSTEM_MUTATION, 1, 2),
                DESTINATION_PROFILE_GENERATION,
                DESCRIPTOR_GENERATION,
                ADAPTER_ENCODING_GENERATION,
                NATIVE_PREPARED_GENERATION,
                ATTEMPT_JOURNAL_GENERATION,
                PULSAR_RECORD_GENERATION,
                HANDOFF_SNAPSHOT_GENERATION,
                EVIDENCE_GENERATION,
                STORE_VALUE_GENERATION,
                environmentResetGeneration,
                p1SourceLockDigest,
                canonicalSchemaBundleHash);
    }

    public int schemaGeneration() {
        return SCHEMA_GENERATION;
    }

    public ProtocolTuple clientCommandTuple() {
        return clientCommandTuple;
    }

    public ProtocolTuple systemMutationTuple() {
        return systemMutationTuple;
    }

    public int destinationProfileGeneration() {
        return destinationProfileGeneration;
    }

    public int descriptorGeneration() {
        return descriptorGeneration;
    }

    public int adapterEncodingGeneration() {
        return adapterEncodingGeneration;
    }

    public int nativePreparedGeneration() {
        return nativePreparedGeneration;
    }

    public int attemptJournalGeneration() {
        return attemptJournalGeneration;
    }

    public int pulsarRecordGeneration() {
        return pulsarRecordGeneration;
    }

    public int handoffSnapshotGeneration() {
        return handoffSnapshotGeneration;
    }

    public int evidenceGeneration() {
        return evidenceGeneration;
    }

    public int storeValueGeneration() {
        return storeValueGeneration;
    }

    public long environmentResetGeneration() {
        return environmentResetGeneration;
    }

    public byte[] p1SourceLockDigest() {
        return Bytes.copy(p1SourceLockDigest);
    }

    public byte[] canonicalSchemaBundleHash() {
        return Bytes.copy(canonicalSchemaBundleHash);
    }

    public byte[] setDigest() {
        return Bytes.copy(setDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsOneToFifteen(output);
            CanonicalProtobuf.bytes(output, 16, setDigest);
        });
    }

    public static ArtifactGenerationSet decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ArtifactGenerationSet");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, "ArtifactGenerationSet");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported ArtifactGenerationSet schema generation");
        }
        final ArtifactGenerationSet result = new ArtifactGenerationSet(
                ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint32(fields.get(3), 4),
                QueryCodecSupport.uint32(fields.get(4), 5),
                QueryCodecSupport.uint32(fields.get(5), 6),
                QueryCodecSupport.uint32(fields.get(6), 7),
                QueryCodecSupport.uint32(fields.get(7), 8),
                QueryCodecSupport.uint32(fields.get(8), 9),
                QueryCodecSupport.uint32(fields.get(9), 10),
                QueryCodecSupport.uint32(fields.get(10), 11),
                QueryCodecSupport.uint32(fields.get(11), 12),
                QueryCodecSupport.uint64Bits(fields.get(12), 13),
                QueryCodecSupport.fixed(fields.get(13), 14, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(14), 15, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(15), 16, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ArtifactGenerationSet");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ArtifactGenerationSet that
                && clientCommandTuple.equals(that.clientCommandTuple)
                && systemMutationTuple.equals(that.systemMutationTuple)
                && destinationProfileGeneration == that.destinationProfileGeneration
                && descriptorGeneration == that.descriptorGeneration
                && adapterEncodingGeneration == that.adapterEncodingGeneration
                && nativePreparedGeneration == that.nativePreparedGeneration
                && attemptJournalGeneration == that.attemptJournalGeneration
                && pulsarRecordGeneration == that.pulsarRecordGeneration
                && handoffSnapshotGeneration == that.handoffSnapshotGeneration
                && evidenceGeneration == that.evidenceGeneration
                && storeValueGeneration == that.storeValueGeneration
                && environmentResetGeneration == that.environmentResetGeneration
                && Arrays.equals(p1SourceLockDigest, that.p1SourceLockDigest)
                && Arrays.equals(canonicalSchemaBundleHash, that.canonicalSchemaBundleHash)
                && Arrays.equals(setDigest, that.setDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                clientCommandTuple,
                systemMutationTuple,
                destinationProfileGeneration,
                descriptorGeneration,
                adapterEncodingGeneration,
                nativePreparedGeneration,
                attemptJournalGeneration,
                pulsarRecordGeneration,
                handoffSnapshotGeneration,
                evidenceGeneration,
                storeValueGeneration,
                environmentResetGeneration,
                Arrays.hashCode(p1SourceLockDigest),
                Arrays.hashCode(canonicalSchemaBundleHash),
                Arrays.hashCode(setDigest));
    }

    private void writeFieldsOneToFifteen(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
        CanonicalProtobuf.bytes(output, 2, clientCommandTuple.canonicalBytes());
        CanonicalProtobuf.bytes(output, 3, systemMutationTuple.canonicalBytes());
        CanonicalProtobuf.uint32(output, 4, destinationProfileGeneration);
        CanonicalProtobuf.uint32(output, 5, descriptorGeneration);
        CanonicalProtobuf.uint32(output, 6, adapterEncodingGeneration);
        CanonicalProtobuf.uint32(output, 7, nativePreparedGeneration);
        CanonicalProtobuf.uint32(output, 8, attemptJournalGeneration);
        CanonicalProtobuf.uint32(output, 9, pulsarRecordGeneration);
        CanonicalProtobuf.uint32(output, 10, handoffSnapshotGeneration);
        CanonicalProtobuf.uint32(output, 11, evidenceGeneration);
        CanonicalProtobuf.uint32(output, 12, storeValueGeneration);
        CanonicalProtobuf.uint64Bits(output, 13, environmentResetGeneration);
        CanonicalProtobuf.bytes(output, 14, p1SourceLockDigest);
        CanonicalProtobuf.bytes(output, 15, canonicalSchemaBundleHash);
    }

    private byte[] computeDigest() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalFieldsOneToFifteen());
    }

    private byte[] canonicalFieldsOneToFifteen() {
        return CanonicalProtobuf.message(this::writeFieldsOneToFifteen);
    }

    private static ProtocolTuple requireTuple(final ProtocolTuple value, final int recordKind, final String name) {
        final ProtocolTuple result = Objects.requireNonNull(value, name);
        if (result.recordKind() != recordKind
                || result.framingVersion() != 1
                || result.logEnvelopeVersion() != 1
                || result.envelopeVersion() != 1) {
            throw new IllegalArgumentException(name + " does not use the active tuple shape");
        }
        return result;
    }

    private static int exact(final int value, final int expected, final String name) {
        if (value != expected) {
            throw new IllegalArgumentException(name + " must be the active generation " + expected);
        }
        return value;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
