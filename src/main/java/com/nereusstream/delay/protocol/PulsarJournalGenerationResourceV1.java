package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry PulsarJournalGenerationResourceV1 branch. */
public final class PulsarJournalGenerationResourceV1 {
    private final BrokerResourceIdentityV1 journalResource;
    private final int partition;
    private final long evidenceGeneration;

    public PulsarJournalGenerationResourceV1(
            final BrokerResourceIdentityV1 journalResource, final int partition, final long evidenceGeneration) {
        this.journalResource = Objects.requireNonNull(journalResource, "journalResource");
        if (journalResource.kind() != BrokerResourceIdentityV1.Kind.PULSAR) {
            throw new IllegalArgumentException("Pulsar Journal resource requires a Pulsar identity");
        }
        this.partition = partition;
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        this.evidenceGeneration = evidenceGeneration;
    }

    public BrokerResourceIdentityV1 journalResource() {
        return journalResource;
    }

    public int partition() {
        return partition;
    }

    public long evidenceGeneration() {
        return evidenceGeneration;
    }

    /** Returns the direct branch bytes; ExactResourceIdentity wraps these under field 5. */
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, journalResource.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 2, partition);
            CanonicalProtobuf.uint64Bits(output, 3, evidenceGeneration);
        });
    }

    /** Returns the full ExactResourceIdentity wrapper for this branch. */
    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, ResourceKind.PULSAR_JOURNAL_GENERATION.wireValue(), canonicalBytes()));
    }

    public static PulsarJournalGenerationResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarJournalGenerationResourceV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PulsarJournalGenerationResourceV1");
        final PulsarJournalGenerationResourceV1 result = new PulsarJournalGenerationResourceV1(
                BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                QueryCodecSupport.uint64Bits(fields.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarJournalGenerationResourceV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarJournalGenerationResourceV1 that
                && journalResource.equals(that.journalResource)
                && partition == that.partition
                && evidenceGeneration == that.evidenceGeneration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(journalResource, partition, evidenceGeneration);
    }
}
