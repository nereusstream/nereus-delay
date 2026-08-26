package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry PulsarJournalGenerationResource branch. */
public final class PulsarJournalGenerationResource {
    private final BrokerResourceIdentity journalResource;
    private final int partition;
    private final long evidenceGeneration;

    public PulsarJournalGenerationResource(
            final BrokerResourceIdentity journalResource, final int partition, final long evidenceGeneration) {
        this.journalResource = Objects.requireNonNull(journalResource, "journalResource");
        if (journalResource.kind() != BrokerResourceIdentity.Kind.PULSAR) {
            throw new IllegalArgumentException("Pulsar Journal resource requires a Pulsar identity");
        }
        this.partition = partition;
        if (evidenceGeneration == 0) {
            throw new IllegalArgumentException("evidenceGeneration must be non-zero");
        }
        this.evidenceGeneration = evidenceGeneration;
    }

    public BrokerResourceIdentity journalResource() {
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

    public static PulsarJournalGenerationResource decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PulsarJournalGenerationResource");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PulsarJournalGenerationResource");
        final PulsarJournalGenerationResource result = new PulsarJournalGenerationResource(
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                QueryCodecSupport.uint64Bits(fields.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PulsarJournalGenerationResource");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PulsarJournalGenerationResource that
                && journalResource.equals(that.journalResource)
                && partition == that.partition
                && evidenceGeneration == that.evidenceGeneration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(journalResource, partition, evidenceGeneration);
    }
}
