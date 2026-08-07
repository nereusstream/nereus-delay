package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical source activation barrier used by a ReadyCertificateV1. */
public final class ActivationBarrierV1 {
    public enum Kind {
        EMPTY,
        KAFKA,
        PULSAR
    }

    private final Kind kind;
    private final BrokerResourceIdentityV1 resource;
    private final int partition;
    private final Long guardedSourceConnectionGeneration;
    private final byte[] resourceGuardAttestationDigest;
    private final long nextOffsetExclusive;
    private final long observedLsoExclusive;
    private final long ledgerId;
    private final long entryId;
    private final int normalizedBatchIndex;
    private final int batchSize;

    private ActivationBarrierV1(final Kind kind, final BrokerResourceIdentityV1 resource, final int partition,
                                final Long guardedSourceConnectionGeneration,
                                final byte[] resourceGuardAttestationDigest, final long nextOffsetExclusive,
                                final long observedLsoExclusive, final long ledgerId, final long entryId,
                                final int normalizedBatchIndex, final int batchSize) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.resource = Objects.requireNonNull(resource, "resource");
        this.partition = partition;
        this.guardedSourceConnectionGeneration = guardedSourceConnectionGeneration;
        if (guardedSourceConnectionGeneration != null && guardedSourceConnectionGeneration <= 0) {
            throw new IllegalArgumentException("guarded source connection generation must be positive");
        }
        this.resourceGuardAttestationDigest = resourceGuardAttestationDigest == null ? null
                : fixed(resourceGuardAttestationDigest, 32, "resourceGuardAttestationDigest");
        if ((guardedSourceConnectionGeneration == null) != (this.resourceGuardAttestationDigest == null)) {
            throw new IllegalArgumentException("source connection generation and guard digest must be paired");
        }
        this.nextOffsetExclusive = nextOffsetExclusive;
        this.observedLsoExclusive = observedLsoExclusive;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        if (kind != Kind.PULSAR && (normalizedBatchIndex != 0 || batchSize != 0)
                || (kind == Kind.PULSAR && (batchSize == 0
                || Integer.compareUnsigned(normalizedBatchIndex, batchSize) >= 0))) {
            throw new IllegalArgumentException("invalid Pulsar batch cursor");
        }
        this.normalizedBatchIndex = normalizedBatchIndex;
        this.batchSize = batchSize;
        if (kind == Kind.KAFKA && resource.kind() != BrokerResourceIdentityV1.Kind.KAFKA) {
            throw new IllegalArgumentException("Kafka barrier requires Kafka resource identity");
        }
        if (kind == Kind.PULSAR && resource.kind() != BrokerResourceIdentityV1.Kind.PULSAR) {
            throw new IllegalArgumentException("Pulsar barrier requires Pulsar resource identity");
        }
        if (kind == Kind.EMPTY && resource.kind() == BrokerResourceIdentityV1.Kind.KAFKA
                && (guardedSourceConnectionGeneration != null || this.resourceGuardAttestationDigest != null)) {
            throw new IllegalArgumentException("Kafka empty barrier cannot carry connection guard fields");
        }
        if (kind == Kind.EMPTY && resource.kind() == BrokerResourceIdentityV1.Kind.PULSAR
                && (guardedSourceConnectionGeneration == null
                || this.resourceGuardAttestationDigest == null)) {
            throw new IllegalArgumentException("Pulsar empty barrier requires connection guard fields");
        }
        if (kind == Kind.PULSAR && (guardedSourceConnectionGeneration == null
                || this.resourceGuardAttestationDigest == null)) {
            throw new IllegalArgumentException("Pulsar barrier requires connection guard fields");
        }
    }

    public static ActivationBarrierV1 empty(final BrokerResourceIdentityV1 resource, final int partition,
                                            final Long guardedSourceConnectionGeneration,
                                            final byte[] resourceGuardAttestationDigest) {
        return new ActivationBarrierV1(Kind.EMPTY, resource, partition, guardedSourceConnectionGeneration,
                resourceGuardAttestationDigest, 0, 0, 0, 0, 0, 0);
    }

    public static ActivationBarrierV1 kafka(final BrokerResourceIdentityV1 resource, final int partition,
                                            final long nextOffsetExclusive, final long observedLsoExclusive) {
        return new ActivationBarrierV1(Kind.KAFKA, resource, partition, null, null, nextOffsetExclusive,
                observedLsoExclusive, 0, 0, 0, 0);
    }

    public static ActivationBarrierV1 pulsar(final BrokerResourceIdentityV1 resource, final int partition,
                                             final long ledgerId, final long entryId, final int normalizedBatchIndex,
                                             final int batchSize, final long guardedSourceConnectionGeneration,
                                             final byte[] resourceGuardAttestationDigest) {
        return new ActivationBarrierV1(Kind.PULSAR, resource, partition, guardedSourceConnectionGeneration,
                resourceGuardAttestationDigest, 0, 0, ledgerId, entryId, normalizedBatchIndex, batchSize);
    }

    public Kind kind() {
        return kind;
    }

    public BrokerResourceIdentityV1 resource() {
        return resource;
    }

    public int partition() {
        return partition;
    }

    public Long guardedSourceConnectionGeneration() {
        return guardedSourceConnectionGeneration;
    }

    public byte[] resourceGuardAttestationDigest() {
        return resourceGuardAttestationDigest == null ? null : Bytes.copy(resourceGuardAttestationDigest);
    }

    public long nextOffsetExclusive() {
        return nextOffsetExclusive;
    }

    public long observedLsoExclusive() {
        return observedLsoExclusive;
    }

    public long ledgerId() {
        return ledgerId;
    }

    public long entryId() {
        return entryId;
    }

    public int normalizedBatchIndex() {
        return normalizedBatchIndex;
    }

    public int batchSize() {
        return batchSize;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            switch (kind) {
                case EMPTY -> CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, resource.canonicalBytes());
                    CanonicalProtobuf.uint32Bits(fields, 2, partition);
                    if (guardedSourceConnectionGeneration != null) {
                        CanonicalProtobuf.uint64(fields, 3, guardedSourceConnectionGeneration);
                        CanonicalProtobuf.bytes(fields, 4, resourceGuardAttestationDigest);
                    }
                }));
                case KAFKA -> CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, resource.canonicalBytes());
                    CanonicalProtobuf.uint32Bits(fields, 2, partition);
                    CanonicalProtobuf.uint64Bits(fields, 3, nextOffsetExclusive);
                    CanonicalProtobuf.uint64Bits(fields, 4, observedLsoExclusive);
                }));
                case PULSAR -> CanonicalProtobuf.bytes(output, 3, CanonicalProtobuf.message(fields -> {
                    CanonicalProtobuf.bytes(fields, 1, resource.canonicalBytes());
                    CanonicalProtobuf.uint32Bits(fields, 2, partition);
                    CanonicalProtobuf.uint64Bits(fields, 3, ledgerId);
                    CanonicalProtobuf.uint64Bits(fields, 4, entryId);
                    CanonicalProtobuf.uint32Bits(fields, 5, normalizedBatchIndex);
                    CanonicalProtobuf.uint32Bits(fields, 6, batchSize);
                    CanonicalProtobuf.uint64(fields, 7, guardedSourceConnectionGeneration);
                    CanonicalProtobuf.bytes(fields, 8, resourceGuardAttestationDigest);
                }));
            }
        });
    }

    public static ActivationBarrierV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = QueryCodecSupport.read(encoded, "ActivationBarrierV1");
        if (outer.size() != 1) {
            throw new IllegalArgumentException("ActivationBarrierV1 must select one branch");
        }
        final CanonicalProtobuf.Reader.Field branch = outer.get(0);
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(
                QueryCodecSupport.nested(branch, branch.number()), "ActivationBarrier branch");
        final ActivationBarrierV1 result;
        switch (branch.number()) {
            case 1 -> {
                if (fields.size() != 2 && fields.size() != 4) {
                    throw new IllegalArgumentException("invalid empty ActivationBarrier fields");
                }
                final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.decode(
                        QueryCodecSupport.nested(fields.get(0), 1));
                final Long generation = fields.size() == 4 ? QueryCodecSupport.uint(fields.get(2), 3) : null;
                final byte[] digest = fields.size() == 4 ? QueryCodecSupport.fixed(fields.get(3), 4, 32) : null;
                if (fields.size() == 4 && fields.get(1).number() != 2) {
                    throw new IllegalArgumentException("invalid empty ActivationBarrier field order");
                }
                result = empty(resource, QueryCodecSupport.uint32Bits(fields.get(1), 2), generation, digest);
            }
            case 2 -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "KafkaActivationBarrier");
                result = kafka(BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                        QueryCodecSupport.uint32Bits(fields.get(1), 2), QueryCodecSupport.uint(fields.get(2), 3),
                        QueryCodecSupport.uint(fields.get(3), 4));
            }
            case 3 -> {
                QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7, 8},
                        "PulsarActivationBarrier");
                result = pulsar(BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                        QueryCodecSupport.uint32Bits(fields.get(1), 2), QueryCodecSupport.uint(fields.get(2), 3),
                        QueryCodecSupport.uint(fields.get(3), 4), QueryCodecSupport.uint32Bits(fields.get(4), 5),
                        QueryCodecSupport.uint32Bits(fields.get(5), 6), QueryCodecSupport.uint(fields.get(6), 7),
                        QueryCodecSupport.fixed(fields.get(7), 8, 32));
            }
            default -> throw new IllegalArgumentException("unknown ActivationBarrierV1 branch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ActivationBarrierV1");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        if ("resourceGuardAttestationDigest".equals(name) && allZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ActivationBarrierV1 that && kind == that.kind && partition == that.partition
                && Objects.equals(resource, that.resource)
                && Objects.equals(guardedSourceConnectionGeneration, that.guardedSourceConnectionGeneration)
                && Arrays.equals(resourceGuardAttestationDigest, that.resourceGuardAttestationDigest)
                && nextOffsetExclusive == that.nextOffsetExclusive && observedLsoExclusive == that.observedLsoExclusive
                && ledgerId == that.ledgerId && entryId == that.entryId
                && normalizedBatchIndex == that.normalizedBatchIndex && batchSize == that.batchSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, resource, partition, guardedSourceConnectionGeneration,
                Arrays.hashCode(resourceGuardAttestationDigest), nextOffsetExclusive, observedLsoExclusive,
                ledgerId, entryId, normalizedBatchIndex, batchSize);
    }
}
