package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Per-partition activation, quota and guarded-Broker evidence in a Route snapshot. */
public final class RoutePartitionPolicyV1 {
    private final int partition;
    private final ActivationBarrierV1 activationBarrier;
    private final QuotaGrantRefV1 quotaGrant;
    private final long brokerGuardAttestationGeneration;
    private final byte[] brokerGuardAttestationDigest;

    public RoutePartitionPolicyV1(final int partition, final ActivationBarrierV1 activationBarrier,
                                  final QuotaGrantRefV1 quotaGrant, final long brokerGuardAttestationGeneration,
                                  final byte[] brokerGuardAttestationDigest) {
        if (partition < 0) {
            throw new IllegalArgumentException("Route partition must be non-negative");
        }
        this.partition = partition;
        this.activationBarrier = Objects.requireNonNull(activationBarrier, "activationBarrier");
        this.quotaGrant = Objects.requireNonNull(quotaGrant, "quotaGrant");
        if (brokerGuardAttestationGeneration == 0) {
            throw new IllegalArgumentException("broker guard attestation generation must be nonzero");
        }
        this.brokerGuardAttestationGeneration = brokerGuardAttestationGeneration;
        Bytes.requireLength(brokerGuardAttestationDigest, 32, "brokerGuardAttestationDigest");
        if (allZero(brokerGuardAttestationDigest)) {
            throw new IllegalArgumentException("brokerGuardAttestationDigest must be non-zero");
        }
        this.brokerGuardAttestationDigest = Bytes.copy(brokerGuardAttestationDigest);
    }

    public int partition() {
        return partition;
    }

    public ActivationBarrierV1 activationBarrier() {
        return activationBarrier;
    }

    public QuotaGrantRefV1 quotaGrant() {
        return quotaGrant;
    }

    public long brokerGuardAttestationGeneration() {
        return brokerGuardAttestationGeneration;
    }

    public byte[] brokerGuardAttestationDigest() {
        return Bytes.copy(brokerGuardAttestationDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32Bits(output, 1, partition);
            CanonicalProtobuf.bytes(output, 2, activationBarrier.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, quotaGrant.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 4, brokerGuardAttestationGeneration);
            CanonicalProtobuf.bytes(output, 5, brokerGuardAttestationDigest);
        });
    }

    public static RoutePartitionPolicyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "RoutePartitionPolicyV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5}, "RoutePartitionPolicyV1");
        final RoutePartitionPolicyV1 result = new RoutePartitionPolicyV1(
                QueryCodecSupport.uint32Bits(fields.get(0), 1),
                ActivationBarrierV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                QueryCodecSupport.fixed(fields.get(4), 5, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RoutePartitionPolicyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RoutePartitionPolicyV1 that && partition == that.partition
                && activationBarrier.equals(that.activationBarrier) && quotaGrant.equals(that.quotaGrant)
                && brokerGuardAttestationGeneration == that.brokerGuardAttestationGeneration
                && Arrays.equals(brokerGuardAttestationDigest, that.brokerGuardAttestationDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, activationBarrier, quotaGrant, brokerGuardAttestationGeneration,
                Arrays.hashCode(brokerGuardAttestationDigest));
    }

    private static boolean allZero(final byte[] value) {
        for (byte item : value) {
            if (item != 0) {
                return false;
            }
        }
        return true;
    }
}
