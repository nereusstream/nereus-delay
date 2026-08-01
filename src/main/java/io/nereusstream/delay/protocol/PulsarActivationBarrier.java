package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Pulsar inclusive last-message barrier, including the final batch member. */
public record PulsarActivationBarrier(
        ShardId shardId,
        byte[] brokerResourceIncarnation,
        String physicalTopic,
        long ledgerId,
        long entryId,
        int normalizedLastBatchIndex,
        long guardedSourceConnectionGeneration,
        byte[] resourceGuardAttestationDigest,
        boolean empty) implements SourceActivationBarrier {
    public PulsarActivationBarrier {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(brokerResourceIncarnation, 32, "brokerResourceIncarnation");
        Bytes.requireLength(resourceGuardAttestationDigest, 32, "resourceGuardAttestationDigest");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (physicalTopic.isBlank() || ledgerId < 0 || entryId < 0 || normalizedLastBatchIndex < 0
                || guardedSourceConnectionGeneration <= 0) {
            throw new IllegalArgumentException("invalid Pulsar activation barrier");
        }
        if (empty && (ledgerId != 0 || entryId != 0 || normalizedLastBatchIndex != 0)) {
            throw new IllegalArgumentException("empty Pulsar barrier must use the sentinel cursor");
        }
        if (allZero(brokerResourceIncarnation) || allZero(resourceGuardAttestationDigest)) {
            throw new IllegalArgumentException("Pulsar barrier identities must be non-zero");
        }
        brokerResourceIncarnation = Bytes.copy(brokerResourceIncarnation);
        resourceGuardAttestationDigest = Bytes.copy(resourceGuardAttestationDigest);
    }

    public static PulsarActivationBarrier empty(final ShardId shardId, final byte[] resourceIncarnation,
                                                final String physicalTopic, final long connectionGeneration,
                                                final byte[] guardAttestationDigest) {
        return new PulsarActivationBarrier(shardId, resourceIncarnation, physicalTopic, 0, 0, 0,
                connectionGeneration, guardAttestationDigest, true);
    }

    @Override
    public SourcePositionKind kind() {
        return SourcePositionKind.PULSAR;
    }

    @Override
    public byte[] brokerResourceIncarnation() {
        return Bytes.copy(brokerResourceIncarnation);
    }

    @Override
    public byte[] resourceGuardAttestationDigest() {
        return Bytes.copy(resourceGuardAttestationDigest);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PulsarActivationBarrier that)) {
            return false;
        }
        return shardId.equals(that.shardId)
                && Arrays.equals(brokerResourceIncarnation, that.brokerResourceIncarnation)
                && physicalTopic.equals(that.physicalTopic)
                && ledgerId == that.ledgerId
                && entryId == that.entryId
                && normalizedLastBatchIndex == that.normalizedLastBatchIndex
                && guardedSourceConnectionGeneration == that.guardedSourceConnectionGeneration
                && Arrays.equals(resourceGuardAttestationDigest, that.resourceGuardAttestationDigest)
                && empty == that.empty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, Arrays.hashCode(brokerResourceIncarnation), physicalTopic, ledgerId, entryId,
                normalizedLastBatchIndex, guardedSourceConnectionGeneration,
                Arrays.hashCode(resourceGuardAttestationDigest), empty);
    }

    /** Exact source connection evidence captured with the guarded barrier. */
    public void validateSourceConnection(final long connectionGeneration, final byte[] attestationDigest) {
        if (connectionGeneration != guardedSourceConnectionGeneration
                || !Bytes.constantTimeEquals(resourceGuardAttestationDigest, attestationDigest)) {
            throw new IllegalArgumentException("Pulsar source connection generation/guard evidence mismatch");
        }
    }

    @Override
    public void validatePosition(final SourcePosition position) {
        if (!(position instanceof PulsarSourcePosition pulsar)
                || !shardId.equals(pulsar.shardId())
                || !Arrays.equals(brokerResourceIncarnation, pulsar.brokerResourceIncarnation())
                || !physicalTopic.equals(pulsar.physicalTopic())) {
            throw new IllegalArgumentException("Pulsar activation barrier source identity mismatch");
        }
    }

    @Override
    public boolean reachedBy(final SourcePosition lastAppliedPosition) {
        if (empty) {
            return true;
        }
        if (lastAppliedPosition == null) {
            return false;
        }
        validatePosition(lastAppliedPosition);
        final PulsarSourcePosition pulsar = (PulsarSourcePosition) lastAppliedPosition;
        if (pulsar.ledgerId() != ledgerId) {
            return pulsar.ledgerId() > ledgerId;
        }
        if (pulsar.entryId() != entryId) {
            return pulsar.entryId() > entryId;
        }
        return pulsar.normalizedBatchIndex() >= normalizedLastBatchIndex;
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }
}
