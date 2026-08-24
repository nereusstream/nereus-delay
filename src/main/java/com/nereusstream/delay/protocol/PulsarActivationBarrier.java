package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
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
        int batchSize,
        long guardedSourceConnectionGeneration,
        byte[] resourceGuardAttestationDigest,
        boolean empty)
        implements SourceActivationBarrier {
    public PulsarActivationBarrier {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(brokerResourceIncarnation, 32, "brokerResourceIncarnation");
        Bytes.requireLength(resourceGuardAttestationDigest, 32, "resourceGuardAttestationDigest");
        physicalTopic = canonicalText(physicalTopic, "physicalTopic");
        if (physicalTopic.isBlank()
                || (empty && batchSize != 0)
                || (!empty && batchSize != 0 && Integer.compareUnsigned(normalizedLastBatchIndex, batchSize) >= 0)
                || guardedSourceConnectionGeneration == 0) {
            throw new IllegalArgumentException("invalid Pulsar activation barrier");
        }
        if (empty && (ledgerId != 0 || entryId != 0 || normalizedLastBatchIndex != 0 || batchSize != 0)) {
            throw new IllegalArgumentException("empty Pulsar barrier must use the sentinel cursor");
        }
        if (allZero(brokerResourceIncarnation) || allZero(resourceGuardAttestationDigest)) {
            throw new IllegalArgumentException("Pulsar barrier identities must be non-zero");
        }
        brokerResourceIncarnation = Bytes.copy(brokerResourceIncarnation);
        resourceGuardAttestationDigest = Bytes.copy(resourceGuardAttestationDigest);
    }

    public static PulsarActivationBarrier empty(
            final ShardId shardId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long connectionGeneration,
            final byte[] guardAttestationDigest) {
        return new PulsarActivationBarrier(
                shardId,
                resourceIncarnation,
                physicalTopic,
                0,
                0,
                0,
                0,
                connectionGeneration,
                guardAttestationDigest,
                true);
    }

    /**
     * Compatibility constructor for callers that predate the pinned batch
     * shape. A zero batch size means the same-entry shape cannot be fenced by
     * this legacy seam; V1 source adapters must provide the batch size.
     */
    @Deprecated
    public PulsarActivationBarrier(
            final ShardId shardId,
            final byte[] brokerResourceIncarnation,
            final String physicalTopic,
            final long ledgerId,
            final long entryId,
            final int normalizedLastBatchIndex,
            final long guardedSourceConnectionGeneration,
            final byte[] resourceGuardAttestationDigest,
            final boolean empty) {
        this(
                shardId,
                brokerResourceIncarnation,
                physicalTopic,
                ledgerId,
                entryId,
                normalizedLastBatchIndex,
                0,
                guardedSourceConnectionGeneration,
                resourceGuardAttestationDigest,
                empty);
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
                && batchSize == that.batchSize
                && guardedSourceConnectionGeneration == that.guardedSourceConnectionGeneration
                && Arrays.equals(resourceGuardAttestationDigest, that.resourceGuardAttestationDigest)
                && empty == that.empty;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                shardId,
                Arrays.hashCode(brokerResourceIncarnation),
                physicalTopic,
                ledgerId,
                entryId,
                normalizedLastBatchIndex,
                batchSize,
                guardedSourceConnectionGeneration,
                Arrays.hashCode(resourceGuardAttestationDigest),
                empty);
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
        if (!empty
                && batchSize != 0
                && pulsar.ledgerId() == ledgerId
                && pulsar.entryId() == entryId
                && pulsar.batchSize() != batchSize) {
            throw new IllegalArgumentException("Pulsar activation barrier batch shape mismatch");
        }
    }

    @Override
    public boolean reachedBy(final SourcePosition lastAppliedPosition) {
        if (empty) {
            // An empty barrier means that no replayed record is required; it
            // does not discard the physical source identity captured by the
            // assignment.  Validate a persisted cursor before accepting it,
            // otherwise a stale DB from another Pulsar resource could satisfy
            // the barrier without any catch-up record.
            if (lastAppliedPosition != null) {
                validatePosition(lastAppliedPosition);
            }
            return true;
        }
        if (lastAppliedPosition == null) {
            return false;
        }
        validatePosition(lastAppliedPosition);
        final PulsarSourcePosition pulsar = (PulsarSourcePosition) lastAppliedPosition;
        if (pulsar.ledgerId() != ledgerId) {
            return Long.compareUnsigned(pulsar.ledgerId(), ledgerId) > 0;
        }
        if (pulsar.entryId() != entryId) {
            return Long.compareUnsigned(pulsar.entryId(), entryId) > 0;
        }
        if (batchSize != 0 && pulsar.batchSize() != batchSize) {
            throw new IllegalArgumentException("Pulsar activation barrier batch shape mismatch");
        }
        return Integer.compareUnsigned(pulsar.normalizedBatchIndex(), normalizedLastBatchIndex) >= 0;
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value)
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be canonical UTF-8");
        }
        return value;
    }
}
