package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable command whose identity and hash are fixed before any I/O. */
public final class PreparedCommand {
    private final ShardId shardId;
    private final CommandId commandId;
    private final DelayMessageId delayMessageId;
    private final CommandType type;
    private final long retryUntilEpochMs;
    private final byte[] canonicalBody;
    private final byte[] commandHash;

    public PreparedCommand(final ShardId shardId, final CommandId commandId, final DelayMessageId delayMessageId,
                           final CommandType type, final long retryUntilEpochMs, final byte[] canonicalBody,
                           final byte[] commandHash) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.commandId = Objects.requireNonNull(commandId, "commandId");
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        this.type = Objects.requireNonNull(type, "type");
        if (!commandId.routingId().shardId().equals(shardId) || !delayMessageId.routingId().shardId().equals(shardId)) {
            throw new IllegalArgumentException("command identities do not belong to shard");
        }
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.canonicalBody = Bytes.copy(Objects.requireNonNull(canonicalBody, "canonicalBody"));
        Bytes.requireLength(commandHash, 32, "commandHash");
        final byte[] expected = CommandHash.compute(type, commandId, delayMessageId, retryUntilEpochMs, this.canonicalBody);
        if (!Bytes.constantTimeEquals(expected, commandHash)) {
            throw new IllegalArgumentException("command hash mismatch");
        }
        this.commandHash = Bytes.copy(commandHash);
    }

    public static PreparedCommand schedule(final ShardId shardId, final ScheduleIntent intent,
                                           final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(shardId, commandId, messageId, CommandType.SCHEDULE, retryUntilEpochMs,
                CommandBodies.schedule(intent));
    }

    public static PreparedCommand prepareLarge(final ShardId shardId, final LargeScheduleIntent intent,
                                               final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(shardId, commandId, messageId, CommandType.PREPARE_LARGE_SCHEDULE, retryUntilEpochMs,
                CommandBodies.prepareLarge(intent));
    }

    public static PreparedCommand commitLarge(final ShardId shardId, final DelayMessageId messageId,
                                              final PayloadCommitProof proof, final long retryUntilEpochMs) {
        if (!messageId.equals(proof.delayMessageId())) {
            throw new IllegalArgumentException("payload proof message identity mismatch");
        }
        return create(shardId, CommandId.random(shardId), messageId, CommandType.COMMIT_LARGE_SCHEDULE,
                retryUntilEpochMs, CommandBodies.commitLarge(proof));
    }

    public static PreparedCommand cancel(final ShardId shardId, final DelayMessageId messageId,
                                         final int expectedGeneration, final long retryUntilEpochMs) {
        return create(shardId, CommandId.random(shardId), messageId, CommandType.CANCEL, retryUntilEpochMs,
                CommandBodies.cancel(expectedGeneration));
    }

    public static PreparedCommand reschedule(final ShardId shardId, final DelayMessageId messageId,
                                             final int expectedGeneration, final long deliverAt,
                                             final long expireAt, final long retryUntilEpochMs) {
        return create(shardId, CommandId.random(shardId), messageId, CommandType.RESCHEDULE, retryUntilEpochMs,
                CommandBodies.reschedule(expectedGeneration, deliverAt, expireAt));
    }

    public static PreparedCommand create(final ShardId shardId, final CommandId commandId,
                                         final DelayMessageId messageId, final CommandType type,
                                         final long retryUntilEpochMs, final byte[] body) {
        final byte[] hash = CommandHash.compute(type, commandId, messageId, retryUntilEpochMs, body);
        return new PreparedCommand(shardId, commandId, messageId, type, retryUntilEpochMs, body, hash);
    }

    public ShardId shardId() {
        return shardId;
    }

    public CommandId commandId() {
        return commandId;
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public CommandType type() {
        return type;
    }

    public long retryUntilEpochMs() {
        return retryUntilEpochMs;
    }

    public byte[] canonicalBody() {
        return Bytes.copy(canonicalBody);
    }

    public byte[] commandHash() {
        return Bytes.copy(commandHash);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PreparedCommand that)) {
            return false;
        }
        return shardId.equals(that.shardId) && commandId.equals(that.commandId)
                && delayMessageId.equals(that.delayMessageId) && type == that.type
                && retryUntilEpochMs == that.retryUntilEpochMs
                && Arrays.equals(canonicalBody, that.canonicalBody)
                && Arrays.equals(commandHash, that.commandHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, commandId, delayMessageId, type, retryUntilEpochMs,
                Arrays.hashCode(canonicalBody), Arrays.hashCode(commandHash));
    }
}
