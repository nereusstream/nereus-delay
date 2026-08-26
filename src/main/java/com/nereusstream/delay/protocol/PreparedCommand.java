package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Immutable command whose identity and hash are fixed before any I/O. */
public final class PreparedCommand {
    private final ShardId shardId;
    private final CommandId commandId;
    private final DelayMessageId delayMessageId;
    private final CommandType type;
    private final ProtocolTuple protocolTuple;
    private final long retryUntilEpochMs;
    private final byte[] canonicalBody;
    private final byte[] commandHash;

    public PreparedCommand(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId delayMessageId,
            final CommandType type,
            final long retryUntilEpochMs,
            final byte[] canonicalBody,
            final byte[] commandHash) {
        this(
                shardId,
                commandId,
                delayMessageId,
                type,
                ProtocolTuple.managedCommand(),
                retryUntilEpochMs,
                canonicalBody,
                commandHash);
    }

    public PreparedCommand(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId delayMessageId,
            final CommandType type,
            final ProtocolTuple protocolTuple,
            final long retryUntilEpochMs,
            final byte[] canonicalBody,
            final byte[] commandHash) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.commandId = Objects.requireNonNull(commandId, "commandId");
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        this.type = Objects.requireNonNull(type, "type");
        this.protocolTuple = Objects.requireNonNull(protocolTuple, "protocolTuple");
        if (protocolTuple.recordKind() != ProtocolTuple.CLIENT_COMMAND) {
            throw new IllegalArgumentException("PreparedCommand requires a Client Command protocol tuple");
        }
        if (!commandId.routingId().shardId().equals(shardId)
                || !delayMessageId.routingId().shardId().equals(shardId)) {
            throw new IllegalArgumentException("command identities do not belong to shard");
        }
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntil must be non-negative");
        }
        this.retryUntilEpochMs = retryUntilEpochMs;
        this.canonicalBody = Bytes.copy(Objects.requireNonNull(canonicalBody, "canonicalBody"));
        Bytes.requireLength(commandHash, 32, "commandHash");
        final byte[] expected = CommandHash.compute(
                protocolTuple, type, commandId, delayMessageId, retryUntilEpochMs, this.canonicalBody);
        if (!Bytes.constantTimeEquals(expected, commandHash)) {
            throw new IllegalArgumentException("command hash mismatch");
        }
        this.commandHash = Bytes.copy(commandHash);
    }

    /** Creates a command using the Registry-shaped Schedule body seam. */
    public static PreparedCommand schedule(
            final ShardId shardId, final ScheduleIntent intent, final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(
                shardId, commandId, messageId, CommandType.SCHEDULE, retryUntilEpochMs, CommandBodies.schedule(intent));
    }

    /** Creates a command using the Registry-shaped Schedule body seam. */
    public static PreparedCommand schedule(
            final ShardId shardId, final CanonicalScheduleIntent intent, final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(
                shardId,
                commandId,
                messageId,
                CommandType.SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.schedule(messageId, retryUntilEpochMs, intent));
    }

    /** Creates a schedule with identities selected by the Semantic Core. */
    public static PreparedCommand schedule(
            final ShardId shardId,
            final UUID logicalMessageUuidV7,
            final UUID logicalCommandUuidV7,
            final CanonicalScheduleIntent intent,
            final long retryUntilEpochMs) {
        final DelayMessageId messageId = new DelayMessageId(
                SelfRoutingId.fromLogicalUuid(shardId, logicalMessageUuidV7).bytes());
        final CommandId commandId = new CommandId(
                SelfRoutingId.fromLogicalUuid(shardId, logicalCommandUuidV7).bytes());
        return create(
                shardId,
                commandId,
                messageId,
                CommandType.SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.schedule(messageId, retryUntilEpochMs, intent));
    }

    /** Creates a command using the Registry-shaped PrepareLargeSchedule body seam. */
    public static PreparedCommand prepareLarge(
            final ShardId shardId, final LargeScheduleIntent intent, final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(
                shardId,
                commandId,
                messageId,
                CommandType.PREPARE_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.prepareLarge(intent));
    }

    /** Creates a command using the Registry-shaped PrepareLargeSchedule body seam. */
    public static PreparedCommand prepareLarge(
            final ShardId shardId,
            final CanonicalScheduleIntent intentWithoutPayload,
            final long expectedPayloadLength,
            final byte[] payloadSha256,
            final long reservationTtlMs,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile,
            final long retryUntilEpochMs) {
        final CommandId commandId = CommandId.random(shardId);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        return create(
                shardId,
                commandId,
                messageId,
                CommandType.PREPARE_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.prepareLarge(
                        messageId,
                        retryUntilEpochMs,
                        intentWithoutPayload,
                        expectedPayloadLength,
                        payloadSha256,
                        reservationTtlMs,
                        trustSet,
                        objectStoreProfile));
    }

    /** Creates a large-payload preparation with identities selected by the Semantic Core. */
    public static PreparedCommand prepareLarge(
            final ShardId shardId,
            final UUID logicalMessageUuidV7,
            final UUID logicalCommandUuidV7,
            final CanonicalScheduleIntent intentWithoutPayload,
            final long expectedPayloadLength,
            final byte[] payloadSha256,
            final long reservationTtlMs,
            final PayloadProofTrustSetRef trustSet,
            final ProfileRef objectStoreProfile,
            final long retryUntilEpochMs) {
        final DelayMessageId messageId = new DelayMessageId(
                SelfRoutingId.fromLogicalUuid(shardId, logicalMessageUuidV7).bytes());
        final CommandId commandId = new CommandId(
                SelfRoutingId.fromLogicalUuid(shardId, logicalCommandUuidV7).bytes());
        return create(
                shardId,
                commandId,
                messageId,
                CommandType.PREPARE_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.prepareLarge(
                        messageId,
                        retryUntilEpochMs,
                        intentWithoutPayload,
                        expectedPayloadLength,
                        payloadSha256,
                        reservationTtlMs,
                        trustSet,
                        objectStoreProfile));
    }

    /** Creates a command using the Registry-shaped CommitLargeSchedule body seam. */
    public static PreparedCommand commitLarge(
            final ShardId shardId,
            final DelayMessageId messageId,
            final PayloadCommitProof proof,
            final long retryUntilEpochMs) {
        if (!messageId.equals(proof.delayMessageId())) {
            throw new IllegalArgumentException("payload proof message identity mismatch");
        }
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.COMMIT_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.commitLarge(proof));
    }

    /** Creates a command using the Registry-shaped CommitLargeSchedule body seam. */
    public static PreparedCommand commitLarge(
            final ShardId shardId,
            final DelayMessageId messageId,
            final byte[] reservationId,
            final CanonicalPayloadCommitProof proof,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.COMMIT_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.commitLarge(messageId, retryUntilEpochMs, reservationId, proof));
    }

    /** Creates a payload commit with a Semantic-Core-selected Command ID. */
    public static PreparedCommand commitLarge(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId messageId,
            final byte[] reservationId,
            final CanonicalPayloadCommitProof proof,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                Objects.requireNonNull(commandId, "commandId"),
                messageId,
                CommandType.COMMIT_LARGE_SCHEDULE,
                retryUntilEpochMs,
                CommandBodies.commitLarge(messageId, retryUntilEpochMs, reservationId, proof));
    }

    /** Creates a command using the Registry-shaped Cancel body seam. */
    public static PreparedCommand cancel(
            final ShardId shardId,
            final DelayMessageId messageId,
            final int expectedGeneration,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.CANCEL,
                retryUntilEpochMs,
                CommandBodies.cancel(expectedGeneration));
    }

    /** Creates a command using the Registry-shaped Cancel body seam. */
    public static PreparedCommand cancel(
            final ShardId shardId,
            final DelayMessageId messageId,
            final MessagePrecondition precondition,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.CANCEL,
                retryUntilEpochMs,
                CommandBodies.cancel(messageId, retryUntilEpochMs, precondition));
    }

    /** Creates a cancel with a Semantic-Core-selected Command ID. */
    public static PreparedCommand cancel(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId messageId,
            final MessagePrecondition precondition,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                Objects.requireNonNull(commandId, "commandId"),
                messageId,
                CommandType.CANCEL,
                retryUntilEpochMs,
                CommandBodies.cancel(messageId, retryUntilEpochMs, precondition));
    }

    /** Creates a command using the Registry-shaped Reschedule body seam. */
    public static PreparedCommand reschedule(
            final ShardId shardId,
            final DelayMessageId messageId,
            final int expectedGeneration,
            final long deliverAt,
            final long expireAt,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.RESCHEDULE,
                retryUntilEpochMs,
                CommandBodies.reschedule(expectedGeneration, deliverAt, expireAt));
    }

    /** Creates a command using the Registry-shaped Reschedule body seam. */
    public static PreparedCommand reschedule(
            final ShardId shardId,
            final DelayMessageId messageId,
            final MessagePrecondition precondition,
            final long deliverAt,
            final long expireAt,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                CommandId.random(shardId),
                messageId,
                CommandType.RESCHEDULE,
                retryUntilEpochMs,
                CommandBodies.reschedule(messageId, retryUntilEpochMs, precondition, deliverAt, expireAt));
    }

    /** Creates a reschedule with a Semantic-Core-selected Command ID. */
    public static PreparedCommand reschedule(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId messageId,
            final MessagePrecondition precondition,
            final long deliverAt,
            final long expireAt,
            final long retryUntilEpochMs) {
        return create(
                shardId,
                Objects.requireNonNull(commandId, "commandId"),
                messageId,
                CommandType.RESCHEDULE,
                retryUntilEpochMs,
                CommandBodies.reschedule(messageId, retryUntilEpochMs, precondition, deliverAt, expireAt));
    }

    public static PreparedCommand create(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId messageId,
            final CommandType type,
            final long retryUntilEpochMs,
            final byte[] body) {
        return create(shardId, commandId, messageId, type, ProtocolTuple.managedCommand(), retryUntilEpochMs, body);
    }

    /** Creates a command under an explicitly selected Client Command tuple. */
    public static PreparedCommand create(
            final ShardId shardId,
            final CommandId commandId,
            final DelayMessageId messageId,
            final CommandType type,
            final ProtocolTuple protocolTuple,
            final long retryUntilEpochMs,
            final byte[] body) {
        final byte[] hash = CommandHash.compute(protocolTuple, type, commandId, messageId, retryUntilEpochMs, body);
        return new PreparedCommand(shardId, commandId, messageId, type, protocolTuple, retryUntilEpochMs, body, hash);
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

    public ProtocolTuple protocolTuple() {
        return protocolTuple;
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
        return shardId.equals(that.shardId)
                && commandId.equals(that.commandId)
                && delayMessageId.equals(that.delayMessageId)
                && type == that.type
                && protocolTuple.equals(that.protocolTuple)
                && retryUntilEpochMs == that.retryUntilEpochMs
                && Arrays.equals(canonicalBody, that.canonicalBody)
                && Arrays.equals(commandHash, that.commandHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                shardId,
                commandId,
                delayMessageId,
                type,
                protocolTuple,
                retryUntilEpochMs,
                Arrays.hashCode(canonicalBody),
                Arrays.hashCode(commandHash));
    }
}
