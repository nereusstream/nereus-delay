package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Validates the local binding between one prepared Control target and the
 * completed System Mutation that is about to be registered or enqueued.
 *
 * <p>This is deliberately not an Oxia or signature authority.  It prevents
 * a caller from pairing a target with a different body, shard, logical
 * identity, or computed mutation hash before those external authorities are
 * consulted.</p>
 */
public final class ControlTargetMutationBindingV1 {
    private static final int HASH_LENGTH = 32;

    private ControlTargetMutationBindingV1() {
    }

    /** Validates one mutation against the exact target in the prepared operation. */
    public static void validate(final PreparedControlOperationV1 prepared,
                                final ControlTargetRefV1 target,
                                final SystemMutation mutation) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(mutation, "mutation");
        final ControlTargetRefV1 preparedTarget = prepared.targets().stream()
                .filter(candidate -> candidate.targetIndex() == target.targetIndex())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("target is not in the prepared operation"));
        if (!preparedTarget.equals(target)) {
            throw new IllegalArgumentException("target bytes differ from the prepared target snapshot");
        }
        final byte[] expectedMutationId = target.expectedMutationId();
        final byte[] expectedMutationHash = target.expectedMutationHash();
        if (expectedMutationId == null || expectedMutationHash == null) {
            throw new IllegalArgumentException("source-ordered target has no expected mutation identity");
        }
        if (!Bytes.constantTimeEquals(expectedMutationId, mutation.systemMutationId())
                || !Bytes.constantTimeEquals(expectedMutationHash, mutation.mutationHash())) {
            throw new IllegalArgumentException("mutation identity does not match the prepared target");
        }

        final SystemMutationType expectedType = expectedMutationType(prepared.kind(), target.targetKind());
        if (mutation.type() != expectedType) {
            throw new IllegalArgumentException("mutation type does not match the Control target");
        }
        validateShardTarget(target, mutation);

        final ControlRef expectedControlRef = new ControlRef(prepared.operationId(), prepared.requestHash(),
                target.targetIndex());
        final byte[] expectedLogicalIdentity;
        switch (mutation.type()) {
            case APPLY_SHARD_CONTROL -> {
                final ApplyShardControlBody body = ApplyShardControlBody.decode(mutation.canonicalBody());
                requireControlRef(expectedControlRef, body.controlRef());
                validateApplyTarget(prepared.kind(), target, body);
                expectedLogicalIdentity = expectedControlRef.logicalOperationIdentity(body.controlKind());
            }
            case REPLAY_DEAD_LETTER -> {
                final ReplayDeadLetterBody body = ReplayDeadLetterBody.decode(mutation.canonicalBody());
                requireControlRef(expectedControlRef, body.controlRef());
                validateReplayTarget(prepared.request(), target, body);
                expectedLogicalIdentity = expectedControlRef.logicalOperationIdentity(mutation.type());
            }
            case RESOLVE_UNCERTAIN -> {
                final ResolveUncertainBody body = ResolveUncertainBody.decode(mutation.canonicalBody());
                requireControlRef(expectedControlRef, body.controlRef());
                validateResolveTarget(prepared.request(), target, body);
                expectedLogicalIdentity = expectedControlRef.logicalOperationIdentity(mutation.type());
            }
            default -> throw new IllegalArgumentException("unsupported Control target mutation type");
        }
        if (!Bytes.constantTimeEquals(expectedLogicalIdentity, mutation.logicalOperationIdentity())) {
            throw new IllegalArgumentException("mutation logical identity does not match the Control target");
        }
    }

    static SystemMutationType expectedMutationType(final ControlOperationKindV1 operationKind,
                                                    final ControlTargetKindV1 targetKind) {
        return switch (operationKind) {
            case REPLAY_DEAD_LETTER -> requireTargetKind(targetKind, ControlTargetKindV1.MESSAGE,
                    SystemMutationType.REPLAY_DEAD_LETTER);
            case RESOLVE_UNCERTAIN -> requireTargetKind(targetKind, ControlTargetKindV1.MESSAGE,
                    SystemMutationType.RESOLVE_UNCERTAIN);
            case STOP_NEW_SCHEDULES, PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE,
                    CLOSE_DESTINATION_LANE, BREAK_ORDERING_DOMAIN, PUBLISH_DESTINATION_PROFILE_VERSION,
                    DEPRECATE_DESTINATION_PROFILE_VERSION, PUBLISH_QUOTA_GRANT -> {
                if (targetKind != ControlTargetKindV1.LANE && targetKind != ControlTargetKindV1.SHARD) {
                    throw new IllegalArgumentException("Control target does not carry a source mutation");
                }
                yield SystemMutationType.APPLY_SHARD_CONTROL;
            }
            case DRAIN_SHARD, FENCE_SHARD_FOR_MAINTENANCE, FORCE_CHECKPOINT, GET_CHECKPOINT_CATALOG,
                    ROTATE_EQUIVALENT_SECRET_REFERENCE ->
                    throw new IllegalArgumentException("operation has no source-ordered mutation target");
        };
    }

    private static SystemMutationType requireTargetKind(final ControlTargetKindV1 actual,
                                                          final ControlTargetKindV1 expected,
                                                          final SystemMutationType type) {
        if (actual != expected) {
            throw new IllegalArgumentException("Control target kind does not match " + type);
        }
        return type;
    }

    private static void validateShardTarget(final ControlTargetRefV1 target, final SystemMutation mutation) {
        final ShardId expectedShard = switch (target.targetKind()) {
            case SHARD -> target.shard().shardId();
            case MESSAGE -> target.message().messageId().routingId().shardId();
            default -> null;
        };
        if (expectedShard != null && !expectedShard.equals(mutation.shardId())) {
            throw new IllegalArgumentException("mutation shard does not match the Control target");
        }
    }

    private static void validateApplyTarget(final ControlOperationKindV1 operationKind,
                                            final ControlTargetRefV1 target,
                                            final ApplyShardControlBody body) {
        final int expectedControlKind = switch (operationKind) {
            case STOP_NEW_SCHEDULES -> 4;
            case PAUSE_DESTINATION_LANE -> 8;
            case RESUME_DESTINATION_LANE -> 9;
            case BREAK_ORDERING_DOMAIN -> 10;
            case CLOSE_DESTINATION_LANE -> 11;
            case PUBLISH_DESTINATION_PROFILE_VERSION -> 2;
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> 3;
            case PUBLISH_QUOTA_GRANT -> -1;
            default -> throw new IllegalArgumentException("operation does not use ApplyShardControl");
        };
        if (expectedControlKind > 0 && body.controlKind() != expectedControlKind) {
            throw new IllegalArgumentException("ApplyShardControl kind does not match Control Operation");
        }
        if (target.targetKind() != ControlTargetKindV1.LANE) {
            return;
        }
        final ApplyShardControlBody.LaneTarget actual = body.laneTarget();
        final LaneControlTargetV1 expected = target.lane();
        if (!actual.laneId().equals(new DestinationLaneId(expected.laneId()))
                || !Arrays.equals(actual.laneIncarnation(), expected.laneIncarnation())
                || actual.expectedControlVersion() != expected.expectedLaneControlVersion()) {
            throw new IllegalArgumentException("ApplyShardControl lane target does not match prepared target");
        }
    }

    private static void validateReplayTarget(final ControlOperationRequestV1 request,
                                             final ControlTargetRefV1 target,
                                             final ReplayDeadLetterBody body) {
        final ReplayDeadLetterRequestV1 expected = branch(request, ReplayDeadLetterRequestV1.class);
        final ControlMessageTargetV1 message = target.message();
        if (!body.messageId().equals(message.messageId())
                || Integer.toUnsignedLong(body.expectedGeneration()) != message.expectedGeneration()
                || body.expectedStateVersion() != message.expectedStateVersion()
                || body.deliverAtEpochMs() != expected.deliverAt() || body.expireAtEpochMs() != expected.expireAt()
                || !Arrays.equals(body.retryPolicy(), expected.retryPolicy().canonicalBytes())
                || body.allowPossibleDuplicate() != expected.allowPossibleDuplicate()) {
            throw new IllegalArgumentException("Replay body does not match the prepared Message target/request");
        }
        final boolean acknowledgementPresent = body.acknowledgementHash().length == HASH_LENGTH;
        if (acknowledgementPresent != expected.allowPossibleDuplicate()) {
            throw new IllegalArgumentException("Replay acknowledgement presence does not match request");
        }
    }

    private static void validateResolveTarget(final ControlOperationRequestV1 request,
                                              final ControlTargetRefV1 target,
                                              final ResolveUncertainBody body) {
        final ResolveUncertainRequestV1 expected = branch(request, ResolveUncertainRequestV1.class);
        final ControlMessageTargetV1 message = target.message();
        if (!body.messageId().equals(message.messageId())
                || Integer.toUnsignedLong(body.generation()) != message.expectedGeneration()
                || !Arrays.equals(body.publishAttemptId(), message.publishAttemptId())
                || body.resolutionKind() != expected.resolutionKind().wireValue()
                || body.allowPossibleDuplicate() != expected.allowPossibleDuplicate()
                || body.allowPossibleDeliveryTerminal() != expected.allowPossibleDeliveryTerminal()) {
            throw new IllegalArgumentException("Resolve body does not match the prepared Message target/request");
        }
        final byte[] expectedEvidence = expected.evidence() == null ? new byte[0] : expected.evidence().canonicalBytes();
        if (!Arrays.equals(body.evidence(), expectedEvidence)) {
            throw new IllegalArgumentException("Resolve evidence does not match the prepared request");
        }
        final boolean acknowledgementPresent = body.acknowledgementHash().length == HASH_LENGTH;
        final boolean expectedAcknowledgement = expected.allowPossibleDuplicate()
                || expected.allowPossibleDeliveryTerminal();
        if (acknowledgementPresent != expectedAcknowledgement) {
            throw new IllegalArgumentException("Resolve acknowledgement presence does not match request");
        }
    }

    private static <T> T branch(final ControlOperationRequestV1 request, final Class<T> type) {
        if (!type.isInstance(request.branch())) {
            throw new IllegalArgumentException("request branch does not match Control Operation kind");
        }
        return type.cast(request.branch());
    }

    private static void requireControlRef(final ControlRef expected, final ControlRef actual) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException("System Mutation ControlRef does not match prepared target");
        }
    }
}
