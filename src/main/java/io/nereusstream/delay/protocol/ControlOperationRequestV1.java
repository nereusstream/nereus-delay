package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/**
 * Registry §6.3 closed request union.  The outer protobuf field number is the
 * {@link ControlOperationKindV1} wire value, so a branch cannot be relabeled
 * without failing canonical decode.
 */
public final class ControlOperationRequestV1 {
    private final ControlOperationKindV1 kind;
    private final ControlOperationRequestBranchV1 branch;

    public ControlOperationRequestV1(final ControlOperationKindV1 kind,
                                     final ControlOperationRequestBranchV1 branch) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.branch = Objects.requireNonNull(branch, "branch");
        validateBranch(kind, branch);
    }

    public ControlOperationKindV1 kind() {
        return kind;
    }

    public ControlOperationRequestBranchV1 branch() {
        return branch;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, kind.wireValue(), branch.canonicalBytes()));
    }

    public static ControlOperationRequestV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ControlOperationRequestV1");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("ControlOperationRequestV1 must select one branch");
        }
        final CanonicalProtobuf.Reader.Field field = fields.get(0);
        if (field.wireType() != 2) {
            throw new IllegalArgumentException("ControlOperationRequestV1 branch must be bytes");
        }
        final ControlOperationKindV1 kind = ControlOperationKindV1.fromWire(field.number());
        final ControlOperationRequestV1 result = new ControlOperationRequestV1(kind,
                decodeBranch(kind, QueryCodecSupport.nested(field, field.number())));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlOperationRequestV1");
        return result;
    }

    public static ControlOperationRequestV1 stopNewSchedules(final ControlReasonV1 reason) {
        return new ControlOperationRequestV1(ControlOperationKindV1.STOP_NEW_SCHEDULES,
                new StopNewSchedulesRequestV1(reason));
    }

    public static ControlOperationRequestV1 pauseDestinationLane(final ControlReasonV1 reason) {
        return laneGate(ControlOperationKindV1.PAUSE_DESTINATION_LANE, new LaneGateRequestV1(reason));
    }

    public static ControlOperationRequestV1 resumeDestinationLane(final ControlReasonV1 reason) {
        return laneGate(ControlOperationKindV1.RESUME_DESTINATION_LANE, new LaneGateRequestV1(reason));
    }

    public static ControlOperationRequestV1 laneGate(final ControlOperationKindV1 kind,
                                                     final LaneGateRequestV1 branch) {
        return new ControlOperationRequestV1(kind, branch);
    }

    public static ControlOperationRequestV1 closeDestinationLane(final CloseLaneRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.CLOSE_DESTINATION_LANE, branch);
    }

    public static ControlOperationRequestV1 breakOrdering(final BreakOrderingRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.BREAK_ORDERING_DOMAIN, branch);
    }

    public static ControlOperationRequestV1 drainShard(final DrainShardRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.DRAIN_SHARD, branch);
    }

    public static ControlOperationRequestV1 fenceShard(final FenceShardRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.FENCE_SHARD_FOR_MAINTENANCE, branch);
    }

    public static ControlOperationRequestV1 forceCheckpoint(final ForceCheckpointRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.FORCE_CHECKPOINT, branch);
    }

    public static ControlOperationRequestV1 getCheckpointCatalog() {
        return new ControlOperationRequestV1(ControlOperationKindV1.GET_CHECKPOINT_CATALOG,
                GetCheckpointCatalogRequestV1.instance());
    }

    public static ControlOperationRequestV1 replayDeadLetter(final ReplayDeadLetterRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.REPLAY_DEAD_LETTER, branch);
    }

    public static ControlOperationRequestV1 resolveUncertain(final ResolveUncertainRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.RESOLVE_UNCERTAIN, branch);
    }

    public static ControlOperationRequestV1 publishDestinationProfile(
            final PublishDestinationProfileRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.PUBLISH_DESTINATION_PROFILE_VERSION, branch);
    }

    public static ControlOperationRequestV1 deprecateDestinationProfile(
            final DeprecateDestinationProfileRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.DEPRECATE_DESTINATION_PROFILE_VERSION, branch);
    }

    public static ControlOperationRequestV1 publishQuotaGrant(final PublishQuotaGrantRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.PUBLISH_QUOTA_GRANT, branch);
    }

    public static ControlOperationRequestV1 rotateEquivalentSecret(
            final RotateEquivalentSecretRequestV1 branch) {
        return new ControlOperationRequestV1(ControlOperationKindV1.ROTATE_EQUIVALENT_SECRET_REFERENCE, branch);
    }

    private static ControlOperationRequestBranchV1 decodeBranch(final ControlOperationKindV1 kind,
                                                                final byte[] encoded) {
        return switch (kind) {
            case STOP_NEW_SCHEDULES -> StopNewSchedulesRequestV1.decode(encoded);
            case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE -> LaneGateRequestV1.decode(encoded);
            case CLOSE_DESTINATION_LANE -> CloseLaneRequestV1.decode(encoded);
            case BREAK_ORDERING_DOMAIN -> BreakOrderingRequestV1.decode(encoded);
            case DRAIN_SHARD -> DrainShardRequestV1.decode(encoded);
            case FENCE_SHARD_FOR_MAINTENANCE -> FenceShardRequestV1.decode(encoded);
            case FORCE_CHECKPOINT -> ForceCheckpointRequestV1.decode(encoded);
            case GET_CHECKPOINT_CATALOG -> GetCheckpointCatalogRequestV1.decode(encoded);
            case REPLAY_DEAD_LETTER -> ReplayDeadLetterRequestV1.decode(encoded);
            case RESOLVE_UNCERTAIN -> ResolveUncertainRequestV1.decode(encoded);
            case PUBLISH_DESTINATION_PROFILE_VERSION -> PublishDestinationProfileRequestV1.decode(encoded);
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> DeprecateDestinationProfileRequestV1.decode(encoded);
            case PUBLISH_QUOTA_GRANT -> PublishQuotaGrantRequestV1.decode(encoded);
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> RotateEquivalentSecretRequestV1.decode(encoded);
        };
    }

    private static void validateBranch(final ControlOperationKindV1 kind,
                                       final ControlOperationRequestBranchV1 branch) {
        final boolean valid = switch (kind) {
            case STOP_NEW_SCHEDULES -> branch instanceof StopNewSchedulesRequestV1;
            case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE -> branch instanceof LaneGateRequestV1;
            case CLOSE_DESTINATION_LANE -> branch instanceof CloseLaneRequestV1;
            case BREAK_ORDERING_DOMAIN -> branch instanceof BreakOrderingRequestV1;
            case DRAIN_SHARD -> branch instanceof DrainShardRequestV1;
            case FENCE_SHARD_FOR_MAINTENANCE -> branch instanceof FenceShardRequestV1;
            case FORCE_CHECKPOINT -> branch instanceof ForceCheckpointRequestV1;
            case GET_CHECKPOINT_CATALOG -> branch instanceof GetCheckpointCatalogRequestV1;
            case REPLAY_DEAD_LETTER -> branch instanceof ReplayDeadLetterRequestV1;
            case RESOLVE_UNCERTAIN -> branch instanceof ResolveUncertainRequestV1;
            case PUBLISH_DESTINATION_PROFILE_VERSION -> branch instanceof PublishDestinationProfileRequestV1;
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> branch instanceof DeprecateDestinationProfileRequestV1;
            case PUBLISH_QUOTA_GRANT -> branch instanceof PublishQuotaGrantRequestV1;
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> branch instanceof RotateEquivalentSecretRequestV1;
        };
        if (!valid) {
            throw new IllegalArgumentException("Control Operation kind does not match request branch");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlOperationRequestV1 that && kind == that.kind
                && branch.equals(that.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, branch);
    }
}
