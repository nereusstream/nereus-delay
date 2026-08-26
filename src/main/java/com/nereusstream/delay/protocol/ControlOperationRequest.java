package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/**
 * Registry §6.3 closed request union. The outer protobuf field number is the
 * {@link ControlOperationKind} wire value, so a branch cannot be relabeled
 * without failing canonical decode.
 */
public final class ControlOperationRequest {
    private final ControlOperationKind kind;
    private final ControlOperationRequestBranch branch;

    public ControlOperationRequest(final ControlOperationKind kind, final ControlOperationRequestBranch branch) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.branch = Objects.requireNonNull(branch, "branch");
        validateBranch(kind, branch);
    }

    public ControlOperationKind kind() {
        return kind;
    }

    public ControlOperationRequestBranch branch() {
        return branch;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(
                output -> CanonicalProtobuf.bytes(output, kind.wireValue(), branch.canonicalBytes()));
    }

    public static ControlOperationRequest decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ControlOperationRequest");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("ControlOperationRequest must select one branch");
        }
        final CanonicalProtobuf.Reader.Field field = fields.get(0);
        if (field.wireType() != 2) {
            throw new IllegalArgumentException("ControlOperationRequest branch must be bytes");
        }
        final ControlOperationKind kind = ControlOperationKind.fromWire(field.number());
        final ControlOperationRequest result =
                new ControlOperationRequest(kind, decodeBranch(kind, QueryCodecSupport.nested(field, field.number())));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlOperationRequest");
        return result;
    }

    public static ControlOperationRequest stopNewSchedules(final ControlReason reason) {
        return new ControlOperationRequest(
                ControlOperationKind.STOP_NEW_SCHEDULES, new StopNewSchedulesRequest(reason));
    }

    public static ControlOperationRequest pauseDestinationLane(final ControlReason reason) {
        return laneGate(ControlOperationKind.PAUSE_DESTINATION_LANE, new LaneGateRequest(reason));
    }

    public static ControlOperationRequest resumeDestinationLane(final ControlReason reason) {
        return laneGate(ControlOperationKind.RESUME_DESTINATION_LANE, new LaneGateRequest(reason));
    }

    public static ControlOperationRequest laneGate(final ControlOperationKind kind, final LaneGateRequest branch) {
        return new ControlOperationRequest(kind, branch);
    }

    public static ControlOperationRequest closeDestinationLane(final CloseLaneRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.CLOSE_DESTINATION_LANE, branch);
    }

    public static ControlOperationRequest breakOrdering(final BreakOrderingRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.BREAK_ORDERING_DOMAIN, branch);
    }

    public static ControlOperationRequest drainShard(final DrainShardRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.DRAIN_SHARD, branch);
    }

    public static ControlOperationRequest fenceShard(final FenceShardRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.FENCE_SHARD_FOR_MAINTENANCE, branch);
    }

    public static ControlOperationRequest forceCheckpoint(final ForceCheckpointRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.FORCE_CHECKPOINT, branch);
    }

    public static ControlOperationRequest getCheckpointCatalog() {
        return new ControlOperationRequest(
                ControlOperationKind.GET_CHECKPOINT_CATALOG, GetCheckpointCatalogRequest.instance());
    }

    public static ControlOperationRequest replayDeadLetter(final ReplayDeadLetterRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.REPLAY_DEAD_LETTER, branch);
    }

    public static ControlOperationRequest resolveUncertain(final ResolveUncertainRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.RESOLVE_UNCERTAIN, branch);
    }

    public static ControlOperationRequest publishDestinationProfile(final PublishDestinationProfileRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.PUBLISH_DESTINATION_PROFILE_VERSION, branch);
    }

    public static ControlOperationRequest deprecateDestinationProfile(final DeprecateDestinationProfileRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.DEPRECATE_DESTINATION_PROFILE_VERSION, branch);
    }

    public static ControlOperationRequest publishQuotaGrant(final PublishQuotaGrantRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.PUBLISH_QUOTA_GRANT, branch);
    }

    public static ControlOperationRequest rotateEquivalentSecret(final RotateEquivalentSecretRequest branch) {
        return new ControlOperationRequest(ControlOperationKind.ROTATE_EQUIVALENT_SECRET_REFERENCE, branch);
    }

    private static ControlOperationRequestBranch decodeBranch(final ControlOperationKind kind, final byte[] encoded) {
        return switch (kind) {
            case STOP_NEW_SCHEDULES -> StopNewSchedulesRequest.decode(encoded);
            case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE -> LaneGateRequest.decode(encoded);
            case CLOSE_DESTINATION_LANE -> CloseLaneRequest.decode(encoded);
            case BREAK_ORDERING_DOMAIN -> BreakOrderingRequest.decode(encoded);
            case DRAIN_SHARD -> DrainShardRequest.decode(encoded);
            case FENCE_SHARD_FOR_MAINTENANCE -> FenceShardRequest.decode(encoded);
            case FORCE_CHECKPOINT -> ForceCheckpointRequest.decode(encoded);
            case GET_CHECKPOINT_CATALOG -> GetCheckpointCatalogRequest.decode(encoded);
            case REPLAY_DEAD_LETTER -> ReplayDeadLetterRequest.decode(encoded);
            case RESOLVE_UNCERTAIN -> ResolveUncertainRequest.decode(encoded);
            case PUBLISH_DESTINATION_PROFILE_VERSION -> PublishDestinationProfileRequest.decode(encoded);
            case DEPRECATE_DESTINATION_PROFILE_VERSION -> DeprecateDestinationProfileRequest.decode(encoded);
            case PUBLISH_QUOTA_GRANT -> PublishQuotaGrantRequest.decode(encoded);
            case ROTATE_EQUIVALENT_SECRET_REFERENCE -> RotateEquivalentSecretRequest.decode(encoded);
        };
    }

    private static void validateBranch(final ControlOperationKind kind, final ControlOperationRequestBranch branch) {
        final boolean valid =
                switch (kind) {
                    case STOP_NEW_SCHEDULES -> branch instanceof StopNewSchedulesRequest;
                    case PAUSE_DESTINATION_LANE, RESUME_DESTINATION_LANE -> branch instanceof LaneGateRequest;
                    case CLOSE_DESTINATION_LANE -> branch instanceof CloseLaneRequest;
                    case BREAK_ORDERING_DOMAIN -> branch instanceof BreakOrderingRequest;
                    case DRAIN_SHARD -> branch instanceof DrainShardRequest;
                    case FENCE_SHARD_FOR_MAINTENANCE -> branch instanceof FenceShardRequest;
                    case FORCE_CHECKPOINT -> branch instanceof ForceCheckpointRequest;
                    case GET_CHECKPOINT_CATALOG -> branch instanceof GetCheckpointCatalogRequest;
                    case REPLAY_DEAD_LETTER -> branch instanceof ReplayDeadLetterRequest;
                    case RESOLVE_UNCERTAIN -> branch instanceof ResolveUncertainRequest;
                    case PUBLISH_DESTINATION_PROFILE_VERSION -> branch instanceof PublishDestinationProfileRequest;
                    case DEPRECATE_DESTINATION_PROFILE_VERSION -> branch instanceof DeprecateDestinationProfileRequest;
                    case PUBLISH_QUOTA_GRANT -> branch instanceof PublishQuotaGrantRequest;
                    case ROTATE_EQUIVALENT_SECRET_REFERENCE -> branch instanceof RotateEquivalentSecretRequest;
                };
        if (!valid) {
            throw new IllegalArgumentException("Control Operation kind does not match request branch");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlOperationRequest that && kind == that.kind && branch.equals(that.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, branch);
    }
}
