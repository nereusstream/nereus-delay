package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;

/**
 * Source-position-pinned semantic resolver for Registry Schedule commands.
 *
 * <p>The wire intent deliberately carries immutable Profile and adapter
 * references rather than a precomputed Lane ID.  An implementation resolves
 * those references against the authenticated Route/Profile/Capability
 * snapshot and returns only the deterministic local projection needed by the
 * shard.  Implementations must be side-effect free: for equal inputs and the
 * same source position they must return equal bytes, or the same stable
 * fail-closed error.</p>
 */
public interface V1ScheduleResolver {
    ResolvedSchedule resolveSchedule(ShardId shardId, DelayMessageId messageId, ScheduleIntentV1 intent,
                                     SourcePosition sourcePosition);

    ResolvedPrepare resolvePrepare(ShardId shardId, DelayMessageId messageId,
                                   PrepareLargeScheduleBodyV1 body, SourcePosition sourcePosition);

    /** Resolved ordinary Schedule projection used by the legacy local index. */
    record ResolvedSchedule(DestinationLaneId laneId, byte[] canonicalLaneTuple,
                            byte[] inlinePayload, PayloadReference payloadReference) {
        public ResolvedSchedule {
            Objects.requireNonNull(laneId, "laneId");
            canonicalLaneTuple = canonicalTuple(canonicalLaneTuple);
            if (!laneId.equals(DestinationLaneId.derive(canonicalLaneTuple))) {
                throw new V1CommandResolutionException(io.nereusstream.delay.protocol.StableCode.INVALID_COMMAND,
                        "resolved Lane ID does not match canonical Lane tuple");
            }
            if ((inlinePayload == null) == (payloadReference == null)) {
                throw new V1CommandResolutionException(io.nereusstream.delay.protocol.StableCode.INVALID_COMMAND,
                        "resolved Schedule must select exactly one payload branch");
            }
            if (inlinePayload != null) {
                inlinePayload = Bytes.copy(inlinePayload);
            }
        }

        @Override
        public byte[] canonicalLaneTuple() {
            return Bytes.copy(canonicalLaneTuple);
        }

        @Override
        public byte[] inlinePayload() {
            return inlinePayload == null ? null : Bytes.copy(inlinePayload);
        }
    }

    /** Resolved Lane projection used before creating a large-payload reservation. */
    record ResolvedPrepare(DestinationLaneId laneId, byte[] canonicalLaneTuple) {
        public ResolvedPrepare {
            Objects.requireNonNull(laneId, "laneId");
            canonicalLaneTuple = canonicalTuple(canonicalLaneTuple);
            if (!laneId.equals(DestinationLaneId.derive(canonicalLaneTuple))) {
                throw new V1CommandResolutionException(io.nereusstream.delay.protocol.StableCode.INVALID_COMMAND,
                        "resolved Lane ID does not match canonical Lane tuple");
            }
        }

        @Override
        public byte[] canonicalLaneTuple() {
            return Bytes.copy(canonicalLaneTuple);
        }
    }

    private static byte[] canonicalTuple(final byte[] value) {
        Objects.requireNonNull(value, "canonicalLaneTuple");
        if (value.length == 0 || value.length > (1 << 20)) {
            throw new V1CommandResolutionException(io.nereusstream.delay.protocol.StableCode.INVALID_COMMAND,
                    "canonical Lane tuple length is outside bounds");
        }
        return Bytes.copy(value);
    }
}
