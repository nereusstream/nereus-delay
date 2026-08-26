package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Checked local admission arithmetic for a Worker capacity envelope.
 *
 * <p>A shard envelope already contains its four component grants. This
 * helper therefore sums only {@link ShardCapacityEnvelope#committed()};
 * adding the component grants again would double-count the same reservation.
 * Oxia placement, lease CAS and artifact publication remain authority
 * operations outside this helper.</p>
 */
public final class WorkerCapacityAdmission {
    private WorkerCapacityAdmission() {}

    /** Returns the checked sum of distinct committed shard envelopes. */
    public static CapacityVector sumCommitted(final List<ShardCapacityEnvelope> shardEnvelopes) {
        Objects.requireNonNull(shardEnvelopes, "shardEnvelopes");
        final Set<String> envelopeIds = new HashSet<>();
        CapacityVector sum = CapacityVector.empty();
        for (ShardCapacityEnvelope envelope : shardEnvelopes) {
            Objects.requireNonNull(envelope, "shard envelope");
            if (!envelopeIds.add(Bytes.hex(envelope.envelopeId()))) {
                throw new IllegalArgumentException("duplicate shard capacity envelope identity");
            }
            sum = sum.add(envelope.committed());
        }
        return sum;
    }

    /**
     * Returns {@code committed shards + fixed worker cost + transition demand}
     * using checked per-dimension arithmetic.
     */
    public static CapacityVector required(
            final List<ShardCapacityEnvelope> shardEnvelopes,
            final CapacityVector workerFixedCost,
            final CapacityVector transitionTemporaryDemand) {
        return sumCommitted(shardEnvelopes)
                .add(Objects.requireNonNull(workerFixedCost, "workerFixedCost"))
                .add(Objects.requireNonNull(transitionTemporaryDemand, "transitionTemporaryDemand"));
    }

    /** Fails closed unless the complete Worker hard-cap vector covers the requirement. */
    public static void requireFits(
            final CapacityVector hardCaps,
            final List<ShardCapacityEnvelope> shardEnvelopes,
            final CapacityVector workerFixedCost,
            final CapacityVector transitionTemporaryDemand) {
        Objects.requireNonNull(hardCaps, "hardCaps");
        final CapacityVector required = required(shardEnvelopes, workerFixedCost, transitionTemporaryDemand);
        if (!hardCaps.covers(required)) {
            throw new IllegalArgumentException("Worker capacity envelopes exceed hard caps");
        }
    }
}
