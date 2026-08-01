package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.ShardCapacityEnvelopeV1;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Checked local admission arithmetic for a Worker capacity envelope.
 *
 * <p>A shard envelope already contains its four component grants.  This
 * helper therefore sums only {@link ShardCapacityEnvelopeV1#committed()};
 * adding the component grants again would double-count the same reservation.
 * Oxia placement, lease CAS and artifact publication remain authority
 * operations outside this helper.</p>
 */
public final class WorkerCapacityAdmission {
    private WorkerCapacityAdmission() {
    }

    /** Returns the checked sum of distinct committed shard envelopes. */
    public static CapacityVectorV1 sumCommitted(final List<ShardCapacityEnvelopeV1> shardEnvelopes) {
        Objects.requireNonNull(shardEnvelopes, "shardEnvelopes");
        final Set<String> envelopeIds = new HashSet<>();
        CapacityVectorV1 sum = CapacityVectorV1.empty();
        for (ShardCapacityEnvelopeV1 envelope : shardEnvelopes) {
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
    public static CapacityVectorV1 required(final List<ShardCapacityEnvelopeV1> shardEnvelopes,
                                            final CapacityVectorV1 workerFixedCost,
                                            final CapacityVectorV1 transitionTemporaryDemand) {
        return sumCommitted(shardEnvelopes)
                .add(Objects.requireNonNull(workerFixedCost, "workerFixedCost"))
                .add(Objects.requireNonNull(transitionTemporaryDemand, "transitionTemporaryDemand"));
    }

    /** Fails closed unless the complete Worker hard-cap vector covers the requirement. */
    public static void requireFits(final CapacityVectorV1 hardCaps,
                                   final List<ShardCapacityEnvelopeV1> shardEnvelopes,
                                   final CapacityVectorV1 workerFixedCost,
                                   final CapacityVectorV1 transitionTemporaryDemand) {
        Objects.requireNonNull(hardCaps, "hardCaps");
        final CapacityVectorV1 required = required(shardEnvelopes, workerFixedCost, transitionTemporaryDemand);
        if (!hardCaps.covers(required)) {
            throw new IllegalArgumentException("Worker capacity envelopes exceed hard caps");
        }
    }
}
