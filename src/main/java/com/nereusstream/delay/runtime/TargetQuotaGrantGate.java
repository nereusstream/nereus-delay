package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import java.util.Arrays;
import java.util.Objects;

/** Pure logical grant policy, after authentication and business validation; never a physical Admission permit. */
public final class TargetQuotaGrantGate {
    public enum Operation {
        FIRST_SCHEDULE(true),
        PREPARE(true),
        DLQ_REPLAY(true),
        RESERVATION_COMMIT(false),
        RESERVATION_EXPIRE(false),
        CANCEL(false),
        RESCHEDULE(false),
        CLAIM(false),
        CLAIM_REVOKE(false),
        ADMISSION(false),
        DEFINITIVE_FAILURE(false),
        UNKNOWN(false),
        OUTCOME(false),
        TERMINAL(false),
        RETAINED_RELEASE(false),
        OLD_ATTEMPT_DRAIN(false);

        private final boolean ingress;

        Operation(final boolean ingress) {
            this.ingress = ingress;
        }
    }

    public enum Decision {
        WITHIN_LOGICAL_GRANTS,
        EXISTING_WORK_DRAIN,
        SHARD_LIMIT,
        TARGET_LIMIT
    }

    private TargetQuotaGrantGate() {}

    /**
     * Limits apply to primary totals, including protected old incarnations and shared metadata.
     * Existing work still needs its frozen reserve, current execution/physical gates and exact ledger ownership.
     * The caller must not relabel a new ingress operation as existing work or bypass first-seen deduplication.
     */
    public static Decision evaluate(
            final Operation operation,
            final TargetQuotaGrant shardGrant,
            final TargetQuotaGrant targetGrant,
            final TargetQuotaAccounting accounting,
            final TargetQuotaAggregate priorShard,
            final TargetQuotaAggregate nextShard,
            final TargetQuotaTotal priorTarget,
            final TargetQuotaTotal nextTarget) {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(accounting, "accounting");
        Objects.requireNonNull(nextTarget, "nextTarget");
        if (shardGrant.scope().target() != null
                || targetGrant.scope().target() == null
                || !shardGrant.scope().equals(targetGrant.scope().shardScope())
                || !targetGrant.scope().equals(nextTarget.scope())
                || !shardGrant.scope().shard().equals(priorShard.shard())
                || !priorShard.shard().equals(nextShard.shard())
                || !Arrays.equals(priorShard.accountingIncarnation(), nextShard.accountingIncarnation())) {
            throw new IllegalStateException("logical quota gate scope/aggregate identity mismatch");
        }
        nextTarget.requireAggregate(nextShard);
        if (priorTarget != null) {
            if (!priorTarget.scope().equals(nextTarget.scope())) {
                throw new IllegalStateException("logical quota gate replaces its Target scope");
            }
            priorTarget.requireAggregate(priorShard);
        }
        if (!operation.ingress) {
            if (priorTarget == null) {
                throw new IllegalStateException("existing work requires its prior Target accounting total");
            }
            return Decision.EXISTING_WORK_DRAIN;
        }
        if (!Arrays.equals(accounting.canonicalBytes(), shardGrant.accounting().canonicalBytes())
                || !Arrays.equals(
                        accounting.canonicalBytes(), targetGrant.accounting().canonicalBytes())) {
            throw new IllegalStateException("new ingress measurement differs from its activated grant artifacts");
        }
        if (!shardGrant.limit().permitsGrowth(priorShard.usage(), nextShard.usage())) {
            return Decision.SHARD_LIMIT;
        }
        if (!targetGrant
                .limit()
                .permitsGrowth(
                        priorTarget == null ? TargetQuotaUsage.empty() : priorTarget.usage(), nextTarget.usage())) {
            return Decision.TARGET_LIMIT;
        }
        return Decision.WITHIN_LOGICAL_GRANTS;
    }
}
