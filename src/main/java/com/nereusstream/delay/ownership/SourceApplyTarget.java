package com.nereusstream.delay.ownership;

import com.nereusstream.delay.runtime.HeadReadIncompleteException;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import java.security.PublicKey;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Internal adapter shared by the bounded source queue and its one-record ACK coordinator. */
abstract class SourceApplyTarget {
    abstract void bind(WorkClassExecutionRegistry registry);

    abstract void requireSubmission(SourceReplayEntry entry, boolean recovery);

    abstract SourceReplayOutcome apply(SourceReplayEntry entry, LongSupplier clock, boolean recovery);

    abstract void fence();

    void beforeAcknowledgement(SourceReplayEntry entry, SourceReplayOutcome outcome, LongSupplier clock) {
        // Legacy targets retain their existing broker-ack boundary; Target additionally rechecks its live lease.
    }

    boolean isReadIncomplete(Throwable failure) {
        return failure instanceof HeadReadIncompleteException;
    }

    static SourceApplyTarget legacy(OwnedDelayShard shard, OxiaOwnerLeaseStore authority, PublicKey key) {
        Objects.requireNonNull(shard, "ownedShard");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(key, "verificationKey");
        return new SourceApplyTarget() {
            @Override
            public void bind(WorkClassExecutionRegistry registry) {
                shard.bindWorkClassExecutionRegistry(registry);
            }

            @Override
            public void requireSubmission(SourceReplayEntry entry, boolean recovery) {
                if (recovery) {
                    shard.requireRecoverySourceApplySubmission(authority, entry, key);
                } else {
                    shard.requireSourceApplySubmission(authority, entry, key);
                }
            }

            @Override
            public SourceReplayOutcome apply(SourceReplayEntry entry, LongSupplier clock, boolean recovery) {
                return recovery
                        ? shard.applyRecoverySourceEntryAuthoritativelyStrict(authority, entry, key, clock)
                        : shard.applySourceEntryAuthoritativelyStrict(authority, entry, key, clock);
            }

            @Override
            public void fence() {
                shard.fence();
            }
        };
    }
}
