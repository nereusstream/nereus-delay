package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One guarded, source-preserving scan step over the Target message expiry index. */
public final class TargetExpiryDiscoveryStore {
    public record Candidate(TargetMessageLocator locator, long expireAtEpochMs) {
        public Candidate {
            Objects.requireNonNull(locator, "locator");
            if (expireAtEpochMs < 0) {
                throw new IllegalArgumentException("negative Target expiry candidate time");
            }
        }
    }

    /** Process-local scan cursor; it never authorizes a write or survives a store instance. */
    public static final class Cursor {
        private final TargetExpiryDiscoveryStore owner;
        private final long cutoffEpochMs;
        private final byte[] after;

        private Cursor(final TargetExpiryDiscoveryStore owner, final long cutoffEpochMs, final byte[] after) {
            this.owner = owner;
            this.cutoffEpochMs = cutoffEpochMs;
            this.after = Bytes.copy(after);
        }
    }

    /** A step can advance without a candidate when it reaches the end of this frozen sweep. */
    public record Discovery(Optional<Candidate> candidate, Cursor nextCursor, boolean sweepComplete) {
        public Discovery {
            Objects.requireNonNull(candidate, "candidate");
            if (sweepComplete != (nextCursor == null) || (candidate.isPresent() && sweepComplete)) {
                throw new IllegalArgumentException("Target expiry candidate and sweep cursor disagree");
            }
        }
    }

    private final TargetStoreBackend backend;
    private final com.nereusstream.delay.protocol.ShardId shard;

    public TargetExpiryDiscoveryStore(final TargetStoreBackend backend, final TargetQuotaScope scope) {
        this.backend = Objects.requireNonNull(backend, "backend");
        final var exactScope = Objects.requireNonNull(scope, "scope");
        if (exactScope.target() != null) {
            throw new IllegalArgumentException("message expiry discovery requires Shard scope");
        }
        shard = exactScope.shard();
    }

    /** Reads at most one persisted row and binds continuation to this sweep's nondecreasing trusted cutoff. */
    public Discovery discover(
            final BoundedReadBudget budget,
            final Cursor cursor,
            final TrustedUtcIntervalEvidence evidence,
            final TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(budget, "budget");
        final var trusted = Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(authority, "authority");
        if (cursor != null && cursor.owner != this) {
            throw new IllegalArgumentException("foreign Target expiry discovery cursor");
        }
        if (cursor != null && trusted.earliestEpochMs() < cursor.cutoffEpochMs) {
            throw new IllegalStateException("trusted expiry cutoff regressed; restart the Target sweep");
        }
        final long cutoff = cursor == null ? trusted.earliestEpochMs() : cursor.cutoffEpochMs;
        return backend.guardedRead(
                budget,
                reader -> {
                    if (!shard.equals(reader.shardId()) || reader.source() == null) {
                        throw new IllegalStateException(
                                "Target expiry discovery requires its established source Store");
                    }
                    final byte[] lower = cursor == null
                            ? TargetKeyCodec.expiryPrefix()
                            : Bytes.concat(cursor.after, new byte[] {0});
                    final var row = reader.first(
                            ColumnFamily.TIMELINE,
                            lower,
                            TargetKeyCodec.expiryUpperBound(cutoff),
                            List.of());
                    if (row == null) {
                        return new Discovery(Optional.empty(), null, true);
                    }
                    final var decodedExpiry = TargetExpiryRef.decode(
                            TargetValueEnvelope.decode(row.value(), TargetExpiryRef.VALUE_TYPE).payload());
                    final byte[] messageKey = TargetKeyCodec.message(decodedExpiry.locator().messageId());
                    final byte[] rawMessage = reader.get(ColumnFamily.ID, messageKey);
                    if (rawMessage == null) {
                        throw new IllegalStateException("Target expiry index points to a missing Message");
                    }
                    final var message = TargetMessageRecord.decodeForStore(
                            messageKey,
                            TargetValueEnvelope.decode(rawMessage, TargetMessageRecord.VALUE_TYPE).payload(),
                            shard);
                    final var expiry = TargetExpiryRef.decodeForMessage(
                            row.key(),
                            decodedExpiry.canonicalBytes(),
                            message);
                    if (expiry.expireAtEpochMs() > cutoff
                            || !shard.equals(expiry.locator().messageId().routingId().shardId())) {
                        throw new IllegalStateException("Target expiry range returned a foreign or future index");
                    }
                    if (!budget.beforeTimedWork()) {
                        throw budget.incomplete();
                    }
                    final var next = new Cursor(this, cutoff, row.key());
                    return new Discovery(
                            Optional.of(new Candidate(expiry.locator(), expiry.expireAtEpochMs())), next, false);
                },
                authority);
    }
}
