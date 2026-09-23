package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Reads bounded, validated Target head summaries for one active source Shard. */
public final class TargetQueueSnapshotReader {
    public enum Stop {
        RANGE_END,
        PAGE_LIMIT,
        READ_BUDGET
    }

    public record Entry(TargetQueueState queue, CanonicalTargetPartition physical) {
        public Entry {
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(physical, "physical");
            if (!queue.targetId().equals(physical.id())) {
                throw new IllegalArgumentException("Target queue and physical identity differ");
            }
        }
    }

    public record Page(List<Entry> entries, TargetPartitionId nextAfter, Stop stop) {
        public Page {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            Objects.requireNonNull(stop, "stop");
            if (!entries.isEmpty() && !entries.getLast().queue().targetId().equals(nextAfter)) {
                throw new IllegalArgumentException("Target scan cursor does not follow its last complete entry");
            }
        }

        public boolean complete() {
            return stop == Stop.RANGE_END;
        }
    }

    private static final byte[] LOWER = {(byte) TargetKeyCodec.STATE_TAG, (byte) TargetKeyCodec.KEY_FORMAT};
    private static final byte[] UPPER = {(byte) TargetKeyCodec.STATE_TAG, (byte) (TargetKeyCodec.KEY_FORMAT + 1)};

    private final TargetStoreBackend backend;
    private final int maximumDomains;

    public TargetQueueSnapshotReader(final TargetStoreBackend backend, final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        if (maximumDomains < 1 || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("invalid activated Target domain limit");
        }
        this.maximumDomains = maximumDomains;
    }

    /** A page limit or read-budget yield is not an empty-prefix proof; resume after nextAfter. */
    public Page scan(
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final TargetStoreBackend.ReadAuthority authority) {
        if (maximumTargets <= 0) {
            throw new IllegalArgumentException("Target scan page limit must be positive");
        }
        return backend.guardedRead(
                Objects.requireNonNull(budget, "budget"),
                reader -> {
                    final var entries = new ArrayList<Entry>();
                    TargetPartitionId cursor = after;
                    byte[] lower = after == null ? LOWER : afterKey(TargetKeyCodec.state(after));
                    for (int index = 0; index < maximumTargets; index++) {
                        try {
                            final var row = reader.first(ColumnFamily.META, lower, UPPER, List.of());
                            if (row == null) {
                                return new Page(entries, cursor, Stop.RANGE_END);
                            }
                            if (row.key().length != 2 + TargetPartitionId.LENGTH) {
                                throw new IllegalArgumentException("Target state key is not fixed length");
                            }
                            final var target =
                                    new TargetPartitionId(Arrays.copyOfRange(row.key(), 2, row.key().length));
                            final byte[] identityKey = TargetKeyCodec.identity(target);
                            final byte[] identityRaw = reader.get(ColumnFamily.META, identityKey);
                            if (identityRaw == null) {
                                throw new IllegalStateException("Target state lacks its physical identity");
                            }
                            final var physical = CanonicalTargetPartition.decodeForStore(
                                    identityKey,
                                    TargetValueEnvelope.decode(identityRaw, CanonicalTargetPartition.VALUE_TYPE)
                                            .payload());
                            final var verified = TargetQueueState.decodeForStore(
                                    row.key(),
                                    TargetValueEnvelope.decode(row.value(), TargetQueueState.VALUE_TYPE)
                                            .payload(),
                                    physical,
                                    reader.shardId(),
                                    maximumDomains);
                            reader.requireWithinElapsedBudget();
                            entries.add(new Entry(verified, physical));
                            cursor = verified.targetId();
                            lower = afterKey(row.key());
                        } catch (ReadIncompleteException incomplete) {
                            return new Page(entries, cursor, Stop.READ_BUDGET);
                        }
                    }
                    return new Page(entries, cursor, Stop.PAGE_LIMIT);
                },
                Objects.requireNonNull(authority, "authority"));
    }

    private static byte[] afterKey(final byte[] key) {
        final byte[] next = Arrays.copyOf(key, key.length + 1);
        next[key.length] = 0;
        return next;
    }
}
