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
import java.util.Optional;

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

    /** A process read fence, not a durable fairness generation or an Owner authority token. */
    public record Cut(byte[] storeIncarnation, long nativeSequence) {
        public Cut {
            if (Objects.requireNonNull(storeIncarnation, "storeIncarnation").length != 16) {
                throw new IllegalArgumentException("invalid Target queue read cut");
            }
            storeIncarnation = Arrays.copyOf(storeIncarnation, storeIncarnation.length);
        }

        @Override
        public byte[] storeIncarnation() {
            return Arrays.copyOf(storeIncarnation, storeIncarnation.length);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Cut that
                    && nativeSequence == that.nativeSequence
                    && Arrays.equals(storeIncarnation, that.storeIncarnation);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(storeIncarnation) + Long.hashCode(nativeSequence);
        }
    }

    public record Page(List<Entry> entries, TargetPartitionId nextAfter, Stop stop, Cut cut) {
        public Page {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            Objects.requireNonNull(stop, "stop");
            Objects.requireNonNull(cut, "cut");
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

    /** Rechecks the same Store revision after a multi-page scan before publishing its ring. */
    public Cut readCut(final BoundedReadBudget budget, final TargetStoreBackend.ReadAuthority authority) {
        return backend.guardedRead(
                Objects.requireNonNull(budget, "budget"),
                reader -> {
                    final Cut cut = cut(reader);
                    reader.requireWithinElapsedBudget();
                    return cut;
                },
                Objects.requireNonNull(authority, "authority"));
    }

    /** Refreshes one exact Target after a Claim or source change without re-reading message bodies. */
    public Optional<Entry> readTarget(
            final BoundedReadBudget budget,
            final TargetPartitionId target,
            final TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(target, "target");
        return backend.guardedRead(
                Objects.requireNonNull(budget, "budget"),
                reader -> {
                    final byte[] queueKey = TargetKeyCodec.state(target);
                    final byte[] queueRaw = reader.get(ColumnFamily.META, queueKey);
                    final byte[] identityKey = TargetKeyCodec.identity(target);
                    final byte[] identityRaw = reader.get(ColumnFamily.META, identityKey);
                    if (queueRaw == null && identityRaw == null) {
                        reader.requireWithinElapsedBudget();
                        return Optional.empty();
                    }
                    if (queueRaw == null || identityRaw == null) {
                        throw new IllegalStateException("Target queue and physical identity must coexist");
                    }
                    final var physical = CanonicalTargetPartition.decodeForStore(
                            identityKey,
                            TargetValueEnvelope.decode(identityRaw, CanonicalTargetPartition.VALUE_TYPE)
                                    .payload());
                    final var queue = TargetQueueState.decodeForStore(
                            queueKey,
                            TargetValueEnvelope.decode(queueRaw, TargetQueueState.VALUE_TYPE)
                                    .payload(),
                            physical,
                            reader.shardId(),
                            maximumDomains);
                    reader.requireWithinElapsedBudget();
                    return Optional.of(new Entry(queue, physical));
                },
                Objects.requireNonNull(authority, "authority"));
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
                    final Cut cut = cut(reader);
                    TargetPartitionId cursor = after;
                    byte[] lower = after == null ? LOWER : afterKey(TargetKeyCodec.state(after));
                    for (int index = 0; index < maximumTargets; index++) {
                        try {
                            final var row = reader.first(ColumnFamily.META, lower, UPPER, List.of());
                            if (row == null) {
                                return new Page(entries, cursor, Stop.RANGE_END, cut);
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
                            return new Page(entries, cursor, Stop.READ_BUDGET, cut);
                        }
                    }
                    return new Page(entries, cursor, Stop.PAGE_LIMIT, cut);
                },
                Objects.requireNonNull(authority, "authority"));
    }

    private static Cut cut(final TargetStoreBackend.Reader reader) {
        return new Cut(reader.metadata().storeIncarnation(), reader.nativeSequence());
    }

    private static byte[] afterKey(final byte[] key) {
        final byte[] next = Arrays.copyOf(key, key.length + 1);
        next[key.length] = 0;
        return next;
    }
}
