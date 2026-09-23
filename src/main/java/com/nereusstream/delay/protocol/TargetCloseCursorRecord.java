package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-bound durable progress for one Target's first accepted Close. */
public final class TargetCloseCursorRecord {
    public static final int VALUE_TYPE = 40;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaMutation.MAX_CANONICAL_BYTES + 206;
    private final TargetPartitionId target;
    private final byte[] markerDigest;
    private final byte[] lineage;
    private final long revision;
    private final byte[] afterMessageId;
    private final boolean complete;
    private final TargetQuotaMutation mutation;
    private final byte[] digest;

    public TargetCloseCursorRecord(
            TargetPartitionId target,
            byte[] markerDigest,
            byte[] lineage,
            long revision,
            byte[] afterMessageId,
            boolean complete,
            TargetQuotaMutation mutation) {
        this.target = Objects.requireNonNull(target, "target");
        this.markerDigest = TargetCompatibilityCodec.assigned(markerDigest, 32, "closeMarkerDigest");
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        if (revision == 0
                || (afterMessageId != null && afterMessageId.length != DelayMessageId.LENGTH)
                || (revision == 1 && (afterMessageId != null || complete || mutation.isLocalMutation()))
                || (revision != 1
                        && (!(mutation.reservationCloseCursor()
                                        || mutation.reservationClosure()
                                        || mutation.reservationExpiry())
                                || !mutation.isLocalMutation()))) {
            throw new IllegalArgumentException("invalid Target Close cursor revision/progress/stamp");
        }
        this.revision = revision;
        this.afterMessageId = afterMessageId == null ? null : Bytes.copy(afterMessageId);
        this.complete = complete;
        digest = Bytes.sha256(Bytes.utf8("nereus-delay-target-close-cursor\0"), fields());
    }

    public static TargetCloseCursorRecord initial(TargetCloseRecord marker) {
        return new TargetCloseCursorRecord(
                marker.body().request().target(),
                Bytes.sha256(marker.canonicalBytes()),
                marker.recoveryLineage(),
                1,
                null,
                false,
                marker.mutation());
    }

    public TargetPartitionId target() {
        return target;
    }

    public long revision() {
        return revision;
    }

    public boolean complete() {
        return complete;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] afterMessageId() {
        return afterMessageId == null ? null : Bytes.copy(afterMessageId);
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public byte[] markerDigest() {
        return Bytes.copy(markerDigest);
    }

    public byte[] key() {
        return TargetKeyCodec.closeCursor(target);
    }

    public void requireMarker(TargetCloseRecord marker) {
        if (!target.equals(marker.body().request().target())
                || !Arrays.equals(lineage, marker.recoveryLineage())
                || !Arrays.equals(markerDigest, Bytes.sha256(marker.canonicalBytes()))
                || !mutation.source().shardId().equals(marker.body().shard())
                || mutation.source().compareTo(marker.mutation().source()) < 0
                || (revision == 1 && !mutation.equals(marker.mutation()))) {
            throw new IllegalStateException("Close cursor differs from its immutable first marker");
        }
    }

    public TargetCloseCursorRecord advance(DelayMessageId message, boolean last, TargetQuotaMutation stamp) {
        Objects.requireNonNull(message, "message");
        if (complete
                || afterMessageId != null && Arrays.compareUnsigned(message.bytes(), afterMessageId) <= 0
                || !(stamp.reservationClosure() || stamp.reservationExpiry())
                || !stamp.isLocalMutation()) {
            throw new IllegalStateException("Close cursor cannot regress, reopen or advance without local authority");
        }
        stamp.requireStoreSuccessorOf(mutation);
        return new TargetCloseCursorRecord(
                target, markerDigest, lineage, TargetQueueState.nextRevision(revision), message.bytes(), last, stamp);
    }

    public TargetCloseCursorRecord completeEmpty(TargetQuotaMutation stamp) {
        if (complete || !stamp.reservationCloseCursor() || !stamp.isLocalMutation()) {
            throw new IllegalStateException("Close cursor cannot complete twice or without local authority");
        }
        stamp.requireStoreSuccessorOf(mutation);
        return new TargetCloseCursorRecord(
                target, markerDigest, lineage, TargetQueueState.nextRevision(revision), afterMessageId, true, stamp);
    }

    public TargetQuotaIdentity ownerIdentity(TargetCloseRecord marker) {
        requireMarker(marker);
        return marker.ownerIdentity();
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, target.bytes());
            CanonicalProtobuf.bytes(out, 3, markerDigest);
            CanonicalProtobuf.bytes(out, 4, lineage);
            CanonicalProtobuf.uint64Bits(out, 5, revision);
            if (afterMessageId != null) {
                CanonicalProtobuf.bytes(out, 6, afterMessageId);
            }
            CanonicalProtobuf.uint32(out, 7, complete ? 2 : 1);
            CanonicalProtobuf.bytes(out, 8, mutation.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetCloseCursorRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target Close cursor");
        final boolean hasAfter = f.size() == 9;
        QueryCodecSupport.requireNumbers(
                f,
                hasAfter ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9} : new int[] {1, 2, 3, 4, 5, 7, 8, 9},
                "Target Close cursor");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Target Close cursor version");
        }
        final int stateIndex = hasAfter ? 6 : 5;
        final int state = QueryCodecSupport.uint32(f.get(stateIndex), 7);
        if (state != 1 && state != 2) {
            throw new IllegalArgumentException("unknown Close cursor phase");
        }
        final var result = new TargetCloseCursorRecord(
                new TargetPartitionId(QueryCodecSupport.fixed(f.get(1), 2, 32)),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.fixed(f.get(3), 4, 16),
                QueryCodecSupport.uint64Bits(f.get(4), 5),
                hasAfter ? QueryCodecSupport.fixed(f.get(5), 6, DelayMessageId.LENGTH) : null,
                state == 2,
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(stateIndex + 1), 8)));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(f.get(stateIndex + 2), 9, 32))) {
            throw new IllegalArgumentException("Close cursor digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Close cursor");
        return result;
    }

    public static TargetCloseCursorRecord decodeForStore(
            byte[] key, byte[] payload, TargetCloseRecord marker, byte[] lineage) {
        final var result = decode(payload);
        if (!Arrays.equals(key, result.key()) || !Arrays.equals(result.lineage, lineage)) {
            throw new IllegalStateException("Close cursor Store key/lineage mismatch");
        }
        result.requireMarker(marker);
        return result;
    }
}
