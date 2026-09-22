package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable first accepted Target Close, retaining the source-ordered fence at the marker. */
public final class TargetCloseRecord {
    public static final int VALUE_TYPE = 39;
    public static final int MAX_CANONICAL_BYTES = 2
            + 4
            + TargetCloseBody.MAX_CANONICAL_BYTES
            + 11
            + 4
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 18
            + 34;
    private final TargetCloseBody body;
    private final long fenceAtClose;
    private final TargetQuotaMutation mutation;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetCloseRecord(TargetCloseBody body, long fenceAtClose, TargetQuotaMutation mutation, byte[] lineage) {
        this.body = Objects.requireNonNull(body, "body");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        mutation.requireSourceApplied();
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        if (fenceAtClose < -1 || !body.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("Target Close marker source/fence mismatch");
        }
        this.fenceAtClose = fenceAtClose;
        digest = Bytes.sha256(Bytes.utf8("nereus-delay-target-close-record\0"), fields());
    }

    public TargetCloseBody body() {
        return body;
    }

    public long fenceAtClose() {
        return fenceAtClose;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public byte[] key() {
        return TargetKeyCodec.close(body.request().target());
    }

    public TargetQuotaIdentity ownerIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                body.shard(),
                body.request().requireControlRef(body.controlRef()).accountingIncarnation(),
                body.request().target(),
                null);
    }

    public void requireQueue(TargetQueueState queue) {
        final var selected = body.request().requireControlRef(body.controlRef());
        if (!body.request().target().equals(queue.targetId())
                || !Arrays.equals(selected.accountingIncarnation(), queue.accountingIncarnation())
                || queue.controlVersion() != TargetQueueState.nextRevision(selected.expectedControlVersion())
                || queue.admissionState() != TargetQueueState.AdmissionState.CLOSED) {
            throw new IllegalStateException("Target Close marker differs from its closed queue projection");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, body.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, fenceAtClose);
            CanonicalProtobuf.bytes(out, 4, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 6, digest);
        });
    }

    public static TargetCloseRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target Close marker");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6}, "Target Close marker");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown Target Close marker version");
        }
        final var result = new TargetCloseRecord(
                TargetCloseBody.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint64Bits(f.get(2), 3),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 16));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(f.get(5), 6, 32))) {
            throw new IllegalArgumentException("Target Close digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Close marker");
        return result;
    }

    public static TargetCloseRecord decodeForStore(byte[] key, byte[] encoded, ShardId shard, byte[] lineage) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key())
                || !shard.equals(result.body.shard())
                || !Arrays.equals(lineage, result.lineage)) {
            throw new IllegalStateException("Target Close Store key/Shard/lineage mismatch");
        }
        return result;
    }
}
