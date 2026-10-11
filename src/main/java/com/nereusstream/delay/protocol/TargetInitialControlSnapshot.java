package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.KeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Root-owned META10 projection of an independently signed input accepted with the first grant Source batch. */
public final class TargetInitialControlSnapshot {
    public static final int VALUE_TYPE = 1;
    public static final int MAX_CANONICAL_BYTES =
            TargetControlSnapshotInput.MAX_CANONICAL_BYTES + TargetQuotaGrantActivation.MAX_CANONICAL_BYTES + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-initial-control-snapshot\0");
    private final TargetControlSnapshotInput input;
    private final TargetQuotaIdentity root;
    private final TargetQuotaMutation mutation;
    private final TargetQuotaGrantActivation initialGrant;
    private final byte[] author;
    private final byte[] digest;

    public TargetInitialControlSnapshot(TargetControlSnapshotInput input, TargetQuotaIdentity root,
            TargetQuotaMutation mutation, TargetQuotaGrantActivation initialGrant, byte[] author) {
        this.input = Objects.requireNonNull(input, "input");
        this.root = Objects.requireNonNull(root, "root");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        mutation.requireSourceApplied();
        if (mutation.sequence() != 1 || root.kind() != TargetQuotaIdentity.Kind.SHARD
                || !root.shard().equals(input.scope().shard())
                || !Arrays.equals(mutation.source().canonicalBytes(), input.source().canonicalBytes())
                || !Arrays.equals(mutation.mutationDigest(), input.envelopeDigest())
                || AuthorIdentity.decode(author).kind() != AuthorIdentity.Kind.CONTROL) {
            throw new IllegalArgumentException("Target initial control record changes its root/Source/author");
        }
        this.initialGrant = Objects.requireNonNull(initialGrant, "initialGrant");
        input.requireInitial(input.scope(), input.lineage(), initialGrant.grant(), mutation.source(),
                mutation.mutationDigest());
        if (initialGrant.request().prior() != null || initialGrant.allocation() != null
                || !initialGrant.mutation().equals(mutation) || !initialGrant.grant().scope().equals(input.scope())) {
            throw new IllegalArgumentException("Target control snapshot changes its original first Shard grant");
        }
        this.author = Bytes.copy(author);
        digest = Bytes.sha256(DOMAIN, fields());
    }

    public TargetControlSnapshotInput input() { return input; }
    public TargetQuotaIdentity root() { return root; }
    public TargetQuotaMutation mutation() { return mutation; }
    public TargetQuotaGrantActivation initialGrant() { return initialGrant; }
    public byte[] authorIdentity() { return Bytes.copy(author); }
    public byte[] snapshotDigest() { return Bytes.copy(digest); }
    public byte[] key() { return KeyCodec.metaFixed(10); }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, input.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, root.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, initialGrant.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, author);
        });
    }

    public static TargetInitialControlSnapshot decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false,
                "Target initial control snapshot");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "Target initial control snapshot");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unsupported Target initial control snapshot version");
        }
        final var value = new TargetInitialControlSnapshot(
                TargetControlSnapshotInput.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                TargetQuotaGrantActivation.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                QueryCodecSupport.bytes(fields.get(5), 6));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(fields.get(6), 7, 32))) {
            throw new IllegalArgumentException("Target initial control snapshot digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target initial control snapshot");
        return value;
    }
}
