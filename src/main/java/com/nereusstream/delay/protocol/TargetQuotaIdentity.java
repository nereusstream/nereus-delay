package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Full local counter identity; tenant counters are mirrors, never additional aggregate owners. */
public final class TargetQuotaIdentity {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2 + 2 + 22 + 18 + 34 + 34 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-identity\0");

    public enum Kind {
        TARGET(1, true, false),
        TENANT_TARGET(2, true, true),
        SHARD(3, false, false),
        TENANT_SHARD(4, false, true);

        private final int wire;
        private final boolean target;
        private final boolean mirror;

        Kind(final int wire, final boolean target, final boolean mirror) {
            this.wire = wire;
            this.target = target;
            this.mirror = mirror;
        }

        public int wireValue() {
            return wire;
        }

        public boolean hasTarget() {
            return target;
        }

        public boolean isMirror() {
            return mirror;
        }

        private static Kind fromWire(final int wire) {
            for (var kind : values()) {
                if (kind.wire == wire) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown Target quota counter kind");
        }
    }

    private final Kind kind;
    private final ShardId shard;
    private final byte[] accountingIncarnation;
    private final TargetPartitionId target;
    private final byte[] tenantScope;
    private final byte[] digest;

    public TargetQuotaIdentity(
            final Kind kind,
            final ShardId shard,
            final byte[] accountingIncarnation,
            final TargetPartitionId target,
            final byte[] tenantScope) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.accountingIncarnation =
                TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accountingIncarnation");
        if (kind.hasTarget() != (target != null) || kind.isMirror() != (tenantScope != null)) {
            throw new IllegalArgumentException("Target quota identity fields disagree with kind");
        }
        this.target = target;
        this.tenantScope =
                tenantScope == null ? null : TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantScope");
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public Kind kind() {
        return kind;
    }

    public ShardId shard() {
        return shard;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public TargetPartitionId target() {
        return target;
    }

    public byte[] tenantScope() {
        return tenantScope == null ? null : Bytes.copy(tenantScope);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public TargetQuotaIdentity primary() {
        return kind.isMirror()
                ? new TargetQuotaIdentity(
                        kind.hasTarget() ? Kind.TARGET : Kind.SHARD, shard, accountingIncarnation, target, null)
                : this;
    }

    /** The identity is in both key and value so store reads validate its full preimage. */
    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_COUNTER_TAG, TargetKeyCodec.KEY_FORMAT, (byte) kind.wireValue()},
                shardBytes(shard),
                accountingIncarnation,
                target == null ? new byte[0] : target.bytes(),
                tenantScope == null ? new byte[0] : tenantScope);
    }

    static byte[] shardBytes(final ShardId shard) {
        return Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition()));
    }

    static ShardId shard(final byte[] raw) {
        Bytes.requireLength(raw, 20, "sourceShard");
        return new ShardId(new RouteIncarnation(Arrays.copyOf(raw, 16)), (int) Bytes.readU32be(raw, 16));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.uint32(out, 2, kind.wireValue());
            CanonicalProtobuf.bytes(out, 3, shardBytes(shard));
            CanonicalProtobuf.bytes(out, 4, accountingIncarnation);
            if (target != null) {
                CanonicalProtobuf.bytes(out, 5, target.bytes());
            }
            if (tenantScope != null) {
                CanonicalProtobuf.bytes(out, 6, tenantScope);
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    public static TargetQuotaIdentity decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "TargetQuotaIdentity");
        if (fields.size() < 5 || QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota identity version");
        }
        final Kind kind = Kind.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int[] numbers =
                switch (kind) {
                    case TARGET -> new int[] {1, 2, 3, 4, 5, 7};
                    case TENANT_TARGET -> new int[] {1, 2, 3, 4, 5, 6, 7};
                    case SHARD -> new int[] {1, 2, 3, 4, 7};
                    case TENANT_SHARD -> new int[] {1, 2, 3, 4, 6, 7};
                };
        QueryCodecSupport.requireNumbers(fields, numbers, "TargetQuotaIdentity");
        final var result = new TargetQuotaIdentity(
                kind,
                shard(QueryCodecSupport.fixed(fields.get(2), 3, 20)),
                QueryCodecSupport.fixed(fields.get(3), 4, 16),
                kind.hasTarget() ? new TargetPartitionId(QueryCodecSupport.fixed(fields.get(4), 5, 32)) : null,
                kind.isMirror() ? QueryCodecSupport.fixed(fields.get(kind.hasTarget() ? 5 : 4), 6, 32) : null);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 7, 32))) {
            throw new IllegalArgumentException("Target quota identity digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaIdentity");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaIdentity that
                && kind == that.kind
                && shard.equals(that.shard)
                && Objects.equals(target, that.target)
                && Arrays.equals(accountingIncarnation, that.accountingIncarnation)
                && Arrays.equals(tenantScope, that.tenantScope);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
