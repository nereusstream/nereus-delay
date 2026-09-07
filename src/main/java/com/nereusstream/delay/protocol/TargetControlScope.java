package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Stable extra send-blocking control and permit groups shared by every member of one execution domain. */
public final class TargetControlScope {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 19;
    public static final int MAX_CONTROL_GROUPS = 32;
    public static final int MAX_PERMIT_GROUPS = 32;
    public static final int MAX_FIELDS = 4 + MAX_CONTROL_GROUPS + MAX_PERMIT_GROUPS;
    public static final int MAX_CANONICAL_BYTES = 2 + 34 + 22 + MAX_CONTROL_GROUPS * 38 + MAX_PERMIT_GROUPS * 34 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-control-scope\0");

    public enum GroupKind {
        BINDING_SEND_CONTROL(1),
        TENANT_SEND_CONTROL(2),
        AUTHORIZATION_CONTROL(3);
        private final int wireValue;

        GroupKind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        private static GroupKind fromWire(final long wire) {
            for (var kind : values()) {
                if (kind.wireValue == wire) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown Target control group kind");
        }
    }

    public record ControlGroup(GroupKind kind, byte[] id) {
        public ControlGroup {
            Objects.requireNonNull(kind, "kind");
            id = TargetCompatibilityCodec.assigned(id, 32, "controlGroupId");
        }

        @Override
        public byte[] id() {
            return Bytes.copy(id);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, kind.wireValue());
                CanonicalProtobuf.bytes(out, 2, id);
            });
        }

        private static ControlGroup decode(final byte[] encoded) {
            final var fields = TargetCompatibilityCodec.read(encoded, 36, 2, false, "TargetControlGroup");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2}, "TargetControlGroup");
            final var result = new ControlGroup(
                    GroupKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                    QueryCodecSupport.fixed(fields.get(1), 2, 32));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetControlGroup");
            return result;
        }

        private int compareTo(final ControlGroup other) {
            final int kindOrder = Integer.compare(kind.wireValue(), other.kind.wireValue());
            return kindOrder != 0 ? kindOrder : Arrays.compareUnsigned(id, other.id);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ControlGroup that && kind == that.kind && Arrays.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return 31 * kind.hashCode() + Arrays.hashCode(id);
        }
    }

    private final TargetPartitionId target;
    private final ShardId sourceShard;
    private final List<ControlGroup> controls;
    private final List<byte[]> permits;
    private final byte[] digest;

    public TargetControlScope(
            final TargetPartitionId target,
            final ShardId sourceShard,
            final List<ControlGroup> controls,
            final List<byte[]> permits) {
        this.target = Objects.requireNonNull(target, "target");
        this.sourceShard = Objects.requireNonNull(sourceShard, "sourceShard");
        if (controls.size() > MAX_CONTROL_GROUPS || permits.size() > MAX_PERMIT_GROUPS) {
            throw new IllegalArgumentException("Target control/permit group count exceeds schema bound");
        }
        this.controls = List.copyOf(controls);
        if (this.controls.size() > MAX_CONTROL_GROUPS) {
            throw new IllegalArgumentException("Target control group count exceeds schema bound");
        }
        for (int n = 1; n < this.controls.size(); n++) {
            if (this.controls.get(n - 1).compareTo(this.controls.get(n)) >= 0) {
                throw new IllegalArgumentException("Target controls must be sorted and unique");
            }
        }
        final List<byte[]> copied = new ArrayList<>();
        for (byte[] permit : permits) {
            if (copied.size() == MAX_PERMIT_GROUPS) {
                throw new IllegalArgumentException("Target permit group count exceeds schema bound");
            }
            final byte[] value = TargetCompatibilityCodec.assigned(permit, 32, "permitGroupId");
            if (!copied.isEmpty() && Arrays.compareUnsigned(copied.getLast(), value) >= 0) {
                throw new IllegalArgumentException("Target permits must be sorted and unique");
            }
            copied.add(value);
        }
        this.permits = List.copyOf(copied);
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetPartitionId target() {
        return target;
    }

    public ShardId sourceShard() {
        return sourceShard;
    }

    public List<ControlGroup> controls() {
        return controls;
    }

    public List<byte[]> permits() {
        return permits.stream().map(Bytes::copy).toList();
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.controlScope(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, target.bytes());
            CanonicalProtobuf.bytes(
                    out,
                    3,
                    Bytes.concat(sourceShard.routeIncarnation().bytes(), Bytes.u32beBits(sourceShard.partition())));
            for (ControlGroup control : controls) {
                CanonicalProtobuf.bytes(out, 4, control.canonicalBytes());
            }
            for (byte[] permit : permits) {
                CanonicalProtobuf.bytes(out, 5, permit);
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 6, digest);
        });
    }

    public static TargetControlScope decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, MAX_FIELDS, true, "TargetControlScope");
        if (fields.size() < 4 || QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("missing or unknown Target control scope schema");
        }
        final var target = new TargetPartitionId(QueryCodecSupport.fixed(fields.get(1), 2, 32));
        final byte[] shard = QueryCodecSupport.fixed(fields.get(2), 3, 20);
        final List<ControlGroup> controls = new ArrayList<>();
        final List<byte[]> permits = new ArrayList<>();
        int index = 3;
        while (index < fields.size() && fields.get(index).number() == 4) {
            if (controls.size() == MAX_CONTROL_GROUPS) {
                throw new IllegalArgumentException("Target control group count exceeds schema bound");
            }
            controls.add(ControlGroup.decode(QueryCodecSupport.bytes(fields.get(index++), 4)));
        }
        while (index < fields.size() && fields.get(index).number() == 5) {
            if (permits.size() == MAX_PERMIT_GROUPS) {
                throw new IllegalArgumentException("Target permit group count exceeds schema bound");
            }
            permits.add(QueryCodecSupport.fixed(fields.get(index++), 5, 32));
        }
        if (index != fields.size() - 1) {
            throw new IllegalArgumentException("unexpected Target control scope fields");
        }
        final var result = new TargetControlScope(
                target,
                new ShardId(new RouteIncarnation(Arrays.copyOf(shard, 16)), (int) Bytes.readU32be(shard, 16)),
                controls,
                permits);
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(index), 6, 32))) {
            throw new IllegalArgumentException("Target control scope digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetControlScope");
        return result;
    }

    /** References are resolved by full bytes; control membership and current gates require source-bound authority. */
    public static TargetControlScope decodeReferenced(
            final byte[] reference, final byte[] encoded, final TargetPartitionId target, final ShardId shard) {
        final var result = decode(encoded);
        if (!Bytes.constantTimeEquals(reference, result.digest)
                || !target.equals(result.target)
                || !shard.equals(result.sourceShard)) {
            throw new IllegalArgumentException("Target control reference/target/Shard mismatch");
        }
        return result;
    }

    public static TargetControlScope decodeForStore(
            final byte[] key, final byte[] encoded, final TargetPartitionId target, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.encodedKey())
                || !target.equals(result.target)
                || !shard.equals(result.sourceShard)) {
            throw new IllegalArgumentException("Target control Store key/target/Shard mismatch");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof TargetControlScope that)
                || !target.equals(that.target)
                || !sourceShard.equals(that.sourceShard)
                || !controls.equals(that.controls)
                || permits.size() != that.permits.size()) {
            return false;
        }
        for (int n = 0; n < permits.size(); n++) {
            if (!Arrays.equals(permits.get(n), that.permits.get(n))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
