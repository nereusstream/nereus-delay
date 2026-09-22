package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Platform Target close over one frozen, complete set of affected source Shards. */
public final class TargetCloseRequest implements ControlOperationRequestBranch {
    public static final int CONTROL_KIND = 18;
    public static final int MAX_SHARDS = 4096;
    public static final int MAX_POLICY_BYTES = 512;
    public static final int MAX_CANONICAL_BYTES = 2 + 34 + MAX_SHARDS * 57 + 3 + MAX_POLICY_BYTES;
    private final TargetPartitionId target;
    private final List<ShardTarget> shards;
    private final CloseLaneRequest policy;

    public record ShardTarget(ShardId shard, byte[] accountingIncarnation, long expectedControlVersion) {
        public ShardTarget {
            Objects.requireNonNull(shard, "shard");
            accountingIncarnation =
                    TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accountingIncarnation");
            if (expectedControlVersion == 0 || expectedControlVersion == -1L) {
                throw new IllegalArgumentException("Target close requires an incrementable control version");
            }
        }

        @Override
        public byte[] accountingIncarnation() {
            return Bytes.copy(accountingIncarnation);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, accountingIncarnation);
                CanonicalProtobuf.uint64Bits(out, 3, expectedControlVersion);
            });
        }

        private static ShardTarget decode(byte[] encoded) {
            final var fields = TargetCompatibilityCodec.read(encoded, 55, 3, false, "Target close Shard");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "Target close Shard");
            final byte[] subject = QueryCodecSupport.bytes(fields.getFirst(), 1);
            TargetCompatibilityCodec.read(subject, 24, 2, false, "Target close Shard subject");
            final var result = new ShardTarget(
                    ShardSubject.decode(subject).shardId(),
                    QueryCodecSupport.fixed(fields.get(1), 2, 16),
                    QueryCodecSupport.uint64Bits(fields.get(2), 3));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target close Shard");
            return result;
        }
    }

    public TargetCloseRequest(TargetPartitionId target, List<ShardTarget> shards, CloseLaneRequest policy) {
        this.target = Objects.requireNonNull(target, "target");
        this.policy = Objects.requireNonNull(policy, "policy");
        if (shards.isEmpty() || shards.size() > MAX_SHARDS || policy.canonicalBytes().length > MAX_POLICY_BYTES) {
            throw new IllegalArgumentException("Target close requires bounded nonempty Shard coverage and policy");
        }
        this.shards = List.copyOf(shards);
        for (int n = 1; n < shards.size(); n++) {
            if (Arrays.compareUnsigned(
                            new ShardSubject(shards.get(n - 1).shard()).canonicalBytes(),
                            new ShardSubject(shards.get(n).shard()).canonicalBytes())
                    >= 0) {
                throw new IllegalArgumentException("Target close Shards must be sorted and unique");
            }
        }
    }

    public TargetPartitionId target() {
        return target;
    }

    public List<ShardTarget> shards() {
        return shards;
    }

    public CloseLaneRequest policy() {
        return policy;
    }

    public ControlOperationKind operationKind() {
        return ControlOperationKind.CLOSE_TARGET;
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operationKind(), this);
    }

    public ShardTarget requireControlRef(ControlRef ref) {
        TargetCompatibilityCodec.assigned(ref.operationId(), 32, "TargetClose operation ID");
        if (ref.targetIndex() < 0
                || ref.targetIndex() >= shards.size()
                || !Arrays.equals(
                        ref.requestHash(), PreparedControlOperation.requestHash(operationKind(), operationRequest()))) {
            throw new IllegalArgumentException("Target close ControlRef request/index mismatch");
        }
        return shards.get((int) ref.targetIndex());
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, target.bytes());
            for (var shard : shards) {
                CanonicalProtobuf.bytes(out, 3, shard.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 4, policy.canonicalBytes());
        });
    }

    public static TargetCloseRequest decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, MAX_SHARDS + 3, true, "Target close request");
        if (fields.size() < 4 || QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("missing or unsupported Target close request version");
        }
        final var shards = new ArrayList<ShardTarget>();
        int n = 2;
        while (n < fields.size() && fields.get(n).number() == 3) {
            shards.add(ShardTarget.decode(QueryCodecSupport.bytes(fields.get(n++), 3)));
        }
        if (n != fields.size() - 1) {
            throw new IllegalArgumentException("unknown Target close request field");
        }
        final byte[] policy = QueryCodecSupport.bytes(fields.get(n), 4);
        TargetCompatibilityCodec.read(policy, MAX_POLICY_BYTES, 4, false, "Target close policy");
        final var result = new TargetCloseRequest(
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(1), 2, 32)),
                shards,
                CloseLaneRequest.decode(policy));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target close request");
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetCloseRequest that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
