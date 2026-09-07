package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One independently authorized common Native policy per actual Target execution/control domain. */
public final class TargetNativePolicyScope {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 24;
    public static final int MAX_CANONICAL_BYTES = 2
            + 34
            + 34
            + 22
            + 34
            + 18
            + 4
            + 11
            + 34
            + 34
            + 10
            + 2
            + 3
            + TargetNativeArtifactSet.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-native-policy-scope\0");
    private final byte[] authorityNamespace;
    private final byte[] controlResourceScope;
    private final ShardId sourceShard;
    private final TargetPartitionId target;
    private final byte[] accountingIncarnation;
    private final TargetKeyCodec.Domain domain;
    private final byte[] dispatchRef;
    private final byte[] controlRef;
    private final long fixedLeadCapMs;
    private final TargetNativeArtifactSet artifacts;
    private final byte[] digest;

    public TargetNativePolicyScope(
            final byte[] authorityNamespace,
            final byte[] controlResourceScope,
            final ShardId sourceShard,
            final TargetPartitionId target,
            final byte[] accountingIncarnation,
            final TargetKeyCodec.Domain domain,
            final byte[] dispatchRef,
            final byte[] controlRef,
            final long fixedLeadCapMs,
            final TargetNativeArtifactSet artifacts) {
        this.authorityNamespace = TargetCompatibilityCodec.assigned(authorityNamespace, 32, "authorityNamespace");
        this.controlResourceScope = TargetCompatibilityCodec.assigned(controlResourceScope, 32, "controlResourceScope");
        this.sourceShard = Objects.requireNonNull(sourceShard, "sourceShard");
        this.target = Objects.requireNonNull(target, "target");
        this.accountingIncarnation =
                TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accountingIncarnation");
        this.domain = Objects.requireNonNull(domain, "domain");
        this.dispatchRef = TargetCompatibilityCodec.assigned(dispatchRef, 32, "dispatchRef");
        this.controlRef = TargetCompatibilityCodec.assigned(controlRef, 32, "controlRef");
        if (fixedLeadCapMs <= 0 || domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("common Native scope requires a positive fixed cap");
        }
        this.fixedLeadCapMs = fixedLeadCapMs;
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        digest = Bytes.sha256(HASH_DOMAIN, fields());
    }

    public byte[] authorityNamespace() {
        return Bytes.copy(authorityNamespace);
    }

    public byte[] controlResourceScope() {
        return Bytes.copy(controlResourceScope);
    }

    public ShardId sourceShard() {
        return sourceShard;
    }

    public TargetPartitionId target() {
        return target;
    }

    public byte[] accountingIncarnation() {
        return Bytes.copy(accountingIncarnation);
    }

    public TargetKeyCodec.Domain domain() {
        return domain;
    }

    public byte[] dispatchRef() {
        return Bytes.copy(dispatchRef);
    }

    public byte[] controlRef() {
        return Bytes.copy(controlRef);
    }

    public long fixedLeadCapMs() {
        return fixedLeadCapMs;
    }

    public int allowedPathBits() {
        return HandoffPath.MANAGED_HANDOFF;
    }

    public TargetNativeArtifactSet artifacts() {
        return artifacts;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.nativePolicyScope(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, authorityNamespace);
            CanonicalProtobuf.bytes(out, 3, controlResourceScope);
            CanonicalProtobuf.bytes(
                    out,
                    4,
                    Bytes.concat(sourceShard.routeIncarnation().bytes(), Bytes.u32beBits(sourceShard.partition())));
            CanonicalProtobuf.bytes(out, 5, target.bytes());
            CanonicalProtobuf.bytes(out, 6, accountingIncarnation);
            CanonicalProtobuf.uint32(out, 7, domain.slot());
            CanonicalProtobuf.uint64Bits(out, 8, domain.generation());
            CanonicalProtobuf.bytes(out, 9, dispatchRef);
            CanonicalProtobuf.bytes(out, 10, controlRef);
            CanonicalProtobuf.uint64Bits(out, 11, fixedLeadCapMs);
            CanonicalProtobuf.uint32(out, 12, allowedPathBits());
            CanonicalProtobuf.bytes(out, 13, artifacts.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 14, digest);
        });
    }

    public static TargetNativePolicyScope decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 14, false, "Target Native scope");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, "Target Native scope");
        if (QueryCodecSupport.uint32(f.get(0), 1) != VERSION
                || QueryCodecSupport.uint32(f.get(11), 12) != HandoffPath.MANAGED_HANDOFF) {
            throw new IllegalArgumentException("unknown Target Native scope generation or managed path");
        }
        final var result = new TargetNativePolicyScope(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                decodeShard(QueryCodecSupport.fixed(f.get(3), 4, 20)),
                new TargetPartitionId(QueryCodecSupport.fixed(f.get(4), 5, 32)),
                QueryCodecSupport.fixed(f.get(5), 6, 16),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(f.get(6), 7), QueryCodecSupport.uint64Bits(f.get(7), 8)),
                QueryCodecSupport.fixed(f.get(8), 9, 32),
                QueryCodecSupport.fixed(f.get(9), 10, 32),
                QueryCodecSupport.uint(f.get(10), 11),
                TargetNativeArtifactSet.decode(QueryCodecSupport.bytes(f.get(12), 13)));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(f.get(13), 14, 32))) {
            throw new IllegalArgumentException("Target Native scope digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Native scope");
        return result;
    }

    public static TargetNativePolicyScope decodeForStore(final byte[] key, final byte[] encoded, final ShardId shard) {
        final var scope = decode(encoded);
        if (!Arrays.equals(key, scope.encodedKey()) || !shard.equals(scope.sourceShard)) {
            throw new IllegalArgumentException("Target Native scope key/Shard mismatch");
        }
        return scope;
    }

    /** Integrity and projection only; authenticated native membership and live execution gates are separate. */
    public void requireReferences(
            final CanonicalTargetPartition physical,
            final TargetDispatchCompatibility dispatch,
            final TargetControlScope controls) {
        dispatch.requireTargetProjection(physical);
        if (physical.resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                || !target.equals(physical.id())
                || !Arrays.equals(dispatchRef, dispatch.digest())
                || !Arrays.equals(controlRef, controls.digest())
                || !controls.target().equals(target)
                || !controls.sourceShard().equals(sourceShard)
                || !TimingCapability.includes(
                        dispatch.capability().timingCapabilityBits(), TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF)) {
            throw new IllegalArgumentException(
                    "Target Native scope lacks its exact managed execution/control contracts");
        }
    }

    public void requireQueue(final TargetQueueState queue) {
        if (!target.equals(queue.targetId())
                || !Arrays.equals(accountingIncarnation, queue.accountingIncarnation())
                || fixedLeadCapMs != queue.nativeIndexLeadCapMs()
                || domain.slot() >= queue.domains().size()) {
            throw new IllegalArgumentException("Target Native scope queue/cap/incarnation mismatch");
        }
        final var state = queue.domains().get(domain.slot());
        if (!domain.equals(state.domain())
                || state.lifecycle() == TargetDomainState.Lifecycle.VACANT
                || !Arrays.equals(dispatchRef, state.dispatchCompatibilityRef())
                || !Arrays.equals(controlRef, state.controlScopeRef())
                || !Arrays.equals(digest, state.nativePolicyScopeRef())) {
            throw new IllegalArgumentException("Target Native scope differs from the exact domain");
        }
    }

    public void requireBinding(final TargetScheduleBinding binding) {
        if (!sourceShard.equals(binding.bindingSource().shardId())
                || !target.equals(binding.target())
                || !domain.equals(binding.domain())
                || !Arrays.equals(accountingIncarnation, binding.accountingIncarnation())
                || !Arrays.equals(dispatchRef, binding.offeredDispatchRef())
                || !Arrays.equals(controlRef, binding.controlScopeRef())
                || !Arrays.equals(digest, binding.nativePolicyScopeRef())) {
            throw new IllegalArgumentException("binding does not carry the exact common Native scope");
        }
    }

    private static ShardId decodeShard(final byte[] raw) {
        return new ShardId(new RouteIncarnation(Arrays.copyOf(raw, 16)), (int) Bytes.readU32be(raw, 16));
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetNativePolicyScope that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
