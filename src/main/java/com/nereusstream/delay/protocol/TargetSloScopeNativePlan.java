package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Frozen finite logical obligations. Source frame bounds do not prove physical Source funding. */
public final class TargetSloScopeNativePlan {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
            + TargetSourcePosition.MAX_CANONICAL_BYTES + 160;
    /** Existing registry birth/entry wrappers are smaller than this bound, including protobuf lengths. */
    public static final int MAX_REGISTRY_WRAPPER_BYTES = 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-scope-native-plan\0");

    private final TargetQuotaIncarnation originalRoot;
    private final SourcePosition sourceAnchor;
    private final long maximumSourceOccurrences;
    private final long maximumSourceFrameBytes;
    private final long maximumSamples;
    private final int maximumWriterEpochs;
    private final int maximumGenerations;
    private final byte[] digest;

    public TargetSloScopeNativePlan(TargetQuotaIncarnation originalRoot, SourcePosition sourceAnchor,
            long maximumSourceOccurrences, long maximumSourceFrameBytes, long maximumSamples,
            int maximumWriterEpochs, int maximumGenerations) {
        this.originalRoot = Objects.requireNonNull(originalRoot);
        this.sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
        if (originalRoot.identity().kind() != TargetQuotaIdentity.Kind.SHARD || originalRoot.draining()
                || originalRoot.canonicalBytes().length > TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
                || maximumSourceOccurrences < 1 || maximumSourceOccurrences == Long.MAX_VALUE
                || maximumSourceFrameBytes < 1 || maximumSourceFrameBytes == Long.MAX_VALUE
                || maximumSamples < 0 || maximumSamples == Long.MAX_VALUE
                || maximumWriterEpochs < 0 || maximumGenerations < 0)
            throw new IllegalArgumentException("Scope plan requires its actual OPEN SHARD Root and finite Source/sample/Control limits");
        this.maximumSourceOccurrences = maximumSourceOccurrences;
        this.maximumSourceFrameBytes = maximumSourceFrameBytes;
        this.maximumSamples = maximumSamples;
        this.maximumWriterEpochs = maximumWriterEpochs;
        this.maximumGenerations = maximumGenerations;
        // The independent logical Source ceiling must itself fit the local checked representation.
        Math.multiplyExact(maximumSourceOccurrences, maximumSourceFrameBytes);
        requireSource(originalRoot.allocation().source());
        if (sourceAnchor.compareTo(originalRoot.allocation().source()) < 0)
            throw new IllegalArgumentException("Scope plan anchor precedes its original Root allocation");
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Scope Native plan exceeds its closed codec bound");
    }

    public TargetQuotaIncarnation originalRoot() { return originalRoot; }
    public TargetQuotaScope scope() { return originalRoot.scope(); }
    public byte[] lineage() { return originalRoot.recoveryLineage(); }
    public TargetQuotaAccounting accounting() { return originalRoot.accounting(); }
    public SourcePosition sourceAnchor() { return sourceAnchor; }
    /** Every possible physical position already includes SDK copies, duplicate, rejected and expired inputs. */
    public long maximumSourceOccurrences() { return maximumSourceOccurrences; }
    /** Complete encodeFrame logical bytes, with no claim about broker units, copies or retention. */
    public long maximumSourceFrameBytes() { return maximumSourceFrameBytes; }
    public long maximumSamples() { return maximumSamples; }
    public int maximumWriterEpochs() { return maximumWriterEpochs; }
    public int maximumGenerations() { return maximumGenerations; }
    public byte[] digest() { return Bytes.copy(digest); }

    public void requireRoot(TargetQuotaIncarnation current) {
        if (current == null || !originalRoot.identity().equals(current.identity())
                || !Arrays.equals(originalRoot.tenantScope(), current.tenantScope())
                || !originalRoot.accounting().equals(current.accounting())
                || !Arrays.equals(originalRoot.recoveryLineage(), current.recoveryLineage())
                || !originalRoot.allocation().equals(current.allocation()))
            throw new IllegalArgumentException("Scope plan changed its frozen Root/tenant/accounting/lineage/origin");
    }
    public void requireSource(SourcePosition source) {
        TargetSourcePosition.requireBounded(source);
        if (!scope().shard().equals(source.shardId()) || !sourceAnchor.sameSourceIdentity(source))
            throw new IllegalArgumentException("Scope plan changed its original physical Source");
    }
    public void requireHeader(TargetSloControlScopeHeader header) {
        final var exact = Objects.requireNonNull(header);
        requireSource(exact.sourceAnchor());
        if (!scope().equals(exact.scope()) || !Arrays.equals(lineage(), exact.lineage())
                || !Arrays.equals(digest, exact.resourceContractDigest())
                || maximumWriterEpochs > exact.limits().maximumSlots()
                || maximumGenerations > exact.limits().maximumSlots()
                || Math.addExact((long) maximumWriterEpochs, maximumGenerations) > exact.limits().maximumSlots())
            throw new IllegalArgumentException("Scope header changed its frozen plan, Source anchor or finite Control slots");
    }

    /**
     * Derives existing Native KV and logical external-registry charges from frozen accounting and codec maxima.
     * It deliberately grants no physical Source/registry authority and exposes no future-to-actual settlement.
     */
    public CapacityVector futureNative(TargetSloControlScopeHeader header) {
        requireHeader(header);
        final var a = accounting();
        CapacityVector future = multiply(a.recordCharge(TargetQuotaAccounting.RecordClass.RESULT,
                34, TargetResultRecord.MAX_CANONICAL_BYTES), maximumSourceOccurrences);
        // Rejected authors can reach the global Result ceiling, even outside the successful SERVICE aliases.
        future = future.add(multiply(a.recordCharge(TargetQuotaAccounting.RecordClass.EVIDENCE,
                Math.addExact(2L, TargetQuotaBookkeeping.maximumSourceBytes(sourceAnchor)),
                TargetResultRecord.MAX_CANONICAL_BYTES), maximumSourceOccurrences));
        final var sample = a.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 34,
                TargetSloObservationSample.MAX_CANONICAL_BYTES).add(multiply(a.recordCharge(
                        TargetQuotaAccounting.RecordClass.STATE, 66, TargetSloObservationSample.MAX_CANONICAL_BYTES), 3));
        future = future.add(multiply(sample, maximumSamples));
        future = future.add(multiply(a.recordCharge(TargetQuotaAccounting.RecordClass.STATE,
                TargetSloWriterEpoch.key(scope(), new byte[32], 1).length, TargetSloWriterEpoch.MAX_CANONICAL_BYTES), maximumWriterEpochs));
        future = future.add(multiply(a.recordCharge(TargetQuotaAccounting.RecordClass.STATE,
                TargetSloObservationGeneration.key(scope(), 1).length, TargetSloObservationGeneration.MAX_CANONICAL_BYTES), maximumGenerations));
        if (maximumGenerations > 0) future = future.add(a.recordCharge(TargetQuotaAccounting.RecordClass.STATE,
                TargetSloObservationState.key(scope()).length, TargetSloObservationState.MAX_CANONICAL_BYTES));
        final var event = a.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 42, TargetSloObservationEventRecord.MAX_CANONICAL_BYTES);
        final var custody = a.recordCharge(TargetQuotaAccounting.RecordClass.STATE, 66, TargetSloCustodyRecord.MAX_CANONICAL_BYTES);
        future = future.add(multiply(event.amount(CapacityDimension.LOGICAL_STATE_BYTES)
                >= custody.amount(CapacityDimension.LOGICAL_STATE_BYTES) ? event : custody, maximumSourceOccurrences));

        final long prefixBytes = Bytes.utf8(header.registryKeyPrefix()).length;
        final long baseBytes = Math.addExact(prefixBytes, Bytes.utf8("/e4-scope/").length + 64L);
        future = future.add(a.externalEvidenceCharge(Math.addExact(baseBytes, 5), TargetSloControlScopeRoot.maximumRootReservation(header)));
        future = future.add(a.externalEvidenceCharge(Math.addExact(baseBytes, 6),
                Math.addExact(header.canonicalBytes().length, MAX_REGISTRY_WRAPPER_BYTES)));
        final long operationKeyBytes = Math.addExact(prefixBytes, Bytes.utf8("/operation/").length + 64L);
        // All slots, including PENDING, retain their complete key, wrapper and accounting overhead.
        future = future.add(multiply(a.externalEvidenceCharge(operationKeyBytes, MAX_REGISTRY_WRAPPER_BYTES), header.limits().maximumSlots()));
        final long[] entryPayload = new long[CapacityDimension.COUNT];
        entryPayload[CapacityDimension.EVIDENCE_BYTES.wireValue() - 1] = header.limits().maximumEntriesBytes();
        future = future.add(new CapacityVector(entryPayload));
        return requireFiniteNative(future);
    }

    static CapacityVector multiply(CapacityVector vector, long count) {
        if (count < 0) throw new IllegalArgumentException("negative Scope obligation count");
        final long[] amounts = vector.amounts();
        for (int i = 0; i < amounts.length; i++) amounts[i] = Math.multiplyExact(amounts[i], count);
        return new CapacityVector(amounts);
    }
    static CapacityVector requireFiniteNative(CapacityVector vector) {
        Objects.requireNonNull(vector); new TargetQuotaUsage(vector, 0, 0, 0, 0);
        for (var dimension : CapacityDimension.values()) if (vector.amount(dimension) == Long.MAX_VALUE)
            throw new IllegalArgumentException("Scope Native obligations must be finite");
        return vector;
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, originalRoot.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, sourceAnchor.canonicalBytes());
            CanonicalProtobuf.uint64(out, 4, maximumSourceOccurrences); CanonicalProtobuf.uint64(out, 5, maximumSourceFrameBytes);
            CanonicalProtobuf.uint64(out, 6, maximumSamples); CanonicalProtobuf.uint32(out, 7, maximumWriterEpochs);
            CanonicalProtobuf.uint32(out, 8, maximumGenerations);
        });
    }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 9, digest); }); }
    public static TargetSloScopeNativePlan decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Scope Native plan");
        QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, "Scope Native plan");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope Native plan schema");
        final var root = QueryCodecSupport.bytes(f.get(1), 2);
        if (root.length > TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES) throw new IllegalArgumentException("Scope plan Root exceeds allocation bound");
        final var value = new TargetSloScopeNativePlan(TargetQuotaIncarnation.decode(root),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(2), 3)), QueryCodecSupport.uint64Bits(f.get(3), 4),
                QueryCodecSupport.uint64Bits(f.get(4), 5), QueryCodecSupport.uint64Bits(f.get(5), 6),
                QueryCodecSupport.uint32(f.get(6), 7), QueryCodecSupport.uint32(f.get(7), 8));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(8), 9, 32))) throw new IllegalArgumentException("Scope Native plan digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope Native plan"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetSloScopeNativePlan that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
