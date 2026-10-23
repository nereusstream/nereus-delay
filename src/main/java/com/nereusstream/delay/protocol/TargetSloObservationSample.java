package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Certification binding around unchanged v1 bytes; all three retained projections remain charged. */
public record TargetSloObservationSample(TargetQuotaScope scope, byte[] lineage, long generation,
        byte[] enableId, TargetQuotaMutation enabledAt, byte[] policyDigest,
        TargetSloObservationPolicy.Declaration declaration, TargetResultRecord first,
        SloObservationOutbox observation, TargetQuotaMutation mutation) {
    public static final int VALUE_TYPE = 59;
    // The v1 Final has fixed hashes, enum tags, uint64s and one bounded endpoint. No variable text.
    public static final int MAX_FINAL_BYTES = 2 + 34 + 34 + 2 + 2 + 11 + 11 + 2
            + (2 + 2 + 11 + 11 + 34) + 34 + 11 + 34;
    public static final int MAX_START_BYTES = TargetSourcePosition.MAX_CANONICAL_BYTES + 256;
    // Both permitted Source encodings have fixed width within the frozen physical identity.
    // A later Source mutation can grow only its uint64 sequence, from one to ten value bytes.
    public static final long MAX_FUTURE_STORED_GROWTH = 3L * (MAX_FINAL_BYTES + 6L + 9L);
    public static final int MAX_CANONICAL_BYTES = TargetQuotaScope.MAX_CANONICAL_BYTES
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + TargetResultRecord.MAX_CANONICAL_BYTES
            + TargetSloObservationPolicy.Declaration.MAX_CANONICAL_BYTES + MAX_START_BYTES + MAX_FINAL_BYTES + 384;
    private static final byte[] BINDING_DOMAIN = Bytes.utf8("nereus-delay-target-slo-sample-binding\0");
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-sample\0");

    public TargetSloObservationSample {
        Objects.requireNonNull(scope); Objects.requireNonNull(enabledAt); Objects.requireNonNull(declaration);
        Objects.requireNonNull(first); Objects.requireNonNull(observation); Objects.requireNonNull(mutation);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "Target SLO sample lineage");
        enableId = TargetCompatibilityCodec.assigned(enableId, 32, "original Target SLO enable ID");
        policyDigest = TargetCompatibilityCodec.assigned(policyDigest, 32, "original Target SLO policy");
        enabledAt.requireSourceApplied(); mutation.requireSourceApplied();
        if (scope.target() != null || generation == 0 || !scope.shard().equals(first.primaryIdentity().shard())
                || !Arrays.equals(scope.tenantScope(), first.tenantScope())
                || !Arrays.equals(lineage, first.recoveryLineage())
                || first.kind() != TargetResultRecord.Kind.COMMAND
                || declaration.objective().name() != SloObjectiveName.COMMAND_APPLIED_LATENCY
                || declaration.path() != SloPath.NOT_APPLICABLE) {
            throw new IllegalArgumentException("Target SLO sample schema1 requires its original Command Source scope");
        }
        first.mutation().requireAfter(enabledAt); first.mutation().requireAtOrBefore(mutation);
        if (!enabledAt.source().sameSourceIdentity(first.mutation().source())
                || !enabledAt.source().sameSourceIdentity(mutation.source())) throw new IllegalArgumentException("Target SLO sample crossed physical Source");
        final var expected = SloAuthoritativeStartFactory.commandApplied(declaration.objective(), first.mutation().source());
        if (!Arrays.equals(expected.canonicalBytes(), observation.start().canonicalBytes())
                || observation.start().canonicalBytes().length > MAX_START_BYTES
                || first.canonicalBytes().length > TargetResultRecord.MAX_CANONICAL_BYTES
                || observation.finalObservation() != null && observation.finalObservation().canonicalBytes().length > MAX_FINAL_BYTES) {
            throw new IllegalArgumentException("Target SLO sample changed its original v1 event/Start");
        }
        observation.start().validateAgainst(declaration.objective());
        if (observation.finalObservation() != null) observation.finalObservation().validateAgainst(observation.start());
    }

    @Override public byte[] lineage() { return Bytes.copy(lineage); }
    @Override public byte[] enableId() { return Bytes.copy(enableId); }
    @Override public byte[] policyDigest() { return Bytes.copy(policyDigest); }
    public byte[] key() { return key(observation.sampleId()); }
    public static byte[] key(byte[] sampleId) {
        Bytes.requireLength(sampleId, 32, "original v1 sample ID");
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_SAMPLE_TAG, TargetKeyCodec.KEY_FORMAT}, sampleId);
    }
    public byte[] positionKey() {
        return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, first.mutation().source().canonicalBytes());
    }
    public static byte[] referencePrefix(byte[] originalResultKey) {
        return Bytes.concat(new byte[]{TargetKeyCodec.SLO_SAMPLE_REFERENCE_TAG, TargetKeyCodec.KEY_FORMAT}, Bytes.sha256(originalResultKey));
    }
    private byte[] referenceKey(byte[] originalResultKey) { return Bytes.concat(referencePrefix(originalResultKey), observation.sampleId()); }
    public List<byte[]> keys() { return List.of(key(), referenceKey(first.key()), referenceKey(positionKey())); }
    public boolean references(byte[] resultKey) { return Arrays.equals(resultKey,first.key()) || Arrays.equals(resultKey,positionKey()); }
    public boolean primary(byte[] key) { return Arrays.equals(key, key()); }
    /** Complete retained Native key/value bytes, including both actual Source reference projections. */
    public long storedBytes() {
        final int valueBytes = TargetValueEnvelope.encode(VALUE_TYPE, canonicalBytes()).length;
        long bytes = 0;
        for (var key : keys()) bytes = Math.addExact(bytes, Math.addExact((long) key.length, valueBytes));
        return bytes;
    }
    /** Reserves bounded v1 Final growth before accepting a Start; no declaration cap is raised. */
    public long maximumStoredBytes() {
        final long growth = maximumGrowth();
        return Math.addExact(storedBytes(), Math.multiplyExact(3L, growth));
    }
    public long maximumRecordBytes() {
        return Math.addExact((long) TargetValueEnvelope.encode(VALUE_TYPE, canonicalBytes()).length,
                Math.addExact(maximumGrowth(), keys().stream().mapToInt(k -> k.length).max().orElseThrow()));
    }
    private long maximumGrowth() {
        return MAX_FINAL_BYTES + 6L + 9L;
    }
    private byte[] bindingFields() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
        CanonicalProtobuf.bytes(out, 3, lineage); CanonicalProtobuf.uint64Bits(out, 4, generation);
        CanonicalProtobuf.bytes(out, 5, enableId); CanonicalProtobuf.bytes(out, 6, enabledAt.canonicalBytes());
        CanonicalProtobuf.bytes(out, 7, policyDigest); CanonicalProtobuf.bytes(out, 8, declaration.canonicalBytes());
        CanonicalProtobuf.bytes(out, 9, first.canonicalBytes());
    }); }
    public byte[] bindingDigest() { return Bytes.sha256(BINDING_DOMAIN, bindingFields(), observation.start().canonicalBytes()); }
    private byte[] fields() { return Bytes.concat(bindingFields(), CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.bytes(out, 10, observation.canonicalBytes()); CanonicalProtobuf.bytes(out, 11, mutation.canonicalBytes());
        CanonicalProtobuf.bytes(out, 12, bindingDigest());
    })); }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() {
        final var value = Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 13, digest())));
        if (value.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Target SLO sample exceeds encoded bound");
        return value;
    }
    public static TargetSloObservationSample decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 13, false, "Target SLO sample");
        QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,4,5,6,7,8,9,10,11,12,13}, "Target SLO sample");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO sample schema");
        final var result = new TargetSloObservationSample(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1),2)),
                QueryCodecSupport.fixed(f.get(2),3,16), QueryCodecSupport.uint64Bits(f.get(3),4), QueryCodecSupport.fixed(f.get(4),5,32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5),6)), QueryCodecSupport.fixed(f.get(6),7,32),
                TargetSloObservationPolicy.Declaration.decode(QueryCodecSupport.bytes(f.get(7),8)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(8),9)), SloObservationOutbox.decode(QueryCodecSupport.bytes(f.get(9),10)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10),11)));
        if (!Arrays.equals(result.bindingDigest(), QueryCodecSupport.fixed(f.get(11),12,32))
                || !Arrays.equals(result.digest(), QueryCodecSupport.fixed(f.get(12),13,32))) throw new IllegalArgumentException("Target SLO sample binding/content digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO sample"); return result;
    }
    public static TargetSloObservationSample decodeForStore(byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var value = decode(encoded);
        if (!scope.equals(value.scope()) || !Arrays.equals(lineage, value.lineage())
                || value.keys().stream().noneMatch(k -> Arrays.equals(k,key))) throw new IllegalArgumentException("Target SLO sample changed key/scope/lineage");
        return value;
    }
}
