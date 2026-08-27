package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Current Oxia value for one handoff policy scope. */
public final class HandoffPolicyHead {
    public static final int SCHEMA_GENERATION = 1;
    public static final int HASH_LENGTH = 32;
    private static final String HASH_DOMAIN = "nereus-delay-handoff-policy-head\0";

    private final byte[] scopeDigest;
    private final long generation;
    private final HandoffPolicyMode mode;
    private final HandoffPolicySnapshot snapshot;
    private final long effectiveDisabledAfterEpochMs;
    private final byte[] headDigest;

    public HandoffPolicyHead(
            final byte[] scopeDigest,
            final long generation,
            final HandoffPolicyMode mode,
            final HandoffPolicySnapshot snapshot,
            final long effectiveDisabledAfterEpochMs) {
        this.scopeDigest = fixed(scopeDigest, "scopeDigest");
        if (generation == 0) {
            throw new IllegalArgumentException("head generation must be non-zero");
        }
        this.generation = generation;
        this.mode = Objects.requireNonNull(mode, "mode");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        if (!Arrays.equals(this.scopeDigest, snapshot.policyScopeDigest())
                || this.generation != snapshot.generation()
                || this.mode != snapshot.mode()) {
            throw new IllegalArgumentException("policy head does not match its snapshot");
        }
        if (effectiveDisabledAfterEpochMs < 0) {
            throw new IllegalArgumentException("effectiveDisabledAfterEpochMs must be non-negative");
        }
        this.effectiveDisabledAfterEpochMs = effectiveDisabledAfterEpochMs;
        this.headDigest = computeDigest();
    }

    private HandoffPolicyHead(
            final byte[] scopeDigest,
            final long generation,
            final HandoffPolicyMode mode,
            final HandoffPolicySnapshot snapshot,
            final long effectiveDisabledAfterEpochMs,
            final byte[] headDigest) {
        this(scopeDigest, generation, mode, snapshot, effectiveDisabledAfterEpochMs);
        Bytes.requireLength(headDigest, HASH_LENGTH, "headDigest");
        if (!Bytes.constantTimeEquals(this.headDigest, headDigest)) {
            throw new IllegalArgumentException("HandoffPolicyHead digest mismatch");
        }
    }

    public byte[] scopeDigest() {
        return Bytes.copy(scopeDigest);
    }

    public long generation() {
        return generation;
    }

    public HandoffPolicyMode mode() {
        return mode;
    }

    public HandoffPolicySnapshot snapshot() {
        return snapshot;
    }

    public long effectiveDisabledAfterEpochMs() {
        return effectiveDisabledAfterEpochMs;
    }

    public byte[] headDigest() {
        return Bytes.copy(headDigest);
    }

    public HandoffPolicyHeadRef ref(final long oxiaVersion) {
        return new HandoffPolicyHeadRef(scopeDigest, generation, snapshot.snapshotDigest(), oxiaVersion);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsOneToSix(output);
            CanonicalProtobuf.bytes(output, 7, headDigest);
        });
    }

    public static HandoffPolicyHead decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "HandoffPolicyHead");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "HandoffPolicyHead");
        if (QueryCodecSupport.uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported HandoffPolicyHead generation");
        }
        final byte[] scope = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final long generation = QueryCodecSupport.uint64Bits(fields.get(2), 3);
        final HandoffPolicyMode mode = HandoffPolicyMode.fromWire(QueryCodecSupport.uint(fields.get(3), 4));
        final HandoffPolicySnapshot snapshot = HandoffPolicySnapshot.decode(QueryCodecSupport.nested(fields.get(4), 5));
        final long disabledAfter = QueryCodecSupport.uint(fields.get(5), 6);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH);
        final HandoffPolicyHead result =
                new HandoffPolicyHead(scope, generation, mode, snapshot, disabledAfter, digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "HandoffPolicyHead");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof HandoffPolicyHead that
                && generation == that.generation
                && mode == that.mode
                && effectiveDisabledAfterEpochMs == that.effectiveDisabledAfterEpochMs
                && snapshot.equals(that.snapshot)
                && Arrays.equals(scopeDigest, that.scopeDigest)
                && Arrays.equals(headDigest, that.headDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(scopeDigest),
                generation,
                mode,
                snapshot,
                effectiveDisabledAfterEpochMs,
                Arrays.hashCode(headDigest));
    }

    private byte[] computeDigest() {
        return Bytes.sha256(Bytes.utf8(HASH_DOMAIN), canonicalFieldsOneToSix());
    }

    private byte[] canonicalFieldsOneToSix() {
        return CanonicalProtobuf.message(this::writeFieldsOneToSix);
    }

    private void writeFieldsOneToSix(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
        CanonicalProtobuf.bytes(output, 2, scopeDigest);
        CanonicalProtobuf.uint64Bits(output, 3, generation);
        CanonicalProtobuf.uint32(output, 4, mode.wireValue());
        CanonicalProtobuf.bytes(output, 5, snapshot.canonicalBytes());
        CanonicalProtobuf.int64(output, 6, effectiveDisabledAfterEpochMs);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }
}
