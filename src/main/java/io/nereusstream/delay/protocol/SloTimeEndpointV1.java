package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical bounded time interval used by a durable SLO sample endpoint. */
public final class SloTimeEndpointV1 {
    public static final int HASH_LENGTH = 32;

    private final SloTimeEndpointKindV1 kind;
    private final long earliestEpochMs;
    private final long latestEpochMs;
    private final byte[] evidenceSha256;

    public SloTimeEndpointV1(final SloTimeEndpointKindV1 kind, final long earliestEpochMs,
                              final long latestEpochMs, final byte[] evidenceSha256) {
        this.kind = Objects.requireNonNull(kind, "kind");
        if (earliestEpochMs < 0 || latestEpochMs < earliestEpochMs) {
            throw new IllegalArgumentException("SLO endpoint interval is invalid");
        }
        this.earliestEpochMs = earliestEpochMs;
        this.latestEpochMs = latestEpochMs;
        Bytes.requireLength(evidenceSha256, HASH_LENGTH, "evidenceSha256");
        this.evidenceSha256 = Bytes.copy(evidenceSha256);
    }

    public SloTimeEndpointKindV1 kind() {
        return kind;
    }

    public long earliestEpochMs() {
        return earliestEpochMs;
    }

    public long latestEpochMs() {
        return latestEpochMs;
    }

    public byte[] evidenceSha256() {
        return Bytes.copy(evidenceSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.int64(output, 2, earliestEpochMs);
            CanonicalProtobuf.int64(output, 3, latestEpochMs);
            CanonicalProtobuf.bytes(output, 4, evidenceSha256);
        });
    }

    public static SloTimeEndpointV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloTimeEndpointV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "SloTimeEndpointV1");
        final SloTimeEndpointV1 result = new SloTimeEndpointV1(
                SloTimeEndpointKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                nonNegative(QueryCodecSupport.uint(fields.get(1), 2), "earliestEpochMs"),
                nonNegative(QueryCodecSupport.uint(fields.get(2), 3), "latestEpochMs"),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloTimeEndpointV1");
        return result;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " exceeds the local signed range");
        }
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloTimeEndpointV1 that
                && kind == that.kind
                && earliestEpochMs == that.earliestEpochMs
                && latestEpochMs == that.latestEpochMs
                && Arrays.equals(evidenceSha256, that.evidenceSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, earliestEpochMs, latestEpochMs, Arrays.hashCode(evidenceSha256));
    }
}
