package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Canonical public query error projection with a closed retry-at rule. */
public record PublicQueryErrorV1(StableCode code, Long retryAtEpochMs) implements QueryResponseBranchV1 {
    public PublicQueryErrorV1 {
        Objects.requireNonNull(code, "code");
        if (!isQueryError(code)) {
            throw new IllegalArgumentException("stable code is not a public query error: " + code);
        }
        if (code == StableCode.SHARD_TRANSITIONING) {
            if (retryAtEpochMs == null || retryAtEpochMs < 0) {
                throw new IllegalArgumentException("SHARD_TRANSITIONING requires non-negative retryAt");
            }
        } else if (retryAtEpochMs != null) {
            throw new IllegalArgumentException("only SHARD_TRANSITIONING may carry retryAt");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, code.wireValue());
            if (retryAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 2, retryAtEpochMs);
            }
        });
    }

    public static PublicQueryErrorV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded);
        if (fields.isEmpty()
                || fields.size() > 2
                || fields.get(0).number() != 1
                || fields.get(0).wireType() != 0
                || (fields.size() == 2 && fields.get(1).number() != 2)) {
            throw new IllegalArgumentException("invalid PublicQueryErrorV1 fields");
        }
        final StableCode code = StableCode.fromWire(QueryCodecSupport.uint32(fields.get(0), 1));
        final Long retryAt = fields.size() == 2 ? QueryCodecSupport.uint(fields.get(1), 2) : null;
        final PublicQueryErrorV1 result = new PublicQueryErrorV1(code, retryAt);
        if (!java.util.Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical PublicQueryErrorV1");
        }
        return result;
    }

    public static boolean isQueryError(final StableCode code) {
        return switch (code) {
            case INVALID_RECEIPT,
                    RECEIPT_MISMATCH,
                    NOT_FOUND_OR_NOT_AUTHORIZED,
                    SHARD_TRANSITIONING,
                    SHARD_UNAVAILABLE,
                    INTEGRITY_ERROR -> true;
            default -> false;
        };
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }
}
