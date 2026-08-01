package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Bounded canonical Command query response.  This increment intentionally
 * implements only the six public error branches; result/view branches wait
 * for the authorization-safe binding and retention projections.
 */
public record CommandQueryResponseV1(CommandQueryResult resultKind, PublicQueryErrorV1 error) {
    public static final int VERSION = 1;

    public CommandQueryResponseV1 {
        Objects.requireNonNull(resultKind, "resultKind");
        Objects.requireNonNull(error, "error");
        if (!matches(resultKind, error.code())) {
            throw new IllegalArgumentException("Command query result tag and error code disagree");
        }
    }

    public static CommandQueryResponseV1 error(final StableCode code, final Long retryAtEpochMs) {
        return new CommandQueryResponseV1(resultFor(code), new PublicQueryErrorV1(code, retryAtEpochMs));
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, errorField(resultKind), error.canonicalBytes());
        });
    }

    public static CommandQueryResponseV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded);
        if (fields.size() != 3 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0 || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid CommandQueryResponseV1 fields");
        }
        if (fields.get(0).unsignedValue() != VERSION) {
            throw new IllegalArgumentException("unsupported CommandQueryResponseV1 version");
        }
        final CommandQueryResult result = CommandQueryResult.fromWire(Math.toIntExact(fields.get(1).unsignedValue()));
        final int expectedField = errorField(result);
        final PublicQueryErrorV1 error = PublicQueryErrorV1.decode(nested(fields.get(2), expectedField));
        final CommandQueryResponseV1 decoded = new CommandQueryResponseV1(result, error);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical CommandQueryResponseV1");
        }
        return decoded;
    }

    private static int errorField(final CommandQueryResult result) {
        return switch (result) {
            case INVALID_RECEIPT -> 16;
            case RECEIPT_MISMATCH -> 17;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> 18;
            case SHARD_TRANSITIONING -> 19;
            case SHARD_UNAVAILABLE -> 20;
            case INTEGRITY_ERROR -> 21;
            default -> throw new IllegalArgumentException("result tag is not an error branch: " + result);
        };
    }

    private static boolean matches(final CommandQueryResult result, final StableCode code) {
        return switch (result) {
            case INVALID_RECEIPT -> code == StableCode.INVALID_RECEIPT;
            case RECEIPT_MISMATCH -> code == StableCode.RECEIPT_MISMATCH;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> code == StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> code == StableCode.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> code == StableCode.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> code == StableCode.INTEGRITY_ERROR;
            default -> false;
        };
    }

    private static CommandQueryResult resultFor(final StableCode code) {
        return switch (code) {
            case INVALID_RECEIPT -> CommandQueryResult.INVALID_RECEIPT;
            case RECEIPT_MISMATCH -> CommandQueryResult.RECEIPT_MISMATCH;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> CommandQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> CommandQueryResult.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> CommandQueryResult.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> CommandQueryResult.INTEGRITY_ERROR;
            default -> throw new IllegalArgumentException("stable code is not a Command query error: " + code);
        };
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Command query error branch " + number);
        }
        return field.rawValue();
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }
}
