package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical public Control Operation query response union. */
public final class ControlOperationQueryResponseV1 {
    public static final int VERSION = 1;

    private final ControlOperationQueryResultV1 resultKind;
    private final CurrentControlOperationV1 current;
    private final PublicQueryErrorV1 error;

    private ControlOperationQueryResponseV1(final ControlOperationQueryResultV1 resultKind,
                                            final CurrentControlOperationV1 current,
                                            final PublicQueryErrorV1 error) {
        this.resultKind = Objects.requireNonNull(resultKind, "resultKind");
        this.current = current;
        this.error = error;
        if (resultKind == ControlOperationQueryResultV1.CURRENT) {
            if (current == null || error != null) {
                throw new IllegalArgumentException("CURRENT control query requires only current projection");
            }
        } else if (current != null || error == null || !matches(resultKind, error.code())) {
            throw new IllegalArgumentException("control query result tag and error branch disagree");
        }
    }

    public static ControlOperationQueryResponseV1 current(final CurrentControlOperationV1 current) {
        return new ControlOperationQueryResponseV1(ControlOperationQueryResultV1.CURRENT,
                Objects.requireNonNull(current, "current"), null);
    }

    public static ControlOperationQueryResponseV1 invalidReceipt() {
        return error(StableCode.INVALID_RECEIPT);
    }

    public static ControlOperationQueryResponseV1 notFoundOrNotAuthorized() {
        return error(StableCode.NOT_FOUND_OR_NOT_AUTHORIZED);
    }

    public static ControlOperationQueryResponseV1 integrityError() {
        return error(StableCode.INTEGRITY_ERROR);
    }

    public static ControlOperationQueryResponseV1 error(final StableCode code) {
        final ControlOperationQueryResultV1 result = resultFor(code);
        return new ControlOperationQueryResponseV1(result, null, new PublicQueryErrorV1(code, null));
    }

    public ControlOperationQueryResultV1 resultKind() {
        return resultKind;
    }

    public CurrentControlOperationV1 current() {
        return current;
    }

    public PublicQueryErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, branchField(resultKind),
                    current == null ? error.canonicalBytes() : current.canonicalBytes());
        });
    }

    public static ControlOperationQueryResponseV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlOperationQueryResponseV1");
        if (fields.size() != 3 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0 || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid ControlOperationQueryResponseV1 fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported ControlOperationQueryResponseV1 version");
        }
        final ControlOperationQueryResultV1 result = ControlOperationQueryResultV1.fromWire(
                QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final byte[] branch = QueryCodecSupport.nested(fields.get(2), branchField);
        final ControlOperationQueryResponseV1 decoded = switch (result) {
            case CURRENT -> current(CurrentControlOperationV1.decode(branch));
            case INVALID_RECEIPT, NOT_FOUND_OR_NOT_AUTHORIZED, INTEGRITY_ERROR -> {
                final PublicQueryErrorV1 error = PublicQueryErrorV1.decode(branch);
                if (!matches(result, error.code())) {
                    throw new IllegalArgumentException("control query error code does not match result tag");
                }
                yield new ControlOperationQueryResponseV1(result, null, error);
            }
        };
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical ControlOperationQueryResponseV1");
        }
        return decoded;
    }

    private static int branchField(final ControlOperationQueryResultV1 result) {
        return switch (result) {
            case CURRENT -> 10;
            case INVALID_RECEIPT -> 11;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> 12;
            case INTEGRITY_ERROR -> 13;
        };
    }

    private static boolean matches(final ControlOperationQueryResultV1 result, final StableCode code) {
        return switch (result) {
            case CURRENT -> false;
            case INVALID_RECEIPT -> code == StableCode.INVALID_RECEIPT;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> code == StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case INTEGRITY_ERROR -> code == StableCode.INTEGRITY_ERROR;
        };
    }

    private static ControlOperationQueryResultV1 resultFor(final StableCode code) {
        return switch (Objects.requireNonNull(code, "code")) {
            case INVALID_RECEIPT -> ControlOperationQueryResultV1.INVALID_RECEIPT;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> ControlOperationQueryResultV1.NOT_FOUND_OR_NOT_AUTHORIZED;
            case INTEGRITY_ERROR -> ControlOperationQueryResultV1.INTEGRITY_ERROR;
            default -> throw new IllegalArgumentException("stable code is not a Control Operation query error");
        };
    }
}
