package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Canonical public Control Operation query response union. */
public final class ControlOperationQueryResponse {
    public static final int VERSION = 1;

    private final ControlOperationQueryResult resultKind;
    private final CurrentControlOperation current;
    private final PublicQueryError error;

    private ControlOperationQueryResponse(
            final ControlOperationQueryResult resultKind,
            final CurrentControlOperation current,
            final PublicQueryError error) {
        this.resultKind = Objects.requireNonNull(resultKind, "resultKind");
        this.current = current;
        this.error = error;
        if (resultKind == ControlOperationQueryResult.CURRENT) {
            if (current == null || error != null) {
                throw new IllegalArgumentException("CURRENT control query requires only current projection");
            }
        } else if (current != null || error == null || !matches(resultKind, error.code())) {
            throw new IllegalArgumentException("control query result tag and error branch disagree");
        }
    }

    public static ControlOperationQueryResponse current(final CurrentControlOperation current) {
        return new ControlOperationQueryResponse(
                ControlOperationQueryResult.CURRENT, Objects.requireNonNull(current, "current"), null);
    }

    public static ControlOperationQueryResponse invalidReceipt() {
        return error(StableCode.INVALID_RECEIPT);
    }

    public static ControlOperationQueryResponse notFoundOrNotAuthorized() {
        return error(StableCode.NOT_FOUND_OR_NOT_AUTHORIZED);
    }

    public static ControlOperationQueryResponse integrityError() {
        return error(StableCode.INTEGRITY_ERROR);
    }

    public static ControlOperationQueryResponse error(final StableCode code) {
        final ControlOperationQueryResult result = resultFor(code);
        return new ControlOperationQueryResponse(result, null, new PublicQueryError(code, null));
    }

    public ControlOperationQueryResult resultKind() {
        return resultKind;
    }

    public CurrentControlOperation current() {
        return current;
    }

    public PublicQueryError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(
                    output,
                    branchField(resultKind),
                    current == null ? error.canonicalBytes() : current.canonicalBytes());
        });
    }

    public static ControlOperationQueryResponse decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlOperationQueryResponse");
        if (fields.size() != 3
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0
                || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid ControlOperationQueryResponse fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported ControlOperationQueryResponse version");
        }
        final ControlOperationQueryResult result =
                ControlOperationQueryResult.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final byte[] branch = QueryCodecSupport.nested(fields.get(2), branchField);
        final ControlOperationQueryResponse decoded =
                switch (result) {
                    case CURRENT -> current(CurrentControlOperation.decode(branch));
                    case INVALID_RECEIPT, NOT_FOUND_OR_NOT_AUTHORIZED, INTEGRITY_ERROR -> {
                        final PublicQueryError error = PublicQueryError.decode(branch);
                        if (!matches(result, error.code())) {
                            throw new IllegalArgumentException("control query error code does not match result tag");
                        }
                        yield new ControlOperationQueryResponse(result, null, error);
                    }
                };
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical ControlOperationQueryResponse");
        }
        return decoded;
    }

    private static int branchField(final ControlOperationQueryResult result) {
        return switch (result) {
            case CURRENT -> 10;
            case INVALID_RECEIPT -> 11;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> 12;
            case INTEGRITY_ERROR -> 13;
        };
    }

    private static boolean matches(final ControlOperationQueryResult result, final StableCode code) {
        return switch (result) {
            case CURRENT -> false;
            case INVALID_RECEIPT -> code == StableCode.INVALID_RECEIPT;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> code == StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case INTEGRITY_ERROR -> code == StableCode.INTEGRITY_ERROR;
        };
    }

    private static ControlOperationQueryResult resultFor(final StableCode code) {
        return switch (Objects.requireNonNull(code, "code")) {
            case INVALID_RECEIPT -> ControlOperationQueryResult.INVALID_RECEIPT;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> ControlOperationQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED;
            case INTEGRITY_ERROR -> ControlOperationQueryResult.INTEGRITY_ERROR;
            default -> throw new IllegalArgumentException("stable code is not a Control Operation query error");
        };
    }
}
