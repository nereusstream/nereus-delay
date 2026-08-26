package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical Command query response with its closed public union branches. */
public record CommandQueryResponse(CommandQueryResult resultKind, PublicQueryError error, QueryResponseBranch branch) {
    public static final int VERSION = 1;

    public CommandQueryResponse(final CommandQueryResult resultKind, final PublicQueryError error) {
        this(resultKind, error, error);
    }

    public CommandQueryResponse(final CommandQueryResult resultKind, final QueryResponseBranch branch) {
        this(resultKind, branch instanceof PublicQueryError queryError ? queryError : null, branch);
    }

    public CommandQueryResponse {
        Objects.requireNonNull(resultKind, "resultKind");
        Objects.requireNonNull(branch, "branch");
        if (branch instanceof PublicQueryError queryError) {
            if (error == null || !error.equals(queryError) || !matches(resultKind, error.code())) {
                throw new IllegalArgumentException("Command query result tag and error code disagree");
            }
        } else if (error != null || !matchesView(resultKind, branch)) {
            throw new IllegalArgumentException("Command query result tag and view branch disagree");
        }
    }

    public static CommandQueryResponse error(final StableCode code, final Long retryAtEpochMs) {
        return new CommandQueryResponse(resultFor(code), new PublicQueryError(code, retryAtEpochMs));
    }

    public static CommandQueryResponse pending(final PendingCommandView view) {
        return new CommandQueryResponse(CommandQueryResult.PENDING, view);
    }

    public static CommandQueryResponse applied(final PublicCommandResult view) {
        return new CommandQueryResponse(CommandQueryResult.APPLIED, view);
    }

    public static CommandQueryResponse rejected(final PublicCommandResult view) {
        return new CommandQueryResponse(CommandQueryResult.REJECTED, view);
    }

    public static CommandQueryResponse resultExpired(final CompactCommandResult view) {
        return new CommandQueryResponse(CommandQueryResult.RESULT_EXPIRED, view);
    }

    public static CommandQueryResponse resultEvidenceExpired() {
        return new CommandQueryResponse(CommandQueryResult.RESULT_EVIDENCE_EXPIRED, EmptyResult.INSTANCE);
    }

    public static CommandQueryResponse unknown() {
        return new CommandQueryResponse(CommandQueryResult.UNKNOWN, EmptyResult.INSTANCE);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, branchField(resultKind), branch.canonicalBytes());
        });
    }

    public static CommandQueryResponse decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CommandQueryResponse");
        if (fields.size() != 3
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0
                || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid CommandQueryResponse fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported CommandQueryResponse version");
        }
        final CommandQueryResult result = CommandQueryResult.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final byte[] branchBytes = QueryCodecSupport.nested(fields.get(2), branchField);
        final QueryResponseBranch branch = decodeBranch(result, branchBytes);
        final CommandQueryResponse decoded = new CommandQueryResponse(result, branch);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical CommandQueryResponse");
        }
        return decoded;
    }

    private static QueryResponseBranch decodeBranch(final CommandQueryResult result, final byte[] encoded) {
        return switch (result) {
            case PENDING -> PendingCommandView.decode(encoded);
            case APPLIED, REJECTED -> PublicCommandResult.decode(encoded);
            case RESULT_EXPIRED -> CompactCommandResult.decode(encoded);
            case RESULT_EVIDENCE_EXPIRED, UNKNOWN -> EmptyResult.decode(encoded);
            case INVALID_RECEIPT,
                    RECEIPT_MISMATCH,
                    NOT_FOUND_OR_NOT_AUTHORIZED,
                    SHARD_TRANSITIONING,
                    SHARD_UNAVAILABLE,
                    INTEGRITY_ERROR -> PublicQueryError.decode(encoded);
        };
    }

    private static int branchField(final CommandQueryResult result) {
        return switch (result) {
            case PENDING -> 10;
            case APPLIED -> 11;
            case REJECTED -> 12;
            case RESULT_EXPIRED -> 13;
            case RESULT_EVIDENCE_EXPIRED -> 14;
            case UNKNOWN -> 15;
            case INVALID_RECEIPT -> 16;
            case RECEIPT_MISMATCH -> 17;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> 18;
            case SHARD_TRANSITIONING -> 19;
            case SHARD_UNAVAILABLE -> 20;
            case INTEGRITY_ERROR -> 21;
        };
    }

    private static boolean matchesView(final CommandQueryResult result, final QueryResponseBranch branch) {
        return switch (result) {
            case PENDING -> branch instanceof PendingCommandView;
            case APPLIED, REJECTED ->
                branch instanceof PublicCommandResult view
                        && (result == CommandQueryResult.APPLIED
                                ? view.status() == CommandApplyStatus.APPLIED
                                : view.status() == CommandApplyStatus.REJECTED);
            case RESULT_EXPIRED -> branch instanceof CompactCommandResult;
            case RESULT_EVIDENCE_EXPIRED, UNKNOWN -> branch instanceof EmptyResult;
            default -> false;
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
}
