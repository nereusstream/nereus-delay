package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical V1 Command query response with its closed public union branches. */
public record CommandQueryResponseV1(
        CommandQueryResult resultKind, PublicQueryErrorV1 error, QueryResponseBranchV1 branch) {
    public static final int VERSION = 1;

    public CommandQueryResponseV1(final CommandQueryResult resultKind, final PublicQueryErrorV1 error) {
        this(resultKind, error, error);
    }

    public CommandQueryResponseV1(final CommandQueryResult resultKind, final QueryResponseBranchV1 branch) {
        this(resultKind, branch instanceof PublicQueryErrorV1 queryError ? queryError : null, branch);
    }

    public CommandQueryResponseV1 {
        Objects.requireNonNull(resultKind, "resultKind");
        Objects.requireNonNull(branch, "branch");
        if (branch instanceof PublicQueryErrorV1 queryError) {
            if (error == null || !error.equals(queryError) || !matches(resultKind, error.code())) {
                throw new IllegalArgumentException("Command query result tag and error code disagree");
            }
        } else if (error != null || !matchesView(resultKind, branch)) {
            throw new IllegalArgumentException("Command query result tag and view branch disagree");
        }
    }

    public static CommandQueryResponseV1 error(final StableCode code, final Long retryAtEpochMs) {
        return new CommandQueryResponseV1(resultFor(code), new PublicQueryErrorV1(code, retryAtEpochMs));
    }

    public static CommandQueryResponseV1 pending(final PendingCommandViewV1 view) {
        return new CommandQueryResponseV1(CommandQueryResult.PENDING, view);
    }

    public static CommandQueryResponseV1 applied(final PublicCommandResultV1 view) {
        return new CommandQueryResponseV1(CommandQueryResult.APPLIED, view);
    }

    public static CommandQueryResponseV1 rejected(final PublicCommandResultV1 view) {
        return new CommandQueryResponseV1(CommandQueryResult.REJECTED, view);
    }

    public static CommandQueryResponseV1 resultExpired(final CompactCommandResultV1 view) {
        return new CommandQueryResponseV1(CommandQueryResult.RESULT_EXPIRED, view);
    }

    public static CommandQueryResponseV1 resultEvidenceExpired() {
        return new CommandQueryResponseV1(CommandQueryResult.RESULT_EVIDENCE_EXPIRED, EmptyResultV1.INSTANCE);
    }

    public static CommandQueryResponseV1 unknown() {
        return new CommandQueryResponseV1(CommandQueryResult.UNKNOWN, EmptyResultV1.INSTANCE);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, branchField(resultKind), branch.canonicalBytes());
        });
    }

    public static CommandQueryResponseV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "CommandQueryResponseV1");
        if (fields.size() != 3
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0
                || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid CommandQueryResponseV1 fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported CommandQueryResponseV1 version");
        }
        final CommandQueryResult result = CommandQueryResult.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final byte[] branchBytes = QueryCodecSupport.nested(fields.get(2), branchField);
        final QueryResponseBranchV1 branch = decodeBranch(result, branchBytes);
        final CommandQueryResponseV1 decoded = new CommandQueryResponseV1(result, branch);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical CommandQueryResponseV1");
        }
        return decoded;
    }

    private static QueryResponseBranchV1 decodeBranch(final CommandQueryResult result, final byte[] encoded) {
        return switch (result) {
            case PENDING -> PendingCommandViewV1.decode(encoded);
            case APPLIED, REJECTED -> PublicCommandResultV1.decode(encoded);
            case RESULT_EXPIRED -> CompactCommandResultV1.decode(encoded);
            case RESULT_EVIDENCE_EXPIRED, UNKNOWN -> EmptyResultV1.decode(encoded);
            case INVALID_RECEIPT,
                    RECEIPT_MISMATCH,
                    NOT_FOUND_OR_NOT_AUTHORIZED,
                    SHARD_TRANSITIONING,
                    SHARD_UNAVAILABLE,
                    INTEGRITY_ERROR -> PublicQueryErrorV1.decode(encoded);
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

    private static boolean matchesView(final CommandQueryResult result, final QueryResponseBranchV1 branch) {
        return switch (result) {
            case PENDING -> branch instanceof PendingCommandViewV1;
            case APPLIED, REJECTED ->
                branch instanceof PublicCommandResultV1 view
                        && (result == CommandQueryResult.APPLIED
                                ? view.status() == CommandApplyStatusV1.APPLIED
                                : view.status() == CommandApplyStatusV1.REJECTED);
            case RESULT_EXPIRED -> branch instanceof CompactCommandResultV1;
            case RESULT_EVIDENCE_EXPIRED, UNKNOWN -> branch instanceof EmptyResultV1;
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
