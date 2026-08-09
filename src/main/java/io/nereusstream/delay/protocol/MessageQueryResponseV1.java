package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical V1 Message query response with its closed public union branches. */
public record MessageQueryResponseV1(MessageQueryResult resultKind, PublicQueryErrorV1 error,
                                     QueryResponseBranchV1 branch) {
    public static final int VERSION = 1;

    public MessageQueryResponseV1(final MessageQueryResult resultKind, final PublicQueryErrorV1 error) {
        this(resultKind, error, error);
    }

    public MessageQueryResponseV1(final MessageQueryResult resultKind, final QueryResponseBranchV1 branch) {
        this(resultKind, branch instanceof PublicQueryErrorV1 queryError ? queryError : null, branch);
    }

    public MessageQueryResponseV1 {
        Objects.requireNonNull(resultKind, "resultKind");
        Objects.requireNonNull(branch, "branch");
        if (branch instanceof PublicQueryErrorV1 queryError) {
            if (error == null || !error.equals(queryError) || !matches(resultKind, error.code())) {
                throw new IllegalArgumentException("Message query result tag and error code disagree");
            }
        } else if (error != null || !matchesView(resultKind, branch)) {
            throw new IllegalArgumentException("Message query result tag and view branch disagree");
        }
    }

    public static MessageQueryResponseV1 error(final StableCode code, final Long retryAtEpochMs) {
        return new MessageQueryResponseV1(resultFor(code), new PublicQueryErrorV1(code, retryAtEpochMs));
    }

    public static MessageQueryResponseV1 reserved(final ReservedMessageViewV1 view) {
        return new MessageQueryResponseV1(MessageQueryResult.RESERVED, view);
    }

    public static MessageQueryResponseV1 active(final ActiveMessageViewV1 view) {
        return new MessageQueryResponseV1(MessageQueryResult.ACTIVE, view);
    }

    public static MessageQueryResponseV1 terminal(final TerminalMessageViewV1 view) {
        return new MessageQueryResponseV1(MessageQueryResult.TERMINAL, view);
    }

    public static MessageQueryResponseV1 identityRetired() {
        return new MessageQueryResponseV1(MessageQueryResult.IDENTITY_RETIRED,
                IdentityRetiredMessageViewV1.INSTANCE);
    }

    public static MessageQueryResponseV1 unknown(final FirstScheduleEligibilityV1 eligibility) {
        return new MessageQueryResponseV1(MessageQueryResult.UNKNOWN, new UnknownMessageViewV1(eligibility));
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, branchField(resultKind), branch.canonicalBytes());
        });
    }

    public static MessageQueryResponseV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "MessageQueryResponseV1");
        if (fields.size() != 3 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0 || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid MessageQueryResponseV1 fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported MessageQueryResponseV1 version");
        }
        final MessageQueryResult result = MessageQueryResult.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final QueryResponseBranchV1 branch = decodeBranch(result,
                QueryCodecSupport.nested(fields.get(2), branchField));
        final MessageQueryResponseV1 decoded = new MessageQueryResponseV1(result, branch);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical MessageQueryResponseV1");
        }
        return decoded;
    }

    private static QueryResponseBranchV1 decodeBranch(final MessageQueryResult result, final byte[] encoded) {
        return switch (result) {
            case RESERVED -> ReservedMessageViewV1.decode(encoded);
            case ACTIVE -> ActiveMessageViewV1.decode(encoded);
            case TERMINAL -> TerminalMessageViewV1.decode(encoded);
            case IDENTITY_RETIRED -> IdentityRetiredMessageViewV1.decode(encoded);
            case UNKNOWN -> UnknownMessageViewV1.decode(encoded);
            case INVALID_RECEIPT, RECEIPT_MISMATCH, NOT_FOUND_OR_NOT_AUTHORIZED, SHARD_TRANSITIONING,
                    SHARD_UNAVAILABLE, INTEGRITY_ERROR -> PublicQueryErrorV1.decode(encoded);
        };
    }

    private static int branchField(final MessageQueryResult result) {
        return switch (result) {
            case RESERVED -> 10;
            case ACTIVE -> 11;
            case TERMINAL -> 12;
            case IDENTITY_RETIRED -> 13;
            case UNKNOWN -> 14;
            case INVALID_RECEIPT -> 15;
            case RECEIPT_MISMATCH -> 16;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> 17;
            case SHARD_TRANSITIONING -> 18;
            case SHARD_UNAVAILABLE -> 19;
            case INTEGRITY_ERROR -> 20;
        };
    }

    private static boolean matchesView(final MessageQueryResult result, final QueryResponseBranchV1 branch) {
        return switch (result) {
            case RESERVED -> branch instanceof ReservedMessageViewV1;
            case ACTIVE -> branch instanceof ActiveMessageViewV1;
            case TERMINAL -> branch instanceof TerminalMessageViewV1;
            case IDENTITY_RETIRED -> branch instanceof IdentityRetiredMessageViewV1;
            case UNKNOWN -> branch instanceof UnknownMessageViewV1;
            default -> false;
        };
    }

    private static boolean matches(final MessageQueryResult result, final StableCode code) {
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

    private static MessageQueryResult resultFor(final StableCode code) {
        return switch (code) {
            case INVALID_RECEIPT -> MessageQueryResult.INVALID_RECEIPT;
            case RECEIPT_MISMATCH -> MessageQueryResult.RECEIPT_MISMATCH;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> MessageQueryResult.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> MessageQueryResult.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> MessageQueryResult.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> MessageQueryResult.INTEGRITY_ERROR;
            default -> throw new IllegalArgumentException("stable code is not a Message query error: " + code);
        };
    }
}
