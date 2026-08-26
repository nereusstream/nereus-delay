package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical Message query response with its closed public union branches. */
public record MessageQueryResponse(MessageQueryResult resultKind, PublicQueryError error, QueryResponseBranch branch) {
    public static final int VERSION = 1;

    public MessageQueryResponse(final MessageQueryResult resultKind, final PublicQueryError error) {
        this(resultKind, error, error);
    }

    public MessageQueryResponse(final MessageQueryResult resultKind, final QueryResponseBranch branch) {
        this(resultKind, branch instanceof PublicQueryError queryError ? queryError : null, branch);
    }

    public MessageQueryResponse {
        Objects.requireNonNull(resultKind, "resultKind");
        Objects.requireNonNull(branch, "branch");
        if (branch instanceof PublicQueryError queryError) {
            if (error == null || !error.equals(queryError) || !matches(resultKind, error.code())) {
                throw new IllegalArgumentException("Message query result tag and error code disagree");
            }
        } else if (error != null || !matchesView(resultKind, branch)) {
            throw new IllegalArgumentException("Message query result tag and view branch disagree");
        }
    }

    public static MessageQueryResponse error(final StableCode code, final Long retryAtEpochMs) {
        return new MessageQueryResponse(resultFor(code), new PublicQueryError(code, retryAtEpochMs));
    }

    public static MessageQueryResponse reserved(final ReservedMessageView view) {
        return new MessageQueryResponse(MessageQueryResult.RESERVED, view);
    }

    public static MessageQueryResponse active(final ActiveMessageView view) {
        return new MessageQueryResponse(MessageQueryResult.ACTIVE, view);
    }

    public static MessageQueryResponse terminal(final TerminalMessageView view) {
        return new MessageQueryResponse(MessageQueryResult.TERMINAL, view);
    }

    public static MessageQueryResponse identityRetired() {
        return new MessageQueryResponse(MessageQueryResult.IDENTITY_RETIRED, IdentityRetiredMessageView.INSTANCE);
    }

    public static MessageQueryResponse unknown(final FirstScheduleEligibility eligibility) {
        return new MessageQueryResponse(MessageQueryResult.UNKNOWN, new UnknownMessageView(eligibility));
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.uint32(output, 2, resultKind.wireValue());
            CanonicalProtobuf.bytes(output, branchField(resultKind), branch.canonicalBytes());
        });
    }

    public static MessageQueryResponse decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "MessageQueryResponse");
        if (fields.size() != 3
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(0).wireType() != 0
                || fields.get(1).wireType() != 0) {
            throw new IllegalArgumentException("invalid MessageQueryResponse fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported MessageQueryResponse version");
        }
        final MessageQueryResult result = MessageQueryResult.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final int branchField = branchField(result);
        final QueryResponseBranch branch = decodeBranch(result, QueryCodecSupport.nested(fields.get(2), branchField));
        final MessageQueryResponse decoded = new MessageQueryResponse(result, branch);
        if (!Arrays.equals(encoded, decoded.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical MessageQueryResponse");
        }
        return decoded;
    }

    private static QueryResponseBranch decodeBranch(final MessageQueryResult result, final byte[] encoded) {
        return switch (result) {
            case RESERVED -> ReservedMessageView.decode(encoded);
            case ACTIVE -> ActiveMessageView.decode(encoded);
            case TERMINAL -> TerminalMessageView.decode(encoded);
            case IDENTITY_RETIRED -> IdentityRetiredMessageView.decode(encoded);
            case UNKNOWN -> UnknownMessageView.decode(encoded);
            case INVALID_RECEIPT,
                    RECEIPT_MISMATCH,
                    NOT_FOUND_OR_NOT_AUTHORIZED,
                    SHARD_TRANSITIONING,
                    SHARD_UNAVAILABLE,
                    INTEGRITY_ERROR -> PublicQueryError.decode(encoded);
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

    private static boolean matchesView(final MessageQueryResult result, final QueryResponseBranch branch) {
        return switch (result) {
            case RESERVED -> branch instanceof ReservedMessageView;
            case ACTIVE -> branch instanceof ActiveMessageView;
            case TERMINAL -> branch instanceof TerminalMessageView;
            case IDENTITY_RETIRED -> branch instanceof IdentityRetiredMessageView;
            case UNKNOWN -> branch instanceof UnknownMessageView;
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
