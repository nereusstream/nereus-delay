package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed upload-handle response with payload-scoped stable errors. */
public final class PayloadUploadHandleResponse {
    private final PayloadUploadHandleOutcome outcome;
    private final OpaquePayloadUploadHandle issued;
    private final StableError error;

    private PayloadUploadHandleResponse(
            final PayloadUploadHandleOutcome outcome, final OpaquePayloadUploadHandle issued, final StableError error) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        if ((outcome == PayloadUploadHandleOutcome.ISSUED) != (issued != null)
                || (outcome == PayloadUploadHandleOutcome.ISSUED) == (error != null)) {
            throw new IllegalArgumentException("upload handle response branch does not match outcome");
        }
        this.issued = issued;
        this.error = error == null
                ? null
                : payloadError(
                        error,
                        stableCode(outcome),
                        outcome == PayloadUploadHandleOutcome.SHARD_TRANSITIONING
                                || outcome == PayloadUploadHandleOutcome.OBJECT_STORE_UNAVAILABLE_RETRYABLE);
    }

    public static PayloadUploadHandleResponse issued(final OpaquePayloadUploadHandle handle) {
        return new PayloadUploadHandleResponse(
                PayloadUploadHandleOutcome.ISSUED, Objects.requireNonNull(handle, "handle"), null);
    }

    public static PayloadUploadHandleResponse error(final PayloadUploadHandleOutcome outcome, final StableError error) {
        if (outcome == PayloadUploadHandleOutcome.ISSUED) {
            throw new IllegalArgumentException("ISSUED requires an upload handle");
        }
        return new PayloadUploadHandleResponse(outcome, null, Objects.requireNonNull(error, "error"));
    }

    public PayloadUploadHandleOutcome outcome() {
        return outcome;
    }

    public OpaquePayloadUploadHandle issued() {
        return issued;
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, outcome.wireValue());
            if (issued != null) {
                CanonicalProtobuf.bytes(output, 10, issued.canonicalBytes());
            } else {
                CanonicalProtobuf.bytes(output, 9 + outcome.wireValue(), error.canonicalBytes());
            }
        });
    }

    public static PayloadUploadHandleResponse decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PayloadUploadHandleResponse");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("upload handle response must contain one branch");
        }
        final PayloadUploadHandleOutcome outcome =
                PayloadUploadHandleOutcome.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final int expectedField = outcome == PayloadUploadHandleOutcome.ISSUED ? 10 : 9 + outcome.wireValue();
        if (fields.get(1).number() != expectedField) {
            throw new IllegalArgumentException("upload handle branch does not match outcome");
        }
        final PayloadUploadHandleResponse result = outcome == PayloadUploadHandleOutcome.ISSUED
                ? issued(OpaquePayloadUploadHandle.decode(QueryCodecSupport.nested(fields.get(1), 10)))
                : error(outcome, StableError.decode(QueryCodecSupport.nested(fields.get(1), expectedField)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadUploadHandleResponse");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadUploadHandleResponse that
                && outcome == that.outcome
                && Objects.equals(issued, that.issued)
                && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, issued, error);
    }

    private static StableCode stableCode(final PayloadUploadHandleOutcome outcome) {
        return switch (outcome) {
            case RESERVATION_EXPIRED -> StableCode.RESERVATION_EXPIRED;
            case RESERVATION_ABANDONED -> StableCode.RESERVATION_ABANDONED;
            case RESERVATION_CLOSED -> StableCode.PAYLOAD_RESERVATION_CLOSED;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> StableCode.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> StableCode.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> StableCode.INTEGRITY_ERROR;
            case OBJECT_STORE_UNAVAILABLE_RETRYABLE -> StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE;
            case ISSUED -> throw new IllegalArgumentException("ISSUED has no stable error");
        };
    }

    private static StableError payloadError(
            final StableError error, final StableCode code, final boolean retryAtRequired) {
        if (error.stage() != FailureStage.PAYLOAD
                || error.code() != code
                || error.command() != null
                || error.nativePrepared() != null
                || (error.retryAtEpochMs() != null) != retryAtRequired) {
            throw new IllegalArgumentException("payload error branch does not match fixed stable code/presence");
        }
        return error;
    }
}
