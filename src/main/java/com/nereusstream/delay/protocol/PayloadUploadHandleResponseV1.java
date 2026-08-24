package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed upload-handle response with payload-scoped stable errors. */
public final class PayloadUploadHandleResponseV1 {
    private final PayloadUploadHandleOutcomeV1 outcome;
    private final OpaquePayloadUploadHandleV1 issued;
    private final StableErrorV1 error;

    private PayloadUploadHandleResponseV1(
            final PayloadUploadHandleOutcomeV1 outcome,
            final OpaquePayloadUploadHandleV1 issued,
            final StableErrorV1 error) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        if ((outcome == PayloadUploadHandleOutcomeV1.ISSUED) != (issued != null)
                || (outcome == PayloadUploadHandleOutcomeV1.ISSUED) == (error != null)) {
            throw new IllegalArgumentException("upload handle response branch does not match outcome");
        }
        this.issued = issued;
        this.error = error == null
                ? null
                : payloadError(
                        error,
                        stableCode(outcome),
                        outcome == PayloadUploadHandleOutcomeV1.SHARD_TRANSITIONING
                                || outcome == PayloadUploadHandleOutcomeV1.OBJECT_STORE_UNAVAILABLE_RETRYABLE);
    }

    public static PayloadUploadHandleResponseV1 issued(final OpaquePayloadUploadHandleV1 handle) {
        return new PayloadUploadHandleResponseV1(
                PayloadUploadHandleOutcomeV1.ISSUED, Objects.requireNonNull(handle, "handle"), null);
    }

    public static PayloadUploadHandleResponseV1 error(
            final PayloadUploadHandleOutcomeV1 outcome, final StableErrorV1 error) {
        if (outcome == PayloadUploadHandleOutcomeV1.ISSUED) {
            throw new IllegalArgumentException("ISSUED requires an upload handle");
        }
        return new PayloadUploadHandleResponseV1(outcome, null, Objects.requireNonNull(error, "error"));
    }

    public PayloadUploadHandleOutcomeV1 outcome() {
        return outcome;
    }

    public OpaquePayloadUploadHandleV1 issued() {
        return issued;
    }

    public StableErrorV1 error() {
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

    public static PayloadUploadHandleResponseV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PayloadUploadHandleResponseV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("upload handle response must contain one branch");
        }
        final PayloadUploadHandleOutcomeV1 outcome =
                PayloadUploadHandleOutcomeV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final int expectedField = outcome == PayloadUploadHandleOutcomeV1.ISSUED ? 10 : 9 + outcome.wireValue();
        if (fields.get(1).number() != expectedField) {
            throw new IllegalArgumentException("upload handle branch does not match outcome");
        }
        final PayloadUploadHandleResponseV1 result = outcome == PayloadUploadHandleOutcomeV1.ISSUED
                ? issued(OpaquePayloadUploadHandleV1.decode(QueryCodecSupport.nested(fields.get(1), 10)))
                : error(outcome, StableErrorV1.decode(QueryCodecSupport.nested(fields.get(1), expectedField)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadUploadHandleResponseV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadUploadHandleResponseV1 that
                && outcome == that.outcome
                && Objects.equals(issued, that.issued)
                && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, issued, error);
    }

    private static StableCode stableCode(final PayloadUploadHandleOutcomeV1 outcome) {
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

    private static StableErrorV1 payloadError(
            final StableErrorV1 error, final StableCode code, final boolean retryAtRequired) {
        if (error.stage() != FailureStageV1.PAYLOAD
                || error.code() != code
                || error.command() != null
                || error.nativePrepared() != null
                || (error.retryAtEpochMs() != null) != retryAtRequired) {
            throw new IllegalArgumentException("payload error branch does not match fixed stable code/presence");
        }
        return error;
    }
}
