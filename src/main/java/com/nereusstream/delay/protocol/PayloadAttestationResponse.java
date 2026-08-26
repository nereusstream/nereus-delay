package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed payload-attestation response with payload-scoped stable errors. */
public final class PayloadAttestationResponse {
    private final PayloadAttestationOutcome outcome;
    private final CanonicalPayloadCommitProof proof;
    private final StableError error;

    private PayloadAttestationResponse(
            final PayloadAttestationOutcome outcome, final CanonicalPayloadCommitProof proof, final StableError error) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        if ((outcome == PayloadAttestationOutcome.ATTESTED) != (proof != null)
                || (outcome == PayloadAttestationOutcome.ATTESTED) == (error != null)) {
            throw new IllegalArgumentException("payload attestation branch does not match outcome");
        }
        this.proof = proof;
        this.error = error == null
                ? null
                : payloadError(
                        error,
                        stableCode(outcome),
                        outcome == PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE
                                || outcome == PayloadAttestationOutcome.OBJECT_STORE_UNAVAILABLE_RETRYABLE
                                || outcome == PayloadAttestationOutcome.SHARD_TRANSITIONING);
    }

    public static PayloadAttestationResponse attested(final CanonicalPayloadCommitProof proof) {
        return new PayloadAttestationResponse(
                PayloadAttestationOutcome.ATTESTED, Objects.requireNonNull(proof, "proof"), null);
    }

    public static PayloadAttestationResponse error(final PayloadAttestationOutcome outcome, final StableError error) {
        if (outcome == PayloadAttestationOutcome.ATTESTED) {
            throw new IllegalArgumentException("ATTESTED requires a commit proof");
        }
        return new PayloadAttestationResponse(outcome, null, Objects.requireNonNull(error, "error"));
    }

    public PayloadAttestationOutcome outcome() {
        return outcome;
    }

    public CanonicalPayloadCommitProof proof() {
        return proof;
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, outcome.wireValue());
            if (proof != null) {
                CanonicalProtobuf.bytes(output, 10, proof.canonicalBytes());
            } else {
                CanonicalProtobuf.bytes(output, 9 + outcome.wireValue(), error.canonicalBytes());
            }
        });
    }

    public static PayloadAttestationResponse decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PayloadAttestationResponse");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("payload attestation response must contain one branch");
        }
        final PayloadAttestationOutcome outcome =
                PayloadAttestationOutcome.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final int expectedField = outcome == PayloadAttestationOutcome.ATTESTED ? 10 : 9 + outcome.wireValue();
        if (fields.get(1).number() != expectedField) {
            throw new IllegalArgumentException("payload attestation branch does not match outcome");
        }
        final PayloadAttestationResponse result = outcome == PayloadAttestationOutcome.ATTESTED
                ? attested(CanonicalPayloadCommitProof.decode(QueryCodecSupport.nested(fields.get(1), 10)))
                : error(outcome, StableError.decode(QueryCodecSupport.nested(fields.get(1), expectedField)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadAttestationResponse");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadAttestationResponse that
                && outcome == that.outcome
                && Objects.equals(proof, that.proof)
                && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, proof, error);
    }

    private static StableCode stableCode(final PayloadAttestationOutcome outcome) {
        return switch (outcome) {
            case OBJECT_NOT_READY_RETRYABLE -> StableCode.OBJECT_NOT_READY_RETRYABLE;
            case OBJECT_STORE_UNAVAILABLE_RETRYABLE -> StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE;
            case OBJECT_IDENTITY_CONFLICT -> StableCode.OBJECT_IDENTITY_CONFLICT;
            case RESERVATION_EXPIRED -> StableCode.RESERVATION_EXPIRED;
            case RESERVATION_ABANDONED -> StableCode.RESERVATION_ABANDONED;
            case RESERVATION_CLOSED -> StableCode.PAYLOAD_RESERVATION_CLOSED;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> StableCode.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> StableCode.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> StableCode.INTEGRITY_ERROR;
            case ATTESTED -> throw new IllegalArgumentException("ATTESTED has no stable error");
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
