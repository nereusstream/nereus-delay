package io.nereusstream.delay.protocol;

import java.util.Objects;

/** Closed payload-attestation response with payload-scoped stable errors. */
public final class PayloadAttestationResponseV1 {
    private final PayloadAttestationOutcomeV1 outcome;
    private final PayloadCommitProof proof;
    private final StableErrorV1 error;

    private PayloadAttestationResponseV1(final PayloadAttestationOutcomeV1 outcome,
                                         final PayloadCommitProof proof, final StableErrorV1 error) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        if ((outcome == PayloadAttestationOutcomeV1.ATTESTED) != (proof != null)
                || (outcome == PayloadAttestationOutcomeV1.ATTESTED) == (error != null)) {
            throw new IllegalArgumentException("payload attestation branch does not match outcome");
        }
        this.proof = proof;
        this.error = error == null ? null : payloadError(error, stableCode(outcome),
                outcome == PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE
                        || outcome == PayloadAttestationOutcomeV1.OBJECT_STORE_UNAVAILABLE_RETRYABLE
                        || outcome == PayloadAttestationOutcomeV1.SHARD_TRANSITIONING);
    }

    public static PayloadAttestationResponseV1 attested(final PayloadCommitProof proof) {
        return new PayloadAttestationResponseV1(PayloadAttestationOutcomeV1.ATTESTED,
                Objects.requireNonNull(proof, "proof"), null);
    }

    public static PayloadAttestationResponseV1 error(final PayloadAttestationOutcomeV1 outcome,
                                                      final StableErrorV1 error) {
        if (outcome == PayloadAttestationOutcomeV1.ATTESTED) {
            throw new IllegalArgumentException("ATTESTED requires a commit proof");
        }
        return new PayloadAttestationResponseV1(outcome, null, Objects.requireNonNull(error, "error"));
    }

    public PayloadAttestationOutcomeV1 outcome() {
        return outcome;
    }

    public PayloadCommitProof proof() {
        return proof;
    }

    public StableErrorV1 error() {
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

    public static PayloadAttestationResponseV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PayloadAttestationResponseV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("payload attestation response must contain one branch");
        }
        final PayloadAttestationOutcomeV1 outcome = PayloadAttestationOutcomeV1.fromWire(
                QueryCodecSupport.uint(fields.get(0), 1));
        final int expectedField = outcome == PayloadAttestationOutcomeV1.ATTESTED ? 10 : 9 + outcome.wireValue();
        if (fields.get(1).number() != expectedField) {
            throw new IllegalArgumentException("payload attestation branch does not match outcome");
        }
        final PayloadAttestationResponseV1 result = outcome == PayloadAttestationOutcomeV1.ATTESTED
                ? attested(PayloadCommitProof.decode(QueryCodecSupport.bytes(fields.get(1), 10)))
                : error(outcome, StableErrorV1.decode(QueryCodecSupport.nested(fields.get(1), expectedField)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadAttestationResponseV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadAttestationResponseV1 that && outcome == that.outcome
                && Objects.equals(proof, that.proof) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, proof, error);
    }

    private static StableCode stableCode(final PayloadAttestationOutcomeV1 outcome) {
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

    private static StableErrorV1 payloadError(final StableErrorV1 error, final StableCode code,
                                               final boolean retryAtRequired) {
        if (error.stage() != FailureStageV1.PAYLOAD || error.code() != code
                || error.command() != null || error.nativePrepared() != null
                || (error.retryAtEpochMs() != null) != retryAtRequired) {
            throw new IllegalArgumentException("payload error branch does not match fixed stable code/presence");
        }
        return error;
    }
}
