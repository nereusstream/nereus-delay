package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Closed Control registration outcome union. */
public final class ControlRegistrationOutcomeMessageV1 {
    private final ControlRegistrationOutcomeV1 outcome;
    private final Object branch;

    private ControlRegistrationOutcomeMessageV1(final ControlRegistrationOutcomeV1 outcome, final Object branch) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.branch = Objects.requireNonNull(branch, "branch");
        validateBranch(outcome, branch);
    }

    public static ControlRegistrationOutcomeMessageV1 recorded(final ControlOperationReceiptV1 receipt) {
        return new ControlRegistrationOutcomeMessageV1(
                ControlRegistrationOutcomeV1.RECORDED, Objects.requireNonNull(receipt, "receipt"));
    }

    public static ControlRegistrationOutcomeMessageV1 definitelyNotRecorded(
            final ControlDefinitelyNotRecordedV1 value) {
        return new ControlRegistrationOutcomeMessageV1(
                ControlRegistrationOutcomeV1.DEFINITELY_NOT_RECORDED, Objects.requireNonNull(value, "value"));
    }

    public static ControlRegistrationOutcomeMessageV1 recordUncertain(final ControlRecordUncertainV1 value) {
        return new ControlRegistrationOutcomeMessageV1(
                ControlRegistrationOutcomeV1.RECORD_UNCERTAIN, Objects.requireNonNull(value, "value"));
    }

    public ControlRegistrationOutcomeV1 outcome() {
        return outcome;
    }

    public ControlOperationReceiptV1 receipt() {
        return branch instanceof ControlOperationReceiptV1 value ? value : null;
    }

    public ControlDefinitelyNotRecordedV1 definitelyNotRecorded() {
        return branch instanceof ControlDefinitelyNotRecordedV1 value ? value : null;
    }

    public ControlRecordUncertainV1 uncertain() {
        return branch instanceof ControlRecordUncertainV1 value ? value : null;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, outcome.wireValue());
            final int field =
                    switch (outcome) {
                        case RECORDED -> 10;
                        case DEFINITELY_NOT_RECORDED -> 11;
                        case RECORD_UNCERTAIN -> 12;
                    };
            final byte[] value =
                    switch (branch) {
                        case ControlOperationReceiptV1 receipt -> receipt.payload();
                        case ControlDefinitelyNotRecordedV1 rejection -> rejection.canonicalBytes();
                        case ControlRecordUncertainV1 uncertain -> uncertain.canonicalBytes();
                        default -> throw new IllegalStateException("unknown Control registration branch");
                    };
            CanonicalProtobuf.bytes(output, field, value);
        });
    }

    public static ControlRegistrationOutcomeMessageV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ControlRegistrationOutcomeMessageV1");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("ControlRegistrationOutcomeMessageV1 must contain outcome and branch");
        }
        final ControlRegistrationOutcomeV1 outcome =
                ControlRegistrationOutcomeV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final int expectedField =
                switch (outcome) {
                    case RECORDED -> 10;
                    case DEFINITELY_NOT_RECORDED -> 11;
                    case RECORD_UNCERTAIN -> 12;
                };
        if (fields.get(1).number() != expectedField) {
            throw new IllegalArgumentException("Control registration branch does not match outcome");
        }
        final Object branch =
                switch (outcome) {
                    case RECORDED ->
                        ControlOperationReceiptV1.decodePayload(QueryCodecSupport.nested(fields.get(1), expectedField));
                    case DEFINITELY_NOT_RECORDED ->
                        ControlDefinitelyNotRecordedV1.decode(QueryCodecSupport.nested(fields.get(1), expectedField));
                    case RECORD_UNCERTAIN ->
                        ControlRecordUncertainV1.decode(QueryCodecSupport.nested(fields.get(1), expectedField));
                };
        final ControlRegistrationOutcomeMessageV1 result = new ControlRegistrationOutcomeMessageV1(outcome, branch);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlRegistrationOutcomeMessageV1");
        return result;
    }

    private static void validateBranch(final ControlRegistrationOutcomeV1 outcome, final Object branch) {
        final boolean valid =
                switch (outcome) {
                    case RECORDED -> branch instanceof ControlOperationReceiptV1;
                    case DEFINITELY_NOT_RECORDED -> branch instanceof ControlDefinitelyNotRecordedV1;
                    case RECORD_UNCERTAIN -> branch instanceof ControlRecordUncertainV1;
                };
        if (!valid) {
            throw new IllegalArgumentException("Control registration branch does not match outcome");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlRegistrationOutcomeMessageV1 that
                && outcome == that.outcome
                && branch.equals(that.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, branch);
    }
}
