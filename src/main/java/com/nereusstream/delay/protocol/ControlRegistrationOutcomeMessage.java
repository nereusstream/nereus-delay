package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Closed Control registration outcome union. */
public final class ControlRegistrationOutcomeMessage {
    private final ControlRegistrationOutcome outcome;
    private final Object branch;

    private ControlRegistrationOutcomeMessage(final ControlRegistrationOutcome outcome, final Object branch) {
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.branch = Objects.requireNonNull(branch, "branch");
        validateBranch(outcome, branch);
    }

    public static ControlRegistrationOutcomeMessage recorded(final ControlOperationReceipt receipt) {
        return new ControlRegistrationOutcomeMessage(
                ControlRegistrationOutcome.RECORDED, Objects.requireNonNull(receipt, "receipt"));
    }

    public static ControlRegistrationOutcomeMessage definitelyNotRecorded(final ControlDefinitelyNotRecorded value) {
        return new ControlRegistrationOutcomeMessage(
                ControlRegistrationOutcome.DEFINITELY_NOT_RECORDED, Objects.requireNonNull(value, "value"));
    }

    public static ControlRegistrationOutcomeMessage recordUncertain(final ControlRecordUncertain value) {
        return new ControlRegistrationOutcomeMessage(
                ControlRegistrationOutcome.RECORD_UNCERTAIN, Objects.requireNonNull(value, "value"));
    }

    public ControlRegistrationOutcome outcome() {
        return outcome;
    }

    public ControlOperationReceipt receipt() {
        return branch instanceof ControlOperationReceipt value ? value : null;
    }

    public ControlDefinitelyNotRecorded definitelyNotRecorded() {
        return branch instanceof ControlDefinitelyNotRecorded value ? value : null;
    }

    public ControlRecordUncertain uncertain() {
        return branch instanceof ControlRecordUncertain value ? value : null;
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
                        case ControlOperationReceipt receipt -> receipt.payload();
                        case ControlDefinitelyNotRecorded rejection -> rejection.canonicalBytes();
                        case ControlRecordUncertain uncertain -> uncertain.canonicalBytes();
                        default -> throw new IllegalStateException("unknown Control registration branch");
                    };
            CanonicalProtobuf.bytes(output, field, value);
        });
    }

    public static ControlRegistrationOutcomeMessage decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ControlRegistrationOutcomeMessage");
        if (fields.size() != 2 || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("ControlRegistrationOutcomeMessage must contain outcome and branch");
        }
        final ControlRegistrationOutcome outcome =
                ControlRegistrationOutcome.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
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
                        ControlOperationReceipt.decodePayload(QueryCodecSupport.nested(fields.get(1), expectedField));
                    case DEFINITELY_NOT_RECORDED ->
                        ControlDefinitelyNotRecorded.decode(QueryCodecSupport.nested(fields.get(1), expectedField));
                    case RECORD_UNCERTAIN ->
                        ControlRecordUncertain.decode(QueryCodecSupport.nested(fields.get(1), expectedField));
                };
        final ControlRegistrationOutcomeMessage result = new ControlRegistrationOutcomeMessage(outcome, branch);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlRegistrationOutcomeMessage");
        return result;
    }

    private static void validateBranch(final ControlRegistrationOutcome outcome, final Object branch) {
        final boolean valid =
                switch (outcome) {
                    case RECORDED -> branch instanceof ControlOperationReceipt;
                    case DEFINITELY_NOT_RECORDED -> branch instanceof ControlDefinitelyNotRecorded;
                    case RECORD_UNCERTAIN -> branch instanceof ControlRecordUncertain;
                };
        if (!valid) {
            throw new IllegalArgumentException("Control registration branch does not match outcome");
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlRegistrationOutcomeMessage that
                && outcome == that.outcome
                && branch.equals(that.branch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outcome, branch);
    }
}
