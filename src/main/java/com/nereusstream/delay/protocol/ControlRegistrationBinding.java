package com.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Checks that a Control registration outcome is bound to one exact prepared
 * operation. Transport and Oxia proof classification remain external.
 */
public final class ControlRegistrationBinding {
    private ControlRegistrationBinding() {}

    public static void validate(
            final PreparedControlOperation prepared, final ControlRegistrationOutcomeMessage outcome) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(outcome, "outcome");
        switch (outcome.outcome()) {
            case RECORDED -> validateReceipt(prepared, outcome.receipt());
            case DEFINITELY_NOT_RECORDED -> {
                final ControlDefinitelyNotRecorded rejection = outcome.definitelyNotRecorded();
                if (!Bytes.constantTimeEquals(prepared.preparedDigest(), rejection.preparedDigest())
                        || !Bytes.constantTimeEquals(
                                prepared.operationId(), rejection.proof().operationId())
                        || !Bytes.constantTimeEquals(
                                prepared.preparedDigest(), rejection.proof().preparedDigest())) {
                    throw new IllegalArgumentException("Control rejection is not bound to the prepared operation");
                }
            }
            case RECORD_UNCERTAIN -> {
                final ControlRecordUncertain uncertain = outcome.uncertain();
                if (!Bytes.constantTimeEquals(prepared.operationId(), uncertain.operationId())
                        || !Bytes.constantTimeEquals(prepared.preparedDigest(), uncertain.preparedDigest())) {
                    throw new IllegalArgumentException(
                            "uncertain Control outcome is not bound to the prepared operation");
                }
            }
        }
    }

    private static void validateReceipt(
            final PreparedControlOperation prepared, final ControlOperationReceipt receipt) {
        Objects.requireNonNull(receipt, "Control receipt");
        if (!Bytes.constantTimeEquals(prepared.operationId(), receipt.operationId())
                || !Bytes.constantTimeEquals(prepared.requestHash(), receipt.requestHash())
                || !Bytes.constantTimeEquals(
                        prepared.author().tenantResourceScopeHash(), receipt.authenticatedScopeHash())
                || !Bytes.constantTimeEquals(prepared.targetSnapshotHash(), receipt.targetSnapshotHash())) {
            throw new IllegalArgumentException("Control receipt is not bound to the prepared operation");
        }
        if (receipt.operationRevision() != 1) {
            throw new IllegalArgumentException("initial Control receipt must have revision one");
        }
        if (receipt.queryUntilEpochMs() < receipt.registeredAt().latestEpochMs()) {
            throw new IllegalArgumentException("Control receipt query boundary is invalid");
        }
    }
}
