package com.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Deterministic local projection produced after a Control registration CAS.
 * It keeps the receipt and its revision-one CURRENT value paired so callers
 * cannot accidentally register a receipt against a different target snapshot.
 */
public record ControlRegistrationProjection(ControlOperationReceipt receipt, CurrentControlOperation current) {
    public ControlRegistrationProjection {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(current, "current");
        if (current.operationRevision() != receipt.operationRevision()) {
            throw new IllegalArgumentException("registration projection revision mismatch");
        }
        if (!Bytes.constantTimeEquals(receipt.operationId(), current.operationId())
                || !Bytes.constantTimeEquals(receipt.requestHash(), current.requestHash())
                || !Bytes.constantTimeEquals(receipt.authenticatedScopeHash(), current.authenticatedScopeHash())) {
            throw new IllegalArgumentException("registration projection identity mismatch");
        }
    }

    /** Creates the revision-one PENDING projection for an exact Prepared operation. */
    public static ControlRegistrationProjection initial(
            final PreparedControlOperation prepared,
            final TrustedUtcIntervalEvidence registeredAt,
            final long queryUntilEpochMs) {
        Objects.requireNonNull(prepared, "prepared");
        final ControlOperationReceipt receipt = ControlOperationReceipt.create(
                prepared.operationId(),
                prepared.requestHash(),
                prepared.author().tenantResourceScopeHash(),
                prepared.targetSnapshotHash(),
                1,
                Objects.requireNonNull(registeredAt, "registeredAt"),
                queryUntilEpochMs);
        final CurrentControlOperation current = prepared.initialCurrentOperation();
        return new ControlRegistrationProjection(receipt, current);
    }

    /** Creates the same projection from a fixed control query window. */
    public static ControlRegistrationProjection initialWithQueryWindow(
            final PreparedControlOperation prepared,
            final TrustedUtcIntervalEvidence registeredAt,
            final long controlOperationQueryWindowMs) {
        Objects.requireNonNull(prepared, "prepared");
        final ControlOperationReceipt receipt = ControlOperationReceipt.createWithQueryWindow(
                prepared.operationId(),
                prepared.requestHash(),
                prepared.author().tenantResourceScopeHash(),
                prepared.targetSnapshotHash(),
                1,
                Objects.requireNonNull(registeredAt, "registeredAt"),
                controlOperationQueryWindowMs);
        return new ControlRegistrationProjection(receipt, prepared.initialCurrentOperation());
    }
}
