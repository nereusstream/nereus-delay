package io.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Deterministic local projection produced after a Control registration CAS.
 * It keeps the receipt and its revision-one CURRENT value paired so callers
 * cannot accidentally register a receipt against a different target snapshot.
 */
public record ControlRegistrationProjectionV1(ControlOperationReceiptV1 receipt,
                                              CurrentControlOperationV1 current) {
    public ControlRegistrationProjectionV1 {
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
    public static ControlRegistrationProjectionV1 initial(final PreparedControlOperationV1 prepared,
                                                          final TrustedUtcIntervalEvidence registeredAt,
                                                          final long queryUntilEpochMs) {
        Objects.requireNonNull(prepared, "prepared");
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.create(prepared.operationId(),
                prepared.requestHash(), prepared.author().tenantResourceScopeHash(),
                prepared.targetSnapshotHash(), 1, Objects.requireNonNull(registeredAt, "registeredAt"),
                queryUntilEpochMs);
        final CurrentControlOperationV1 current = prepared.initialCurrentOperation();
        return new ControlRegistrationProjectionV1(receipt, current);
    }

    /** Creates the same projection from a fixed control query window. */
    public static ControlRegistrationProjectionV1 initialWithQueryWindow(
            final PreparedControlOperationV1 prepared, final TrustedUtcIntervalEvidence registeredAt,
            final long controlOperationQueryWindowMs) {
        Objects.requireNonNull(prepared, "prepared");
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.createWithQueryWindow(
                prepared.operationId(), prepared.requestHash(), prepared.author().tenantResourceScopeHash(),
                prepared.targetSnapshotHash(), 1, Objects.requireNonNull(registeredAt, "registeredAt"),
                controlOperationQueryWindowMs);
        return new ControlRegistrationProjectionV1(receipt, prepared.initialCurrentOperation());
    }
}
