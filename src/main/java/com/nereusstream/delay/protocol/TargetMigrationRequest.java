package com.nereusstream.delay.protocol;

/** Closed independent migration Control purposes; SERVICE authorizations cannot issue either purpose. */
public sealed interface TargetMigrationRequest extends ControlOperationRequestBranch
        permits TargetMigrationActivateRequest, TargetMigrationSettlementRequest,
                TargetMigrationRetirementIntentRequest, TargetMigrationRetirementConfirmRequest, TargetMigrationArchiveRequest {
    TargetQuotaScope scope();
    ControlOperationKind operationKind();
    int controlKind();
    ProtocolTuple tuple();
    default ControlOperationRequest operationRequest() { return new ControlOperationRequest(operationKind(), this); }
}
