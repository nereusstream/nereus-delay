package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.concurrent.CompletionStage;

/**
 * Tenant-bound payload authority consumed by the Gateway payload RPCs.
 * Implementations own Object Store credentials, reservation registration and
 * proof-key custody; the Gateway never accepts those as request fields.
 */
public interface GatewayPayloadAuthority {
    CompletionStage<PayloadUploadHandleResponse> issueUploadHandle(
            AuthenticatedTenantContext tenant,
            PayloadReservationReceipt receipt,
            UploadHandleKind kind,
            long nowEpochMs);

    CompletionStage<PayloadAttestationResponse> attestUpload(
            AuthenticatedTenantContext tenant,
            PayloadReservationReceipt receipt,
            OpaquePayloadUploadHandle handle,
            long nowEpochMs);
}
