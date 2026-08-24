package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import com.nereusstream.delay.protocol.UploadHandleKindV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.concurrent.CompletionStage;

/**
 * Tenant-bound payload authority consumed by the Gateway payload RPCs.
 * Implementations own Object Store credentials, reservation registration and
 * proof-key custody; the Gateway never accepts those as request fields.
 */
public interface GatewayPayloadAuthority {
    CompletionStage<PayloadUploadHandleResponseV1> issueUploadHandle(
            AuthenticatedTenantContext tenant,
            PayloadReservationReceiptV1 receipt,
            UploadHandleKindV1 kind,
            long nowEpochMs);

    CompletionStage<PayloadAttestationResponseV1> attestUpload(
            AuthenticatedTenantContext tenant,
            PayloadReservationReceiptV1 receipt,
            OpaquePayloadUploadHandleV1 handle,
            long nowEpochMs);
}
