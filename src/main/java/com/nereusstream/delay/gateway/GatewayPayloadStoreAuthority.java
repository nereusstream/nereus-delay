package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Gateway composition for one tenant-bound payload store authority.
 *
 * <p>The store owns reservation identity, provider credentials and proof-key
 * custody. This adapter adds the Gateway's authenticated tenant scope fence
 * and exposes only the two receipt-bound operations required by the payload
 * RPCs. Store calls remain synchronous at this boundary; the returned
 * completed stages preserve the Gateway async API.</p>
 */
public final class GatewayPayloadStoreAuthority implements GatewayPayloadAuthority {
    private final byte[] tenantRoutingScope;
    private final UploadHandleIssuer handleIssuer;
    private final PayloadAttestor attestor;

    public GatewayPayloadStoreAuthority(
            final byte[] tenantRoutingScope, final UploadHandleIssuer handleIssuer, final PayloadAttestor attestor) {
        com.nereusstream.delay.protocol.Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        this.tenantRoutingScope = com.nereusstream.delay.protocol.Bytes.copy(tenantRoutingScope);
        this.handleIssuer = Objects.requireNonNull(handleIssuer, "handleIssuer");
        this.attestor = Objects.requireNonNull(attestor, "attestor");
    }

    @Override
    public CompletionStage<PayloadUploadHandleResponse> issueUploadHandle(
            final AuthenticatedTenantContext tenant,
            final PayloadReservationReceipt receipt,
            final UploadHandleKind kind,
            final long nowEpochMs) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(kind, "kind");
        if (!Arrays.equals(tenantRoutingScope, tenant.tenantRoutingScope())) {
            return CompletableFuture.completedFuture(PayloadUploadHandleResponse.error(
                    PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, unauthorizedError()));
        }
        return CompletableFuture.completedFuture(handleIssuer.issue(receipt, kind, nowEpochMs));
    }

    @Override
    public CompletionStage<PayloadAttestationResponse> attestUpload(
            final AuthenticatedTenantContext tenant,
            final PayloadReservationReceipt receipt,
            final OpaquePayloadUploadHandle handle,
            final long nowEpochMs) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        if (!Arrays.equals(tenantRoutingScope, tenant.tenantRoutingScope())) {
            return CompletableFuture.completedFuture(PayloadAttestationResponse.error(
                    PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, unauthorizedError()));
        }
        return CompletableFuture.completedFuture(attestor.attest(receipt, handle, nowEpochMs));
    }

    private static StableError unauthorizedError() {
        return StableError.of(FailureStage.PAYLOAD, StableCode.NOT_FOUND_OR_NOT_AUTHORIZED, null, null, null, null);
    }

    @FunctionalInterface
    public interface UploadHandleIssuer {
        PayloadUploadHandleResponse issue(PayloadReservationReceipt receipt, UploadHandleKind kind, long nowEpochMs);
    }

    @FunctionalInterface
    public interface PayloadAttestor {
        PayloadAttestationResponse attest(
                PayloadReservationReceipt receipt, OpaquePayloadUploadHandle handle, long nowEpochMs);
    }
}
