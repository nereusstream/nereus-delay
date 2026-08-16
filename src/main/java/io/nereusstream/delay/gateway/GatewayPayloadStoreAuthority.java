package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;

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

    public GatewayPayloadStoreAuthority(final byte[] tenantRoutingScope,
                                         final UploadHandleIssuer handleIssuer,
                                         final PayloadAttestor attestor) {
        io.nereusstream.delay.protocol.Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        this.tenantRoutingScope = io.nereusstream.delay.protocol.Bytes.copy(tenantRoutingScope);
        this.handleIssuer = Objects.requireNonNull(handleIssuer, "handleIssuer");
        this.attestor = Objects.requireNonNull(attestor, "attestor");
    }

    @Override
    public CompletionStage<PayloadUploadHandleResponseV1> issueUploadHandle(
            final AuthenticatedTenantContext tenant, final PayloadReservationReceiptV1 receipt,
            final UploadHandleKindV1 kind, final long nowEpochMs) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(kind, "kind");
        if (!Arrays.equals(tenantRoutingScope, tenant.tenantRoutingScope())) {
            return CompletableFuture.completedFuture(PayloadUploadHandleResponseV1.error(
                    PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, unauthorizedError()));
        }
        return CompletableFuture.completedFuture(handleIssuer.issue(receipt, kind, nowEpochMs));
    }

    @Override
    public CompletionStage<PayloadAttestationResponseV1> attestUpload(
            final AuthenticatedTenantContext tenant, final PayloadReservationReceiptV1 receipt,
            final OpaquePayloadUploadHandleV1 handle, final long nowEpochMs) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        if (!Arrays.equals(tenantRoutingScope, tenant.tenantRoutingScope())) {
            return CompletableFuture.completedFuture(PayloadAttestationResponseV1.error(
                    PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, unauthorizedError()));
        }
        return CompletableFuture.completedFuture(attestor.attest(receipt, handle, nowEpochMs));
    }

    private static StableErrorV1 unauthorizedError() {
        return StableErrorV1.of(FailureStageV1.PAYLOAD, StableCode.NOT_FOUND_OR_NOT_AUTHORIZED,
                null, null, null, null);
    }

    @FunctionalInterface
    public interface UploadHandleIssuer {
        PayloadUploadHandleResponseV1 issue(PayloadReservationReceiptV1 receipt,
                                             UploadHandleKindV1 kind, long nowEpochMs);
    }

    @FunctionalInterface
    public interface PayloadAttestor {
        PayloadAttestationResponseV1 attest(PayloadReservationReceiptV1 receipt,
                                             OpaquePayloadUploadHandleV1 handle, long nowEpochMs);
    }
}
