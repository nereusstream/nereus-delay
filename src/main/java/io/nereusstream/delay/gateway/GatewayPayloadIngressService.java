package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/** Authenticated Gateway composition for receipt-bound payload operations. */
public final class GatewayPayloadIngressService {
    private static final byte[] KEY_DOMAIN = Bytes.utf8("nereus-delay-gateway-payload-audit-key-v1\0");
    private static final byte[] BODY_DOMAIN = Bytes.utf8("nereus-delay-gateway-payload-audit-body-v1\0");

    private final GatewayPayloadAuthority payloadAuthority;
    private final GatewayTenantAuthority tenantAuthority;
    private final GatewayAdmissionController admission;
    private final GatewayAuditSink audit;
    private final TrustedClock trustedClock;

    public GatewayPayloadIngressService(final GatewayPayloadAuthority payloadAuthority,
                                        final GatewayTenantAuthority tenantAuthority,
                                        final GatewayAdmissionController admission,
                                        final GatewayAuditSink audit,
                                        final TrustedClock trustedClock) {
        this.payloadAuthority = Objects.requireNonNull(payloadAuthority, "payloadAuthority");
        this.tenantAuthority = Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.audit = Objects.requireNonNull(audit, "audit");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    public CompletionStage<PayloadUploadHandleResponseV1> issueUploadHandle(
            final GatewayPeerContext peerContext, final GatewayIssuePayloadUploadHandleRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        return invoke(tenant, request.canonicalBodyBytes(),
                (context, now) -> payloadAuthority.issueUploadHandle(context, request.reservation(), request.kind(), now));
    }

    public CompletionStage<PayloadAttestationResponseV1> attestUpload(
            final GatewayPeerContext peerContext, final GatewayAttestPayloadUploadRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        return invoke(tenant, request.canonicalBodyBytes(),
                (context, now) -> payloadAuthority.attestUpload(context, request.reservation(), request.handle(), now));
    }

    private <T> CompletionStage<T> invoke(final AuthenticatedTenantContext tenant, final byte[] canonicalBody,
                                          final PayloadCall<T> call) {
        final Digest32 keyHash = new Digest32(Bytes.sha256(KEY_DOMAIN, tenant.authenticatedTenantScopeHash(),
                canonicalBody));
        final Digest32 bodyHash = new Digest32(Bytes.sha256(BODY_DOMAIN, canonicalBody));
        final GatewayAdmissionLease lease;
        try {
            final GatewayAdmissionController.Decision decision = admission.reserve(
                    new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.CONTROL, canonicalBody.length));
            if (decision.state() == GatewayAdmissionController.State.REJECTED) {
                recordFailure(keyHash, bodyHash);
                throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE,
                        new IllegalStateException("Gateway payload admission rejected: " + decision.rejectionCode()));
            }
            lease = decision.lease();
        } catch (GatewayIngressException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            recordFailure(keyHash, bodyHash);
            throw new GatewayIngressException(GatewayIngressException.Kind.INTERNAL, failure);
        }
        try {
            audit.record(new GatewayAuditEventV1(GatewayIngressOperationV1.CONTROL, keyHash, bodyHash,
                    GatewayAuditPhaseV1.RECEIVED, null, now()));
        } catch (RuntimeException failure) {
            lease.close();
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }

        final CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(call.invoke(tenant, now()), "payload authority stage");
        } catch (RuntimeException failure) {
            lease.close();
            recordFailure(keyHash, bodyHash);
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }
        return stage.handle((response, failure) -> {
            try {
                if (failure != null || response == null) {
                    recordFailure(keyHash, bodyHash);
                    throw new CompletionException(new GatewayIngressException(
                            GatewayIngressException.Kind.UNAVAILABLE,
                            failure == null ? new IllegalStateException("payload authority returned no response")
                                    : failure));
                }
                audit.record(GatewayAuditEventV1.completed(GatewayIngressOperationV1.CONTROL, keyHash, bodyHash,
                        canonicalBytes(response), now()));
                return response;
            } catch (GatewayIngressException failureFromAudit) {
                throw new CompletionException(failureFromAudit);
            } finally {
                lease.close();
            }
        });
    }

    private AuthenticatedTenantContext authenticate(final GatewayPeerContext peerContext) {
        try {
            final AuthenticatedTenantContext tenant = tenantAuthority.authenticate(peerContext);
            if (tenant == null) {
                throw new IllegalArgumentException("tenant authority returned no context");
            }
            return tenant;
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.AUTHENTICATION, failure);
        }
    }

    private void recordFailure(final Digest32 keyHash, final Digest32 bodyHash) {
        try {
            audit.record(new GatewayAuditEventV1(GatewayIngressOperationV1.CONTROL, keyHash, bodyHash,
                    GatewayAuditPhaseV1.FAILED, null, now()));
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }
    }

    private long now() {
        try {
            final long now = trustedClock.nowEpochMs();
            if (now < 0) {
                throw new IllegalStateException("trusted clock returned a negative time");
            }
            return now;
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }
    }

    private static byte[] canonicalBytes(final Object response) {
        if (response instanceof PayloadUploadHandleResponseV1 upload) {
            return upload.canonicalBytes();
        }
        if (response instanceof PayloadAttestationResponseV1 attestation) {
            return attestation.canonicalBytes();
        }
        throw new IllegalArgumentException("unsupported payload response type");
    }

    @FunctionalInterface
    private interface PayloadCall<T> {
        CompletionStage<T> invoke(AuthenticatedTenantContext tenant, long nowEpochMs);
    }
}
