package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/** Authenticated, bounded Gateway query composition. */
public final class GatewayQueryIngressService {
    private static final byte[] KEY_DOMAIN = Bytes.utf8("nereus-delay-gateway-query-audit-key-v1\0");
    private static final byte[] BODY_DOMAIN = Bytes.utf8("nereus-delay-gateway-query-audit-body-v1\0");

    private final GatewayQueryAuthority queryAuthority;
    private final GatewayTenantAuthority tenantAuthority;
    private final GatewayAdmissionController admission;
    private final GatewayAuditSink audit;
    private final TrustedClock trustedClock;
    private final int maxAwaitResponses;

    public GatewayQueryIngressService(final GatewayQueryAuthority queryAuthority,
                                      final GatewayTenantAuthority tenantAuthority,
                                      final GatewayAdmissionController admission,
                                      final GatewayAuditSink audit,
                                      final TrustedClock trustedClock,
                                      final int maxAwaitResponses) {
        this.queryAuthority = Objects.requireNonNull(queryAuthority, "queryAuthority");
        this.tenantAuthority = Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.audit = Objects.requireNonNull(audit, "audit");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        if (maxAwaitResponses <= 0 || maxAwaitResponses > 64) {
            throw new IllegalArgumentException("maxAwaitResponses must be 1..64");
        }
        this.maxAwaitResponses = maxAwaitResponses;
    }

    public CompletionStage<CommandQueryResponseV1> getCommandResult(
            final GatewayPeerContext peerContext, final GatewayGetCommandResultRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        return invoke(tenant, request.canonicalBodyBytes(),
                context -> queryAuthority.getCommandResult(context, request),
                response -> response.canonicalBytes());
    }

    public CompletionStage<List<CommandQueryResponseV1>> awaitApplied(
            final GatewayPeerContext peerContext, final GatewayAwaitAppliedRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        return invoke(tenant, request.canonicalBodyBytes(),
                context -> queryAuthority.awaitApplied(context, request), this::encodeAwaitResponses);
    }

    public CompletionStage<MessageQueryResponseV1> getMessage(
            final GatewayPeerContext peerContext, final GatewayGetMessageRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        return invoke(tenant, request.canonicalBodyBytes(),
                context -> queryAuthority.getMessage(context, request),
                response -> response.canonicalBytes());
    }

    private <T> CompletionStage<T> invoke(final AuthenticatedTenantContext tenant, final byte[] canonicalBody,
                                          final QueryCall<T> call, final Function<T, byte[]> responseEncoder) {
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
                        new IllegalStateException("Gateway query admission rejected: " + decision.rejectionCode()));
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
            stage = Objects.requireNonNull(call.invoke(tenant), "query authority stage");
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
                            failure == null ? new IllegalStateException("query authority returned no response")
                                    : failure));
                }
                audit.record(GatewayAuditEventV1.completed(GatewayIngressOperationV1.CONTROL, keyHash, bodyHash,
                        responseEncoder.apply(response), now()));
                return response;
            } catch (GatewayIngressException failureFromAudit) {
                throw new CompletionException(failureFromAudit);
            } finally {
                lease.close();
            }
        });
    }

    private byte[] encodeAwaitResponses(final List<CommandQueryResponseV1> responses) {
        if (responses == null || responses.isEmpty() || responses.size() > maxAwaitResponses) {
            throw new IllegalArgumentException("await response stream is outside the bounded Gateway limit");
        }
        return CanonicalProtobuf.message(output -> {
            int field = 1;
            for (CommandQueryResponseV1 response : responses) {
                CanonicalProtobuf.bytes(output, field++, Objects.requireNonNull(response, "response").canonicalBytes());
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

    @FunctionalInterface
    private interface QueryCall<T> {
        CompletionStage<T> invoke(AuthenticatedTenantContext tenant);
    }
}
