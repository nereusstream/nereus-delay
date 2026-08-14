package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Authenticated Gateway ingress shared by gRPC and future HTTP adapters.
 * Authentication, quota admission and safe audit happen before the domain
 * service can prepare bytes or acquire a physical attempt.
 */
public final class GatewayIngressService {
    private final GatewayScheduleService scheduleService;
    private final GatewayTenantAuthority tenantAuthority;
    private final GatewayAdmissionController admission;
    private final GatewayAuditSink audit;
    private final TrustedClock trustedClock;

    public GatewayIngressService(final GatewayScheduleService scheduleService,
                                 final GatewayTenantAuthority tenantAuthority,
                                 final GatewayAdmissionController admission,
                                 final GatewayAuditSink audit,
                                 final TrustedClock trustedClock) {
        this.scheduleService = Objects.requireNonNull(scheduleService, "scheduleService");
        this.tenantAuthority = Objects.requireNonNull(tenantAuthority, "tenantAuthority");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.audit = Objects.requireNonNull(audit, "audit");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    public CompletionStage<GatewaySubmissionOutcomeV1> schedule(final GatewayPeerContext peerContext,
                                                                 final GatewayScheduleRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        final Digest32 keyHash = keyHash(tenant, request.idempotencyKey());
        final Digest32 bodyHash = bodyHash(GatewayOperationKindV1.SCHEDULE, request.canonicalBodyBytes());
        final GatewayAdmissionLease lease;
        try {
            lease = admit(tenant, GatewayIngressOperationV1.SCHEDULE, request.canonicalBodyBytes().length);
        } catch (GatewayAdmissionRejectedException denied) {
            recordFailure(GatewayIngressOperationV1.SCHEDULE, keyHash, bodyHash);
            return completed(preparationError(denied.code()));
        }
        try {
            recordReceived(GatewayIngressOperationV1.SCHEDULE, keyHash, bodyHash);
        } catch (RuntimeException failure) {
            lease.close();
            throw failure;
        }
        return submitAndAudit(lease, GatewayIngressOperationV1.SCHEDULE, keyHash, bodyHash,
                () -> scheduleService.schedule(tenant, request));
    }

    public CompletionStage<GatewaySubmissionOutcomeV1> retryUncertain(
            final GatewayPeerContext peerContext, final GatewayRetryUncertainRequestV1 request) {
        Objects.requireNonNull(peerContext, "peerContext");
        Objects.requireNonNull(request, "request");
        final AuthenticatedTenantContext tenant = authenticate(peerContext);
        final Digest32 keyHash = keyHash(tenant, request.originalIdempotencyKey());
        final Digest32 requestHash = GatewayIdempotencyHashV1.retryRequestHash(keyHash,
                request.expectedPriorPhysicalAttemptId(), request.retryRequestId());
        final GatewayAdmissionLease lease;
        try {
            lease = admit(tenant, GatewayIngressOperationV1.RETRY_UNCERTAIN,
                    request.originalIdempotencyKey().length + 32L);
        } catch (GatewayAdmissionRejectedException denied) {
            recordFailure(GatewayIngressOperationV1.RETRY_UNCERTAIN, keyHash, requestHash);
            return completed(preparationError(denied.code()));
        }
        try {
            recordReceived(GatewayIngressOperationV1.RETRY_UNCERTAIN, keyHash, requestHash);
        } catch (RuntimeException failure) {
            lease.close();
            throw failure;
        }
        return submitAndAudit(lease, GatewayIngressOperationV1.RETRY_UNCERTAIN, keyHash, requestHash,
                () -> scheduleService.retryUncertain(tenant, request));
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

    private GatewayAdmissionLease admit(final AuthenticatedTenantContext tenant,
                                        final GatewayIngressOperationV1 operation,
                                        final long estimatedBytes) {
        final GatewayAdmissionController.Decision decision;
        try {
            decision = admission.reserve(new GatewayAdmissionRequestV1(tenant, operation, estimatedBytes));
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.INTERNAL, failure);
        }
        if (decision.state() == GatewayAdmissionController.State.REJECTED) {
            throw new GatewayAdmissionRejectedException(decision.rejectionCode());
        }
        return decision.lease();
    }

    private void recordReceived(final GatewayIngressOperationV1 operation, final Digest32 keyHash,
                                final Digest32 bodyHash) {
        try {
            audit.record(new GatewayAuditEventV1(operation, keyHash, bodyHash, GatewayAuditPhaseV1.RECEIVED,
                    null, now()));
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }
    }

    private CompletionStage<GatewaySubmissionOutcomeV1> submitAndAudit(final GatewayAdmissionLease lease,
                                                                         final GatewayIngressOperationV1 operation,
                                                                         final Digest32 keyHash,
                                                                         final Digest32 bodyHash,
                                                                         final SubmissionCall call) {
        final CompletionStage<GatewaySubmissionOutcomeV1> stage;
        try {
            stage = Objects.requireNonNull(call.invoke(), "Gateway domain stage");
        } catch (RuntimeException failure) {
            lease.close();
            recordFailure(operation, keyHash, bodyHash);
            throw failure;
        }
        return stage.handle((outcome, failure) -> {
            try {
                if (failure != null || outcome == null) {
                    recordFailure(operation, keyHash, bodyHash);
                    throw new CompletionException(failure == null
                            ? new IllegalStateException("Gateway domain stage returned no outcome") : failure);
                }
                try {
                    audit.record(GatewayAuditEventV1.completed(operation, keyHash, bodyHash,
                            outcome.hasSubmissionOutcome() ? outcome.submissionOutcome().canonicalBytes()
                                    : outcome.preparationError().canonicalBytes(), now()));
                } catch (RuntimeException auditFailure) {
                    throw new CompletionException(new GatewayIngressException(
                            GatewayIngressException.Kind.UNAVAILABLE, auditFailure));
                }
                return outcome;
            } finally {
                lease.close();
            }
        });
    }

    private void recordFailure(final GatewayIngressOperationV1 operation, final Digest32 keyHash,
                               final Digest32 bodyHash) {
        try {
            audit.record(new GatewayAuditEventV1(operation, keyHash, bodyHash, GatewayAuditPhaseV1.FAILED,
                    null, now()));
        } catch (RuntimeException ignored) {
            // The original failure remains the safe caller-visible boundary.
        }
    }

    private long now() {
        final long value;
        try {
            value = trustedClock.nowEpochMs();
        } catch (RuntimeException failure) {
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE, failure);
        }
        if (value < 0) {
            throw new GatewayIngressException(GatewayIngressException.Kind.UNAVAILABLE,
                    new IllegalStateException("trusted Gateway clock returned a negative epoch"));
        }
        return value;
    }

    private static Digest32 keyHash(final AuthenticatedTenantContext tenant, final byte[] idempotencyKey) {
        return GatewayIdempotencyHashV1.keyHash(tenant.authenticatedTenantScopeHash(), idempotencyKey);
    }

    private static Digest32 bodyHash(final GatewayOperationKindV1 operation, final byte[] body) {
        return GatewayIdempotencyHashV1.bodyHash(operation, body);
    }

    private static GatewaySubmissionOutcomeV1 preparationError(final StableCode code) {
        return GatewaySubmissionOutcomeV1.preparationError(
                StableErrorV1.of(FailureStageV1.PREPARATION, code, null, null, null, null));
    }

    private static <T> CompletionStage<T> completed(final T value) {
        return java.util.concurrent.CompletableFuture.completedFuture(value);
    }

    private static final class GatewayAdmissionRejectedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final StableCode code;

        private GatewayAdmissionRejectedException(final StableCode code) {
            super("Gateway admission rejected before preparation");
            this.code = Objects.requireNonNull(code, "code");
        }

        private StableCode code() {
            return code;
        }
    }

    @FunctionalInterface
    private interface SubmissionCall {
        CompletionStage<GatewaySubmissionOutcomeV1> invoke();
    }
}
