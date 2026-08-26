package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.transport.Digest32;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Deterministic local admission implementation with separate schedule,
 * retry and control pools. It is a conformance implementation, not a
 * distributed quota authority.
 */
public final class InMemoryGatewayAdmissionController implements GatewayAdmissionController {
    private final int maxScheduleInFlight;
    private final long maxScheduleBytes;
    private final int maxRetryInFlight;
    private final int maxControlInFlight;
    private final Map<Digest32, Usage> usage = new HashMap<>();

    public InMemoryGatewayAdmissionController(
            final int maxScheduleInFlight,
            final long maxScheduleBytes,
            final int maxRetryInFlight,
            final int maxControlInFlight) {
        if (maxScheduleInFlight <= 0 || maxScheduleBytes <= 0 || maxRetryInFlight <= 0 || maxControlInFlight <= 0) {
            throw new IllegalArgumentException("Gateway admission limits must be positive");
        }
        this.maxScheduleInFlight = maxScheduleInFlight;
        this.maxScheduleBytes = maxScheduleBytes;
        this.maxRetryInFlight = maxRetryInFlight;
        this.maxControlInFlight = maxControlInFlight;
    }

    @Override
    public synchronized Decision reserve(final GatewayAdmissionRequest request) {
        Objects.requireNonNull(request, "request");
        final Digest32 tenant = new Digest32(request.tenant().authenticatedTenantScopeHash());
        final Usage current = usage.computeIfAbsent(tenant, ignored -> new Usage());
        final boolean accepted =
                switch (request.operation()) {
                    case SCHEDULE ->
                        current.scheduleInFlight < maxScheduleInFlight
                                && current.scheduleBytes <= maxScheduleBytes - request.estimatedRequestBytes();
                    case RETRY_UNCERTAIN -> current.retryInFlight < maxRetryInFlight;
                    case CONTROL -> current.controlInFlight < maxControlInFlight;
                };
        if (!accepted) {
            if (current.scheduleBytes > maxScheduleBytes - request.estimatedRequestBytes()
                    && request.operation() == GatewayIngressOperation.SCHEDULE) {
                return Decision.rejected(StableCode.HARD_QUOTA_EXCEEDED);
            }
            return Decision.rejected(StableCode.ADMISSION_CAPACITY_GATED);
        }
        switch (request.operation()) {
            case SCHEDULE -> {
                current.scheduleInFlight++;
                current.scheduleBytes = Math.addExact(current.scheduleBytes, request.estimatedRequestBytes());
            }
            case RETRY_UNCERTAIN -> current.retryInFlight++;
            case CONTROL -> current.controlInFlight++;
        }
        return Decision.accepted(new Lease(this, tenant, request.operation(), request.estimatedRequestBytes()));
    }

    private synchronized void release(
            final Digest32 tenant, final GatewayIngressOperation operation, final long bytes) {
        final Usage current = usage.get(tenant);
        if (current == null) {
            throw new IllegalStateException("Gateway admission tenant usage is missing");
        }
        switch (operation) {
            case SCHEDULE -> {
                if (current.scheduleInFlight <= 0 || current.scheduleBytes < bytes) {
                    throw new IllegalStateException("Gateway schedule admission usage underflow");
                }
                current.scheduleInFlight--;
                current.scheduleBytes -= bytes;
            }
            case RETRY_UNCERTAIN -> {
                if (current.retryInFlight <= 0) {
                    throw new IllegalStateException("Gateway retry admission usage underflow");
                }
                current.retryInFlight--;
            }
            case CONTROL -> {
                if (current.controlInFlight <= 0) {
                    throw new IllegalStateException("Gateway control admission usage underflow");
                }
                current.controlInFlight--;
            }
        }
        if (current.scheduleInFlight == 0 && current.retryInFlight == 0 && current.controlInFlight == 0) {
            usage.remove(tenant);
        }
    }

    private static final class Usage {
        private int scheduleInFlight;
        private long scheduleBytes;
        private int retryInFlight;
        private int controlInFlight;
    }

    private static final class Lease implements GatewayAdmissionLease {
        private final InMemoryGatewayAdmissionController owner;
        private final Digest32 tenant;
        private final GatewayIngressOperation operation;
        private final long estimatedRequestBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(
                final InMemoryGatewayAdmissionController owner,
                final Digest32 tenant,
                final GatewayIngressOperation operation,
                final long estimatedRequestBytes) {
            this.owner = owner;
            this.tenant = tenant;
            this.operation = operation;
            this.estimatedRequestBytes = estimatedRequestBytes;
        }

        @Override
        public GatewayIngressOperation operation() {
            return operation;
        }

        @Override
        public long estimatedRequestBytes() {
            return estimatedRequestBytes;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(tenant, operation, estimatedRequestBytes);
            }
        }
    }
}
