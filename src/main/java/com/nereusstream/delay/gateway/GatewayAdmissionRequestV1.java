package com.nereusstream.delay.gateway;

import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.Objects;

/** Tenant-scoped, pre-preparation admission input; it carries no raw request bytes. */
public record GatewayAdmissionRequestV1(
        AuthenticatedTenantContext tenant, GatewayIngressOperationV1 operation, long estimatedRequestBytes) {
    public GatewayAdmissionRequestV1 {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(operation, "operation");
        if (estimatedRequestBytes <= 0) {
            throw new IllegalArgumentException("estimatedRequestBytes must be positive");
        }
    }
}
