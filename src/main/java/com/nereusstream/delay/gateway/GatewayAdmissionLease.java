package com.nereusstream.delay.gateway;

/** Idempotent release handle for one tenant-scoped Gateway admission slot. */
public interface GatewayAdmissionLease extends AutoCloseable {
    GatewayIngressOperationV1 operation();

    long estimatedRequestBytes();

    @Override
    void close();
}
