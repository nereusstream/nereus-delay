package com.nereusstream.delay.gateway;

/** Idempotent release handle for one tenant-scoped Gateway admission slot. */
public interface GatewayAdmissionLease extends AutoCloseable {
    GatewayIngressOperation operation();

    long estimatedRequestBytes();

    @Override
    void close();
}
