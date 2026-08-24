package com.nereusstream.delay.gateway;

/** Safe Gateway audit lifecycle; event payloads contain digests, never raw credentials or request bytes. */
public enum GatewayAuditPhaseV1 {
    RECEIVED,
    COMPLETED,
    FAILED
}
