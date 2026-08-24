package com.nereusstream.delay.gateway;

/** Gateway ingress pools are deliberately separated for schedule, retry and control traffic. */
public enum GatewayIngressOperationV1 {
    SCHEDULE,
    RETRY_UNCERTAIN,
    CONTROL
}
