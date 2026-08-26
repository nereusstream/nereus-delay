package com.nereusstream.delay.gateway;

/** Gateway ingress pools are deliberately separated for schedule, retry and control traffic. */
public enum GatewayIngressOperation {
    SCHEDULE,
    RETRY_UNCERTAIN,
    CONTROL
}
