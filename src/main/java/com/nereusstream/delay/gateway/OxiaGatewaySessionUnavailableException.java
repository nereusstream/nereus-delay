package com.nereusstream.delay.gateway;

/** Raised when an Oxia-backed Gateway client no longer owns its session marker. */
final class OxiaGatewaySessionUnavailableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    OxiaGatewaySessionUnavailableException(final Throwable cause) {
        super("Oxia Gateway session is absent or changed; durable operation is fenced", cause);
    }
}
