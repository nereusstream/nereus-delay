package io.nereusstream.delay.gateway;

/** Transport-facing failure before a canonical Gateway domain response can be emitted. */
public final class GatewayIngressException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public enum Kind {
        AUTHENTICATION,
        UNAVAILABLE,
        INTERNAL
    }

    private final Kind kind;

    public GatewayIngressException(final Kind kind, final Throwable cause) {
        super("Gateway ingress could not safely accept the request", cause);
        this.kind = java.util.Objects.requireNonNull(kind, "kind");
    }

    public Kind kind() {
        return kind;
    }
}
