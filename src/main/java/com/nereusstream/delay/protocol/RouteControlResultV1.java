package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Public-safe result for one Route control target. */
public final class RouteControlResultV1 {
    private final byte[] routeUuid;
    private final RouteLifecycleV1 lifecycle;
    private final long controlVersion;

    public RouteControlResultV1(final byte[] routeUuid, final RouteLifecycleV1 lifecycle, final long controlVersion) {
        Bytes.requireLength(routeUuid, RouteIncarnation.LENGTH, "routeUuid");
        this.routeUuid = Bytes.copy(routeUuid);
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        if (controlVersion <= 0) {
            throw new IllegalArgumentException("controlVersion must be positive");
        }
        this.controlVersion = controlVersion;
    }

    public byte[] routeUuid() {
        return Bytes.copy(routeUuid);
    }

    public RouteLifecycleV1 lifecycle() {
        return lifecycle;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, routeUuid);
            CanonicalProtobuf.uint32(output, 2, lifecycle.wireValue());
            CanonicalProtobuf.uint64(output, 3, controlVersion);
        });
    }

    public static RouteControlResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "RouteControlResultV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "RouteControlResultV1");
        final RouteControlResultV1 result = new RouteControlResultV1(
                QueryCodecSupport.fixed(fields.get(0), 1, RouteIncarnation.LENGTH),
                RouteLifecycleV1.fromWire(QueryCodecSupport.uint(fields.get(1), 2)),
                QueryCodecSupport.uint(fields.get(2), 3));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RouteControlResultV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RouteControlResultV1 that
                && controlVersion == that.controlVersion
                && lifecycle == that.lifecycle
                && Arrays.equals(routeUuid, that.routeUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(routeUuid), lifecycle, controlVersion);
    }
}
