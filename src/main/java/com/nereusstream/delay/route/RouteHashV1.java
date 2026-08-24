package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.RoutingHashVersionV1;
import java.util.Objects;

/** Exact ROUTING_HASH_V1 partition calculation; no display name participates. */
public final class RouteHashV1 {
    private RouteHashV1() {}

    public static int partition(
            final RouteSnapshotV1 snapshot, final byte[] tenantRoutingScope, final byte[] routingKey) {
        Objects.requireNonNull(snapshot, "snapshot");
        Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        Objects.requireNonNull(routingKey, "routingKey");
        if (snapshot.routingHashVersion() != RoutingHashVersionV1.ROUTING_HASH_V1) {
            throw new IllegalArgumentException("unsupported Route routing hash");
        }
        final byte[] digest = Bytes.sha256(
                Bytes.utf8("nereus-delay-routing-v1"),
                Bytes.lp32(snapshot.routeIncarnation().bytes()),
                Bytes.lp32(tenantRoutingScope),
                Bytes.lp32(routingKey));
        final long unsignedFirst64 = Bytes.readU64be(digest, 0);
        return (int) Long.remainderUnsigned(
                unsignedFirst64, Integer.toUnsignedLong(snapshot.ingress().partitionCount()));
    }
}
