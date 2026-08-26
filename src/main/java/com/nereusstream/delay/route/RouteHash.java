package com.nereusstream.delay.route;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import java.util.Objects;

/** Exact ROUTING_HASH partition calculation; no display name participates. */
public final class RouteHash {
    private RouteHash() {}

    public static int partition(
            final RouteSnapshot snapshot, final byte[] tenantRoutingScope, final byte[] routingKey) {
        Objects.requireNonNull(snapshot, "snapshot");
        Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        Objects.requireNonNull(routingKey, "routingKey");
        if (snapshot.routingHashVersion() != RoutingHashVersion.ROUTING_HASH) {
            throw new IllegalArgumentException("unsupported Route routing hash");
        }
        final byte[] digest = Bytes.sha256(
                Bytes.utf8("nereus-delay-routing"),
                Bytes.lp32(snapshot.routeIncarnation().bytes()),
                Bytes.lp32(tenantRoutingScope),
                Bytes.lp32(routingKey));
        final long unsignedFirst64 = Bytes.readU64be(digest, 0);
        return (int) Long.remainderUnsigned(
                unsignedFirst64, Integer.toUnsignedLong(snapshot.ingress().partitionCount()));
    }
}
