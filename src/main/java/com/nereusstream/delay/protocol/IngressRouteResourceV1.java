package com.nereusstream.delay.protocol;

import java.util.List;

/** Closed oneof for the exact physical resource used by a signed Ingress Route. */
public interface IngressRouteResourceV1 {
    String authenticatedClusterId();

    int partitionCount();

    AdapterKindV1 adapterKind();

    byte[] canonicalBytes();

    static IngressRouteResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "IngressRouteResourceV1");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("IngressRouteResourceV1 must select one branch");
        }
        return switch (fields.get(0).number()) {
            case 1 -> KafkaIngressRouteResourceV1.decode(encoded);
            case 2 -> PulsarIngressRouteResourceV1.decode(encoded);
            default -> throw new IllegalArgumentException("unknown IngressRouteResourceV1 branch");
        };
    }
}
