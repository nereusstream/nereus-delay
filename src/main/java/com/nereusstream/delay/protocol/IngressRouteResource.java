package com.nereusstream.delay.protocol;

import java.util.List;

/** Closed oneof for the exact physical resource used by a signed Ingress Route. */
public interface IngressRouteResource {
    String authenticatedClusterId();

    int partitionCount();

    AdapterKind adapterKind();

    byte[] canonicalBytes();

    static IngressRouteResource decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "IngressRouteResource");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("IngressRouteResource must select one branch");
        }
        return switch (fields.get(0).number()) {
            case 1 -> KafkaIngressRouteResource.decode(encoded);
            case 2 -> PulsarIngressRouteResource.decode(encoded);
            default -> throw new IllegalArgumentException("unknown IngressRouteResource branch");
        };
    }
}
