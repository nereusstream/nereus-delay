package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.transport.Digest32;

/** Canonical Gateway idempotency key/body hash construction. */
public final class GatewayIdempotencyHashV1 {
    private GatewayIdempotencyHashV1() {
    }

    public static Digest32 keyHash(final byte[] tenantScopeHash, final byte[] idempotencyKey) {
        Bytes.requireLength(tenantScopeHash, 32, "tenantScopeHash");
        return new Digest32(Bytes.sha256(Bytes.utf8("nereus-delay-gateway-idempotency-key-v1\0"),
                tenantScopeHash, Bytes.lp32(idempotencyKey)));
    }

    public static Digest32 bodyHash(final GatewayOperationKindV1 operation, final byte[] canonicalBody) {
        return new Digest32(Bytes.sha256(Bytes.utf8("nereus-delay-gateway-request-v1\0"),
                Bytes.u16be(operation.wireValue()), Bytes.lp32(canonicalBody)));
    }
}
