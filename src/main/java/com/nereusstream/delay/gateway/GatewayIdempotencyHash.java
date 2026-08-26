package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

/** Canonical Gateway idempotency key/body hash construction. */
public final class GatewayIdempotencyHash {
    private GatewayIdempotencyHash() {}

    public static Digest32 keyHash(final byte[] tenantScopeHash, final byte[] idempotencyKey) {
        Bytes.requireLength(tenantScopeHash, 32, "tenantScopeHash");
        return new Digest32(Bytes.sha256(
                Bytes.utf8("nereus-delay-gateway-idempotency-key\0"), tenantScopeHash, Bytes.lp32(idempotencyKey)));
    }

    public static Digest32 bodyHash(final GatewayOperationKind operation, final byte[] canonicalBody) {
        return new Digest32(Bytes.sha256(
                Bytes.utf8("nereus-delay-gateway-request\0"),
                Bytes.u16be(operation.wireValue()),
                Bytes.lp32(canonicalBody)));
    }

    public static Digest32 retryRequestHash(
            final Digest32 keyHash,
            final PhysicalEnqueueAttemptId expectedPrior,
            final PhysicalEnqueueAttemptId retryRequestId) {
        return new Digest32(Bytes.sha256(
                Bytes.utf8("nereus-delay-gateway-retry-request\0"),
                keyHash.bytes(),
                expectedPrior.bytes(),
                retryRequestId.bytes()));
    }
}
