package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GatewayPayloadStoreAuthorityTest {
    @Test
    void tenantRoutingScopeIsCheckedBeforeStoreInvocation() {
        final byte[] expectedScope = bytes(32, 1);
        final PayloadReservationReceiptV1 receipt = receipt();
        final OpaquePayloadUploadHandleV1 handle = OpaquePayloadUploadHandleV1.create(receipt.reservationId(),
                receipt.objectStoreProfile(), UploadHandleKindV1.OPAQUE_SINGLE_PUT, 10_000, bytes(32, 4));
        final GatewayPayloadStoreAuthority authority = new GatewayPayloadStoreAuthority(expectedScope,
                (ignoredReceipt, ignoredKind, ignoredNow) -> PayloadUploadHandleResponseV1.issued(handle),
                (ignoredReceipt, ignoredHandle, ignoredNow) -> PayloadAttestationResponseV1.error(
                        PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE, null));

        final AuthenticatedTenantContext foreign = new AuthenticatedTenantContext(bytes(32, 5), bytes(32, 6),
                bytes(32, 7));
        assertEquals(io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.issueUploadHandle(foreign, receipt, UploadHandleKindV1.OPAQUE_SINGLE_PUT, 100)
                        .toCompletableFuture().join().outcome());
        assertEquals(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority.attestUpload(foreign, receipt, handle, 100).toCompletableFuture().join().outcome());
    }

    private static PayloadReservationReceiptV1 receipt() {
        final io.nereusstream.delay.protocol.RouteIncarnation route =
                io.nereusstream.delay.protocol.RouteIncarnation.random();
        final io.nereusstream.delay.protocol.ShardId shard =
                new io.nereusstream.delay.protocol.ShardId(route, 0);
        final io.nereusstream.delay.protocol.DelayMessageId message =
                io.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("payload"), 1, bytes(32, 2),
                ProfileKindV1.OBJECT_STORE);
        final io.nereusstream.delay.protocol.KafkaSourcePosition source =
                new io.nereusstream.delay.protocol.KafkaSourcePosition(shard, "cluster", UUID.randomUUID(),
                        1, null, 100);
        return PayloadReservationReceiptV1.create(bytes(32, 3), message, shard, source, 1, profile,
                Bytes.utf8("container"), Bytes.utf8("object"), 1, bytes(32, 8), 9_000,
                new io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1(1, bytes(32, 9)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
