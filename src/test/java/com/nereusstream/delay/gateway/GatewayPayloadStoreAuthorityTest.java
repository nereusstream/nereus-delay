package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class GatewayPayloadStoreAuthorityTest {
    @Test
    void tenantRoutingScopeIsCheckedBeforeStoreInvocation() {
        final byte[] expectedScope = bytes(32, 1);
        final PayloadReservationReceipt receipt = receipt();
        final OpaquePayloadUploadHandle handle = OpaquePayloadUploadHandle.create(
                receipt.reservationId(),
                receipt.objectStoreProfile(),
                UploadHandleKind.OPAQUE_SINGLE_PUT,
                10_000,
                bytes(32, 4));
        final GatewayPayloadStoreAuthority authority = new GatewayPayloadStoreAuthority(
                expectedScope,
                (ignoredReceipt, ignoredKind, ignoredNow) -> PayloadUploadHandleResponse.issued(handle),
                (ignoredReceipt, ignoredHandle, ignoredNow) ->
                        PayloadAttestationResponse.error(PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE, null));

        final AuthenticatedTenantContext foreign =
                new AuthenticatedTenantContext(bytes(32, 5), bytes(32, 6), bytes(32, 7));
        assertEquals(
                com.nereusstream.delay.protocol.PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority
                        .issueUploadHandle(foreign, receipt, UploadHandleKind.OPAQUE_SINGLE_PUT, 100)
                        .toCompletableFuture()
                        .join()
                        .outcome());
        assertEquals(
                PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED,
                authority
                        .attestUpload(foreign, receipt, handle, 100)
                        .toCompletableFuture()
                        .join()
                        .outcome());
    }

    private static PayloadReservationReceipt receipt() {
        final com.nereusstream.delay.protocol.RouteIncarnation route =
                com.nereusstream.delay.protocol.RouteIncarnation.random();
        final com.nereusstream.delay.protocol.ShardId shard = new com.nereusstream.delay.protocol.ShardId(route, 0);
        final com.nereusstream.delay.protocol.DelayMessageId message =
                com.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final ProfileRef profile = new ProfileRef(Bytes.utf8("payload"), 1, bytes(32, 2), ProfileKind.OBJECT_STORE);
        final com.nereusstream.delay.protocol.KafkaSourcePosition source =
                new com.nereusstream.delay.protocol.KafkaSourcePosition(
                        shard, "cluster", UUID.randomUUID(), 1, null, 100);
        return PayloadReservationReceipt.create(
                bytes(32, 3),
                message,
                shard,
                source,
                1,
                profile,
                Bytes.utf8("container"),
                Bytes.utf8("object"),
                1,
                bytes(32, 8),
                9_000,
                new com.nereusstream.delay.protocol.PayloadProofTrustSetRef(1, bytes(32, 9)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
