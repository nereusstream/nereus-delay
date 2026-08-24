package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.protobuf.ByteString;
import com.nereusstream.delay.gateway.v1.GatewayAttestPayloadUploadRequestV1;
import com.nereusstream.delay.gateway.v1.GatewayIssuePayloadUploadHandleRequestV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.FailureStageV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePreconditionV1;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import com.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import com.nereusstream.delay.protocol.PayloadCommitProofV1;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableErrorV1;
import com.nereusstream.delay.protocol.SubmissionModeV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.protocol.UploadHandleKindV1;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GatewayPayloadGrpcServiceTest {
    @Test
    void receiptBoundPayloadRpcDecodesThroughAuthenticatedIngress() {
        final TrustedClock clock = () -> 100;
        final AuthenticatedTenantContext tenant = tenant();
        final PayloadFixture fixture = fixture();
        final GatewayPayloadAuthority authority = new GatewayPayloadAuthority() {
            @Override
            public CompletionStage<PayloadUploadHandleResponseV1> issueUploadHandle(
                    final AuthenticatedTenantContext context,
                    final PayloadReservationReceiptV1 receipt,
                    final UploadHandleKindV1 kind,
                    final long nowEpochMs) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, receipt);
                assertEquals(UploadHandleKindV1.OPAQUE_SINGLE_PUT, kind);
                assertEquals(100, nowEpochMs);
                return CompletableFuture.completedFuture(PayloadUploadHandleResponseV1.issued(fixture.handle));
            }

            @Override
            public CompletionStage<PayloadAttestationResponseV1> attestUpload(
                    final AuthenticatedTenantContext context,
                    final PayloadReservationReceiptV1 receipt,
                    final OpaquePayloadUploadHandleV1 handle,
                    final long nowEpochMs) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, receipt);
                assertEquals(fixture.handle, handle);
                return CompletableFuture.completedFuture(PayloadAttestationResponseV1.error(
                        PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE,
                        StableErrorV1.of(
                                FailureStageV1.PAYLOAD,
                                StableCode.OBJECT_NOT_READY_RETRYABLE,
                                1_100L,
                                null,
                                null,
                                null)));
            }
        };
        final GatewayAuditSink audit = new InMemoryGatewayAuditSink(8);
        final GatewayPayloadIngressService payloadIngress = new GatewayPayloadIngressService(
                authority, peer -> tenant, new InMemoryGatewayAdmissionController(1, 4096, 1, 1), audit, clock);
        final GatewayGrpcService service = new GatewayGrpcService(
                noopIngress(clock, tenant),
                () -> new GatewayPeerContext(new Metadata(), Attributes.EMPTY),
                payloadIngress);

        final UploadObserver upload = new UploadObserver();
        service.issuePayloadUploadHandle(
                GatewayIssuePayloadUploadHandleRequestV1.newBuilder()
                        .setPayloadReservationReceiptV1(ByteString.copyFrom(fixture.receipt.payload()))
                        .setUploadHandleKind(UploadHandleKindV1.OPAQUE_SINGLE_PUT.wireValue())
                        .build(),
                upload);
        assertNull(upload.failure);
        assertTrue(upload.completed);
        assertEquals(
                fixture.handle,
                PayloadUploadHandleResponseV1.decode(upload.response
                                .getPayloadUploadHandleResponseV1()
                                .toByteArray())
                        .issued());

        final AttestationObserver attestation = new AttestationObserver();
        service.attestPayloadUpload(
                GatewayAttestPayloadUploadRequestV1.newBuilder()
                        .setPayloadReservationReceiptV1(ByteString.copyFrom(fixture.receipt.payload()))
                        .setOpaquePayloadUploadHandleV1(ByteString.copyFrom(fixture.handle.canonicalBytes()))
                        .build(),
                attestation);
        assertNull(attestation.failure);
        assertTrue(attestation.completed);
        assertEquals(
                PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE,
                PayloadAttestationResponseV1.decode(attestation
                                .response
                                .getPayloadAttestationResponseV1()
                                .toByteArray())
                        .outcome());
    }

    private static GatewayIngressService noopIngress(
            final TrustedClock clock, final AuthenticatedTenantContext tenant) {
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 0);
        final GatewayScheduleService domain = new GatewayScheduleService(
                new NoopCore(), new InMemoryGatewayIdempotencyStore(clock, 10, 20), new NoopCoordinator(), clock);
        return new GatewayIngressService(
                domain,
                peer -> tenant,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1),
                new InMemoryGatewayAuditSink(8),
                clock);
    }

    private static PayloadFixture fixture() {
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 0);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final ProfileRefV1 profile =
                new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 30), ProfileKindV1.OBJECT_STORE);
        final PayloadProofTrustSetRefV1 trustSet = new PayloadProofTrustSetRefV1(1, bytes(32, 31));
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3, null, 100);
        final PayloadReservationReceiptV1 receipt = PayloadReservationReceiptV1.create(
                bytes(32, 32),
                messageId,
                shard,
                source,
                1,
                profile,
                Bytes.utf8("container"),
                Bytes.utf8("object"),
                7,
                bytes(32, 33),
                5_000,
                trustSet);
        final OpaquePayloadUploadHandleV1 handle = OpaquePayloadUploadHandleV1.create(
                receipt.reservationId(),
                profile,
                UploadHandleKindV1.OPAQUE_SINGLE_PUT,
                4_000,
                Bytes.utf8("capability"));
        return new PayloadFixture(receipt, handle);
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record PayloadFixture(PayloadReservationReceiptV1 receipt, OpaquePayloadUploadHandleV1 handle) {}

    private static final class UploadObserver
            implements StreamObserver<com.nereusstream.delay.gateway.v1.GatewayPayloadUploadHandleResponseV1> {
        private com.nereusstream.delay.gateway.v1.GatewayPayloadUploadHandleResponseV1 response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.v1.GatewayPayloadUploadHandleResponseV1 value) {
            response = value;
        }

        @Override
        public void onError(final Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static final class AttestationObserver
            implements StreamObserver<com.nereusstream.delay.gateway.v1.GatewayPayloadAttestationResponseV1> {
        private com.nereusstream.delay.gateway.v1.GatewayPayloadAttestationResponseV1 response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.v1.GatewayPayloadAttestationResponseV1 value) {
            response = value;
        }

        @Override
        public void onError(final Throwable throwable) {
            failure = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static final class NoopCore implements DelaySemanticCore {
        @Override
        public com.nereusstream.delay.protocol.PreparedSubmissionV1 prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final ScheduleIntentV1 intent,
                final long retryUntilEpochMs,
                final SubmissionModeV1 submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparationV1 request,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceiptV1 reservation,
                final PayloadCommitProofV1 proof,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePreconditionV1 precondition,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePreconditionV1 precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.nereusstream.delay.protocol.PreparedSubmissionV1 prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopCoordinator implements SubmissionCoordinator {
        @Override
        public CompletionStage<SubmissionOutcomeMessageV1> submit(
                final AuthenticatedTenantContext tenant,
                final com.nereusstream.delay.protocol.PreparedSubmissionV1 submission,
                final TransportOwnershipPermit permit) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
