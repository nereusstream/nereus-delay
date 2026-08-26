package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.protobuf.ByteString;
import com.nereusstream.delay.gateway.wire.GatewayAttestPayloadUploadRequest;
import com.nereusstream.delay.gateway.wire.GatewayIssuePayloadUploadHandleRequest;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
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
            public CompletionStage<PayloadUploadHandleResponse> issueUploadHandle(
                    final AuthenticatedTenantContext context,
                    final PayloadReservationReceipt receipt,
                    final UploadHandleKind kind,
                    final long nowEpochMs) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, receipt);
                assertEquals(UploadHandleKind.OPAQUE_SINGLE_PUT, kind);
                assertEquals(100, nowEpochMs);
                return CompletableFuture.completedFuture(PayloadUploadHandleResponse.issued(fixture.handle));
            }

            @Override
            public CompletionStage<PayloadAttestationResponse> attestUpload(
                    final AuthenticatedTenantContext context,
                    final PayloadReservationReceipt receipt,
                    final OpaquePayloadUploadHandle handle,
                    final long nowEpochMs) {
                assertEquals(tenant, context);
                assertEquals(fixture.receipt, receipt);
                assertEquals(fixture.handle, handle);
                return CompletableFuture.completedFuture(PayloadAttestationResponse.error(
                        PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE,
                        StableError.of(
                                FailureStage.PAYLOAD,
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
                GatewayIssuePayloadUploadHandleRequest.newBuilder()
                        .setPayloadReservationReceipt(ByteString.copyFrom(fixture.receipt.payload()))
                        .setUploadHandleKind(UploadHandleKind.OPAQUE_SINGLE_PUT.wireValue())
                        .build(),
                upload);
        assertNull(upload.failure);
        assertTrue(upload.completed);
        assertEquals(
                fixture.handle,
                PayloadUploadHandleResponse.decode(
                                upload.response.getPayloadUploadHandleResponse().toByteArray())
                        .issued());

        final AttestationObserver attestation = new AttestationObserver();
        service.attestPayloadUpload(
                GatewayAttestPayloadUploadRequest.newBuilder()
                        .setPayloadReservationReceipt(ByteString.copyFrom(fixture.receipt.payload()))
                        .setOpaquePayloadUploadHandle(ByteString.copyFrom(fixture.handle.canonicalBytes()))
                        .build(),
                attestation);
        assertNull(attestation.failure);
        assertTrue(attestation.completed);
        assertEquals(
                PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE,
                PayloadAttestationResponse.decode(attestation
                                .response
                                .getPayloadAttestationResponse()
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
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 30), ProfileKind.OBJECT_STORE);
        final PayloadProofTrustSetRef trustSet = new PayloadProofTrustSetRef(1, bytes(32, 31));
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "gateway", UUID.randomUUID(), 3, null, 100);
        final PayloadReservationReceipt receipt = PayloadReservationReceipt.create(
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
        final OpaquePayloadUploadHandle handle = OpaquePayloadUploadHandle.create(
                receipt.reservationId(), profile, UploadHandleKind.OPAQUE_SINGLE_PUT, 4_000, Bytes.utf8("capability"));
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

    private record PayloadFixture(PayloadReservationReceipt receipt, OpaquePayloadUploadHandle handle) {}

    private static final class UploadObserver
            implements StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse> {
        private com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.wire.GatewayPayloadUploadHandleResponse value) {
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
            implements StreamObserver<com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse> {
        private com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse response;
        private Throwable failure;
        private boolean completed;

        @Override
        public void onNext(final com.nereusstream.delay.gateway.wire.GatewayPayloadAttestationResponse value) {
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
        public com.nereusstream.delay.protocol.PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareLargeSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final LargeSchedulePreparation request,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(
                final AuthenticatedTenantContext tenant,
                final PayloadReservationReceipt reservation,
                final CanonicalPayloadCommitProof proof,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(
                final AuthenticatedTenantContext tenant,
                final DelayMessageId messageId,
                final MessagePrecondition precondition,
                final long deliverAtEpochMs,
                final long expireAtEpochMs,
                final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.nereusstream.delay.protocol.PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoopCoordinator implements SubmissionCoordinator {
        @Override
        public CompletionStage<SubmissionOutcomeMessage> submit(
                final AuthenticatedTenantContext tenant,
                final com.nereusstream.delay.protocol.PreparedSubmission submission,
                final TransportOwnershipPermit permit) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }
    }
}
