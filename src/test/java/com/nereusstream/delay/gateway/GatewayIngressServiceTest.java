package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.DelaySemanticCore;
import com.nereusstream.delay.semantic.LargeSchedulePreparation;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.submission.SubmissionCoordinator;
import com.nereusstream.delay.transport.TransportOwnershipPermit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

class GatewayIngressServiceTest {
    @Test
    void controlReserveIsIndependentFromSchedulePool() {
        final InMemoryGatewayAdmissionController admission = new InMemoryGatewayAdmissionController(1, 4096, 1, 1);
        final AuthenticatedTenantContext tenant = tenant(1);
        final GatewayAdmissionLease schedule = admission
                .reserve(new GatewayAdmissionRequest(tenant, GatewayIngressOperation.SCHEDULE, 90))
                .lease();

        assertEquals(
                GatewayAdmissionController.State.REJECTED,
                admission
                        .reserve(new GatewayAdmissionRequest(tenant, GatewayIngressOperation.SCHEDULE, 1))
                        .state());
        final GatewayAdmissionController.Decision control =
                admission.reserve(new GatewayAdmissionRequest(tenant, GatewayIngressOperation.CONTROL, 1));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, control.state());

        control.lease().close();
        schedule.close();
        schedule.close();
        assertEquals(
                GatewayAdmissionController.State.ACCEPTED,
                admission
                        .reserve(new GatewayAdmissionRequest(tenant, GatewayIngressOperation.SCHEDULE, 1))
                        .state());
    }

    @Test
    void authenticationFailureStopsBeforePreparation() {
        final Fixture fixture = fixture(
                (peer) -> {
                    throw new IllegalArgumentException("invalid peer");
                },
                new InMemoryGatewayAuditSink(4));

        final GatewayIngressException failure = assertThrows(GatewayIngressException.class, () -> fixture.ingress
                .schedule(peer(), request())
                .toCompletableFuture()
                .join());

        assertEquals(GatewayIngressException.Kind.AUTHENTICATION, failure.kind());
        assertEquals(0, fixture.core.prepareCalls);
    }

    @Test
    void successfulIngressAuditsDigestsAndReleasesAdmission() {
        final InMemoryGatewayAuditSink audit = new InMemoryGatewayAuditSink(4);
        final Fixture fixture = fixture((peer) -> tenant(1), audit);

        final GatewaySubmissionOutcome outcome = fixture.ingress
                .schedule(peer(), request())
                .toCompletableFuture()
                .join();

        assertTrue(outcome.hasSubmissionOutcome());
        assertEquals(1, fixture.core.prepareCalls);
        assertEquals(2, audit.canonicalEvents().size());
        final GatewayAdmissionController.Decision after =
                fixture.admission.reserve(new GatewayAdmissionRequest(tenant(1), GatewayIngressOperation.SCHEDULE, 90));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, after.state());
        after.lease().close();
    }

    @Test
    void auditFailureReleasesAdmissionBeforeReturningUnavailable() {
        final InMemoryGatewayAdmissionController admission = new InMemoryGatewayAdmissionController(1, 4096, 1, 1);
        final Fixture fixture = fixture(
                (peer) -> tenant(1),
                event -> {
                    throw new IllegalStateException("audit unavailable");
                },
                admission);

        final GatewayIngressException failure = assertThrows(GatewayIngressException.class, () -> fixture.ingress
                .schedule(peer(), request())
                .toCompletableFuture()
                .join());

        assertEquals(GatewayIngressException.Kind.UNAVAILABLE, failure.kind());
        final GatewayAdmissionController.Decision after =
                admission.reserve(new GatewayAdmissionRequest(tenant(1), GatewayIngressOperation.SCHEDULE, 90));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, after.state());
        after.lease().close();
        assertEquals(0, fixture.core.prepareCalls);
    }

    private static Fixture fixture(final GatewayTenantAuthority authority, final GatewayAuditSink audit) {
        return fixture(authority, audit, new InMemoryGatewayAdmissionController(1, 4096, 1, 1));
    }

    private static Fixture fixture(
            final GatewayTenantAuthority authority,
            final GatewayAuditSink audit,
            final InMemoryGatewayAdmissionController admission) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, scheduleIntent(), 600);
        final Fixture fixture = new Fixture();
        fixture.core = new FakeCore(PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command)));
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(
                fixture.core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20),
                new CountingCoordinator(command),
                clock);
        fixture.admission = admission;
        fixture.ingress = new GatewayIngressService(service, authority, admission, audit, clock);
        return fixture;
    }

    private static GatewayPeerContext peer() {
        return new GatewayPeerContext(new io.grpc.Metadata(), io.grpc.Attributes.EMPTY);
    }

    private static GatewayScheduleRequest request() {
        return new GatewayScheduleRequest(
                bytes(16, 40),
                new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary")),
                scheduleIntent(),
                600,
                SubmissionMode.MANAGED);
    }

    private static CanonicalScheduleIntent scheduleIntent() {
        return CanonicalScheduleIntent.create(
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static AuthenticatedTenantContext tenant(final int seed) {
        return new AuthenticatedTenantContext(bytes(32, seed), bytes(32, seed + 1), bytes(32, seed + 2));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class Fixture {
        private FakeCore core;
        private InMemoryGatewayAdmissionController admission;
        private GatewayIngressService ingress;
    }

    private static final class FakeCore implements DelaySemanticCore {
        private final PreparedSubmission prepared;
        private int prepareCalls;

        private FakeCore(final PreparedSubmission prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmission prepareSchedule(
                final AuthenticatedTenantContext tenant,
                final RouteSelectionHint route,
                final CanonicalScheduleIntent intent,
                final long retryUntilEpochMs,
                final SubmissionMode submissionMode) {
            prepareCalls++;
            return prepared;
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
        public PreparedSubmission prepareManaged(
                final AuthenticatedTenantContext tenant, final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CountingCoordinator implements SubmissionCoordinator {
        private final PreparedCommand command;

        private CountingCoordinator(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessage> submit(
                final AuthenticatedTenantContext tenant,
                final PreparedSubmission submission,
                final TransportOwnershipPermit permit) {
            return CompletableFuture.completedFuture(SubmissionOutcomeMessage.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }
}
