package io.nereusstream.delay.gateway;

import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.DelaySemanticCore;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.submission.SubmissionCoordinator;
import io.nereusstream.delay.transport.TransportOwnershipPermit;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GatewayIngressServiceTest {
    @Test
    void controlReserveIsIndependentFromSchedulePool() {
        final InMemoryGatewayAdmissionController admission = new InMemoryGatewayAdmissionController(1, 4096, 1, 1);
        final AuthenticatedTenantContext tenant = tenant(1);
        final GatewayAdmissionLease schedule = admission.reserve(new GatewayAdmissionRequestV1(tenant,
                GatewayIngressOperationV1.SCHEDULE, 90)).lease();

        assertEquals(GatewayAdmissionController.State.REJECTED,
                admission.reserve(new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 1))
                        .state());
        final GatewayAdmissionController.Decision control = admission.reserve(new GatewayAdmissionRequestV1(tenant,
                GatewayIngressOperationV1.CONTROL, 1));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, control.state());

        control.lease().close();
        schedule.close();
        schedule.close();
        assertEquals(GatewayAdmissionController.State.ACCEPTED,
                admission.reserve(new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 1))
                        .state());
    }

    @Test
    void authenticationFailureStopsBeforePreparation() {
        final Fixture fixture = fixture((peer) -> {
            throw new IllegalArgumentException("invalid peer");
        }, new InMemoryGatewayAuditSink(4));

        final GatewayIngressException failure = assertThrows(GatewayIngressException.class,
                () -> fixture.ingress.schedule(peer(), request()).toCompletableFuture().join());

        assertEquals(GatewayIngressException.Kind.AUTHENTICATION, failure.kind());
        assertEquals(0, fixture.core.prepareCalls);
    }

    @Test
    void successfulIngressAuditsDigestsAndReleasesAdmission() {
        final InMemoryGatewayAuditSink audit = new InMemoryGatewayAuditSink(4);
        final Fixture fixture = fixture((peer) -> tenant(1), audit);

        final GatewaySubmissionOutcomeV1 outcome = fixture.ingress.schedule(peer(), request())
                .toCompletableFuture().join();

        assertTrue(outcome.hasSubmissionOutcome());
        assertEquals(1, fixture.core.prepareCalls);
        assertEquals(2, audit.canonicalEvents().size());
        final GatewayAdmissionController.Decision after = fixture.admission.reserve(
                new GatewayAdmissionRequestV1(tenant(1), GatewayIngressOperationV1.SCHEDULE, 90));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, after.state());
        after.lease().close();
    }

    @Test
    void auditFailureReleasesAdmissionBeforeReturningUnavailable() {
        final InMemoryGatewayAdmissionController admission = new InMemoryGatewayAdmissionController(1, 4096, 1, 1);
        final Fixture fixture = fixture((peer) -> tenant(1), event -> {
            throw new IllegalStateException("audit unavailable");
        }, admission);

        final GatewayIngressException failure = assertThrows(GatewayIngressException.class,
                () -> fixture.ingress.schedule(peer(), request()).toCompletableFuture().join());

        assertEquals(GatewayIngressException.Kind.UNAVAILABLE, failure.kind());
        final GatewayAdmissionController.Decision after = admission.reserve(
                new GatewayAdmissionRequestV1(tenant(1), GatewayIngressOperationV1.SCHEDULE, 90));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, after.state());
        after.lease().close();
        assertEquals(0, fixture.core.prepareCalls);
    }

    private static Fixture fixture(final GatewayTenantAuthority authority, final GatewayAuditSink audit) {
        return fixture(authority, audit,
                new InMemoryGatewayAdmissionController(1, 4096, 1, 1));
    }

    private static Fixture fixture(final GatewayTenantAuthority authority, final GatewayAuditSink audit,
                                   final InMemoryGatewayAdmissionController admission) {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, scheduleIntent(), 600);
        final Fixture fixture = new Fixture();
        fixture.core = new FakeCore(PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command)));
        final TrustedClock clock = () -> 100;
        final GatewayScheduleService service = new GatewayScheduleService(fixture.core,
                new InMemoryGatewayIdempotencyStore(clock, 10, 20), new CountingCoordinator(command), clock);
        fixture.admission = admission;
        fixture.ingress = new GatewayIngressService(service, authority, admission, audit, clock);
        return fixture;
    }

    private static GatewayPeerContext peer() {
        return new GatewayPeerContext(new io.grpc.Metadata(), io.grpc.Attributes.EMPTY);
    }

    private static GatewayScheduleRequestV1 request() {
        return new GatewayScheduleRequestV1(bytes(16, 40),
                new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary")), scheduleIntent(), 600,
                SubmissionModeV1.MANAGED);
    }

    private static ScheduleIntentV1 scheduleIntent() {
        return ScheduleIntentV1.create(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300, 800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                Bytes.utf8("payload"), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null, null);
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
        private final PreparedSubmissionV1 prepared;
        private int prepareCalls;

        private FakeCore(final PreparedSubmissionV1 prepared) {
            this.prepared = prepared;
        }

        @Override
        public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                     final long retryUntilEpochMs,
                                                     final SubmissionModeV1 submissionMode) {
            prepareCalls++;
            return prepared;
        }

        @Override
        public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                     final RouteSelectionHint route,
                                                     final LargeSchedulePreparationV1 request,
                                                     final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                    final PayloadReservationReceiptV1 reservation,
                                                    final PayloadCommitProofV1 proof,
                                                    final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition,
                                             final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                                 final DelayMessageId messageId,
                                                 final MessagePreconditionV1 precondition,
                                                 final long deliverAtEpochMs, final long expireAtEpochMs,
                                                 final long retryUntilEpochMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand command) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CountingCoordinator implements SubmissionCoordinator {
        private final PreparedCommand command;

        private CountingCoordinator(final PreparedCommand command) {
            this.command = command;
        }

        @Override
        public CompletionStage<SubmissionOutcomeMessageV1> submit(final AuthenticatedTenantContext tenant,
                                                                    final PreparedSubmissionV1 submission,
                                                                    final TransportOwnershipPermit permit) {
            return CompletableFuture.completedFuture(SubmissionOutcomeMessageV1.managed(
                    WireIngressOutcomeSupport.localDefinite(command, StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED)));
        }
    }
}
