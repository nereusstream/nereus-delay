package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.NativePreparedDeliveryV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SelfRoutingId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.route.RouteHashV1;
import io.nereusstream.delay.route.RouteSnapshotProvider;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Single zero-I/O preparation pipeline shared by Direct SDK and Gateway.
 * Route lookup is a local immutable-cache read; all network and transport
 * ownership starts after this class returns.
 */
public final class DefaultDelaySemanticCore implements DelaySemanticCore {
    private final RouteSnapshotProvider routes;
    private final LogicalUuidV7Generator uuidGenerator;
    private final TrustedClock trustedClock;
    private final NativePreparationSnapshotProvider nativeSnapshots;
    private final NativeDeliveryIdGenerator nativeDeliveryIds;

    public DefaultDelaySemanticCore(final RouteSnapshotProvider routes, final LogicalUuidV7Generator uuidGenerator,
                                    final TrustedClock trustedClock) {
        this(routes, uuidGenerator, trustedClock, null, NativeDeliveryIdGenerator.random());
    }

    public DefaultDelaySemanticCore(final RouteSnapshotProvider routes, final LogicalUuidV7Generator uuidGenerator,
                                    final TrustedClock trustedClock,
                                    final NativePreparationSnapshotProvider nativeSnapshots,
                                    final NativeDeliveryIdGenerator nativeDeliveryIds) {
        this.routes = Objects.requireNonNull(routes, "routes");
        this.uuidGenerator = Objects.requireNonNull(uuidGenerator, "uuidGenerator");
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
        this.nativeSnapshots = nativeSnapshots;
        this.nativeDeliveryIds = Objects.requireNonNull(nativeDeliveryIds, "nativeDeliveryIds");
    }

    @Override
    public PreparedSubmissionV1 prepareSchedule(final AuthenticatedTenantContext tenant,
                                                final RouteSelectionHint route, final ScheduleIntentV1 intent,
                                                final long retryUntilEpochMs,
                                                final SubmissionModeV1 submissionMode) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(submissionMode, "submissionMode");
        final TrustedTimeSnapshot trustedTime = time();
        final RouteSnapshotV1 snapshot = activeRoute(tenant, route, trustedTime);
        requireIngressRoute(snapshot, route.adapterKind());
        requireRetryWindow(snapshot, retryUntilEpochMs, trustedTime);
        if (intent.hasInlinePayload() && intent.inlinePayload().length > snapshot.maxInlinePayloadBytes()) {
            throw SemanticPreparationException.of(StableCode.PAYLOAD_TOO_LARGE, null);
        }

        final UUID logicalMessageUuid = nextUuid(trustedTime);
        final byte[] routingKey = intent.orderingKey().length == 0
                ? uuidBytes(logicalMessageUuid) : intent.orderingKey();
        final int partition = RouteHashV1.partition(snapshot, tenant.tenantRoutingScope(), routingKey);
        final io.nereusstream.delay.protocol.ShardId shard =
                new io.nereusstream.delay.protocol.ShardId(snapshot.routeIncarnation(), partition);
        final UUID logicalCommandUuid = nextIndependentUuid(trustedTime, logicalMessageUuid);
        final PreparedCommand managed = PreparedCommand.scheduleV1(shard, logicalMessageUuid, logicalCommandUuid,
                intent, retryUntilEpochMs);
        final byte[] managedFrame = strictFrameWithinRoute(managed, snapshot);
        if (submissionMode == SubmissionModeV1.MANAGED || nativeSnapshots == null || !intent.hasInlinePayload()) {
            return PreparedSubmissionV1.managed(managedFrame);
        }
        final Optional<NativePreparationSnapshotV1> nativeCandidate;
        try {
            nativeCandidate = Objects.requireNonNull(nativeSnapshots.eligibleFor(tenant, snapshot, intent, managed,
                            trustedTime), "native snapshot provider returned null");
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, failure);
        }
        if (nativeCandidate.isEmpty()) {
            return PreparedSubmissionV1.managed(managedFrame);
        }
        try {
            return PreparedSubmissionV1.nativePrepared(prepareNative(tenant, managed, intent, nativeCandidate.get(),
                    snapshot, trustedTime));
        } catch (SemanticPreparationException failure) {
            if (failure.error().code() == StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE) {
                return PreparedSubmissionV1.managed(managedFrame);
            }
            throw failure;
        }
    }

    @Override
    public PreparedCommand prepareLargeSchedule(final AuthenticatedTenantContext tenant,
                                                final RouteSelectionHint route,
                                                final LargeSchedulePreparationV1 request,
                                                final long retryUntilEpochMs) {
        Objects.requireNonNull(request, "request");
        final TrustedTimeSnapshot trustedTime = time();
        final RouteSnapshotV1 snapshot = activeRoute(tenant, route, trustedTime);
        requireIngressRoute(snapshot, route.adapterKind());
        requireRetryWindow(snapshot, retryUntilEpochMs, trustedTime);
        final UUID logicalMessageUuid = nextUuid(trustedTime);
        final byte[] routingKey = request.intentWithoutPayload().orderingKey().length == 0
                ? uuidBytes(logicalMessageUuid) : request.intentWithoutPayload().orderingKey();
        final int partition = RouteHashV1.partition(snapshot, tenant.tenantRoutingScope(),
                routingKey);
        final io.nereusstream.delay.protocol.ShardId shard =
                new io.nereusstream.delay.protocol.ShardId(snapshot.routeIncarnation(), partition);
        final PreparedCommand command = PreparedCommand.prepareLargeV1(shard, logicalMessageUuid,
                nextIndependentUuid(trustedTime, logicalMessageUuid), request.intentWithoutPayload(),
                request.expectedPayloadLength(), request.payloadSha256(), request.reservationTtlMs(),
                request.trustSet(), request.objectStoreProfile(), retryUntilEpochMs);
        strictFrameWithinRoute(command, snapshot);
        return command;
    }

    @Override
    public PreparedCommand preparePayloadCommit(final AuthenticatedTenantContext tenant,
                                                final PayloadReservationReceiptV1 reservation,
                                                final PayloadCommitProofV1 proof, final long retryUntilEpochMs) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(reservation, "reservation");
        Objects.requireNonNull(proof, "proof");
        if (!reservation.delayMessageId().equals(proof.delayMessageId())
                || !java.util.Arrays.equals(reservation.reservationId(), proof.reservationId())
                || !reservation.objectStoreProfile().equals(proof.objectStoreProfile())) {
            throw SemanticPreparationException.of(StableCode.PAYLOAD_PROOF_INVALID, null);
        }
        final TrustedTimeSnapshot trustedTime = time();
        final RouteSnapshotV1 snapshot = exactRoute(tenant, reservation.shardId());
        requireRetryWindow(snapshot, retryUntilEpochMs, trustedTime);
        if (!java.util.Arrays.equals(tenant.tenantRoutingScope(), proof.tenantRoutingScope())) {
            throw SemanticPreparationException.of(StableCode.UNAUTHORIZED, null);
        }
        final CommandId commandId = commandId(reservation.shardId(), nextUuid(trustedTime));
        final PreparedCommand command = PreparedCommand.commitLargeV1(reservation.shardId(), commandId,
                reservation.delayMessageId(), reservation.reservationId(), proof, retryUntilEpochMs);
        strictFrameWithinRoute(command, snapshot);
        return command;
    }

    @Override
    public PreparedCommand prepareCancel(final AuthenticatedTenantContext tenant, final DelayMessageId messageId,
                                         final MessagePreconditionV1 precondition, final long retryUntilEpochMs) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(precondition, "precondition");
        final TrustedTimeSnapshot trustedTime = time();
        final io.nereusstream.delay.protocol.ShardId shard = messageId.routingId().shardId();
        final RouteSnapshotV1 snapshot = exactRoute(tenant, shard);
        requireRetryWindow(snapshot, retryUntilEpochMs, trustedTime);
        final PreparedCommand command = PreparedCommand.cancelV1(shard, commandId(shard, nextUuid(trustedTime)),
                messageId, precondition, retryUntilEpochMs);
        strictFrameWithinRoute(command, snapshot);
        return command;
    }

    @Override
    public PreparedCommand prepareReschedule(final AuthenticatedTenantContext tenant,
                                             final DelayMessageId messageId,
                                             final MessagePreconditionV1 precondition, final long deliverAtEpochMs,
                                             final long expireAtEpochMs, final long retryUntilEpochMs) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(precondition, "precondition");
        final TrustedTimeSnapshot trustedTime = time();
        final io.nereusstream.delay.protocol.ShardId shard = messageId.routingId().shardId();
        final RouteSnapshotV1 snapshot = exactRoute(tenant, shard);
        requireRetryWindow(snapshot, retryUntilEpochMs, trustedTime);
        final PreparedCommand command = PreparedCommand.rescheduleV1(shard,
                commandId(shard, nextUuid(trustedTime)), messageId, precondition, deliverAtEpochMs,
                expireAtEpochMs, retryUntilEpochMs);
        strictFrameWithinRoute(command, snapshot);
        return command;
    }

    @Override
    public PreparedSubmissionV1 prepareManaged(final AuthenticatedTenantContext tenant,
                                               final PreparedCommand command) {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(command, "command");
        final RouteSnapshotV1 snapshot = exactRoute(tenant, command.shardId());
        return PreparedSubmissionV1.managed(strictFrameWithinRoute(command, snapshot));
    }

    private NativePreparedDeliveryV1 prepareNative(final AuthenticatedTenantContext tenant,
                                                   final PreparedCommand managed, final ScheduleIntentV1 intent,
                                                   final NativePreparationSnapshotV1 candidate,
                                                   final RouteSnapshotV1 snapshot,
                                                   final TrustedTimeSnapshot trustedTime) {
        try {
            NativePreparationEligibilityV1.require(tenant, snapshot, intent, managed, candidate, trustedTime);
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, failure);
        }
        try {
            return NativePreparedDeliveryV1.create(
                    NativeDeliveryIdGenerator.require(nativeDeliveryIds.next(managed, intent)),
                    candidate.destination().ref(), candidate.capability().ref(), candidate.target(),
                    candidate.physicalPartition(), intent.inlinePayload(), intent.adapterMetadata().pulsar(),
                    intent.eventTimeEpochMs(), intent.deliverAtEpochMs(), candidate.brokerDeliverAtEpochMs(),
                    candidate.capabilitySnapshot());
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE, failure);
        }
    }

    private RouteSnapshotV1 activeRoute(final AuthenticatedTenantContext tenant, final RouteSelectionHint route,
                                        final TrustedTimeSnapshot trustedTime) {
        final RouteSnapshotV1 snapshot;
        try {
            snapshot = routes.activeForNewSchedule(tenant, route);
            if (snapshot == null) {
                throw new IllegalArgumentException("Route cache returned no active snapshot");
            }
            snapshot.requireUsableForNewSchedule(tenant.authenticatedTenantScopeHash(),
                    tenant.tenantRoutingScope(), trustedTime.epochMs());
        } catch (SemanticPreparationException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure);
        }
        return snapshot;
    }

    private RouteSnapshotV1 exactRoute(final AuthenticatedTenantContext tenant,
                                       final io.nereusstream.delay.protocol.ShardId shard) {
        try {
            final RouteSnapshotV1 snapshot = routes.exact(shard.routeIncarnation(), tenant);
            if (snapshot == null) {
                throw new IllegalArgumentException("Route cache returned no historical snapshot");
            }
            snapshot.requireTenantScope(tenant.authenticatedTenantScopeHash(), tenant.tenantRoutingScope());
            if (!snapshot.routeIncarnation().equals(shard.routeIncarnation())
                    || shard.partition() < 0 || shard.partition() >= snapshot.ingress().partitionCount()) {
                throw new IllegalArgumentException("command identity is outside exact Route");
            }
            return snapshot;
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure);
        }
    }

    private byte[] strictFrameWithinRoute(final PreparedCommand command, final RouteSnapshotV1 snapshot) {
        try {
            final byte[] frame = CommandCodec.encodeFrameV1(command);
            if (frame.length > snapshot.maxCommandBytes()) {
                throw SemanticPreparationException.of(StableCode.PAYLOAD_TOO_LARGE, null);
            }
            return frame;
        } catch (SemanticPreparationException failure) {
            throw failure;
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.INVALID_PREPARED_COMMAND, failure);
        }
    }

    /**
     * The Gateway route selects the command ingress adapter.  The intent
     * metadata selects the destination adapter and is checked later by the
     * immutable Destination Profile/Claim materialization boundary.
     */
    private static void requireIngressRoute(final RouteSnapshotV1 snapshot, final AdapterKindV1 selected) {
        if (snapshot.ingress().adapterKind() != selected) {
            throw SemanticPreparationException.of(StableCode.INGRESS_ROUTE_MISMATCH, null);
        }
    }

    private static void requireRetryWindow(final RouteSnapshotV1 snapshot, final long retryUntilEpochMs,
                                           final TrustedTimeSnapshot trustedTime) {
        if (retryUntilEpochMs < trustedTime.epochMs()) {
            throw SemanticPreparationException.of(StableCode.PREPARED_COMMAND_EXPIRED, null);
        }
        final long latest;
        try {
            latest = Math.addExact(trustedTime.epochMs(), snapshot.maximumPreparationAgeMs());
        } catch (ArithmeticException overflow) {
            throw SemanticPreparationException.of(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, overflow);
        }
        if (retryUntilEpochMs > Math.min(latest, snapshot.validUntilEpochMs())) {
            throw SemanticPreparationException.of(StableCode.PREPARED_COMMAND_EXPIRED, null);
        }
    }

    private TrustedTimeSnapshot time() {
        try {
            return new TrustedTimeSnapshot(trustedClock.nowEpochMs());
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure);
        }
    }

    private UUID nextUuid(final TrustedTimeSnapshot trustedTime) {
        try {
            final UUID value = Objects.requireNonNull(uuidGenerator.next(trustedTime), "uuidGenerator returned null");
            SelfRoutingId.requireLogicalUuidV7(value);
            return value;
        } catch (RuntimeException failure) {
            throw SemanticPreparationException.of(StableCode.INVALID_COMMAND, failure);
        }
    }

    private UUID nextIndependentUuid(final TrustedTimeSnapshot trustedTime, final UUID first) {
        final UUID second = nextUuid(trustedTime);
        if (first.equals(second)) {
            throw SemanticPreparationException.of(StableCode.INVALID_COMMAND, null);
        }
        return second;
    }

    private static CommandId commandId(final io.nereusstream.delay.protocol.ShardId shard, final UUID uuid) {
        return new CommandId(SelfRoutingId.fromLogicalUuid(shard, uuid).bytes());
    }

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

}
