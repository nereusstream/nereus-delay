package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CommitLargeScheduleBody;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadProofTrustSet;
import com.nereusstream.delay.protocol.PayloadProofTrustSetControlState;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RescheduleCommandBody;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPayloadReference;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.UnsignedInt32;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** First Command business with immutable results and atomic Message/Claim/terminal accounting. */
public final class TargetCommandStore {
    public record DeliveryWindow(long maximumDelayMs, long minimumWindowMs, long maximumLifetimeMs) {
        public DeliveryWindow {
            if (maximumDelayMs <= 0 || minimumWindowMs <= 0 || maximumLifetimeMs < minimumWindowMs) {
                throw new IllegalArgumentException("finite positive delivery window bounds required");
            }
        }

        boolean permits(long deliverAt, long expireAt, long brokerTime) {
            try {
                return deliverAt <= Math.addExact(brokerTime, maximumDelayMs)
                        && expireAt >= Math.addExact(Math.max(deliverAt, brokerTime), minimumWindowMs)
                        && expireAt <= Math.addExact(brokerTime, maximumLifetimeMs);
            } catch (ArithmeticException invalid) {
                return false;
            }
        }
    }

    /** Authenticated Route inputs. The first-command commit guard must retain their exact source activation. */
    public record Policy(
            TargetQuotaScope scope,
            long retryWindowMs,
            long maximumPreparationAgeMs,
            long maximumFutureSkewMs,
            Set<ProtocolTuple> activatedTuples,
            DeliveryWindow deliveryWindow) {
        public Policy {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(deliveryWindow, "deliveryWindow");
            activatedTuples = Set.copyOf(activatedTuples);
            if (scope.target() != null
                    || retryWindowMs <= 0
                    || maximumPreparationAgeMs <= 0
                    || maximumFutureSkewMs < 0
                    || activatedTuples.isEmpty()
                    || activatedTuples.size() > 32) {
                throw new IllegalArgumentException("first Command requires bounded authenticated Route policy");
            }
        }
    }

    /** Resolve source-ordered channel/legacy closure protection for this exact original binding; no default. */
    @FunctionalInterface
    public interface CancellationControls {
        boolean closed(TargetStoreBackend.Reader reader, TargetScheduleBinding binding, SourcePosition source);
    }

    public static final class Prepared {
        private final TargetCommandStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final CommandResult result;

        private Prepared(TargetCommandStore owner, TargetStoreBackend.Prepared batch, CommandResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    /**
     * Source-pinned Route/retry/payload authorization, including Prepare size/TTL/trust-set limits, object proof
     * and all destination control closures. Resolve only for a new identity. Exceptions are authority failures,
     * not business results.
     * The first-command commit guard must hold the exact accepted snapshot; there is no permissive default.
     */
    @FunctionalInterface
    public interface Schedules {
        ScheduleAdmission resolve(PreparedCommand command, SourcePosition source);
    }

    /** Source-pinned, bounded proof catalog/control snapshot, held by the first-command commit guard. */
    @FunctionalInterface
    public interface PayloadProofControls {
        PayloadProofAuthority resolve(TargetScheduleBinding binding, SourcePosition source);
    }

    public record PayloadProofAuthority(
            PayloadProofTrustSetSemantic semantic, PayloadProofTrustSetControlState controls) {
        public PayloadProofAuthority {
            Objects.requireNonNull(semantic, "semantic");
            Objects.requireNonNull(controls, "controls");
        }
    }

    public record ScheduleAdmission(
            StableCode code,
            TargetScheduleRegistration.Authority registration,
            TargetOrderState.OrderingContract orderingContract) {
        public ScheduleAdmission {
            Objects.requireNonNull(code, "code");
            if (code == StableCode.OK) {
                Objects.requireNonNull(registration, "registration");
                Objects.requireNonNull(orderingContract, "orderingContract");
            } else if (registration != null || orderingContract != null) {
                throw new IllegalArgumentException("rejected Schedule authorization has no accepted binding");
            }
        }
    }

    private record Decision(
            TargetMessageStore.Input input,
            CommandResult result,
            TargetQuotaIncarnation owner,
            TargetQuotaAccounting ingress) {
        private Decision(TargetMessageStore.Input input, CommandResult result, TargetQuotaIncarnation owner) {
            this(input, result, owner, null);
        }
    }

    private final TargetStoreBackend backend;
    private final TargetMessageStore messages;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetCommandStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > 64
                || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("first Command needs bounded Shard accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
        messages = new TargetMessageStore(backend, 1, 1, maximumDomains);
    }

    /** Must follow the replay probe; absence is independently rechecked in this actual write-plan view. */
    public Prepared prepareFirst(
            BoundedReadBudget budget,
            PreparedCommand command,
            SourcePosition source,
            Policy policy,
            CancellationControls controls,
            Schedules schedules,
            PayloadProofControls payloadProofs) {
        Objects.requireNonNull(payloadProofs, "payloadProofs");
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(schedules, "schedules");
        Objects.requireNonNull(controls, "controls");
        TargetSourcePosition.requireBounded(source);
        if (!scope.equals(policy.scope())
                || !scope.shard().equals(command.shardId())
                || !scope.shard().equals(source.shardId())) {
            throw new IllegalArgumentException("first Command source/Route policy scope differs");
        }
        final var result = new CommandResult[1];
        final var ingress = new TargetQuotaAccounting[1];
        final byte[] digest = Bytes.sha256(CommandCodec.encodeFrame(command));
        final var batch = messages.prepareAccountedWithQuotaRejection(
                budget,
                reader -> {
                    if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                        throw new IllegalStateException(
                                "first Command requires a strictly advancing established source");
                    }
                    final var stamp = new TargetQuotaMutation(
                            TargetQuotaMutation.increment(reader.sourceSequence()), source, digest);
                    for (var key : List.of(commandKey(command), queryKey(command), positionKey(source))) {
                        if (reader.get(ColumnFamily.DEDUPE, key) != null) {
                            throw new IllegalStateException(
                                    "first Command has existing logical/query/physical evidence");
                        }
                    }
                    // Expired first records must use a standalone physical audit, never resurrect a GC'd logical ID.
                    if (source.brokerPersistenceTimeEpochMs() > command.retryUntilEpochMs()
                            || reader.closedIngressDeadlineThrough() >= command.retryUntilEpochMs()) {
                        throw new IllegalStateException(
                                "expired Command must use the physical-only replay preparation");
                    }
                    final var root = descriptor(
                            reader,
                            new TargetQuotaIdentity(
                                    TargetQuotaIdentity.Kind.SHARD,
                                    scope.shard(),
                                    reader.aggregate().accountingIncarnation(),
                                    null,
                                    null));
                    final StableCode invalid = invalid(command, source, policy);
                    final Decision decision;
                    if (invalid != null) {
                        decision = unchanged(rejected(invalid, source), root);

                    } else if (command.type() == CommandType.SCHEDULE) {
                        final ScheduleCommandBody body;
                        try {
                            if (command.canonicalBody().length > TargetScheduleBinding.MAX_BODY_BYTES) {
                                throw new IllegalArgumentException("Schedule body exceeds Target bound");
                            }
                            body = ScheduleCommandBody.decodeForTarget(
                                    command.canonicalBody(), command.delayMessageId());
                            if (!command.delayMessageId().equals(body.delayMessageId())
                                    || command.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                                throw new IllegalArgumentException("Schedule body differs from envelope");
                            }
                        } catch (IllegalArgumentException malformed) {
                            return withResults(
                                    reader,
                                    command,
                                    stamp,
                                    root,
                                    unchanged(rejected(StableCode.INVALID_COMMAND, source), root),
                                    result);
                        }
                        decision = ingress(reader, command, body.intent(), null, stamp, root, policy, schedules);
                    } else if (command.type() == CommandType.PREPARE_LARGE_SCHEDULE) {
                        final PrepareLargeScheduleBody body;
                        try {
                            body = PrepareLargeScheduleBody.decodeForTarget(
                                    command.canonicalBody(), command.delayMessageId());
                            if (command.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                                throw new IllegalArgumentException("Prepare retry window differs from envelope");
                            }
                        } catch (IllegalArgumentException malformed) {
                            return withResults(
                                    reader,
                                    command,
                                    stamp,
                                    root,
                                    unchanged(rejected(StableCode.INVALID_COMMAND, source), root),
                                    result);
                        }
                        decision = ingress(
                                reader, command, body.intentWithoutPayload(), body, stamp, root, policy, schedules);
                    } else if (command.type() == CommandType.COMMIT_LARGE_SCHEDULE) {
                        final CommitLargeScheduleBody body;
                        try {
                            body = CommitLargeScheduleBody.decodeForTarget(command.canonicalBody());
                            if (!command.delayMessageId().equals(body.delayMessageId())
                                    || command.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                                throw new IllegalArgumentException("Commit body differs from envelope");
                            }
                        } catch (IllegalArgumentException malformed) {
                            return withResults(
                                    reader,
                                    command,
                                    stamp,
                                    root,
                                    unchanged(rejected(StableCode.INVALID_COMMAND, source), root),
                                    result);
                        }
                        decision = commitReservation(reader, command, body, stamp, root, controls, payloadProofs);
                    } else if (command.type() == CommandType.CANCEL) {
                        final CancelCommandBody body;
                        try {
                            body = CancelCommandBody.decode(command.canonicalBody());
                            if (!command.delayMessageId().equals(body.delayMessageId())
                                    || command.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                                throw new IllegalArgumentException("Cancel body differs from envelope");
                            }
                        } catch (IllegalArgumentException malformed) {
                            return withResults(
                                    reader,
                                    command,
                                    stamp,
                                    root,
                                    unchanged(rejected(StableCode.INVALID_COMMAND, source), root),
                                    result);
                        }
                        decision = modifyMessage(
                                reader, command, body.precondition(), null, stamp, root, controls, policy);
                    } else if (command.type() == CommandType.RESCHEDULE) {
                        final RescheduleCommandBody body;
                        try {
                            body = RescheduleCommandBody.decode(command.canonicalBody());
                            if (!command.delayMessageId().equals(body.delayMessageId())
                                    || command.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                                throw new IllegalArgumentException("Reschedule body differs from envelope");
                            }
                        } catch (IllegalArgumentException malformed) {
                            return withResults(
                                    reader,
                                    command,
                                    stamp,
                                    root,
                                    unchanged(rejected(StableCode.INVALID_COMMAND, source), root),
                                    result);
                        }
                        decision = modifyMessage(
                                reader, command, body.precondition(), body, stamp, root, controls, policy);
                    } else {
                        throw new IllegalStateException(
                                "first Target Command business branch is not wired yet; retain source");
                    }
                    ingress[0] = decision.ingress();
                    return withResults(reader, command, stamp, root, decision, result);
                },
                (reader, business) -> {
                    final var assembled = new TargetSourceAccounting(
                                    scope, lineage, source, digest, maximumCounters, 1, maximumDomains)
                            .assemble(reader, business);
                    new TargetQuotaStoreGate(scope, lineage, 1)
                            .check(
                                    reader,
                                    assembled,
                                    ingress[0] != null
                                            ? command.type() == CommandType.PREPARE_LARGE_SCHEDULE
                                                    ? TargetQuotaGrantGate.Operation.PREPARE
                                                    : TargetQuotaGrantGate.Operation.FIRST_SCHEDULE
                                            : command.type() == CommandType.COMMIT_LARGE_SCHEDULE
                                                            && result[0].stableCode() == StableCode.SCHEDULED
                                                    ? TargetQuotaGrantGate.Operation.RESERVATION_COMMIT
                                                    : command.type() == CommandType.RESCHEDULE
                                                            ? TargetQuotaGrantGate.Operation.RESCHEDULE
                                                            : TargetQuotaGrantGate.Operation.CANCEL,
                                    ingress[0]);
                    return assembled;
                },
                (reader, exceeded) -> {
                    if (ingress[0] == null
                            || (command.type() != CommandType.SCHEDULE
                                    && command.type() != CommandType.PREPARE_LARGE_SCHEDULE)) {
                        throw exceeded;
                    }
                    ingress[0] = null;
                    final var root = descriptor(
                            reader,
                            new TargetQuotaIdentity(
                                    TargetQuotaIdentity.Kind.SHARD,
                                    scope.shard(),
                                    reader.aggregate().accountingIncarnation(),
                                    null,
                                    null));
                    final var stamp = new TargetQuotaMutation(
                            TargetQuotaMutation.increment(reader.sourceSequence()), source, digest);
                    return withResults(
                            reader,
                            command,
                            stamp,
                            root,
                            unchanged(rejected(StableCode.HARD_QUOTA_EXCEEDED, source), root),
                            result);
                });
        return new Prepared(this, batch, result[0]);
    }

    public CommandResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        if (Objects.requireNonNull(prepared, "prepared").owner != this) {
            throw new IllegalArgumentException("foreign first Command plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
    }

    private Decision ingress(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            CanonicalScheduleIntent intent,
            PrepareLargeScheduleBody prepare,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation root,
            Policy policy,
            Schedules schedules) {
        final var source = stamp.source();
        if (!policy.deliveryWindow()
                .permits(intent.deliverAtEpochMs(), intent.expireAtEpochMs(), source.brokerPersistenceTimeEpochMs())) {
            return unchanged(rejected(StableCode.INVALID_DELIVERY_WINDOW, source), root);
        }
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                command.delayMessageId().bytes());
        final byte[] existing = reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId()));
        final byte[] retained = reader.get(ColumnFamily.META, payloadKey);
        if ((existing != null
                        || reader.get(
                                        ColumnFamily.ID,
                                        Bytes.concat(
                                                new byte[] {TargetKeyCodec.RESERVATION_TAG, 1},
                                                command.delayMessageId().bytes()))
                                != null)
                && retained == null) {
            throw new IllegalStateException("existing Schedule identity has no payload owner");
        }
        if (retained != null) {
            final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                    payloadKey,
                    TargetValueEnvelope.decode(retained, TargetQuotaPayloadOwner.VALUE_TYPE)
                            .payload(),
                    scope.shard(),
                    scope.tenantScope());
            final var owner = descriptor(reader, payloadOwner.primaryIdentity());
            owner.requirePayloadOwner(payloadOwner);
            payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
            if (existing != null) {
                final var message = TargetMessageRecord.decodeForStore(
                        TargetKeyCodec.message(command.delayMessageId()),
                        TargetValueEnvelope.decode(existing, TargetMessageRecord.VALUE_TYPE)
                                .payload(),
                        scope.shard());
                payloadOwner.requireMessagePayload(message);
                final var projection = applied(StableCode.DELAY_MESSAGE_ID_CONFLICT, source, message);
                return unchanged(
                        new CommandResult(
                                ApplyStatus.REJECTED,
                                projection.stableCode(),
                                projection.generation(),
                                projection.stateVersion(),
                                projection.messageStatus(),
                                projection.appliedSourcePosition()),
                        owner);
            }
            return unchanged(rejected(StableCode.DELAY_MESSAGE_ID_CONFLICT, source), owner);
        }
        try {
            final long identityTime = command.delayMessageId().routingId().logicalTimestampEpochMs();
            if (identityTime > Math.addExact(source.brokerPersistenceTimeEpochMs(), policy.maximumFutureSkewMs())) {
                return unchanged(rejected(StableCode.INVALID_COMMAND, source), root);
            }
            final long deadline = Math.addExact(identityTime, policy.maximumPreparationAgeMs());
            if (source.brokerPersistenceTimeEpochMs() > deadline || reader.closedIngressDeadlineThrough() >= deadline) {
                return unchanged(rejected(StableCode.DELAY_MESSAGE_ID_EXPIRED, source), root);
            }
        } catch (ArithmeticException overflow) {
            return unchanged(rejected(StableCode.INVALID_COMMAND, source), root);
        }
        final ScheduleAdmission admitted;
        try {
            admitted = Objects.requireNonNull(schedules.resolve(command, source), "Schedule admission");
        } catch (ReadIncompleteException external) {
            throw new IllegalStateException("external Schedule authorization did not complete", external);
        }
        if (admitted.code() != StableCode.OK) {
            return unchanged(rejected(admitted.code(), source), root);
        }
        final var registration = TargetScheduleRegistration.prepare(
                reader, command, source, scope, lineage, admitted.registration(), maximumDomains);
        if (registration.code() != StableCode.OK) {
            return unchanged(rejected(registration.code(), source), root);
        }
        final var binding = registration.binding();
        final var locator = new TargetMessageLocator(
                binding.messageId(),
                0,
                binding.target(),
                binding.domain(),
                binding.accountingIncarnation(),
                intent.orderingMode(),
                binding.orderingDomain(),
                binding.digest());
        if (prepare != null) {
            if (intent.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                final byte[] orderKey = TargetKeyCodec.orderState(binding.target(), binding.orderingDomain());
                final byte[] rawOrder = reader.get(ColumnFamily.META, orderKey);
                if (rawOrder != null) {
                    final var order = TargetOrderState.decodeForStore(
                            orderKey,
                            TargetValueEnvelope.decode(rawOrder, TargetOrderState.VALUE_TYPE)
                                    .payload(),
                            scope.shard(),
                            registration.queue());
                    if (order.orderingContract() != admitted.orderingContract()) {
                        throw new IllegalStateException("Prepare cannot change the existing ordering contract");
                    }
                    if (order.gate() != TargetOrderState.Gate.OPEN) {
                        return unchanged(
                                rejected(
                                        order.gate() == TargetOrderState.Gate.CLOSED
                                                ? StableCode.LANE_CLOSED
                                                : StableCode.ORDERING_DOMAIN_BROKEN,
                                        source),
                                registration.owner());
                    }
                }
            }
            final long expiry;
            try {
                expiry = Math.addExact(source.brokerPersistenceTimeEpochMs(), prepare.reservationTtlMs());
            } catch (ArithmeticException overflow) {
                return unchanged(rejected(StableCode.INVALID_COMMAND, source), registration.owner());
            }
            final var reservation = TargetReservationRecord.prepare(
                    locator, command, stamp, expiry, lineage, admitted.orderingContract());
            final var payloadOwner = TargetQuotaPayloadOwner.reserved(
                    binding,
                    reservation.reservationId(),
                    scope.tenantScope(),
                    registration.owner().accounting(),
                    lineage,
                    stamp);
            reservation.requireBinding(binding);
            reservation.requireOwner(payloadOwner);
            if (reader.get(ColumnFamily.ID, reservation.key()) != null
                    || reader.get(ColumnFamily.ID, reservation.lookupKey()) != null
                    || reader.get(ColumnFamily.TIMELINE, reservation.expiryKey()) != null) {
                throw new IllegalStateException("new reservation encountered retained identity/index");
            }
            final var edits = new ArrayList<>(registration.edits());
            edits.add(reader.replace(
                    ColumnFamily.ID,
                    reservation.key(),
                    TargetReservationRecord.VALUE_TYPE,
                    reservation.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.ID,
                    reservation.lookupKey(),
                    TargetReservationRecord.VALUE_TYPE,
                    reservation.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.TIMELINE,
                    reservation.expiryKey(),
                    TargetReservationRecord.VALUE_TYPE,
                    reservation.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.META,
                    payloadOwner.key(),
                    TargetQuotaPayloadOwner.VALUE_TYPE,
                    payloadOwner.canonicalBytes()));
            return new Decision(
                    new TargetMessageStore.Input(List.of(), List.of(), edits),
                    applied(StableCode.OK, source, null),
                    registration.owner(),
                    registration.owner().accounting());
        }
        final var payloadOwner = TargetQuotaPayloadOwner.scheduled(
                binding, scope.tenantScope(), registration.owner().accounting(), lineage, stamp);
        return scheduledDecision(
                reader,
                binding,
                registration.queue(),
                registration.owner(),
                admitted.orderingContract(),
                stamp,
                intent,
                payloadOwner,
                new ArrayList<>(registration.edits()),
                true);
    }

    private Decision scheduledDecision(
            TargetStoreBackend.Reader reader,
            TargetScheduleBinding binding,
            TargetQueueState queue,
            TargetQuotaIncarnation owner,
            TargetOrderState.OrderingContract orderingContract,
            TargetQuotaMutation stamp,
            CanonicalScheduleIntent intent,
            TargetQuotaPayloadOwner payloadOwner,
            ArrayList<TargetStoreBackend.Edit> edits,
            boolean newIngress) {
        final var source = stamp.source();
        final var locator = new TargetMessageLocator(
                payloadOwner.messageId(),
                0,
                binding.target(),
                binding.domain(),
                binding.accountingIncarnation(),
                intent.orderingMode(),
                binding.orderingDomain(),
                binding.digest());
        final var work = new TargetTimelineWorkRef(
                locator,
                TimelineWorkKind.INITIAL_SCHEDULE,
                intent.deliverAtEpochMs(),
                intent.deliverAtEpochMs(),
                source.sourceOrderToken(),
                1,
                1,
                UncertainRetryAuthority.NONE,
                null,
                null,
                binding.nativePolicyScopeRef() != null);
        final var orders = new ArrayList<TargetMessageStore.OrderTransition>();
        if (intent.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
            final byte[] orderKey = TargetKeyCodec.orderState(binding.target(), binding.orderingDomain());
            final byte[] raw = reader.get(ColumnFamily.META, orderKey);
            final var before = raw == null
                    ? null
                    : TargetOrderState.decodeForStore(
                            orderKey,
                            TargetValueEnvelope.decode(raw, TargetOrderState.VALUE_TYPE)
                                    .payload(),
                            scope.shard(),
                            queue);
            if (before != null) {
                if (before.orderingContract() != orderingContract) {
                    throw new IllegalStateException("Schedule cannot change an existing strict ordering contract");
                }
                if (before.gate() != TargetOrderState.Gate.OPEN) {
                    return unchanged(
                            rejected(
                                    before.gate() == TargetOrderState.Gate.CLOSED
                                            ? StableCode.LANE_CLOSED
                                            : StableCode.ORDERING_DOMAIN_BROKEN,
                                    source),
                            owner);
                }
                if (before.lastAdmittedOrder() != null
                        && Arrays.compareUnsigned(
                                        work.ordinaryKey(),
                                        before.lastAdmittedOrder().encodedKey())
                                <= 0) {
                    return unchanged(rejected(StableCode.ORDER_BEFORE_ADMISSION_WATERMARK, source), owner);
                }
            }
            final var order = new TargetOrderState(
                    binding.target(),
                    binding.orderingDomain(),
                    scope.shard(),
                    binding.domain(),
                    binding.accountingIncarnation(),
                    orderingContract,
                    before == null ? 1 : TargetQueueState.nextRevision(before.stateRevision()),
                    before == null ? 1 : before.controlVersion(),
                    TargetOrderState.Gate.OPEN,
                    before == null || before.lastAdmittedOrder() == null
                            ? null
                            : before.lastAdmittedOrder().encodedKey(),
                    before == null ? null : before.serviceableHead(),
                    before == null ? null : before.barrier());
            orders.add(new TargetMessageStore.OrderTransition(before, order));
        }
        final var message = new TargetMessageRecord(
                locator,
                1,
                intent.deliverAtEpochMs(),
                intent.expireAtEpochMs(),
                intent.deliverAtEpochMs(),
                intent.nativeDeliveryPolicy(),
                source,
                intent.hasInlinePayload() ? intent.inlinePayload() : null,
                payloadOwner.committedPayload(),
                new TargetGenerationRuntimeIndex(
                        0,
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        work,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        1));
        payloadOwner.requireMessagePayload(message);
        edits.add(reader.replace(
                ColumnFamily.META,
                payloadOwner.key(),
                TargetQuotaPayloadOwner.VALUE_TYPE,
                payloadOwner.canonicalBytes()));
        return new Decision(
                new TargetMessageStore.Input(List.of(new TargetMessageStore.Transition(null, message)), orders, edits),
                applied(StableCode.SCHEDULED, source, message),
                owner,
                newIngress ? owner.accounting() : null);
    }

    private Decision commitReservation(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            CommitLargeScheduleBody body,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation root,
            CancellationControls closures,
            PayloadProofControls proofs) {
        final var source = stamp.source();
        final byte[] lookupKey =
                Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_LOOKUP_TAG, 1}, body.reservationId());
        final byte[] raw = reader.get(ColumnFamily.ID, lookupKey);
        if (raw == null) {
            return unchanged(rejected(StableCode.RESERVATION_NOT_COMMITTED, source), root);
        }
        final var reservation =
                TargetReservationRecord.decode(TargetValueEnvelope.decode(raw, TargetReservationRecord.VALUE_TYPE)
                        .payload());
        if (!Arrays.equals(lookupKey, reservation.lookupKey())
                || !Arrays.equals(lineage, reservation.recoveryLineage())) {
            throw new IllegalStateException("Commit reservation lookup/lineage differs from actual Store");
        }
        if (!reservation.locator().messageId().equals(command.delayMessageId())) {
            return unchanged(rejected(StableCode.RESERVATION_NOT_COMMITTED, source), root);
        }
        if (!Arrays.equals(
                reservation.canonicalBytes(),
                payload(reader, ColumnFamily.ID, reservation.key(), TargetReservationRecord.VALUE_TYPE))) {
            throw new IllegalStateException("Commit reservation projections differ");
        }
        reservation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                command.delayMessageId().bytes());
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey,
                payload(reader, ColumnFamily.META, payloadKey, TargetQuotaPayloadOwner.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        reservation.requireOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final var owner = descriptor(reader, payloadOwner.primaryIdentity());
        owner.requirePayloadOwner(payloadOwner);
        final byte[] bindingKey =
                TargetKeyCodec.scheduleBinding(reservation.locator().scheduleBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                scope.shard());
        reservation.requireBinding(binding);
        payloadOwner.requireInitialBinding(binding);
        final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        final byte[] queueKey = TargetKeyCodec.state(binding.target());
        final var queue =
                TargetQueueState.decode(payload(reader, ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE));
        binding.requireQueueProjection(queue);
        final var effectiveStatus = reservation.effectiveStatus(reader.closedIngressDeadlineThrough());
        final byte[] expiry = reader.get(ColumnFamily.TIMELINE, reservation.expiryKey());
        if (reservation.status() == PayloadReservationStatus.RESERVED) {
            if (expiry == null
                    || !Arrays.equals(
                            reservation.canonicalBytes(),
                            TargetValueEnvelope.decode(expiry, TargetReservationRecord.VALUE_TYPE)
                                    .payload())) {
                throw new IllegalStateException("Commit reservation expiry differs");
            }
            if (reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId())) != null) {
                throw new IllegalStateException("uncommitted reservation already owns a Message");
            }
            if (effectiveStatus == PayloadReservationStatus.RESERVED
                    && (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED
                            || closures.closed(reader, binding, source))) {
                return unchanged(rejected(StableCode.PAYLOAD_RESERVATION_CLOSED, source), owner);
            }
        } else if (expiry != null) {
            throw new IllegalStateException("terminal reservation retains expiry");
        }
        final var proof = body.proof();
        if (!proof.objectStoreProfile().equals(prepare.objectStoreProfile())
                || !Arrays.equals(proof.tenantRoutingScope(), scope.tenantScope())
                || proof.trustSetVersion() != prepare.trustSet().version()
                || proof.length() != prepare.expectedPayloadLength()
                || !Arrays.equals(proof.payloadSha256(), prepare.payloadSha256())) {
            return unchanged(rejected(StableCode.PAYLOAD_PROOF_INVALID, source), owner);
        }
        final var reference = TargetPayloadReference.requireBounded(new PayloadReference(
                proof.objectStoreProfileHash(),
                proof.container(),
                proof.objectKey(),
                proof.immutableObjectVersion(),
                proof.etag(),
                proof.length(),
                proof.payloadSha256(),
                proof.reservationId(),
                proof.proofId()));
        final boolean historical = reservation.status() == PayloadReservationStatus.COMMITTED;
        if (historical) {
            if (!reference.equals(reservation.committedPayload())) {
                return unchanged(rejected(StableCode.PAYLOAD_COMMIT_CONFLICT, source), owner);
            }
            final var messageKey = TargetKeyCodec.message(command.delayMessageId());
            final var message = TargetMessageRecord.decodeForStore(
                    messageKey,
                    payload(reader, ColumnFamily.ID, messageKey, TargetMessageRecord.VALUE_TYPE),
                    scope.shard());
            payloadOwner.requireMessagePayload(message);
        } else if (reservation.status() == PayloadReservationStatus.ABANDONED) {
            return unchanged(rejected(StableCode.PAYLOAD_RESERVATION_CLOSED, source), owner);
        } else if (effectiveStatus == PayloadReservationStatus.EXPIRED) {
            return unchanged(rejected(StableCode.RESERVATION_EXPIRED, source), owner);
        } else if (source.brokerPersistenceTimeEpochMs() > reservation.expiryEpochMs()
                || source.brokerPersistenceTimeEpochMs() > proof.notAfterEpochMs()
                || proof.notAfterEpochMs() > reservation.expiryEpochMs()) {
            return unchanged(rejected(StableCode.PAYLOAD_PROOF_INVALID, source), owner);
        }
        final PayloadProofAuthority authority;
        try {
            authority = Objects.requireNonNull(proofs.resolve(binding, source), "payload proof authority");
        } catch (ReadIncompleteException external) {
            throw new IllegalStateException("external payload proof authority did not complete", external);
        }
        if (!authority.semantic().ref().equals(prepare.trustSet())
                || !authority.controls().activatedAt(prepare.trustSet(), binding.bindingSource())) {
            throw new IllegalStateException("proof catalog differs from the immutable Prepare trust set");
        }
        final var verifier = PayloadProofTrustSet.fromSemantic(authority.semantic());
        final boolean authorized = historical
                ? authority
                                .controls()
                                .historicalVerificationAllowed(prepare.trustSet(), proof.proofKeyVersion(), source)
                        && verifier.verifiesHistoricalSignature(proof)
                : authority.controls().firstSeenIssuanceOpen(prepare.trustSet(), proof.proofKeyVersion(), source)
                        && verifier.verifies(proof, source.brokerPersistenceTimeEpochMs());
        if (!authorized) {
            return unchanged(rejected(StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION, source), owner);
        }
        if (historical) {
            return unchanged(applied(StableCode.ALREADY_COMMITTED, source, null), owner);
        }
        final var committed = reservation.finish(PayloadReservationStatus.COMMITTED, stamp, reference);
        final var activeOwner = payloadOwner.commit(reference, stamp, (prior, next, floor) -> {
            if (prior != payloadOwner || !next.mutation().equals(stamp) || floor != null) {
                throw new IllegalStateException("Commit changed its exact source owner");
            }
        });
        committed.requireOwner(activeOwner);
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        edits.add(reader.replace(
                ColumnFamily.ID, reservation.key(), TargetReservationRecord.VALUE_TYPE, committed.canonicalBytes()));
        edits.add(reader.replace(
                ColumnFamily.ID,
                reservation.lookupKey(),
                TargetReservationRecord.VALUE_TYPE,
                committed.canonicalBytes()));
        edits.add(reader.replace(
                ColumnFamily.TIMELINE, reservation.expiryKey(), TargetReservationRecord.VALUE_TYPE, null));
        return scheduledDecision(
                reader,
                binding,
                queue,
                owner,
                reservation.orderingContract(),
                stamp,
                prepare.intentWithoutPayload(),
                activeOwner,
                edits,
                false);
    }

    private Decision modifyReservation(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            MessagePrecondition precondition,
            byte[] payloadKey,
            byte[] payloadRaw,
            TargetQuotaMutation stamp,
            CancellationControls controls) {
        final var source = stamp.source();
        final byte[] key = Bytes.concat(
                new byte[] {TargetKeyCodec.RESERVATION_TAG, 1},
                command.delayMessageId().bytes());
        final var reservation = TargetReservationRecord.decode(
                payload(reader, ColumnFamily.ID, key, TargetReservationRecord.VALUE_TYPE));
        if (!Arrays.equals(key, reservation.key()) || !Arrays.equals(reservation.recoveryLineage(), lineage)) {
            throw new IllegalStateException("reservation identity/lineage differs from actual Store");
        }
        reservation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (!Arrays.equals(
                reservation.canonicalBytes(),
                payload(reader, ColumnFamily.ID, reservation.lookupKey(), TargetReservationRecord.VALUE_TYPE))) {
            throw new IllegalStateException("reservation lookup differs from its Message projection");
        }
        if (reservation.status() != PayloadReservationStatus.RESERVED
                && reader.get(ColumnFamily.TIMELINE, reservation.expiryKey()) != null) {
            throw new IllegalStateException("terminal reservation retains an expiry index");
        }
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey,
                TargetValueEnvelope.decode(payloadRaw, TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        reservation.requireOwner(payloadOwner);
        final var owner = descriptor(reader, payloadOwner.primaryIdentity());
        owner.requirePayloadOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if ((precondition.expectedGeneration() != null && precondition.expectedGeneration() != 0)
                || (precondition.expectedStateVersion() != null
                        && precondition.expectedStateVersion() != reservation.stateVersion())) {
            return unchanged(applied(StableCode.VERSION_CONFLICT, source, null), owner);
        }
        final var bindingKey =
                TargetKeyCodec.scheduleBinding(reservation.locator().scheduleBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                scope.shard());
        reservation.requireBinding(binding);
        payloadOwner.requireInitialBinding(binding);
        final var queueKey = TargetKeyCodec.state(binding.target());
        final var queue =
                TargetQueueState.decode(payload(reader, ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE));
        binding.requireQueueProjection(queue);
        if (reservation.status() != PayloadReservationStatus.RESERVED) {
            final var code =
                    switch (reservation.status()) {
                        case ABANDONED -> StableCode.ALREADY_ABANDONED;
                        case EXPIRED -> StableCode.RESERVATION_EXPIRED;
                        case COMMITTED ->
                            throw new IllegalStateException("committed reservation is missing its Message");
                        default -> throw new IllegalStateException("unexpected reservation state");
                    };
            return unchanged(applied(code, source, null), owner);
        }
        final var index =
                payload(reader, ColumnFamily.TIMELINE, reservation.expiryKey(), TargetReservationRecord.VALUE_TYPE);
        if (!Arrays.equals(index, reservation.canonicalBytes())) {
            throw new IllegalStateException("reservation expiry projection differs");
        }
        if (reservation.effectiveStatus(reader.closedIngressDeadlineThrough()) == PayloadReservationStatus.EXPIRED) {
            return unchanged(applied(StableCode.RESERVATION_EXPIRED, source, null), owner);
        }
        if (command.type() == CommandType.RESCHEDULE) {
            return unchanged(applied(StableCode.RESERVATION_NOT_COMMITTED, source, null), owner);
        }
        if (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED
                || controls.closed(reader, binding, source)) {
            return unchanged(applied(StableCode.PAYLOAD_RESERVATION_CLOSED, source, null), owner);
        }
        final var after = reservation.finish(PayloadReservationStatus.ABANDONED, stamp, null);
        final var retained = payloadOwner.retain(stamp, (prior, next, floor) -> {
            if (prior != payloadOwner || !next.mutation().equals(stamp) || floor != null) {
                throw new IllegalStateException("reservation release changed its exact source owner");
            }
        });
        after.requireOwner(retained);
        return new Decision(
                new TargetMessageStore.Input(
                        List.of(),
                        List.of(),
                        List.of(
                                reader.replace(
                                        ColumnFamily.ID,
                                        key,
                                        TargetReservationRecord.VALUE_TYPE,
                                        after.canonicalBytes()),
                                reader.replace(
                                        ColumnFamily.ID,
                                        reservation.lookupKey(),
                                        TargetReservationRecord.VALUE_TYPE,
                                        after.canonicalBytes()),
                                reader.replace(
                                        ColumnFamily.TIMELINE,
                                        reservation.expiryKey(),
                                        TargetReservationRecord.VALUE_TYPE,
                                        null),
                                reader.replace(
                                        ColumnFamily.META,
                                        payloadKey,
                                        TargetQuotaPayloadOwner.VALUE_TYPE,
                                        retained.canonicalBytes()))),
                applied(StableCode.PAYLOAD_RESERVATION_ABANDONED, source, null),
                owner);
    }

    private Decision modifyMessage(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            MessagePrecondition precondition,
            RescheduleCommandBody reschedule,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation root,
            CancellationControls controls,
            Policy policy) {
        final var source = stamp.source();
        final byte[] raw = reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId()));
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                command.delayMessageId().bytes());
        final byte[] payloadRaw = reader.get(ColumnFamily.META, payloadKey);
        if (raw == null) {
            if (payloadRaw != null) {
                return modifyReservation(reader, command, precondition, payloadKey, payloadRaw, stamp, controls);
            }
            return unchanged(applied(StableCode.NOT_FOUND, source, null), root);
        }
        if (payloadRaw == null) {
            throw new IllegalStateException("existing Target Message has no payload owner");
        }
        final var before = TargetMessageRecord.decodeForStore(
                TargetKeyCodec.message(command.delayMessageId()),
                TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(),
                scope.shard());
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey,
                TargetValueEnvelope.decode(payloadRaw, TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        payloadOwner.requireMessagePayload(before);
        final var owner = descriptor(reader, payloadOwner.primaryIdentity());
        owner.requirePayloadOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if ((precondition.expectedGeneration() != null
                        && precondition.expectedGeneration()
                                != Integer.toUnsignedLong(before.locator().generation()))
                || (precondition.expectedStateVersion() != null
                        && precondition.expectedStateVersion() != before.stateVersion())) {
            return unchanged(applied(StableCode.VERSION_CONFLICT, source, before), owner);
        }
        final var bindingKey = TargetKeyCodec.scheduleBinding(before.locator().scheduleBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                scope.shard());
        payloadOwner.requireInitialBinding(binding);
        binding.requireLocator(before.locator());
        binding.requireMessageSource(before.scheduleSource());
        final byte[] queueKey = TargetKeyCodec.state(before.locator().target());
        final var queue =
                TargetQueueState.decode(payload(reader, ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE));
        before.locator().requireQueueProjection(queue);
        binding.requireQueueProjection(queue);
        final boolean closed = controls.closed(reader, binding, source)
                || queue.admissionState() == TargetQueueState.AdmissionState.CLOSED;
        if (closed
                && before.runtime().admissionsUsed() == 0
                && !before.runtime().terminal()) {
            return unchanged(
                    applied(
                            reschedule == null ? StableCode.ALREADY_DEAD_LETTERED : StableCode.LANE_CLOSED,
                            source,
                            before),
                    owner);
        }
        if (before.runtime().terminal()) {
            final var terminal = terminal(reader, before);
            final var code = reschedule == null && before.aggregateState() == GenerationAggregateState.CANCELED
                    ? StableCode.ALREADY_CANCELED
                    : terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION
                            ? reschedule == null ? StableCode.ALREADY_DEAD_LETTERED : StableCode.LANE_CLOSED
                            : StableCode.TOO_LATE;
            return unchanged(applied(code, source, before), owner);
        }
        if (!before.runtime().attemptObligations().isEmpty()
                || (before.runtime().currentWorkKind() != CurrentSendWorkKind.TIMELINE
                        && before.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED)) {
            return unchanged(applied(StableCode.TOO_LATE, source, before), owner);
        }
        if (payloadOwner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            throw new IllegalStateException("live cancellable Message has inactive payload accounting");
        }
        if (reschedule != null) {
            return reschedule(reader, before, payloadOwner, owner, reschedule, stamp, policy, queue, binding);
        }
        final var runtime = before.runtime();
        final var nextRuntime = new TargetGenerationRuntimeIndex(
                runtime.generation(),
                GenerationAggregateState.CANCELED,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                runtime.attemptObligations(),
                runtime.admissionsUsed(),
                runtime.uncertainRetryAdmissionsUsed(),
                runtime.possibleDestinationDuplicate(),
                TargetQueueState.nextRevision(runtime.runtimeRevision()));
        final var after = new TargetMessageRecord(
                before.locator(),
                TargetQueueState.nextRevision(before.stateVersion()),
                before.deliverAtEpochMs(),
                before.expireAtEpochMs(),
                before.retryEligibilityAtEpochMs(),
                before.nativeDeliveryPolicy(),
                before.scheduleSource(),
                before.inlinePayload(),
                before.payloadReference(),
                nextRuntime);
        final var terminal = new TargetTerminalGenerationRecord(
                after.locator(), after.stateVersion(), StableCode.CANCELED, nextRuntime, stamp, lineage);
        terminal.requireOwner(payloadOwner);
        if (reader.get(ColumnFamily.TERMINAL, terminal.key()) != null) {
            throw new IllegalStateException("cancellable generation already has terminal history");
        }
        final var retained = payloadOwner.retain(stamp, (prior, next, floor) -> {
            if (floor != null
                    || !Arrays.equals(prior.canonicalBytes(), payloadOwner.canonicalBytes())
                    || !next.mutation().equals(stamp)
                    || next.phase() != TargetQuotaPayloadOwner.Phase.RETAINED
                    || !after.runtime().attemptObligations().isEmpty()) {
                throw new IllegalStateException("Cancel retention changed exact payload/terminal transition");
            }
            next.requireMessagePayload(after);
        });
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        edits.add(reader.replace(
                ColumnFamily.META, retained.key(), TargetQuotaPayloadOwner.VALUE_TYPE, retained.canonicalBytes()));
        edits.add(reader.replace(
                ColumnFamily.TERMINAL,
                terminal.key(),
                TargetTerminalGenerationRecord.VALUE_TYPE,
                terminal.canonicalBytes()));
        if (runtime.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final var claim = TargetClaimStore.current(reader, before);
            stamp.requireStoreSuccessorOf(claim.creation());
            edits.add(reader.replace(ColumnFamily.INFLIGHT, claim.key(), TargetClaimRecord.VALUE_TYPE, null));
            edits.add(reader.replace(ColumnFamily.META, claim.chargeKey(), TargetQuotaClaimCharge.VALUE_TYPE, null));
        }
        return new Decision(
                new TargetMessageStore.Input(
                        List.of(new TargetMessageStore.Transition(before, after)), releaseOrder(reader, before), edits),
                applied(StableCode.CANCELED, source, after),
                owner);
    }

    private Decision reschedule(
            TargetStoreBackend.Reader reader,
            TargetMessageRecord before,
            TargetQuotaPayloadOwner payloadOwner,
            TargetQuotaIncarnation owner,
            RescheduleCommandBody request,
            TargetQuotaMutation stamp,
            Policy policy,
            TargetQueueState queue,
            TargetScheduleBinding binding) {
        final var source = stamp.source();
        if (!policy.deliveryWindow()
                .permits(
                        request.newDeliverAtEpochMs(),
                        request.newExpireAtEpochMs(),
                        source.brokerPersistenceTimeEpochMs())) {
            return unchanged(rejected(StableCode.INVALID_DELIVERY_WINDOW, source), owner);
        }
        final int generation;
        try {
            generation = UnsignedInt32.successor(before.locator().generation());
        } catch (ArithmeticException exhausted) {
            return unchanged(rejected(StableCode.INVALID_COMMAND, source), owner);
        }
        final var old = before.locator();
        final var locator = new TargetMessageLocator(
                old.messageId(),
                generation,
                old.target(),
                old.domain(),
                old.accountingIncarnation(),
                old.orderingMode(),
                old.orderingDomain(),
                old.scheduleBindingDigest());
        final byte[] identityKey = TargetKeyCodec.identity(old.target());
        final var identity = CanonicalTargetPartition.decodeForStore(
                identityKey, payload(reader, ColumnFamily.META, identityKey, CanonicalTargetPartition.VALUE_TYPE));
        final boolean nativeCandidate = binding.nativePolicyScopeRef() != null
                && before.nativeDeliveryPolicy() != NativeDeliveryPolicy.FORBID
                && identity.resource().kind() == BrokerResourceIdentity.Kind.PULSAR
                && old.orderingMode() == OrderingMode.BEST_EFFORT
                && queue.domains().get(old.domain().slot()).nativePolicyScopeRef() != null;
        final var work = new TargetTimelineWorkRef(
                locator,
                TimelineWorkKind.INITIAL_SCHEDULE,
                request.newDeliverAtEpochMs(),
                request.newDeliverAtEpochMs(),
                source.sourceOrderToken(),
                1,
                1,
                UncertainRetryAuthority.NONE,
                null,
                null,
                nativeCandidate);
        if (old.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
            final var orderKey = TargetKeyCodec.orderState(old.target(), old.orderingDomain());
            final var order =
                    TargetOrderState.decode(payload(reader, ColumnFamily.META, orderKey, TargetOrderState.VALUE_TYPE));
            if (order.orderingContract() == TargetOrderState.OrderingContract.ADMISSION_WATERMARK
                    && order.lastAdmittedOrder() != null
                    && Arrays.compareUnsigned(
                                    work.ordinaryKey(),
                                    order.lastAdmittedOrder().encodedKey())
                            <= 0) {
                return unchanged(rejected(StableCode.ORDER_BEFORE_ADMISSION_WATERMARK, source), owner);
            }
        }
        final long stateVersion = TargetQueueState.nextRevision(before.stateVersion());
        final var after = new TargetMessageRecord(
                locator,
                stateVersion,
                request.newDeliverAtEpochMs(),
                request.newExpireAtEpochMs(),
                request.newDeliverAtEpochMs(),
                before.nativeDeliveryPolicy(),
                source,
                before.inlinePayload(),
                before.payloadReference(),
                new TargetGenerationRuntimeIndex(
                        generation,
                        GenerationAggregateState.SCHEDULED,
                        CurrentSendWorkKind.TIMELINE,
                        work,
                        null,
                        null,
                        List.of(),
                        0,
                        0,
                        false,
                        1));
        payloadOwner.requireMessagePayload(after);
        final var prior = before.runtime();
        final var history = new TargetTerminalGenerationRecord(
                old,
                stateVersion,
                StableCode.SUPERSEDED,
                new TargetGenerationRuntimeIndex(
                        old.generation(),
                        GenerationAggregateState.SUPERSEDED,
                        CurrentSendWorkKind.NONE,
                        null,
                        null,
                        null,
                        prior.attemptObligations(),
                        prior.admissionsUsed(),
                        prior.uncertainRetryAdmissionsUsed(),
                        prior.possibleDestinationDuplicate(),
                        TargetQueueState.nextRevision(prior.runtimeRevision())),
                stamp,
                lineage);
        history.requireOwner(payloadOwner);
        if (reader.get(ColumnFamily.TERMINAL, history.key()) != null) {
            throw new IllegalStateException("rescheduled generation already has terminal history");
        }
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        edits.add(reader.replace(
                ColumnFamily.TERMINAL,
                history.key(),
                TargetTerminalGenerationRecord.VALUE_TYPE,
                history.canonicalBytes()));
        if (prior.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final var claim = TargetClaimStore.current(reader, before);
            stamp.requireStoreSuccessorOf(claim.creation());
            edits.add(reader.replace(ColumnFamily.INFLIGHT, claim.key(), TargetClaimRecord.VALUE_TYPE, null));
            edits.add(reader.replace(ColumnFamily.META, claim.chargeKey(), TargetQuotaClaimCharge.VALUE_TYPE, null));
        }
        return new Decision(
                new TargetMessageStore.Input(
                        List.of(new TargetMessageStore.Transition(before, after)), releaseOrder(reader, before), edits),
                applied(StableCode.SUPERSEDED, source, after),
                owner);
    }

    private TargetTerminalGenerationRecord terminal(TargetStoreBackend.Reader reader, TargetMessageRecord message) {
        final var value = TargetTerminalGenerationRecord.decode(payload(
                reader,
                ColumnFamily.TERMINAL,
                TargetTerminalGenerationRecord.key(message.locator()),
                TargetTerminalGenerationRecord.VALUE_TYPE));
        if (!value.locator().equals(message.locator())
                || value.stateVersion() != message.stateVersion()
                || !Arrays.equals(
                        value.runtime().canonicalBytes(), message.runtime().canonicalBytes())
                || !Arrays.equals(value.recoveryLineage(), lineage)) {
            throw new IllegalStateException("terminal history differs from current Message generation");
        }
        value.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return value;
    }

    private static List<TargetMessageStore.OrderTransition> releaseOrder(
            TargetStoreBackend.Reader reader, TargetMessageRecord before) {
        if (before.locator().orderingMode() != OrderingMode.DELIVERY_TIME_FIFO) {
            return List.of();
        }
        final var key = TargetKeyCodec.orderState(
                before.locator().target(), before.locator().orderingDomain());
        final var old = TargetOrderState.decode(payload(reader, ColumnFamily.META, key, TargetOrderState.VALUE_TYPE));
        final TargetOrderBarrier barrier;
        if (before.runtime().currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            old.requireBarrierProjection(before);
            barrier = null;
        } else {
            if (old.barrier() != null
                    && old.barrier()
                            .locator()
                            .messageId()
                            .equals(before.locator().messageId())) {
                throw new IllegalStateException("timeline Message unexpectedly owns a strict barrier");
            }
            barrier = old.barrier();
        }
        final var next = new TargetOrderState(
                old.target(),
                old.orderingDomain(),
                old.sourceShard(),
                old.executionDomain(),
                old.accountingIncarnation(),
                old.orderingContract(),
                TargetQueueState.nextRevision(old.stateRevision()),
                old.controlVersion(),
                old.gate(),
                old.lastAdmittedOrder() == null ? null : old.lastAdmittedOrder().encodedKey(),
                null,
                barrier);
        return List.of(new TargetMessageStore.OrderTransition(old, next));
    }

    private TargetMessageStore.Input withResults(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation root,
            Decision decision,
            CommandResult[] result) {
        final TargetResultRecord.CreationAuthority authority = (record, first) -> {
            record.requireOwner(record.kind().position() ? root : decision.owner());
            if (!record.mutation().equals(stamp)
                    || !Arrays.equals(record.logicalId(), command.commandId().bytes())
                    || reader.get(ColumnFamily.DEDUPE, record.key()) != null) {
                throw new IllegalStateException("first Command result differs from its accepted source/absence");
            }
            if (first != null) {
                record.requireFirst(first);
            }
        };
        final var first = TargetResultRecord.command(
                decision.owner(),
                command.commandId(),
                command.protocolTuple(),
                command.commandHash(),
                decision.result(),
                stamp,
                authority);
        final var query = TargetResultRecord.result(first, authority);
        final var physical = TargetResultRecord.position(root, first, stamp, authority);
        final var edits = new ArrayList<>(decision.input().extra());
        for (var record : List.of(first, query, physical)) {
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, record.key(), TargetResultRecord.VALUE_TYPE, record.canonicalBytes()));
        }
        result[0] = decision.result();
        return new TargetMessageStore.Input(
                decision.input().messages(), decision.input().orders(), edits);
    }

    private TargetQuotaIncarnation descriptor(TargetStoreBackend.Reader reader, TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner = TargetQuotaIncarnation.decodeForStore(
                key,
                payload(reader, ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!owner.identity().equals(identity) || !Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("first Command owner belongs to another lineage/identity");
        }
        owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());
        return owner;
    }

    private static StableCode invalid(PreparedCommand command, SourcePosition source, Policy policy) {
        try {
            final long time = command.commandId().routingId().logicalTimestampEpochMs();
            if (Math.addExact(time, policy.retryWindowMs()) != command.retryUntilEpochMs()
                    || time
                            < Math.subtractExact(
                                    source.brokerPersistenceTimeEpochMs(), policy.maximumPreparationAgeMs())
                    || time > Math.addExact(source.brokerPersistenceTimeEpochMs(), policy.maximumFutureSkewMs())) {
                return StableCode.INVALID_COMMAND;
            }
        } catch (ArithmeticException invalid) {
            return StableCode.INVALID_COMMAND;
        }
        if (!policy.activatedTuples().contains(command.protocolTuple())) {
            return StableCode.UNACTIVATED_PROTOCOL_VERSION;
        }
        return command.protocolTuple().equals(ProtocolTuple.managedCommand())
                        || command.protocolTuple().equals(ProtocolTuple.currentClientCommand())
                ? null
                : StableCode.UNSUPPORTED_ACTIVATED_PROTOCOL;
    }

    private static Decision unchanged(CommandResult result, TargetQuotaIncarnation owner) {
        return new Decision(new TargetMessageStore.Input(List.of(), List.of(), List.of()), result, owner);
    }

    private static CommandResult rejected(StableCode code, SourcePosition source) {
        return new CommandResult(ApplyStatus.REJECTED, code, -1, 0, null, source.canonicalBytes());
    }

    private static CommandResult applied(StableCode code, SourcePosition source, TargetMessageRecord message) {
        final var state = message == null ? null : message.aggregateState();
        final var status = state == null
                ? null
                : state == GenerationAggregateState.RETRY_WAIT
                        ? MessageStatus.SCHEDULED
                        : MessageStatus.valueOf(state.name());
        return new CommandResult(
                ApplyStatus.APPLIED,
                code,
                message == null ? -1 : message.locator().generation(),
                message == null ? 0 : message.stateVersion(),
                status,
                source.canonicalBytes());
    }

    private static byte[] payload(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key, int type) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("first Command dependency is absent");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static byte[] commandKey(PreparedCommand command) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_COMMAND_TAG, 1},
                command.commandId().bytes());
    }

    private static byte[] queryKey(PreparedCommand command) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_QUERY_TAG, 1},
                command.commandId().bytes());
    }

    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, source.canonicalBytes());
    }
}
