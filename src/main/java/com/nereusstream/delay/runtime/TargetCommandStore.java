package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CancelCommandBody;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
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
    /** Authenticated Route inputs. The first-command commit guard must retain their exact source activation. */
    public record Policy(
            TargetQuotaScope scope,
            long retryWindowMs,
            long maximumPreparationAgeMs,
            long maximumFutureSkewMs,
            Set<ProtocolTuple> activatedTuples) {
        public Policy {
            Objects.requireNonNull(scope, "scope");
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

    private record Decision(TargetMessageStore.Input input, CommandResult result, TargetQuotaIncarnation owner) {}

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
            CancellationControls controls) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(controls, "controls");
        TargetSourcePosition.requireBounded(source);
        if (!scope.equals(policy.scope())
                || !scope.shard().equals(command.shardId())
                || !scope.shard().equals(source.shardId())) {
            throw new IllegalArgumentException("first Command source/Route policy scope differs");
        }
        final var result = new CommandResult[1];
        final byte[] digest = Bytes.sha256(CommandCodec.encodeFrame(command));
        final var batch = messages.prepareAccounted(
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
                                "standalone expired Command POSITION writer is not wired yet; retain source");
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
                        decision = cancel(reader, command, body, stamp, root, controls);
                    } else {
                        throw new IllegalStateException(
                                "first Target Command business branch is not wired yet; retain source");
                    }
                    return withResults(reader, command, stamp, root, decision, result);
                },
                new TargetQuotaStoreGate(scope, lineage, 1)
                        .wrap(
                                new TargetSourceAccounting(
                                        scope, lineage, source, digest, maximumCounters, 1, maximumDomains),
                                TargetQuotaGrantGate.Operation.CANCEL,
                                null));
        return new Prepared(this, batch, result[0]);
    }

    public CommandResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        if (Objects.requireNonNull(prepared, "prepared").owner != this) {
            throw new IllegalArgumentException("foreign first Command plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
    }

    private Decision cancel(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            CancelCommandBody body,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation root,
            CancellationControls controls) {
        final var source = stamp.source();
        final byte[] raw = reader.get(ColumnFamily.ID, TargetKeyCodec.message(command.delayMessageId()));
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                command.delayMessageId().bytes());
        final byte[] payloadRaw = reader.get(ColumnFamily.META, payloadKey);
        if (raw == null) {
            if (payloadRaw != null) {
                throw new IllegalStateException(
                        "Cancel reservation/retained identity requires its pending business handler");
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
        final var precondition = body.precondition();
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
            return unchanged(applied(StableCode.ALREADY_DEAD_LETTERED, source, before), owner);
        }
        if (before.runtime().terminal()) {
            final var terminal = terminal(reader, before);
            final var code = before.aggregateState() == GenerationAggregateState.CANCELED
                    ? StableCode.ALREADY_CANCELED
                    : terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION
                            ? StableCode.ALREADY_DEAD_LETTERED
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
                        List.of(new TargetMessageStore.Transition(before, after)), cancelOrder(reader, before), edits),
                applied(StableCode.CANCELED, source, after),
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

    private static List<TargetMessageStore.OrderTransition> cancelOrder(
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
