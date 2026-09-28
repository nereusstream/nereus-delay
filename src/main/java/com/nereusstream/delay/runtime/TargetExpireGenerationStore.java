package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
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
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** First EXPIRE_GENERATION application with its terminal, indexes and source result in one Target batch. */
public final class TargetExpireGenerationStore {
    public static final class Prepared {
        private final TargetExpireGenerationStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetExpireGenerationStore owner,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private record Transition(
            TargetMessageRecord before,
            TargetMessageRecord after,
            StableCode resultCode,
            List<TargetMessageStore.OrderTransition> orders,
            List<TargetStoreBackend.Edit> edits) {
        private Transition {
            orders = List.copyOf(orders);
            edits = List.copyOf(edits);
        }

        boolean changed() {
            return after != null;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetMessageStore messages;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;
    private final TargetQuotaStoreGate grants;

    public TargetExpireGenerationStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final int maximumCounters,
            final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null
                || maximumCounters < 2
                || maximumDomains <= 0
                || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS
                || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("Target expiry requires bounded Shard accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
        messages = new TargetMessageStore(backend, 1, 1, maximumDomains);
        grants = new TargetQuotaStoreGate(scope, lineage, 1);
    }

    /** Immutable duplicate replay must be checked first; authority remains valid through the commit guard. */
    public Prepared prepareFirst(
            final BoundedReadBudget budget,
            final SystemMutation mutation,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final TargetExpireGenerationVerifier.Authority authority) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.EXPIRE_GENERATION) {
            throw new IllegalArgumentException("Target expiry source/operation mismatch");
        }
        final var result = new SystemMutationResult[1];
        final var terminalized = new boolean[1];
        final var digest = Bytes.sha256(mutation.canonicalEnvelope());
        final var accounting = new TargetSourceAccounting(
                scope, lineage, source, digest, maximumCounters, 1, maximumDomains);
        final var batch = messages.prepareAccounted(
                budget,
                reader -> {
                    if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                        throw new IllegalStateException(
                                "first Target expiry needs an established strictly earlier source frontier");
                    }
                    final byte[] firstKey = systemKey(mutation.systemMutationId());
                    final byte[] sourceKey = positionKey(source);
                    requireAbsent(reader, firstKey);
                    requireAbsent(reader, sourceKey);
                    final var stamp = new TargetQuotaMutation(
                            TargetQuotaMutation.increment(reader.sourceSequence()), source, digest);
                    final var root = root(reader, stamp);
                    final TargetExpireGenerationVerifier.Decision decision;
                    try {
                        decision = TargetExpireGenerationVerifier.decideFirstApplication(
                                scope, mutation, source, authority);
                    } catch (ReadIncompleteException unavailable) {
                        throw new IllegalStateException(
                                "external Target expiry authority did not complete", unavailable);
                    }
                    final SystemMutationResult outcome;
                    final List<TargetMessageStore.Transition> messageTransitions;
                    final List<TargetMessageStore.OrderTransition> orderTransitions;
                    final var edits = new ArrayList<TargetStoreBackend.Edit>();
                    if (decision.rejection() != null) {
                        outcome = SystemMutationResult.from(
                                mutation,
                                ApplyStatus.REJECTED,
                                decision.rejection(),
                                source.canonicalBytes());
                        messageTransitions = List.of();
                        orderTransitions = List.of();
                    } else {
                        final Transition transition = expire(reader, decision.body(), stamp);
                        terminalized[0] = transition.changed();
                        outcome = SystemMutationResult.from(
                                mutation, ApplyStatus.APPLIED, transition.resultCode(), source.canonicalBytes());
                        messageTransitions = transition.changed()
                                ? List.of(new TargetMessageStore.Transition(transition.before(), transition.after()))
                                : List.of();
                        orderTransitions = transition.orders();
                        edits.addAll(transition.edits());
                    }
                    final var first = TargetResultRecord.system(root, outcome, stamp, null, (record, prior) -> {
                        record.requireOwner(root);
                        if (prior != null
                                || !record.mutation().equals(stamp)
                                || record.allocation() != null
                                || !Arrays.equals(record.key(), firstKey)
                                || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                            throw new IllegalStateException("first Target expiry result differs from its decision");
                        }
                        requireAbsent(reader, record.key());
                    });
                    final var position = TargetResultRecord.position(root, first, stamp, (record, prior) -> {
                        record.requireOwner(root);
                        if (prior == null
                                || !Arrays.equals(prior.canonicalBytes(), first.canonicalBytes())
                                || !record.mutation().equals(stamp)
                                || !Arrays.equals(record.key(), sourceKey)) {
                            throw new IllegalStateException(
                                    "Target expiry physical result differs from first result");
                        }
                        record.requireFirst(first);
                        requireAbsent(reader, record.key());
                    });
                    edits.add(reader.replace(
                            ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
                    edits.add(reader.replace(
                            ColumnFamily.DEDUPE,
                            position.key(),
                            TargetResultRecord.VALUE_TYPE,
                            position.canonicalBytes()));
                    result[0] = outcome;
                    return new TargetMessageStore.Input(messageTransitions, orderTransitions, edits);
                },
                (reader, business) -> {
                    final var mutationPlan = accounting.assemble(reader, business);
                    if (terminalized[0]) {
                        grants.check(reader, mutationPlan, TargetQuotaGrantGate.Operation.TERMINAL, null);
                    }
                    return mutationPlan;
                });
        return new Prepared(this, batch, result[0]);
    }

    public SystemMutationResult commit(
            final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign Target expiry plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
    }

    private Transition expire(
            final TargetStoreBackend.Reader reader,
            final TargetExpireGenerationBody body,
            final TargetQuotaMutation stamp) {
        final byte[] messageKey = TargetKeyCodec.message(body.messageId());
        final byte[] raw = reader.get(ColumnFamily.ID, messageKey);
        if (raw == null) {
            return unchanged(StableCode.NOT_FOUND);
        }
        final var before = TargetMessageRecord.decodeForStore(
                messageKey,
                TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(),
                scope.shard());
        final int generationOrder = Integer.compareUnsigned(before.locator().generation(), body.generation());
        if (generationOrder != 0) {
            return unchanged(generationOrder > 0
                    ? StableCode.GENERATION_SUPERSEDED
                    : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (before.expireAtEpochMs() != body.expireAt()) {
            return unchanged(StableCode.STALE_SYSTEM_MUTATION);
        }
        if (before.runtime().terminal()) {
            return unchanged(before.aggregateState() == GenerationAggregateState.EXPIRED
                    ? StableCode.ALREADY_EXPIRED
                    : StableCode.TOO_LATE);
        }
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                body.messageId().bytes());
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey,
                TargetValueEnvelope.decode(
                                required(reader, ColumnFamily.META, payloadKey),
                                TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        payloadOwner.requireMessagePayload(before);
        final var owner = descriptor(reader, payloadOwner.primaryIdentity());
        owner.requirePayloadOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(before.locator().scheduleBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(
                                required(reader, ColumnFamily.ID, bindingKey),
                                TargetScheduleBinding.VALUE_TYPE)
                        .payload(),
                scope.shard());
        payloadOwner.requireInitialBinding(binding);
        binding.requireLocator(before.locator());
        binding.requireMessageSource(before.scheduleSource());
        final byte[] queueKey = TargetKeyCodec.state(before.locator().target());
        final var queue = TargetQueueState.decode(
                TargetValueEnvelope.decode(required(reader, ColumnFamily.META, queueKey), TargetQueueState.VALUE_TYPE)
                        .payload());
        before.locator().requireQueueProjection(queue);
        binding.requireQueueProjection(queue);
        if (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED
                && before.runtime().admissionsUsed() == 0) {
            return unchanged(StableCode.LANE_CLOSED_BEFORE_ADMISSION);
        }
        if (!before.runtime().attemptObligations().isEmpty()
                || (before.runtime().currentWorkKind() != CurrentSendWorkKind.TIMELINE
                        && before.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED)) {
            return unchanged(StableCode.TOO_LATE);
        }
        if (payloadOwner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            throw new IllegalStateException("live expiring Message has inactive payload accounting");
        }
        if (before.stateVersion() == Long.MAX_VALUE || before.runtime().runtimeRevision() == Long.MAX_VALUE) {
            return unchanged(StableCode.STALE_SYSTEM_MUTATION);
        }
        final var runtime = before.runtime();
        final var nextRuntime = new TargetGenerationRuntimeIndex(
                runtime.generation(),
                GenerationAggregateState.EXPIRED,
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
                after.locator(), after.stateVersion(), StableCode.ALREADY_EXPIRED, nextRuntime, stamp, lineage);
        terminal.requireOwner(payloadOwner);
        if (reader.get(ColumnFamily.TERMINAL, terminal.key()) != null) {
            throw new IllegalStateException("live expiring generation already has terminal history");
        }
        final var retained = payloadOwner.retain(stamp, (prior, next, floor) -> {
            if (floor != null
                    || !Arrays.equals(prior.canonicalBytes(), payloadOwner.canonicalBytes())
                    || !next.mutation().equals(stamp)
                    || next.phase() != TargetQuotaPayloadOwner.Phase.RETAINED
                    || !after.runtime().attemptObligations().isEmpty()) {
                throw new IllegalStateException("expiry retention changed payload ownership or terminal transition");
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
        return new Transition(before, after, StableCode.OK, releaseOrder(reader, before), edits);
    }

    private List<TargetMessageStore.OrderTransition> releaseOrder(
            final TargetStoreBackend.Reader reader, final TargetMessageRecord before) {
        if (before.locator().orderingMode() != OrderingMode.DELIVERY_TIME_FIFO) {
            return List.of();
        }
        final byte[] key = TargetKeyCodec.orderState(
                before.locator().target(), before.locator().orderingDomain());
        final var old = TargetOrderState.decode(TargetValueEnvelope.decode(
                        required(reader, ColumnFamily.META, key), TargetOrderState.VALUE_TYPE)
                .payload());
        final TargetOrderBarrier barrier;
        if (before.runtime().currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            old.requireBarrierProjection(before);
            barrier = null;
        } else {
            if (old.barrier() != null
                    && old.barrier().locator().messageId().equals(before.locator().messageId())) {
                throw new IllegalStateException("timeline Message unexpectedly owns the strict barrier");
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

    private TargetQuotaIncarnation root(
            final TargetStoreBackend.Reader reader, final TargetQuotaMutation operation) {
        final var identity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final var descriptor = descriptor(reader, identity);
        final var prior = reader.aggregate().mutation();
        if (prior == null) {
            throw new IllegalStateException("Target expiry root lacks its applied mutation frontier");
        }
        descriptor.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return descriptor;
    }

    private TargetQuotaIncarnation descriptor(
            final TargetStoreBackend.Reader reader, final TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var descriptor = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(
                                required(reader, ColumnFamily.META, key),
                                TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!descriptor.identity().equals(identity) || !Arrays.equals(descriptor.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target expiry root descriptor differs from Shard/lineage");
        }
        return descriptor;
    }

    private static byte[] systemKey(final byte[] logicalId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, logicalId);
    }

    private static byte[] positionKey(final com.nereusstream.delay.protocol.SourcePosition source) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private static void requireAbsent(final TargetStoreBackend.Reader reader, final byte[] key) {
        if (reader.get(ColumnFamily.DEDUPE, key) != null) {
            throw new IllegalStateException("first Target expiry encountered an existing result/audit record");
        }
    }

    private static byte[] required(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        final byte[] value = reader.get(family, key);
        if (value == null) {
            throw new IllegalStateException("Target expiry required record is missing from its source view");
        }
        return value;
    }

    private static Transition unchanged(final StableCode code) {
        return new Transition(null, null, code, List.of(), List.of());
    }
}
