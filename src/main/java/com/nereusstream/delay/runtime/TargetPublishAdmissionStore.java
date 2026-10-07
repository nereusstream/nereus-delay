package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
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

/** Source-ordered Target Admission: Message, strict barrier, attempt budget, result and position commit together. */
public final class TargetPublishAdmissionStore {
    public static final class Prepared {
        private final TargetPublishAdmissionStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetPublishAdmissionStore owner,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private static final class StaleAdmission extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumTargets;
    private final int maximumDomains;
    private final TargetMessageStore messages;
    private final TargetQuotaStoreGate grants;

    public TargetPublishAdmissionStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] recoveryLineage,
            final int maximumCounters,
            final int maximumTargets,
            final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(recoveryLineage, 16, "recoveryLineage");
        if (scope.target() != null
                || maximumCounters < 2
                || maximumTargets < 1
                || maximumDomains < 1
                || maximumDomains > 64
                || Arrays.equals(recoveryLineage, new byte[16])) {
            throw new IllegalArgumentException("Target Admission requires a bounded Shard/lineage accounting scope");
        }
        lineage = Bytes.copy(recoveryLineage);
        this.maximumCounters = maximumCounters;
        this.maximumTargets = maximumTargets;
        this.maximumDomains = maximumDomains;
        messages = new TargetMessageStore(backend, 1, 1, maximumDomains);
        grants = new TargetQuotaStoreGate(scope, lineage, maximumTargets);
    }

    /** Immutable first-result replay must be routed before this method. */
    public Prepared prepareFirst(
            final BoundedReadBudget budget,
            final SystemMutation mutation,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final TargetPublishAdmissionVerifier.Authority authority) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (mutation.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !scope.shard().equals(mutation.shardId())
                || !scope.shard().equals(source.shardId())) {
            throw new IllegalArgumentException("Target Admission mutation/source differs from its Shard scope");
        }
        final SystemMutationResult[] applied = new SystemMutationResult[1];
        final var sourceAccounting = new TargetSourceAccounting(
                scope,
                lineage,
                source,
                Bytes.sha256(mutation.canonicalEnvelope()),
                maximumCounters,
                maximumTargets,
                maximumDomains);
        final TargetMessageStore.AccountingAssembler accounting = grants.wrap(
                (reader, business) -> {
                    final var mutationPlan = sourceAccounting.assemble(reader, business);
                    if (applied[0].applyStatus() == ApplyStatus.APPLIED
                            && mutationPlan.quota().changes().isEmpty()) {
                        throw new IllegalStateException("accepted Target Admission has no Target accounting change");
                    }
                    return mutationPlan;
                },
                TargetQuotaGrantGate.Operation.ADMISSION,
                null);
        final var batch = messages.prepareAccounted(
                budget,
                reader -> {
                    requireFirstPosition(reader, source);
                    final var stamp = stamp(reader, source, mutation);
                    final var decision = TargetPublishAdmissionVerifier.decideFirstApplication(
                            scope, mutation, source, authority);
                    StableCode rejection = decision.rejection();
                    List<TargetMessageStore.Transition> transitions = List.of();
                    List<TargetMessageStore.OrderTransition> orders = List.of();
                    final var extra = new ArrayList<TargetStoreBackend.Edit>();
                    if (rejection == null) {
                        try {
                            final var projection = admission(reader, decision, mutation, source, stamp);
                            transitions = List.of(new TargetMessageStore.Transition(
                                    projection.before(), projection.after()));
                            orders = projection.order() == null ? List.of() : List.of(projection.order());
                            extra.addAll(projection.extra());
                        } catch (StaleAdmission stale) {
                            rejection = StableCode.STALE_SYSTEM_MUTATION;
                        }
                    }
                    final var result = SystemMutationResult.from(
                            mutation,
                            rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                            rejection == null ? StableCode.OK : rejection,
                            source.canonicalBytes());
                    extra.addAll(resultEdits(reader, mutation, stamp, result));
                    applied[0] = result;
                    return new TargetMessageStore.Input(transitions, orders, extra);
                },
                accounting);
        return new Prepared(this, batch, Objects.requireNonNull(applied[0], "Target Admission result"));
    }

    public SystemMutationResult commit(
            final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("Target Admission plan belongs to another Store wrapper");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "writeAuthority"));
        return prepared.result;
    }

    private AdmissionProjection admission(
            final TargetStoreBackend.Reader reader,
            final TargetPublishAdmissionVerifier.Decision decision,
            final SystemMutation mutation,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final TargetQuotaMutation stamp) {
        final var body = decision.body();
        final TargetMessageLocator locator = body.locator();
        final byte[] messageKey = TargetKeyCodec.message(locator.messageId());
        final byte[] rawMessage = reader.get(ColumnFamily.ID, messageKey);
        if (rawMessage == null) {
            throw new StaleAdmission();
        }
        final var before = TargetMessageRecord.decodeForStore(
                messageKey,
                TargetValueEnvelope.decode(rawMessage, TargetMessageRecord.VALUE_TYPE).payload(),
                scope.shard());
        if (!before.locator().equals(locator)
                || before.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED
                || !Arrays.equals(before.runtime().claimId(), body.claimId())) {
            throw new StaleAdmission();
        }
        final byte[] claimKey = TargetClaimRecord.key(body.claimId());
        final byte[] rawClaim = reader.get(ColumnFamily.INFLIGHT, claimKey);
        if (rawClaim == null) {
            throw new StaleAdmission();
        }
        final var claim = TargetClaimRecord.decode(
                TargetValueEnvelope.decode(rawClaim, TargetClaimRecord.VALUE_TYPE).payload());
        claim.requireStored(claimKey);
        if (!claim.owner().equals(body.owner())
                || !Arrays.equals(claim.storeIncarnation(), body.storeIncarnation())
                || !claim.work().locator().equals(locator)
                || claim.work().candidateAttemptNo() != body.attemptNo()
                || claim.executionBytes() != body.executionBytes()) {
            throw new StaleAdmission();
        }
        try {
            claim.requireCurrent(before);
        } catch (IllegalStateException stale) {
            throw new StaleAdmission();
        }
        TargetClaimStore.current(reader, before);
        if (body.publication() != null) {
            if (!Arrays.equals(claim.canonicalBytes(), body.claimProof().canonicalBytes())) {
                throw new StaleAdmission();
            }
            final var publication = body.publication();
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(locator.scheduleBindingDigest());
            final byte[] bindingRaw = reader.get(ColumnFamily.ID, bindingKey);
            final byte[] identityKey = TargetKeyCodec.identity(locator.target());
            final byte[] identityRaw = reader.get(ColumnFamily.META, identityKey);
            final byte[] channelKey = publication.channel().encodedKey();
            final byte[] channelRaw = reader.get(ColumnFamily.META, channelKey);
            if (bindingRaw == null || identityRaw == null || channelRaw == null) {
                throw new IllegalStateException("materialized Admission lacks its applied binding/Target/channel");
            }
            final var binding = com.nereusstream.delay.protocol.TargetScheduleBinding.decodeForStore(
                    bindingKey,
                    TargetValueEnvelope.decode(bindingRaw,
                                    com.nereusstream.delay.protocol.TargetScheduleBinding.VALUE_TYPE)
                            .payload(),
                    scope.shard());
            final var identity = com.nereusstream.delay.protocol.CanonicalTargetPartition.decodeForStore(
                    identityKey,
                    TargetValueEnvelope.decode(identityRaw,
                                    com.nereusstream.delay.protocol.CanonicalTargetPartition.VALUE_TYPE)
                            .payload());
            final var channel = com.nereusstream.delay.protocol.TargetChannelIdentity.decodeForStore(
                    channelKey,
                    TargetValueEnvelope.decode(channelRaw,
                                    com.nereusstream.delay.protocol.TargetChannelIdentity.VALUE_TYPE)
                            .payload(),
                    scope.shard());
            if (!Arrays.equals(identity.canonicalBytes(), publication.physical().canonicalBytes())) {
                throw new StaleAdmission();
            }
            channel.requireExactFrozenIdentity(publication.channel());
            channel.requireQueueProjection(queue(reader, before));
            publication.requireClaim(claim, before, binding);
            if (body.decisionTime().latestEpochMs() >= channel.credentialLease().validUntilEpochMs()
                    || body.decisionTime().latestEpochMs() >= claim.deadlineEpochMs()) {
                throw new StaleAdmission();
            }
        }
        final var time = body.decisionTime();
        final long requiredEarliest = claim.work().retryEligibilityAtEpochMs();
        final long notAfter = before.expireAtEpochMs();
        final long brokerBound;
        try {
            brokerBound = Math.addExact(
                    source.brokerPersistenceTimeEpochMs(),
                    decision.authorization().maximumBrokerTimestampDivergenceMs());
        } catch (ArithmeticException overflow) {
            throw new StaleAdmission();
        }
        if (body.publication() != null
                && brokerBound >= Math.min(
                        claim.deadlineEpochMs(), body.publication().channel().credentialLease().validUntilEpochMs())) {
            throw new StaleAdmission();
        }
        if (time.earliestEpochMs() < requiredEarliest
                || time.latestEpochMs() >= notAfter
                || brokerBound >= notAfter
                || (claim.work().nativeCandidate() && time.latestEpochMs() >= before.deliverAtEpochMs())) {
            throw new StaleAdmission();
        }
        final var root = root(reader);
        final var targetIdentity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                scope.shard(),
                locator.accountingIncarnation(),
                locator.target(),
                null);
        final var target = descriptor(reader, targetIdentity);
        final var attemptBudget = TargetQuotaAttemptBudget.admit(
                locator,
                scope.tenantScope(),
                body.publishAttemptId(),
                mutation.mutationHash(),
                target.accounting(),
                body.executionBytes(),
                body.commitment(),
                body.allocated(),
                stamp,
                lineage);
        if (reader.get(ColumnFamily.META, attemptBudget.key()) != null) {
            throw new StaleAdmission();
        }
        final var obligation = body.obligation();
        final var admitted = claim.admitted(before, obligation, body.attemptNo());
        final TargetStoreBackend.Edit claimEdit = reader.replace(
                ColumnFamily.INFLIGHT, claimKey, TargetClaimRecord.VALUE_TYPE, null);
        final TargetStoreBackend.Edit chargeEdit = reader.replace(
                ColumnFamily.META,
                claim.chargeKey(),
                com.nereusstream.delay.protocol.TargetQuotaClaimCharge.VALUE_TYPE,
                null);
        final TargetStoreBackend.Edit budgetEdit = reader.replace(
                ColumnFamily.META,
                attemptBudget.key(),
                TargetQuotaAttemptBudget.VALUE_TYPE,
                attemptBudget.canonicalBytes());
        final List<TargetMessageStore.OrderTransition> order = strictOrder(reader, before, admitted);
        return new AdmissionProjection(
                before,
                admitted,
                order.isEmpty() ? null : order.getFirst(),
                List.of(claimEdit, chargeEdit, budgetEdit));
    }

    private List<TargetMessageStore.OrderTransition> strictOrder(
            final TargetStoreBackend.Reader reader,
            final TargetMessageRecord before,
            final TargetMessageRecord admitted) {
        if (before.locator().orderingMode() != com.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO) {
            return List.of();
        }
        final byte[] key = TargetKeyCodec.orderState(before.locator().target(), before.locator().orderingDomain());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("strict Target Admission lacks its durable order state");
        }
        final var state = TargetOrderState.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetOrderState.VALUE_TYPE).payload(),
                scope.shard(),
                queue(reader, before));
        try {
            return List.of(new TargetMessageStore.OrderTransition(state, state.afterAdmission(before, admitted)));
        } catch (IllegalArgumentException stale) {
            throw new StaleAdmission();
        }
    }

    private List<TargetStoreBackend.Edit> resultEdits(
            final TargetStoreBackend.Reader reader,
            final SystemMutation mutation,
            final TargetQuotaMutation stamp,
            final SystemMutationResult outcome) {
        final var root = root(reader);
        final var authority = (TargetResultRecord.CreationAuthority) (record, first) -> {
            record.requireOwner(root);
            if (!record.mutation().equals(stamp)
                    || record.allocation() != null
                    || reader.get(ColumnFamily.DEDUPE, record.key()) != null) {
                throw new IllegalStateException("Target Admission result changed its actual source/absence");
            }
            if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                if (first != null
                        || !Arrays.equals(record.key(), systemResultKey(mutation))
                        || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                    throw new IllegalStateException("Target Admission first result differs from its decision");
                }
            } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                if (first == null
                        || !Arrays.equals(first.typedPayload(), outcome.encode())
                        || !Arrays.equals(record.key(), positionResultKey(stamp.source()))) {
                    throw new IllegalStateException("Target Admission POSITION differs from its first result");
                }
                record.requireFirst(first);
            } else {
                throw new IllegalStateException("unexpected Target Admission result kind");
            }
        };
        final var first = TargetResultRecord.system(root, outcome, stamp, null, authority);
        final var position = TargetResultRecord.position(root, first, stamp, authority);
        return List.of(
                reader.replace(
                        ColumnFamily.DEDUPE,
                        first.key(),
                        TargetResultRecord.VALUE_TYPE,
                        first.canonicalBytes()),
                reader.replace(
                        ColumnFamily.DEDUPE,
                        position.key(),
                        TargetResultRecord.VALUE_TYPE,
                        position.canonicalBytes()));
    }

    private static void requireFirstPosition(
            final TargetStoreBackend.Reader reader, final com.nereusstream.delay.protocol.SourcePosition source) {
        if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
            throw new IllegalStateException("first Target Admission requires an established earlier source frontier");
        }
    }

    private TargetQuotaMutation stamp(
            final TargetStoreBackend.Reader reader,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final SystemMutation mutation) {
        final var stamp = new TargetQuotaMutation(
                TargetQuotaMutation.increment(reader.sourceSequence()),
                source,
                Bytes.sha256(mutation.canonicalEnvelope()));
        final byte[] firstKey = systemResultKey(mutation);
        final byte[] positionKey = positionResultKey(source);
        if (reader.get(ColumnFamily.DEDUPE, firstKey) != null || reader.get(ColumnFamily.DEDUPE, positionKey) != null) {
            throw new IllegalStateException("first Target Admission has an existing system or position result");
        }
        return stamp;
    }

    private TargetQuotaIncarnation root(final TargetStoreBackend.Reader reader) {
        final var identity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        return descriptor(reader, identity);
    }

    private TargetQuotaIncarnation descriptor(
            final TargetStoreBackend.Reader reader, final TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("Target Admission quota incarnation is missing");
        }
        final var descriptor = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE).payload(),
                scope.shard(),
                scope.tenantScope());
        if (!descriptor.identity().equals(identity) || !Arrays.equals(descriptor.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target Admission quota descriptor identity/lineage mismatch");
        }
        return descriptor;
    }

    private TargetQueueState queue(final TargetStoreBackend.Reader reader, final TargetMessageRecord message) {
        final byte[] key = TargetKeyCodec.state(message.locator().target());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("strict Target Admission lacks its queue");
        }
        return TargetQueueState.decode(
                TargetValueEnvelope.decode(raw, TargetQueueState.VALUE_TYPE).payload());
    }

    private static byte[] systemResultKey(final SystemMutation mutation) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, mutation.systemMutationId());
    }

    private static byte[] positionResultKey(final com.nereusstream.delay.protocol.SourcePosition source) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                source.canonicalBytes());
    }

    private record AdmissionProjection(
            TargetMessageRecord before,
            TargetMessageRecord after,
            TargetMessageStore.OrderTransition order,
            List<TargetStoreBackend.Edit> extra) {}
}
