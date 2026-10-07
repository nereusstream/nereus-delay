package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.RetryJitter;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.UncertainPolicy;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Applies initial Target UNKNOWN or authenticated current PUBLISHED outcomes in one accounted source batch. */
public final class TargetPublishOutcomeStore {
    public static final class Prepared {
        private final TargetPublishOutcomeStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                final TargetPublishOutcomeStore owner,
                final TargetStoreBackend.Prepared batch,
                final SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumTargets;
    private final int maximumDomains;
    private final TargetMessageStore messages;
    private final TargetQuotaStoreGate grants;

    public TargetPublishOutcomeStore(
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
            throw new IllegalArgumentException("Target Outcome requires a bounded Shard/lineage accounting scope");
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
            final TargetPublishOutcomeVerifier.Authority authority) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (mutation.type() != SystemMutationType.PUBLISH_OUTCOME
                || !scope.shard().equals(mutation.shardId())
                || !scope.shard().equals(source.shardId())) {
            throw new IllegalArgumentException("Target Outcome mutation/source differs from its Shard scope");
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
                (reader, business) -> sourceAccounting.assemble(reader, business),
                TargetQuotaGrantGate.Operation.OUTCOME,
                null);
        final var batch = messages.prepareAccounted(
                budget,
                reader -> {
                    requireFirstPosition(reader, source);
                    final var stamp = stamp(reader, source, mutation);
                    final var decision = TargetPublishOutcomeVerifier.decideFirstApplication(
                            scope, mutation, source, authority);
                    StableCode rejection = decision.rejection();
                    List<TargetMessageStore.Transition> transitions = List.of();
                    List<TargetMessageStore.OrderTransition> orders = List.of();
                    final var extra = new java.util.ArrayList<TargetStoreBackend.Edit>();
                    if (rejection == null) {
                        final var outcome = decision.body();
                        if (outcome.sideEffect() == 2) {
                            throw new IllegalStateException(
                                    "Target definitive Publish Outcome application is not enabled yet");
                        }
                        final var writer = AuthorIdentity.decode(mutation.authorIdentity()).asOwnerIdentity();
                        final var projection = outcome.sideEffect() == 1
                                ? published(reader, outcome, stamp, writer, decision.authorization())
                                : unknown(reader, outcome, stamp, writer, decision.authorization());
                        rejection = projection.rejection();
                        if (rejection == null) {
                            transitions = List.of(new TargetMessageStore.Transition(
                                    projection.before(), projection.after()));
                            if (projection.order() != null) {
                                orders = List.of(projection.order());
                            }
                            extra.addAll(projection.extra());
                        }
                    }
                    final var result = SystemMutationResult.from(
                            mutation,
                            rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                            rejection == null ? decision.body().stableCode() : rejection,
                            source.canonicalBytes());
                    extra.addAll(resultEdits(reader, mutation, stamp, result));
                    applied[0] = result;
                    return new TargetMessageStore.Input(transitions, orders, extra);
                },
                accounting);
        return new Prepared(this, batch, Objects.requireNonNull(applied[0], "Target Outcome result"));
    }

    public SystemMutationResult commit(
            final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("Target Outcome plan belongs to another Store wrapper");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "writeAuthority"));
        return prepared.result;
    }

    private OutcomeProjection unknown(
            final TargetStoreBackend.Reader reader,
            final PublishOutcomeBody outcome,
            final TargetQuotaMutation stamp,
            final OwnerIdentity writer,
            final TargetPublishOutcomeVerifier.Authorization authorization) {
        final byte[] budgetKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                outcome.publishAttemptId());
        final byte[] rawBudget = reader.get(ColumnFamily.META, budgetKey);
        if (rawBudget == null) {
            return OutcomeProjection.stale(StableCode.STALE_SYSTEM_MUTATION);
        }
        final var budget = TargetQuotaAttemptBudget.decodeForStore(
                budgetKey,
                TargetValueEnvelope.decode(rawBudget, TargetQuotaAttemptBudget.VALUE_TYPE).payload(),
                scope.shard());
        if (!Arrays.equals(outcome.publishAttemptId(), budget.publishAttemptId())
                || budget.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED) {
            return OutcomeProjection.stale(StableCode.STALE_SYSTEM_MUTATION);
        }
        final TargetMessageLocator locator = budget.locator();
        final byte[] messageKey = TargetKeyCodec.message(locator.messageId());
        final byte[] rawMessage = reader.get(ColumnFamily.ID, messageKey);
        if (rawMessage == null) {
            throw new IllegalStateException("open Target attempt budget lacks its Message");
        }
        final var before = TargetMessageRecord.decodeForStore(
                messageKey,
                TargetValueEnvelope.decode(rawMessage, TargetMessageRecord.VALUE_TYPE).payload(),
                scope.shard());
        if (!before.locator().equals(locator)) {
            throw new IllegalStateException("Target attempt budget points to another Message locator");
        }
        if (before.runtime().terminal()) {
            throw new IllegalStateException("Target terminal Publish Outcome application is not enabled yet");
        }
        final var obligation = before.runtime().attemptObligations().stream()
                .filter(candidate -> Arrays.equals(candidate.publishAttemptId(), outcome.publishAttemptId()))
                .findFirst()
                .orElse(null);
        if (obligation == null || obligation.ledgerState() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("ADMITTED Target attempt budget lacks its PUBLISHING obligation");
        }
        final boolean typed = outcome.retryDecision().hasFullShape();
        if (typed) {
            requireRetryContext(reader, before, budget, outcome, authorization);
        }
        final boolean admittedWriter = typed
                ? TargetPublishAdmissionBody.decode(authorization.retryContext().admission().canonicalBody())
                        .owner()
                        .equals(writer)
                : obligation.ownerEpoch() == writer.ownerEpoch();
        if (!admittedWriter && !(writer.equals(authorization.activeOwner()) && isRecoveryUnknown(outcome))) {
            return OutcomeProjection.stale(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final boolean retryRequested = typed && outcome.retryDecision().hasNextRetryAt();
        final TargetQueueState currentQueue = retryRequested ? queue(reader, before) : null;
        if (currentQueue != null) {
            before.locator().requireQueueProjection(currentQueue);
        }
        final TargetMessageRecord after = currentQueue != null
                        && currentQueue.admissionState() != TargetQueueState.AdmissionState.CLOSED
                ? before.unknownOutcomeWithRetry(outcome.publishAttemptId(), outcome.retryDecision().nextRetryAt())
                : before.unknownOutcome(outcome.publishAttemptId());
        final TargetQuotaAttemptBudget unknown = budget.unknown(budget.allocated(), stamp);
        final var budgetEdit = reader.replace(
                ColumnFamily.META,
                budgetKey,
                TargetQuotaAttemptBudget.VALUE_TYPE,
                unknown.canonicalBytes());
        final TargetMessageStore.OrderTransition order = strictOrder(reader, before, after);
        return new OutcomeProjection(
                before,
                after,
                order,
                List.of(budgetEdit),
                null);
    }

    private OutcomeProjection published(
            final TargetStoreBackend.Reader reader,
            final PublishOutcomeBody outcome,
            final TargetQuotaMutation stamp,
            final OwnerIdentity writer,
            final TargetPublishOutcomeVerifier.Authorization authorization) {
        final byte[] budgetKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                outcome.publishAttemptId());
        final byte[] raw = reader.get(ColumnFamily.META, budgetKey);
        if (raw == null) {
            return OutcomeProjection.stale(StableCode.STALE_SYSTEM_MUTATION);
        }
        final var budget = TargetQuotaAttemptBudget.decodeForStore(
                budgetKey, TargetValueEnvelope.decode(raw, TargetQuotaAttemptBudget.VALUE_TYPE).payload(),
                scope.shard());
        if (budget.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED) {
            return OutcomeProjection.stale(StableCode.STALE_SYSTEM_MUTATION);
        }
        final var context = authorization.evidenceContext();
        if (context == null) {
            throw new IllegalStateException("Target physical evidence authority/history is unavailable");
        }
        final var admission = TargetPublishAdmissionBody.decode(context.admission().canonicalBody());
        final var proof = admissionProof(reader, context.admission());
        if (!admission.locator().equals(budget.locator())
                || !Arrays.equals(context.admission().mutationHash(), budget.admissionDigest())
                || !proof.mutation().equals(budget.mutation())
                || !Arrays.equals(outcome.publishAttemptId(), admission.publishAttemptId())
                || budget.executionBytes() != admission.executionBytes()
                || !budget.commitment().equals(admission.commitment())
                || !budget.allocated().equals(admission.allocated())) {
            throw new IllegalStateException("Target evidence history differs from its exact Admission/budget");
        }
        if (!admission.owner().equals(writer)) {
            return OutcomeProjection.stale(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (!Arrays.equals(outcome.transfer(), admission.outcomeTransfer())) {
            return OutcomeProjection.stale(StableCode.STALE_SYSTEM_MUTATION);
        }
        final var evidence = PublishEvidence.decode(outcome.evidence());
        evidence.requireOrdinaryTargetPublishedBinding(admission.publication());
        final byte[] messageKey = TargetKeyCodec.message(budget.locator().messageId());
        final var before = TargetMessageRecord.decodeForStore(
                messageKey, TargetValueEnvelope.decode(
                                required(reader, ColumnFamily.ID, messageKey), TargetMessageRecord.VALUE_TYPE)
                        .payload(),
                scope.shard());
        if (!before.locator().equals(budget.locator())) {
            throw new IllegalStateException("Target evidence budget points to another Message locator");
        }
        if (before.runtime().terminal()
                || before.runtime().currentWorkKind() != CurrentSendWorkKind.PUBLISHING
                || !Arrays.equals(before.runtime().publishAttemptId(), outcome.publishAttemptId())) {
            throw new IllegalStateException("Target terminal/historical PUBLISHED source application is not enabled");
        }
        if (before.runtime().admissionsUsed() != admission.attemptNo()
                || !before.runtime().attemptObligations().contains(admission.obligation())) {
            throw new IllegalStateException("Target current PUBLISHING projection differs from its Admission proof");
        }
        requireRetryContext(reader, before, budget, outcome, authorization);
        if (outcome.retryDecision().cause() != StableCode.OK) {
            throw new IllegalArgumentException("Target PUBLISHED retry decision changes its successful cause");
        }
        context.authority().requireAuthenticated(
                admission.publication(), evidence, proof.mutation().source(), stamp.source());
        final var after = before.publishedOutcome(outcome.publishAttemptId());
        final var terminal = new TargetTerminalGenerationRecord(
                after.locator(), after.stateVersion(), StableCode.OK, after.runtime(), stamp, lineage);
        if (reader.get(ColumnFamily.TERMINAL, terminal.key()) != null) {
            throw new IllegalStateException("current Target publication already has terminal history");
        }
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                before.locator().messageId().bytes());
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey, TargetValueEnvelope.decode(
                                required(reader, ColumnFamily.META, payloadKey), TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(), scope.tenantScope());
        payloadOwner.requireMessagePayload(before);
        terminal.requireOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (payloadOwner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            throw new IllegalStateException("current publishing Target Message lacks active payload ownership");
        }
        final var extra = new java.util.ArrayList<TargetStoreBackend.Edit>();
        if (after.runtime().attemptObligations().isEmpty()) {
            final var retained = payloadOwner.retain(stamp, (prior, next, floor) -> {
                if (floor != null || !Arrays.equals(prior.canonicalBytes(), payloadOwner.canonicalBytes())
                        || next.phase() != TargetQuotaPayloadOwner.Phase.RETAINED
                        || !next.mutation().equals(stamp)) {
                    throw new IllegalStateException("Target success changed its frozen payload retention");
                }
                next.requireMessagePayload(after);
            });
            extra.add(reader.replace(
                    ColumnFamily.META, retained.key(), TargetQuotaPayloadOwner.VALUE_TYPE, retained.canonicalBytes()));
        }
        final var resolved = budget.resolve(evidence.verificationStatus(), budget.allocated(), stamp);
        extra.add(reader.replace(
                ColumnFamily.META, budgetKey, TargetQuotaAttemptBudget.VALUE_TYPE, resolved.canonicalBytes()));
        extra.add(reader.replace(
                ColumnFamily.TERMINAL, terminal.key(), TargetTerminalGenerationRecord.VALUE_TYPE,
                terminal.canonicalBytes()));
        return new OutcomeProjection(before, after, strictOrder(reader, before, after), extra, null);
    }

    private static byte[] required(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("Target publication lacks a retained primary record");
        }
        return raw;
    }

    private void requireRetryContext(
            final TargetStoreBackend.Reader reader,
            final TargetMessageRecord message,
            final TargetQuotaAttemptBudget budget,
            final PublishOutcomeBody outcome,
            final TargetPublishOutcomeVerifier.Authorization authorization) {
        final var context = authorization.retryContext();
        if (context == null) {
            throw new IllegalStateException("Target retry source-history context is unavailable");
        }
        final var current = TargetPublishAdmissionBody.decode(context.admission().canonicalBody());
        final var first = TargetPublishAdmissionBody.decode(context.firstAdmission().canonicalBody());
        final var currentResult = admissionProof(reader, context.admission());
        final var firstResult = admissionProof(reader, context.firstAdmission());
        firstResult.mutation().requireAtOrBefore(currentResult.mutation());
        if (!current.locator().equals(budget.locator())
                || !first.locator().equals(budget.locator())
                || first.attemptNo() != 1
                || current.attemptNo() != message.runtime().admissionsUsed()
                || !Arrays.equals(context.admission().mutationHash(), budget.admissionDigest())
                || !currentResult.mutation().equals(budget.mutation())
                || !message.runtime().attemptObligations().contains(current.obligation())) {
            throw new IllegalStateException("Target retry history differs from the exact applied Admission/budget");
        }
        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
        final byte[] rawBinding = reader.get(ColumnFamily.ID, bindingKey);
        if (rawBinding == null) {
            throw new IllegalStateException("Target retry lacks its retained Schedule binding");
        }
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(rawBinding, TargetScheduleBinding.VALUE_TYPE).payload(),
                scope.shard());
        binding.requireLocator(message.locator());
        final var policy = context.policy();
        if (!binding.intent().retryPolicy().equals(policy.ref())) {
            throw new IllegalStateException("Target retry policy differs from the immutable Schedule binding");
        }
        requireRetryDecision(message, current, first, policy, outcome);
    }

    static void requireRetryDecision(
            final TargetMessageRecord message,
            final TargetPublishAdmissionBody current,
            final TargetPublishAdmissionBody first,
            final RetryPolicySemantic policy,
            final PublishOutcomeBody outcome) {
        final var retry = outcome.retryDecision();
        final long firstAt = first.decisionTime().latestEpochMs();
        final long deadline;
        try {
            deadline = Math.min(message.expireAtEpochMs(), Math.addExact(firstAt, policy.maxRetryDurationMs()));
        } catch (ArithmeticException invalidWindow) {
            throw new IllegalArgumentException("Target retry deadline overflows its admitted window", invalidWindow);
        }
        if (!retry.policy().matches(policy)
                || retry.retryDomain() != RetryJitter.MESSAGE_PUBLISH
                || retry.completedAttemptNo() != Integer.toUnsignedLong(current.attemptNo())
                || retry.firstAttemptAt() != firstAt
                || firstAt >= message.expireAtEpochMs()
                || retry.retryDeadline() != deadline) {
            throw new IllegalArgumentException("Target RetryDecision differs from its admitted attempt/window/policy");
        }
        if (retry.hasNextRetryAt()) {
            final long next;
            try {
                next = Math.addExact(
                        outcome.observedAt().latestEpochMs(),
                        RetryJitter.delayMs(
                                RetryJitter.MESSAGE_PUBLISH,
                                message.locator().messageId(),
                                Integer.toUnsignedLong(message.locator().generation()),
                                retry.completedAttemptNo(),
                                policy.retryBackoffCap(retry.completedAttemptNo())));
            } catch (ArithmeticException invalidRetryTime) {
                throw new IllegalArgumentException("Target retry jitter time overflows", invalidRetryTime);
            }
            if (retry.kind() != 2
                    || message.locator().orderingMode() != OrderingMode.BEST_EFFORT
                    || policy.uncertainPolicy() != UncertainPolicy.BOUNDED_RETRY_POSSIBLE_DUPLICATE
                    || retry.nextRetryAt() != next
                    || next > deadline
                    || Math.max(message.deliverAtEpochMs(), next) >= message.expireAtEpochMs()
                    || message.runtime().uncertainRetryAdmissionsUsed() >= policy.maxUncertainRetries()
                    || message.runtime().admissionsUsed() >= policy.maxPublishAdmissions()) {
                throw new IllegalArgumentException("Target uncertain retry differs from its policy/jitter/budget");
            }
        }
    }

    private TargetResultRecord admissionProof(final TargetStoreBackend.Reader reader, final SystemMutation image) {
        final byte[] raw = reader.get(ColumnFamily.DEDUPE, systemResultKey(image));
        if (raw == null) {
            throw new IllegalStateException("Target retry lacks its retained Admission first result");
        }
        final var record = TargetResultRecord.decode(
                TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload());
        record.requireOwner(root(reader));
        record.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final var result = SystemMutationResult.decode(record.typedPayload());
        if (record.kind() != TargetResultRecord.Kind.SYSTEM
                || record.allocation() != null
                || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || result.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !Arrays.equals(record.logicalId(), image.systemMutationId())
                || !Arrays.equals(result.mutationId(), image.systemMutationId())
                || !Arrays.equals(result.mutationHash(), image.mutationHash())
                || result.retryUntilEpochMs() != image.retryUntilEpochMs()
                || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                || !Arrays.equals(record.mutation().mutationDigest(), Bytes.sha256(image.canonicalEnvelope()))
                || !Arrays.equals(result.appliedSourcePosition(), record.mutation().source().canonicalBytes())) {
            throw new IllegalStateException("Target retry Admission image differs from its authenticated first result");
        }
        return record;
    }

    private static boolean isRecoveryUnknown(final PublishOutcomeBody outcome) {
        return outcome.disposition() == 4
                && outcome.stableCode() == StableCode.RECOVERY_FIRST_SEND_UNCERTAIN
                && outcome.retryDecision().hasFullShape()
                && outcome.retryDecision().kind() == 5
                && !outcome.retryDecision().hasNextRetryAt();
    }

    private TargetMessageStore.OrderTransition strictOrder(
            final TargetStoreBackend.Reader reader,
            final TargetMessageRecord before,
            final TargetMessageRecord after) {
        if (before.locator().orderingMode() != OrderingMode.DELIVERY_TIME_FIFO) {
            return null;
        }
        final byte[] key = TargetKeyCodec.orderState(before.locator().target(), before.locator().orderingDomain());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("strict Target Outcome lacks its durable order state");
        }
        final var state = TargetOrderState.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetOrderState.VALUE_TYPE).payload(),
                scope.shard(),
                queue(reader, before));
        state.requireBarrierProjection(before);
        return new TargetMessageStore.OrderTransition(state,
                after.aggregateState() == GenerationAggregateState.PUBLISHED
                        ? state.afterPublishedOutcome(before, after) : state.afterUnknownOutcome(before, after));
    }

    private TargetQueueState queue(final TargetStoreBackend.Reader reader, final TargetMessageRecord message) {
        final byte[] key = TargetKeyCodec.state(message.locator().target());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("strict Target Outcome lacks its queue");
        }
        return TargetQueueState.decode(TargetValueEnvelope.decode(raw, TargetQueueState.VALUE_TYPE).payload());
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
                throw new IllegalStateException("Target Outcome result changed its actual source/absence");
            }
            if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                if (first != null
                        || !Arrays.equals(record.key(), systemResultKey(mutation))
                        || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                    throw new IllegalStateException("Target Outcome first result differs from its decision");
                }
            } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                if (first == null
                        || !Arrays.equals(first.typedPayload(), outcome.encode())
                        || !Arrays.equals(record.key(), positionResultKey(stamp.source()))) {
                    throw new IllegalStateException("Target Outcome POSITION differs from its first result");
                }
                record.requireFirst(first);
            } else {
                throw new IllegalStateException("unexpected Target Outcome result kind");
            }
        };
        final var first = TargetResultRecord.system(root, outcome, stamp, null, authority);
        final var position = TargetResultRecord.position(root, first, stamp, authority);
        return List.of(
                reader.replace(ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()),
                reader.replace(
                        ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
    }

    private TargetQuotaIncarnation root(final TargetStoreBackend.Reader reader) {
        final var identity = new com.nereusstream.delay.protocol.TargetQuotaIdentity(
                com.nereusstream.delay.protocol.TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("Target Outcome quota incarnation is missing");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE).payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("Target Outcome quota root belongs to another lineage");
        }
        return root;
    }

    private static void requireFirstPosition(
            final TargetStoreBackend.Reader reader, final com.nereusstream.delay.protocol.SourcePosition source) {
        if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
            throw new IllegalStateException("first Target Outcome requires an established earlier source frontier");
        }
    }

    private static TargetQuotaMutation stamp(
            final TargetStoreBackend.Reader reader,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final SystemMutation mutation) {
        final var stamp = new TargetQuotaMutation(
                TargetQuotaMutation.increment(reader.sourceSequence()),
                source,
                Bytes.sha256(mutation.canonicalEnvelope()));
        if (reader.get(ColumnFamily.DEDUPE, systemResultKey(mutation)) != null
                || reader.get(ColumnFamily.DEDUPE, positionResultKey(source)) != null) {
            throw new IllegalStateException("first Target Outcome has an existing system or position result");
        }
        return stamp;
    }

    private static byte[] systemResultKey(final SystemMutation mutation) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, mutation.systemMutationId());
    }

    private static byte[] positionResultKey(final com.nereusstream.delay.protocol.SourcePosition source) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private record OutcomeProjection(
            TargetMessageRecord before,
            TargetMessageRecord after,
            TargetMessageStore.OrderTransition order,
            List<TargetStoreBackend.Edit> extra,
            StableCode rejection) {
        private OutcomeProjection {
            extra = List.copyOf(extra);
        }

        private static OutcomeProjection stale(final StableCode rejection) {
            return new OutcomeProjection(null, null, null, List.of(), rejection);
        }
    }
}
