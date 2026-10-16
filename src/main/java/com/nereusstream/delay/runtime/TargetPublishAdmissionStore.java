package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
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
    /** Actual bounded Store read, not a caller-created assertion that Admission was applied. */
    public static final class Applied {
        private final SystemMutation image;
        private final TargetPublishAdmissionBody body;
        private final SourcePosition source;
        private final TargetMessageRecord message;
        private final com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping;

        private Applied(SystemMutation image, TargetPublishAdmissionBody body,
                SourcePosition source, TargetMessageRecord message,
                com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping) {
            this.image = image;
            this.body = body;
            this.source = source;
            this.message = message;
            this.kafkaMapping = kafkaMapping;
        }

        public SystemMutation image() { return image; }
        public TargetPublishAdmissionBody body() { return body; }
        public SourcePosition source() { return source; }
        public TargetMessageRecord message() { return message; }
        public com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping() { return kafkaMapping; }

        public void requireSame(final Applied current) {
            if (!Arrays.equals(image.canonicalEnvelope(), current.image.canonicalEnvelope())
                    || !Arrays.equals(source.canonicalBytes(), current.source.canonicalBytes())
                    || !Arrays.equals(message.canonicalBytes(), current.message.canonicalBytes())
                    || (kafkaMapping == null) != (current.kafkaMapping == null)
                    || kafkaMapping != null && !Arrays.equals(kafkaMapping.canonicalBytes(), current.kafkaMapping.canonicalBytes())) {
                throw new IllegalStateException("Target send snapshot changed after its applied Admission read");
            }
        }
    }
    private record Facts(SystemMutation image, TargetPublishAdmissionBody body,
            SourcePosition source, RetainedGeneration generation,
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping) {}

    /** Retained admitted facts; the current Message can be newer than the admitted terminal generation. */
    public static final class Recovery {
        private final Facts retained;

        private Recovery(Facts retained) { this.retained = retained; }

        public SystemMutation image() { return retained.image(); }
        public TargetPublishAdmissionBody body() { return retained.body(); }
        public SourcePosition source() { return retained.source(); }
        public com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping() { return retained.kafkaMapping(); }
        public TargetMessageRecord message() { return retained.generation().current(); }
        public TargetGenerationRuntimeIndex runtime() { return retained.generation().runtime(); }
        public java.util.Optional<TargetTerminalGenerationRecord> terminal() {
            return java.util.Optional.ofNullable(retained.generation().terminal());
        }
    }

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

    public static final class StaleAdmission extends RuntimeException {
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

    /** Verifies current PUBLISHING work, Store identity, full signed image and retained first/budget/source joins. */
    public Applied readApplied(
            final BoundedReadBudget budget, final SystemMutation image,
            final TargetStoreBackend.ReadAuthority authority) {
        final var facts = readFacts(budget, image, authority, true);
        return new Applied(facts.image(), facts.body(), facts.source(), facts.generation().current(), facts.kafkaMapping());
    }

    /**
     * Read-only append preflight for the exact reversible Claim. This does not authenticate the prospective
     * Source position, apply Admission, allocate an attempt, or grant SDK submission authority.
     */
    public void requireCurrentSubmission(
            final BoundedReadBudget budget, final SystemMutation image,
            final TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(image, "image");
        if (image.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION || !image.shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("Target Admission submission belongs to another operation/Shard");
        }
        final var body = TargetPublishAdmissionBody.decode(image.canonicalBody());
        final var author = com.nereusstream.delay.protocol.AuthorIdentity.decode(image.authorIdentity());
        if (!body.shard().equals(scope.shard()) || body.retryUntilEpochMs() != image.retryUntilEpochMs()
                || !Arrays.equals(body.publishAttemptId(), image.logicalOperationIdentity())
                || !body.owner().equals(author.asOwnerIdentity())) {
            throw new IllegalArgumentException("Target Admission submission changes its signed body identity");
        }
        backend.guardedRead(Objects.requireNonNull(budget, "budget"), reader -> {
            if (!Arrays.equals(body.storeIncarnation(), reader.metadata().storeIncarnation())) {
                throw new IllegalStateException("Target Admission submission belongs to another Store incarnation");
            }
            currentClaim(reader, body);
            reader.requireWithinElapsedBudget();
            return Boolean.TRUE;
        }, Objects.requireNonNull(authority, "readAuthority"));
    }

    /** Materializes exact committed Claim and Source-applied channel facts; this only prepares an unsigned request. */
    public TargetPublishAdmissionBody prepareBody(
            final BoundedReadBudget budget, final TargetClaimRecord expected,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final byte[] artifacts, final long retryUntilEpochMs,
            final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final TargetStoreBackend.ReadAuthority authority) {
        return prepareBody(budget, expected, channel, capability, artifacts, retryUntilEpochMs, commitment,
                allocated, time, policies, null, authority);
    }

    public TargetPublishAdmissionBody prepareBody(
            final BoundedReadBudget budget, final TargetClaimRecord expected,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final byte[] artifacts, final long retryUntilEpochMs,
            final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final com.nereusstream.delay.protocol.TargetKafkaReceiptBinding kafkaReceipt,
            final TargetStoreBackend.ReadAuthority authority) {
        if ((channel.context().kind() == com.nereusstream.delay.protocol.ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT)
                != (kafkaReceipt != null)) { throw new StaleAdmission(); }
        Objects.requireNonNull(expected, "expectedClaim");
        Objects.requireNonNull(channel, "channel");
        return backend.guardedRead(budget, reader -> {
            final var locator = expected.work().locator();
            final byte[] messageKey = TargetKeyCodec.message(locator.messageId());
            final byte[] raw = reader.get(ColumnFamily.ID, messageKey);
            if (raw == null) { throw new StaleAdmission(); }
            final var message = TargetMessageRecord.decodeForStore(messageKey,
                    TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(), scope.shard());
            final var claim = TargetClaimStore.current(reader, message);
            if (!Arrays.equals(claim.canonicalBytes(), expected.canonicalBytes())
                    || !Arrays.equals(claim.storeIncarnation(), reader.metadata().storeIncarnation())) {
                throw new StaleAdmission();
            }
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(locator.scheduleBindingDigest());
            final byte[] bindingRaw = reader.get(ColumnFamily.ID, bindingKey);
            final byte[] identityKey = TargetKeyCodec.identity(locator.target());
            final byte[] identityRaw = reader.get(ColumnFamily.META, identityKey);
            if (bindingRaw == null || identityRaw == null) {
                throw new IllegalStateException("Claim publication lacks its applied binding/Target identity");
            }
            final var binding = com.nereusstream.delay.protocol.TargetScheduleBinding.decodeForStore(bindingKey,
                    TargetValueEnvelope.decode(bindingRaw,
                            com.nereusstream.delay.protocol.TargetScheduleBinding.VALUE_TYPE).payload(), scope.shard());
            final var physical = com.nereusstream.delay.protocol.CanonicalTargetPartition.decodeForStore(identityKey,
                    TargetValueEnvelope.decode(identityRaw,
                            com.nereusstream.delay.protocol.CanonicalTargetPartition.VALUE_TYPE).payload());
            final int attemptNo = claim.work().candidateAttemptNo();
            final byte[] attempt = SystemMutation.computePublishAttemptLogicalIdentity(claim.claimId(),
                    locator.messageId(), Integer.toUnsignedLong(locator.generation()), Integer.toUnsignedLong(attemptNo));
            final var obligation = new AttemptObligationRef(attempt, locator.generation(), AttemptLedgerState.PUBLISHING,
                    com.nereusstream.delay.store.KeyCodec.inflight((byte) 2, claim.owner().ownerEpoch(), attempt));
            final TargetPublishAdmissionBody body;
            if (claim.selected().nativeCandidate()) {
                if (claim.policyHead() == null) {
                    throw new IllegalStateException("legacy Native Claim has no frozen policy head; revoke before reselecting");
                }
                final var nativeScope = TargetNativePolicyStoreAuthority.requireScope(
                        reader, scope, claim.policyHead().scopeDigest());
                nativeScope.requireQueue(queue(reader, message));
                final var publication = Objects.requireNonNull(policies, "nativePolicies")
                        .requireCurrent(nativeScope.digest());
                final long actionAt = message.deliverAtEpochMs() - publication.head().snapshot().effectiveLeadMs();
                final com.nereusstream.delay.protocol.TargetNativePolicySnapshot snapshot;
                try {
                    snapshot = com.nereusstream.delay.scheduler.TargetNativePolicyChecks.freezeCurrent(
                            nativeScope, claim.policyHead(), message.deliverAtEpochMs(), actionAt, policies,
                            TargetNativePolicyStoreAuthority.inView(reader, scope, lineage), reader.source(), time);
                } catch (IllegalArgumentException ineligible) {
                    throw new StaleAdmission();
                }
                final var frozen = com.nereusstream.delay.protocol.TargetNativePublicationBinding.fromClaim(
                        claim, message, binding, physical, channel, capability, artifacts,
                        nativeScope, snapshot, claim.policyHead(), actionAt);
                body = TargetPublishAdmissionBody.nativeMaterialized(scope.shard(), retryUntilEpochMs, claim.owner(),
                        claim.storeIncarnation(), claim.claimId(), locator, attemptNo, attempt, obligation,
                        claim.executionBytes(), commitment, allocated, time, frozen, claim);
            } else {
                final var frozen = com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding.fromClaim(
                        claim, message, binding, physical, channel, capability, artifacts);
                body = kafkaReceipt == null ? new TargetPublishAdmissionBody(scope.shard(), retryUntilEpochMs, claim.owner(),
                        claim.storeIncarnation(), claim.claimId(), locator, attemptNo, attempt, obligation,
                        claim.executionBytes(), commitment, allocated, time, frozen, claim)
                        : new TargetPublishAdmissionBody(scope.shard(), retryUntilEpochMs, claim.owner(),
                                claim.storeIncarnation(), claim.claimId(), locator, attemptNo, attempt, obligation,
                                claim.executionBytes(), commitment, allocated, time, frozen, claim, kafkaReceipt);
            }
            currentClaim(reader, body);
            reader.requireWithinElapsedBudget();
            return body;
        }, Objects.requireNonNull(authority, "readAuthority"));
    }

    /** Before a first Journal ownership marker, revalidate only the frozen lease and historical Admission trust. */
    public void requireNativeOwnership(final BoundedReadBudget budget, final Applied expected,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final TargetStoreBackend.ReadAuthority authority) {
        expected.requireSame(readApplied(budget, expected.image(), authority));
        final var nativeBinding = Objects.requireNonNull(expected.body().nativePublication(), "nativePublication");
        backend.guardedRead(budget, reader -> {
            com.nereusstream.delay.scheduler.TargetNativePolicyChecks.requireFrozen(nativeBinding.policyScope(),
                    nativeBinding.policySnapshot(), nativeBinding.deliverAtEpochMs(), nativeBinding.actionAtEpochMs(),
                    TargetNativePolicyStoreAuthority.inView(reader, scope, lineage), expected.source(), time, false);
            final var lease = nativeBinding.channel().credentialLease();
            if (time.latestEpochMs() >= lease.validUntilEpochMs()
                    || time.latestEpochMs() >= nativeBinding.expireAtEpochMs()) {
                throw new IllegalArgumentException("Native first ownership exceeds its frozen credential/message lease");
            }
            reader.requireWithinElapsedBudget();
            return Boolean.TRUE;
        }, authority);
    }

    /** Reads old Owner/Store Admission facts under the current read guard; this never authorizes a first send. */
    public Recovery readRecovery(
            final BoundedReadBudget budget, final SystemMutation image,
            final TargetStoreBackend.ReadAuthority authority) {
        return new Recovery(readFacts(budget, image, authority, false));
    }

    private Facts readFacts(
            final BoundedReadBudget budget, final SystemMutation image,
            final TargetStoreBackend.ReadAuthority authority, final boolean firstSend) {
        Objects.requireNonNull(image, "image");
        if (image.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION || !image.shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("Target send image belongs to another operation/Shard");
        }
        final var body = TargetPublishAdmissionBody.decode(image.canonicalBody());
        if (firstSend && !body.materialized()) {
            throw new IllegalArgumentException("Target send requires materialized Admission v4/v5");
        }
        return backend.guardedRead(budget, reader -> {
            if (firstSend && !Arrays.equals(body.storeIncarnation(), reader.metadata().storeIncarnation())) {
                throw new IllegalStateException("Target first send belongs to another Store incarnation");
            }
            final byte[] raw = reader.get(ColumnFamily.DEDUPE, systemResultKey(image));
            if (raw == null) {
                throw new IllegalStateException("Target first send lacks its retained Admission first result");
            }
            final var first = TargetResultRecord.decode(
                    TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload());
            first.requireOwner(root(reader));
            first.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final var result = SystemMutationResult.decode(first.typedPayload());
            final byte[] budgetKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT},
                    body.publishAttemptId());
            final var attempt = TargetQuotaAttemptBudget.decodeForStore(
                    budgetKey, TargetValueEnvelope.decode(required(reader, ColumnFamily.META, budgetKey),
                            TargetQuotaAttemptBudget.VALUE_TYPE).payload(), scope.shard());
            final var generation = retainedGeneration(reader, scope, lineage, body.locator());
            final var runtime = generation.runtime();
            if (first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
                    || !Arrays.equals(first.logicalId(), image.systemMutationId())
                    || !Arrays.equals(result.mutationId(), image.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), image.mutationHash())
                    || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                    || result.retryUntilEpochMs() != image.retryUntilEpochMs()
                    || result.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || result.applyStatus() != ApplyStatus.APPLIED || result.stableCode() != StableCode.OK
                    || !Arrays.equals(first.mutation().mutationDigest(), Bytes.sha256(image.canonicalEnvelope()))
                    || !Arrays.equals(result.appliedSourcePosition(), first.mutation().source().canonicalBytes())
                    || !attempt.mutation().equals(first.mutation())
                    || attempt.phase() != TargetQuotaAttemptBudget.Phase.ADMITTED
                    || !Arrays.equals(attempt.admissionDigest(), image.mutationHash())
                    || attempt.executionBytes() != body.executionBytes()
                    || !attempt.commitment().equals(body.commitment()) || !attempt.allocated().equals(body.allocated())
                    || !attempt.locator().equals(body.locator())
                    || firstSend && generation.terminal() != null
                    || (runtime.terminal()
                            ? Integer.compareUnsigned(body.attemptNo(), runtime.admissionsUsed()) > 0
                            : runtime.currentWorkKind() != CurrentSendWorkKind.PUBLISHING
                                    || !Arrays.equals(runtime.publishAttemptId(), body.publishAttemptId())
                                    || runtime.admissionsUsed() != body.attemptNo())
                    || !runtime.attemptObligations().contains(body.obligation())) {
                throw new IllegalStateException("Target Admission image differs from actual retained work/proof");
            }
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping kafkaMapping = null;
            if (body.kafkaReceipt() != null) {
                final byte[] slotKey = com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.key(body.channel());
                final var slot = com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(slotKey,
                        TargetValueEnvelope.decode(required(reader, ColumnFamily.META, slotKey),
                                com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE).payload(), scope.shard());
                if (slot.pendingMappingId() == null) { throw new IllegalStateException("admitted Target K2 has no pending Source slot"); }
                final byte[] mappingKey = Bytes.concat(new byte[] {TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG, TargetKeyCodec.KEY_FORMAT}, slot.pendingMappingId());
                kafkaMapping = com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.decodeForStore(mappingKey,
                        TargetValueEnvelope.decode(required(reader, ColumnFamily.META, mappingKey),
                                com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE).payload(), scope.shard());
                kafkaMapping.requireOriginal(image, first.mutation().source());
                slot.requirePending(kafkaMapping);
                if (firstSend) { TargetExternalEvidenceStore.readAdoption(reader, kafkaMapping, scope, lineage); }
                if (!kafkaMapping.mutation().equals(first.mutation()) || !slot.mutation().equals(first.mutation())
                        || !Arrays.equals(kafkaMapping.recoveryLineage(), lineage)
                        || !Arrays.equals(slot.recoveryLineage(), lineage)) {
                    throw new IllegalStateException("Target K2 mapping/slot differ from actual Admission first batch");
                }
            } else if (firstSend && body.materialized() && body.channel().context().kind()
                    == com.nereusstream.delay.protocol.ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT) {
                throw new IllegalStateException("historical Target K2 Admission without v6 context has no first-send authority");
            }
            reader.requireWithinElapsedBudget();
            return new Facts(image, body, first.mutation().source(), generation, kafkaMapping);
        }, Objects.requireNonNull(authority, "authority"));
    }

    record RetainedGeneration(TargetMessageRecord current, TargetTerminalGenerationRecord terminal) {
        TargetGenerationRuntimeIndex runtime() {
            return terminal == null ? current.runtime() : terminal.runtime();
        }
    }

    /** Shared proof lookup for bounded recovery discovery and the full retained Admission read. */
    static RetainedGeneration retainedGeneration(
            TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage, TargetMessageLocator locator) {
        final byte[] key = TargetKeyCodec.message(locator.messageId());
        final var current = TargetMessageRecord.decodeForStore(key, TargetValueEnvelope.decode(
                required(reader, ColumnFamily.ID, key), TargetMessageRecord.VALUE_TYPE).payload(), scope.shard());
        final boolean historical = !current.locator().equals(locator);
        if (historical && Integer.compareUnsigned(current.locator().generation(), locator.generation()) <= 0) {
            throw new IllegalStateException("Target retained Admission points to another or future generation");
        }
        if (!historical && !current.runtime().terminal()) {
            return new RetainedGeneration(current, null);
        }
        final var terminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                required(reader, ColumnFamily.TERMINAL, TargetTerminalGenerationRecord.key(locator)),
                TargetTerminalGenerationRecord.VALUE_TYPE).payload());
        if (!terminal.locator().equals(locator) || !Arrays.equals(terminal.recoveryLineage(), lineage)
                || !historical && (terminal.stateVersion() != current.stateVersion()
                        || !terminal.runtime().equals(current.runtime()))) {
            throw new IllegalStateException("Target retained Admission lacks its exact terminal generation");
        }
        terminal.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                locator.messageId().bytes());
        final var payload = TargetQuotaPayloadOwner.decodeForStore(payloadKey, TargetValueEnvelope.decode(
                required(reader, ColumnFamily.META, payloadKey), TargetQuotaPayloadOwner.VALUE_TYPE).payload(),
                scope.shard(), scope.tenantScope());
        terminal.requireOwner(payload);
        payload.requireMessagePayload(current);
        payload.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (!terminal.runtime().attemptObligations().isEmpty()
                && payload.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            throw new IllegalStateException("unresolved Target terminal Admission lacks active payload ownership");
        }
        if (locator.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                && !terminal.runtime().attemptObligations().isEmpty()) {
            final byte[] queueKey = TargetKeyCodec.state(locator.target());
            final var queue = TargetQueueState.decode(TargetValueEnvelope.decode(
                    required(reader, ColumnFamily.META, queueKey), TargetQueueState.VALUE_TYPE).payload());
            final byte[] orderKey = TargetKeyCodec.orderState(locator.target(), locator.orderingDomain());
            final var order = TargetOrderState.decodeForStore(orderKey, TargetValueEnvelope.decode(
                    required(reader, ColumnFamily.META, orderKey), TargetOrderState.VALUE_TYPE).payload(),
                    scope.shard(), queue);
            if (historical) {
                order.requireTerminalBarrierProjection(terminal);
            } else {
                order.requireBarrierProjection(current);
            }
        }
        return new RetainedGeneration(current, terminal);
    }

    private static byte[] required(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        final byte[] value = reader.get(family, key);
        if (value == null) {
            throw new IllegalStateException("Target first send lacks retained primary records");
        }
        return value;
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
                    final var decision = reader.closedIngressDeadlineThrough() >= mutation.retryUntilEpochMs()
                            ? null
                            : TargetPublishAdmissionVerifier.decideFirstApplication(scope, mutation, source, authority);
                    StableCode rejection = decision == null
                            ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED : decision.rejection();
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

    private record CurrentClaim(TargetMessageRecord message, TargetClaimRecord claim) {}

    private CurrentClaim currentClaim(final TargetStoreBackend.Reader reader, final TargetPublishAdmissionBody body) {
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
        if (body.materialized()) {
            if (!Arrays.equals(claim.canonicalBytes(), body.claimProof().canonicalBytes())) {
                throw new StaleAdmission();
            }
            final var publication = body.publication();
            final var nativePublication = body.nativePublication();
            final var frozenChannel = body.channel();
            final var frozenPhysical = publication != null ? publication.physical() : nativePublication.physical();
            final byte[] bindingKey = TargetKeyCodec.scheduleBinding(locator.scheduleBindingDigest());
            final byte[] bindingRaw = reader.get(ColumnFamily.ID, bindingKey);
            final byte[] identityKey = TargetKeyCodec.identity(locator.target());
            final byte[] identityRaw = reader.get(ColumnFamily.META, identityKey);
            final byte[] channelKey = frozenChannel.encodedKey();
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
            if (!Arrays.equals(identity.canonicalBytes(), frozenPhysical.canonicalBytes())) {
                throw new StaleAdmission();
            }
            channel.requireExactFrozenIdentity(frozenChannel);
            final var slot = TargetChannelControlVerifier.readSlot(reader, channel, lineage);
            if (slot == null) {
                throw new IllegalStateException("materialized Admission lacks its Source-applied channel slot");
            }
            try {
                slot.requireNewWork(channel);
            } catch (IllegalStateException stoppedOrRenewed) {
                throw new StaleAdmission();
            }
            channel.requireQueueProjection(queue(reader, before));
            if (publication != null) {
                publication.requireClaim(claim, before, binding);
            } else {
                nativePublication.requireClaim(claim, before, binding);
                nativePublication.policyScope().requireQueue(queue(reader, before));
                final var storedScope = TargetNativePolicyStoreAuthority.requireScope(
                        reader, scope, nativePublication.policyScope().digest());
                if (!storedScope.equals(nativePublication.policyScope())) { throw new StaleAdmission(); }
            }
            if (body.decisionTime().latestEpochMs() >= channel.credentialLease().validUntilEpochMs()
                    || body.decisionTime().latestEpochMs() >= claim.deadlineEpochMs()) {
                throw new StaleAdmission();
            }
        }
        return new CurrentClaim(before, claim);
    }

    private AdmissionProjection admission(
            final TargetStoreBackend.Reader reader,
            final TargetPublishAdmissionVerifier.Decision decision,
            final SystemMutation mutation,
            final com.nereusstream.delay.protocol.SourcePosition source,
            final TargetQuotaMutation stamp) {
        final var body = decision.body();
        final var current = currentClaim(reader, body);
        final var before = current.message();
        final var claim = current.claim();
        final var locator = body.locator();
        final byte[] claimKey = claim.key();
        final var time = body.decisionTime();
        final long requiredEarliest = body.nativePublication() != null
                ? body.nativePublication().actionAtEpochMs() : claim.work().ordinaryEligibilityAtEpochMs();
        final long notAfter = before.expireAtEpochMs();
        final long brokerBound;
        try {
            brokerBound = Math.addExact(
                    source.brokerPersistenceTimeEpochMs(),
                    decision.authorization().maximumBrokerTimestampDivergenceMs());
        } catch (ArithmeticException overflow) {
            throw new StaleAdmission();
        }
        if (body.materialized()
                && brokerBound >= Math.min(
                        claim.deadlineEpochMs(), body.channel().credentialLease().validUntilEpochMs())) {
            throw new StaleAdmission();
        }
        if (time.earliestEpochMs() < requiredEarliest
                || time.latestEpochMs() >= notAfter
                || brokerBound >= notAfter
                || (claim.selected().nativeCandidate() && time.latestEpochMs() >= before.deliverAtEpochMs())) {
            throw new StaleAdmission();
        }
        if (body.nativePublication() != null) {
            final var nativeBinding = body.nativePublication();
            final var policies = decision.authorization().nativePolicies();
            final var observed = policies.requireCurrent(nativeBinding.policyScope().digest());
            if (!observed.head().ref(observed.revision()).equals(nativeBinding.policyHead())) {
                throw new StaleAdmission();
            }
            final com.nereusstream.delay.protocol.TargetNativePolicySnapshot frozen;
            try {
                com.nereusstream.delay.scheduler.TargetNativePolicyChecks.requireFrozen(
                        nativeBinding.policyScope(), observed.head().snapshot(), nativeBinding.deliverAtEpochMs(),
                        nativeBinding.actionAtEpochMs(), TargetNativePolicyStoreAuthority.inView(reader, scope, lineage),
                        source, time, true);
                frozen = observed.head().snapshot();
            } catch (IllegalArgumentException ineligible) {
                throw new StaleAdmission();
            }
            if (!observed.sameHead(policies.requireCurrent(nativeBinding.policyScope().digest()))
                    || !Arrays.equals(frozen.canonicalBytes(), nativeBinding.policySnapshot().canonicalBytes())
                    || brokerBound >= frozen.validUntilEpochMs()) {
                throw new StaleAdmission();
            }
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
        final var receiptEdits = new ArrayList<TargetStoreBackend.Edit>();
        if (body.kafkaReceipt() != null) {
            final byte[] slotKey = com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.key(body.channel());
            final byte[] slotRaw = reader.get(ColumnFamily.META, slotKey);
            final var previous = slotRaw == null ? null
                    : com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(slotKey,
                            TargetValueEnvelope.decode(slotRaw, com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE).payload(), scope.shard());
            if (previous != null && (previous.pendingMappingId() != null
                    || !Arrays.equals(previous.recoveryLineage(), lineage))) { throw new StaleAdmission(); }
            if (previous != null && previous.lastAllocatedSequence() == -1L) { throw new StaleAdmission(); }
            final long sequence = previous == null ? 1 : previous.nextSequence();
            final var mapping = new com.nereusstream.delay.protocol.TargetKafkaReceiptMapping(body, mutation.mutationHash(), stamp, sequence, lineage);
            if (reader.get(ColumnFamily.META, mapping.key()) != null) { throw new IllegalStateException("new Source mapping already exists"); }
            final var slot = new com.nereusstream.delay.protocol.TargetKafkaReceiptSlot(body.channel(), sequence, mapping.mappingId(), stamp, lineage);
            receiptEdits.add(reader.replace(ColumnFamily.META, mapping.key(), com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE, mapping.canonicalBytes()));
            receiptEdits.add(reader.replace(ColumnFamily.META, slotKey, com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE, slot.canonicalBytes()));
            final var artifacts = decision.authorization().materialization().externalEvidence(body, source);
            receiptEdits.addAll(TargetExternalEvidenceStore.adopt(reader, scope, lineage, mapping, attemptBudget, artifacts));
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
        receiptEdits.addAll(List.of(claimEdit, chargeEdit, budgetEdit));
        return new AdmissionProjection(
                before,
                admitted,
                order.isEmpty() ? null : order.getFirst(),
                List.copyOf(receiptEdits));
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
