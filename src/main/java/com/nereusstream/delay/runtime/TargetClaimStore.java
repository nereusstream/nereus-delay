package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Atomic reversible Target Claim/revoke, using actual Message, heads, Claim and frozen accounting records. */
public final class TargetClaimStore {
    public static final class Prepared {
        private final TargetClaimStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final TargetClaimRecord claim;

        private Prepared(
                final TargetClaimStore owner, final TargetStoreBackend.Prepared batch, final TargetClaimRecord claim) {
            this.owner = owner;
            this.batch = batch;
            this.claim = claim;
        }

        /** Inspection only; preparation does not create durable ownership or Producer permission. */
        public TargetClaimRecord claim() {
            return claim;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetMessageStore messages;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumDomains;
    private final TargetQuotaDelta.LocalClaimAuthority quotaAuthority;

    public TargetClaimStore(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final int maximumDomains,
            final TargetQuotaDelta.LocalClaimAuthority quotaAuthority) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        this.lineage = Bytes.copy(lineage);
        this.maximumDomains = maximumDomains;
        this.quotaAuthority = Objects.requireNonNull(quotaAuthority, "quotaAuthority");
        messages = new TargetMessageStore(backend, 1, 1, maximumDomains);
    }

    /** Live native policy, clock safety, Owner lease and physical permits remain mandatory commit-guard inputs. */
    public Prepared prepareClaim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest) {
        Objects.requireNonNull(selected, "selected");
        Objects.requireNonNull(owner, "owner");
        if (nowEpochMs < 0
                || (!selected.nativeCandidate() && selected.timeEpochMs() > nowEpochMs)
                || deadlineEpochMs <= nowEpochMs) {
            throw new IllegalArgumentException("Target Claim is not eligible or has an expired deadline");
        }
        final var result = new TargetClaimRecord[1];
        final var prepared = messages.prepareAccounted(
                budget,
                reader -> {
                    final var message = message(reader, selected);
                    if (message.runtime().currentWorkKind() != CurrentSendWorkKind.TIMELINE
                            || nowEpochMs >= message.expireAtEpochMs()
                            || (selected.nativeCandidate() && nowEpochMs >= message.deliverAtEpochMs())) {
                        throw new IllegalStateException("Target Claim lacks eligible current work");
                    }
                    final var queue = queue(reader, message);
                    final byte[] bindingKey =
                            TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
                    final var binding = TargetScheduleBinding.decodeForStore(
                            bindingKey,
                            payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                            reader.shardId());
                    binding.requireLocator(message.locator());
                    binding.requireMessageSource(message.scheduleSource());
                    binding.requireQueueProjection(queue);
                    if (selected.nativeCandidate()
                            && nowEpochMs < Math.max(0, message.deliverAtEpochMs() - queue.nativeIndexLeadCapMs())) {
                        throw new IllegalStateException("Target native Claim precedes its pinned maximum lead");
                    }
                    final var domain =
                            queue.domains().get(message.locator().domain().slot());
                    if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN
                            || domain.lifecycle() != TargetDomainState.Lifecycle.ACTIVE
                            || !selected.equals(
                                    selected.nativeCandidate() ? domain.nativeHead() : domain.ordinaryHead())) {
                        throw new IllegalStateException("Target Claim selected head/control/domain changed");
                    }
                    final var operation = localOperation(reader, operationDigest);
                    final var claim = new TargetClaimRecord(
                            message.runtime(),
                            message.stateVersion(),
                            message.digest(),
                            selected,
                            owner,
                            reader.metadata().storeIncarnation(),
                            TargetQuotaMutation.increment(reader.aggregate().revision()),
                            deadlineEpochMs,
                            executionBytes,
                            queue.controlVersion(),
                            operation);
                    final var locator = message.locator();
                    final var identity = new TargetQuotaIdentity(
                            TargetQuotaIdentity.Kind.TARGET,
                            scope.shard(),
                            locator.accountingIncarnation(),
                            locator.target(),
                            null);
                    final byte[] descriptorKey = identity.key();
                    descriptorKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                    final var descriptor = TargetQuotaIncarnation.decodeForStore(
                            descriptorKey,
                            payload(reader, ColumnFamily.META, descriptorKey, TargetQuotaIncarnation.VALUE_TYPE),
                            scope.shard(),
                            scope.tenantScope());
                    final var charge = new TargetQuotaClaimCharge(
                            claim.claimId(),
                            claim.work(),
                            identity,
                            scope.tenantScope(),
                            descriptor.accounting(),
                            owner,
                            claim.storeIncarnation(),
                            claim.sequence(),
                            deadlineEpochMs,
                            executionBytes,
                            Bytes.sha256(claim.canonicalBytes()),
                            operation,
                            lineage);
                    charge.requireDescriptor(descriptor);
                    claim.requireCharge(charge);
                    if (reader.get(ColumnFamily.INFLIGHT, claim.key()) != null
                            || reader.get(ColumnFamily.META, charge.key()) != null) {
                        throw new IllegalStateException("Target Claim identity has already been used");
                    }
                    final var next = claim.claimed(message);
                    result[0] = claim;
                    return new TargetMessageStore.Input(
                            List.of(new TargetMessageStore.Transition(message, next)),
                            order(reader, message, next, true),
                            List.of(
                                    reader.replace(
                                            ColumnFamily.INFLIGHT,
                                            claim.key(),
                                            TargetClaimRecord.VALUE_TYPE,
                                            claim.canonicalBytes()),
                                    reader.replace(
                                            ColumnFamily.META,
                                            charge.key(),
                                            TargetQuotaClaimCharge.VALUE_TYPE,
                                            charge.canonicalBytes())));
                },
                accounting(operationDigest, TargetQuotaDelta.LocalClaimKind.CLAIM));
        return new Prepared(this, prepared, result[0]);
    }

    /** Direct current-Claim lookup from the Message reference; no prior Owner epoch scan is required. */
    public static TargetClaimRecord current(final TargetStoreBackend.Reader reader, final TargetMessageRecord message) {
        if (message.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED) {
            throw new IllegalStateException("Message has no current Claim");
        }
        final byte[] actual = payload(reader, ColumnFamily.ID, message.encodedKey(), TargetMessageRecord.VALUE_TYPE);
        if (!Arrays.equals(actual, message.canonicalBytes())) {
            throw new IllegalStateException("current Claim lookup used a stale Message");
        }
        final byte[] key = TargetClaimRecord.key(message.runtime().claimId());
        final var claim =
                TargetClaimRecord.decode(payload(reader, ColumnFamily.INFLIGHT, key, TargetClaimRecord.VALUE_TYPE));
        claim.requireStored(key);
        claim.requireCurrent(message);
        final var charge = TargetQuotaClaimCharge.decode(
                payload(reader, ColumnFamily.META, claim.chargeKey(), TargetQuotaClaimCharge.VALUE_TYPE));
        claim.requireCharge(charge);
        return claim;
    }

    /** Revoke restores only the same semantic work and increments its instance revision; no Admission is spent. */
    public Prepared prepareRevoke(
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final OwnerIdentity currentOwner,
            final byte[] operationDigest) {
        Objects.requireNonNull(expected, "expected");
        final var prepared = messages.prepareAccounted(
                budget,
                reader -> {
                    final var claim = TargetClaimRecord.decode(
                            payload(reader, ColumnFamily.INFLIGHT, expected.key(), TargetClaimRecord.VALUE_TYPE));
                    claim.requireStored(expected.key());
                    if (!Arrays.equals(claim.canonicalBytes(), expected.canonicalBytes())
                            || !claim.owner().equals(currentOwner)
                            || !Arrays.equals(
                                    claim.storeIncarnation(), reader.metadata().storeIncarnation())) {
                        throw new IllegalStateException("local Target revoke changed exact Claim/Owner/Store");
                    }
                    final var charge = TargetQuotaClaimCharge.decode(
                            payload(reader, ColumnFamily.META, claim.chargeKey(), TargetQuotaClaimCharge.VALUE_TYPE));
                    claim.requireCharge(charge);
                    localOperation(reader, operationDigest).requireStoreSuccessorOf(claim.creation());
                    final var message = message(reader, claim.selected());
                    final var next = claim.revoked(message);
                    return new TargetMessageStore.Input(
                            List.of(new TargetMessageStore.Transition(message, next)),
                            order(reader, message, next, false),
                            List.of(
                                    reader.replace(
                                            ColumnFamily.INFLIGHT, claim.key(), TargetClaimRecord.VALUE_TYPE, null),
                                    reader.replace(
                                            ColumnFamily.META,
                                            claim.chargeKey(),
                                            TargetQuotaClaimCharge.VALUE_TYPE,
                                            null)));
                },
                accounting(operationDigest, TargetQuotaDelta.LocalClaimKind.REVOKE));
        return new Prepared(this, prepared, expected);
    }

    /** The supplied guard must retain actual Owner/lease/clock/policy/permit/grant authority through native commit. */
    public void commit(final Prepared prepared, final TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(authority, "authority");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("Claim plan belongs to another Store wrapper");
        }
        backend.commit(prepared.batch, (metadata, targetScope, mutation) -> {
            if (!Arrays.equals(metadata.storeIncarnation(), prepared.claim.storeIncarnation())) {
                throw new IllegalStateException("Target Claim Store identity changed");
            }
            return authority.acquire(metadata, targetScope, mutation);
        });
    }

    private TargetLocalClaimAccounting accounting(final byte[] digest, final TargetQuotaDelta.LocalClaimKind kind) {
        return new TargetLocalClaimAccounting(scope, lineage, digest, kind, quotaAuthority, maximumDomains);
    }

    private static TargetQuotaMutation localOperation(final TargetStoreBackend.Reader reader, final byte[] digest) {
        if (reader.source() == null || reader.sourceSequence() == 0) {
            throw new IllegalStateException("Target Claim requires applied source");
        }
        final var prior = reader.aggregate().mutation();
        final long ordinal = prior != null && prior.sequence() == reader.sourceSequence()
                ? TargetQuotaMutation.increment(prior.localClaimOrdinal())
                : 1;
        return new TargetQuotaMutation(reader.sourceSequence(), reader.source(), digest, ordinal);
    }

    private static TargetMessageRecord message(final TargetStoreBackend.Reader reader, final TargetHeadRef selected) {
        final byte[] key = TargetKeyCodec.message(selected.messageId());
        return TargetMessageRecord.decodeForStore(
                key, payload(reader, ColumnFamily.ID, key, TargetMessageRecord.VALUE_TYPE), reader.shardId());
    }

    private static TargetQueueState queue(final TargetStoreBackend.Reader reader, final TargetMessageRecord message) {
        final byte[] key = TargetKeyCodec.state(message.locator().target());
        final var queue = TargetQueueState.decode(payload(reader, ColumnFamily.META, key, TargetQueueState.VALUE_TYPE));
        message.locator().requireQueueProjection(queue);
        return queue;
    }

    private static List<TargetMessageStore.OrderTransition> order(
            final TargetStoreBackend.Reader reader,
            final TargetMessageRecord before,
            final TargetMessageRecord after,
            final boolean claim) {
        if (before.locator().orderingMode() != OrderingMode.DELIVERY_TIME_FIFO) {
            return List.of();
        }
        final byte[] key = TargetKeyCodec.orderState(
                before.locator().target(), before.locator().orderingDomain());
        final var old = TargetOrderState.decode(payload(reader, ColumnFamily.META, key, TargetOrderState.VALUE_TYPE));
        if (claim) {
            if (old.gate() != TargetOrderState.Gate.OPEN || old.barrier() != null) {
                throw new IllegalStateException("strict Target Claim is blocked");
            }
        } else {
            old.requireBarrierProjection(before);
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
                claim ? TargetOrderBarrier.fromMessage(after) : null);
        return List.of(new TargetMessageStore.OrderTransition(old, next));
    }

    private static byte[] payload(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key, final int type) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("Target Claim dependency is absent");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }
}
