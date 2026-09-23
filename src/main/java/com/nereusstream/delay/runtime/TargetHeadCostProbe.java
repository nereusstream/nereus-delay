package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Objects;

/** Bounded lazy cost lookup for one verified current head; it grants no Claim or send permission. */
public final class TargetHeadCostProbe {
    public record Cost(
            TargetHeadRef head,
            TargetQueueState queue,
            TargetQuotaAccounting accounting,
            long executionBytes,
            long schedulingCost,
            long deliverAtEpochMs,
            long expireAtEpochMs) {
        public Cost {
            Objects.requireNonNull(head, "head");
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(accounting, "accounting");
            if (!head.target().equals(queue.targetId())
                    || executionBytes <= 0
                    || schedulingCost < executionBytes
                    || deliverAtEpochMs < 0
                    || expireAtEpochMs < deliverAtEpochMs) {
                throw new IllegalArgumentException("invalid Target head cost projection");
            }
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final int maximumDomains;

    public TargetHeadCostProbe(
            final TargetStoreBackend backend, final TargetQuotaScope scope, final int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        if (scope.target() != null || maximumDomains < 1 || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("Target cost probe requires bounded Shard scope");
        }
        this.maximumDomains = maximumDomains;
    }

    public Cost probe(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(selected, "selected");
        return backend.guardedRead(
                Objects.requireNonNull(budget, "budget"),
                reader -> {
                    final byte[] messageKey = TargetKeyCodec.message(selected.messageId());
                    final var message = TargetMessageRecord.decodeForStore(
                            messageKey,
                            payload(reader, ColumnFamily.ID, messageKey, TargetMessageRecord.VALUE_TYPE),
                            reader.shardId());
                    if (message.runtime().currentWorkKind() != CurrentSendWorkKind.TIMELINE) {
                        throw new IllegalStateException("Target cost probe selected non-timeline work");
                    }
                    message.requireTimelineProjection(
                            selected.key(),
                            payload(reader, ColumnFamily.TIMELINE, selected.key(), TargetTimelineWorkRef.VALUE_TYPE));
                    final byte[] queueKey = TargetKeyCodec.state(selected.target());
                    final byte[] identityKey = TargetKeyCodec.identity(selected.target());
                    final var physical = CanonicalTargetPartition.decodeForStore(
                            identityKey,
                            payload(reader, ColumnFamily.META, identityKey, CanonicalTargetPartition.VALUE_TYPE));
                    final var queue = TargetQueueState.decodeForStore(
                            queueKey,
                            payload(reader, ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE),
                            physical,
                            reader.shardId(),
                            maximumDomains);
                    message.locator().requireQueueProjection(queue);
                    if (selected.domain().slot() >= queue.domains().size()) {
                        throw new IllegalStateException("Target cost probe selected a missing domain");
                    }
                    final TargetDomainState domain =
                            queue.domains().get(selected.domain().slot());
                    if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN
                            || domain.lifecycle() != TargetDomainState.Lifecycle.ACTIVE
                            || !selected.equals(
                                    selected.nativeCandidate() ? domain.nativeHead() : domain.ordinaryHead())) {
                        throw new IllegalStateException("Target cost probe selected a stale or blocked head");
                    }
                    final byte[] bindingKey =
                            TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
                    final var binding = TargetScheduleBinding.decodeForStore(
                            bindingKey,
                            payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                            reader.shardId());
                    binding.requireLocator(message.locator());
                    binding.requireMessageSource(message.scheduleSource());
                    binding.requireQueueProjection(queue);
                    final var metadata = binding.intent().adapterMetadata();
                    final AdapterKind adapter =
                            switch (metadata.kind()) {
                                case KAFKA -> AdapterKind.KAFKA;
                                case PULSAR -> AdapterKind.PULSAR;
                            };
                    if ((adapter == AdapterKind.KAFKA)
                            != (physical.resource().kind() == BrokerResourceIdentity.Kind.KAFKA)) {
                        throw new IllegalStateException("Target cost probe adapter differs from physical resource");
                    }
                    final var identity = new TargetQuotaIdentity(
                            TargetQuotaIdentity.Kind.TARGET,
                            scope.shard(),
                            message.locator().accountingIncarnation(),
                            selected.target(),
                            null);
                    final byte[] descriptorKey = identity.key();
                    descriptorKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                    final var descriptor = TargetQuotaIncarnation.decodeForStore(
                            descriptorKey,
                            payload(reader, ColumnFamily.META, descriptorKey, TargetQuotaIncarnation.VALUE_TYPE),
                            scope.shard(),
                            scope.tenantScope());
                    final long payloadBytes = message.payloadLength();
                    final long metadataBytes = metadata.canonicalBytes().length;
                    final var accounting = descriptor.accounting();
                    final long executionBytes = accounting.accountedPublishBytes(adapter, payloadBytes, metadataBytes);
                    final long schedulingCost = accounting.schedulingCost(adapter, payloadBytes, metadataBytes);
                    reader.requireWithinElapsedBudget();
                    return new Cost(
                            selected,
                            queue,
                            accounting,
                            executionBytes,
                            schedulingCost,
                            message.deliverAtEpochMs(),
                            message.expireAtEpochMs());
                },
                Objects.requireNonNull(authority, "authority"));
    }

    private static byte[] payload(
            final TargetStoreBackend.Reader reader, final ColumnFamily family, final byte[] key, final int type) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("Target cost probe dependency is absent");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }
}
