package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
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
import java.util.Arrays;
import java.util.Objects;

/** Bounded lazy cost lookup for one verified current head; it grants no Claim or send permission. */
public final class TargetHeadCostProbe {
    /** Exact source-Store projection required before a persisted Native key can be considered. */
    public record NativeProjection(
            TargetMessageRecord message,
            TargetTimelineWorkRef work,
            TargetScheduleBinding binding,
            TargetNativePolicyScope scope) {
        public NativeProjection {
            Objects.requireNonNull(message, "message");
            Objects.requireNonNull(work, "work");
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(scope, "scope");
            if (!work.nativeCandidate()
                    || !work.equals(message.runtime().timeline())
                    || message.nativeDeliveryPolicy() == NativeDeliveryPolicy.FORBID) {
                throw new IllegalArgumentException("invalid Target Native message/work projection");
            }
        }
    }

    public record Cost(
            TargetHeadRef head,
            TargetQueueState queue,
            TargetQuotaAccounting accounting,
            long executionBytes,
            long schedulingCost,
            long deliverAtEpochMs,
            long expireAtEpochMs,
            NativeProjection nativeProjection) {
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
            if (nativeProjection != null && !head.nativeCandidate()) {
                throw new IllegalArgumentException("ordinary head cannot carry Target Native cost projection");
            }
        }

        public Cost(
                final TargetHeadRef head,
                final TargetQueueState queue,
                final TargetQuotaAccounting accounting,
                final long executionBytes,
                final long schedulingCost,
                final long deliverAtEpochMs,
                final long expireAtEpochMs) {
            this(head, queue, accounting, executionBytes, schedulingCost, deliverAtEpochMs, expireAtEpochMs, null);
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
                    final byte[] selectedWorkValue = reader.get(ColumnFamily.TIMELINE, selected.key());
                    if (selectedWorkValue == null) {
                        throw new IllegalStateException("Target cost probe selected a missing timeline row");
                    }
                    final byte[] selectedWork = TargetValueEnvelope.decode(
                                    selectedWorkValue, TargetTimelineWorkRef.VALUE_TYPE)
                            .payload();
                    final TargetTimelineWorkRef selectedTimelineWork = message.locator().orderingMode()
                                    != OrderingMode.DELIVERY_TIME_FIFO
                            ? message.requireTimelineProjection(selected.key(), selectedWork)
                            : null;
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
                    if (message.locator().orderingMode() == OrderingMode.DELIVERY_TIME_FIFO) {
                        if (selected.nativeCandidate()) {
                            throw new IllegalStateException("strict Target head cannot select Native work");
                        }
                        final var orderedHead = TargetKeyCodec.decodeOrderedHead(selected.key());
                        if (!orderedHead.target().equals(selected.target())
                                || !orderedHead.domain().equals(selected.domain())
                                || orderedHead.eligibleAtEpochMs() != selected.timeEpochMs()) {
                            throw new IllegalStateException(
                                    "Target strict head identity differs from its selected key");
                        }
                        final byte[] orderKey =
                                TargetKeyCodec.orderState(selected.target(), orderedHead.orderingDomain());
                        final var order = TargetOrderState.decodeForStore(
                                orderKey,
                                payload(reader, ColumnFamily.META, orderKey, TargetOrderState.VALUE_TYPE),
                                reader.shardId(),
                                queue);
                        final var orderedWork =
                                order.requireServiceableProjection(selected.key(), selectedWork, message);
                        if (!Arrays.equals(
                                reader.get(ColumnFamily.TIMELINE, orderedWork.ordinaryKey()), selectedWorkValue)) {
                            throw new IllegalStateException("strict Target head lacks its exact ORDERED work");
                        }
                    }
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
                    final NativeProjection nativeProjection;
                    if (selected.nativeCandidate()) {
                        if (selectedTimelineWork == null
                                || !selectedTimelineWork.nativeCandidate()
                                || !Arrays.equals(binding.nativePolicyScopeRef(), domain.nativePolicyScopeRef())) {
                            throw new IllegalStateException("Target Native head lacks its exact schedule scope");
                        }
                        final byte[] scopeRef = domain.nativePolicyScopeRef();
                        final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(scopeRef);
                        final var scope = TargetNativePolicyScope.decodeForStore(
                                scopeKey,
                                payload(reader, ColumnFamily.META, scopeKey, TargetNativePolicyScope.VALUE_TYPE),
                                reader.shardId());
                        final byte[] dispatchKey = TargetKeyCodec.dispatchCompatibility(scope.dispatchRef());
                        final var dispatch = TargetDispatchCompatibility.decodeForStore(
                                dispatchKey,
                                payload(
                                        reader,
                                        ColumnFamily.META,
                                        dispatchKey,
                                        TargetDispatchCompatibility.VALUE_TYPE),
                                physical);
                        final byte[] controlKey = TargetKeyCodec.controlScope(scope.controlRef());
                        final var controls = TargetControlScope.decodeForStore(
                                controlKey,
                                payload(reader, ColumnFamily.META, controlKey, TargetControlScope.VALUE_TYPE),
                                selected.target(),
                                reader.shardId());
                        scope.requireReferences(physical, dispatch, controls);
                        scope.requireQueue(queue);
                        scope.requireBinding(binding);
                        nativeProjection = new NativeProjection(message, selectedTimelineWork, binding, scope);
                    } else {
                        nativeProjection = null;
                    }
                    reader.requireWithinElapsedBudget();
                    return new Cost(
                            selected,
                            queue,
                            accounting,
                            executionBytes,
                            schedulingCost,
                            message.deliverAtEpochMs(),
                            message.expireAtEpochMs(),
                            nativeProjection);
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
