package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Rebuilds one actual business record's frozen contribution; never accepts a persisted usage field as its oracle. */
public final class TargetRecordAccounting {
    public record Charge(TargetQuotaIncarnation owner, TargetQuotaUsage primary) {
        public Charge {
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(primary, "primary");
        }

        public TargetQuotaUsage mirror() {
            return resources(primary.resources());
        }
    }

    private final TargetStoreBackend.Reader reader;
    private final List<TargetStoreBackend.Edit> overlay;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetQuotaMutation operation;
    private final TargetQuotaBookkeeping root;
    private final int maximumDomains;

    public TargetRecordAccounting(
            final TargetStoreBackend.Reader reader,
            final List<TargetStoreBackend.Edit> overlay,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final TargetQuotaMutation operation,
            final TargetQuotaBookkeeping root,
            final int maximumDomains) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.overlay = List.copyOf(overlay);
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        this.lineage = Bytes.copy(lineage);
        this.operation = Objects.requireNonNull(operation, "operation");
        this.root = Objects.requireNonNull(root, "root");
        if (scope.target() != null
                || !scope.shard().equals(reader.shardId())
                || maximumDomains < 1
                || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("invalid record accounting scope or domain limit");
        }
        this.maximumDomains = maximumDomains;
    }

    /** Null is reserved for grant activation storage, which is counted by the root's fixed inventory reserve. */
    public Charge charge(final ColumnFamily family, final byte[] key, final byte[] raw) {
        final var value = TargetValueEnvelope.decodeAny(raw);
        final byte[] payload = value.payload();
        if (!Arrays.equals(raw, reader.projected(family, key, overlay))) {
            throw new IllegalStateException("accounting record is not the exact selected Store view");
        }
        final Charge result;
        switch (value.valueType()) {
            case TargetMessageRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.ID);
                final var message = TargetMessageRecord.decodeForStore(key, payload, scope.shard());
                final var owner = payloadOwner(message.locator().messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.message(owner, key, payload));
            }
            case TargetScheduleBinding.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.ID);
                final var binding = TargetScheduleBinding.decodeForStore(key, payload, scope.shard());
                final var owner = payloadOwner(binding.messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.initialBinding(owner, key, payload));
            }
            case TargetTimelineWorkRef.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.TIMELINE);
                final var work = TargetTimelineWorkRef.decode(payload);
                final var message = message(work.locator().messageId());
                final var owner = payloadOwner(work.locator().messageId());
                result = messageCharge(
                        owner,
                        key[0] == TargetKeyCodec.ORDER_HEAD_TAG
                                ? TargetQuotaMessageRecords.orderHead(
                                        owner,
                                        message,
                                        order(
                                                work.locator().target(),
                                                work.locator().orderingDomain()),
                                        key,
                                        payload)
                                : TargetQuotaMessageRecords.timeline(owner, message, key, payload));
            }
            case TargetExpiryRef.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.TIMELINE);
                final var expiry = TargetExpiryRef.decode(payload);
                final var message = message(expiry.locator().messageId());
                final var owner = payloadOwner(expiry.locator().messageId());
                result = messageCharge(owner, TargetQuotaMessageRecords.expiry(owner, message, key, payload));
            }
            case TargetQuotaPayloadOwner.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var owner =
                        TargetQuotaPayloadOwner.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                final var descriptor = descriptor(owner.primaryIdentity());
                descriptor.requirePayloadOwner(owner);
                result = new Charge(
                        descriptor,
                        resources(TargetQuotaMessageRecords.payloadOwner(owner, key, payload)
                                .charge()
                                .add(owner.payloadCharge())));
            }
            case TargetQuotaIncarnation.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var owner =
                        TargetQuotaIncarnation.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                result = new Charge(owner, owner.ownContribution());
            }
            case TargetQueueState.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var queue = TargetQueueState.decode(payload);
                result = metadata(TargetQuotaMetadataRecords.queue(
                        targetOwner(queue.targetId(), queue.accountingIncarnation()),
                        physical(queue.targetId()),
                        maximumDomains,
                        key,
                        payload));
            }
            case TargetOrderState.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var state = TargetOrderState.decode(payload);
                result = metadata(TargetQuotaMetadataRecords.strictDomain(
                        targetOwner(state.target(), state.accountingIncarnation()), key, payload));
            }
            case TargetChannelIdentity.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var channel = TargetChannelIdentity.decodeForStore(key, payload, scope.shard());
                result = metadata(TargetQuotaMetadataRecords.channel(
                        targetOwner(
                                channel.context().target(), channel.context().accountingIncarnation()),
                        key,
                        payload));
            }
            case TargetNativePolicyScope.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var nativeScope = TargetNativePolicyScope.decodeForStore(key, payload, scope.shard());
                result = metadata(TargetQuotaMetadataRecords.nativeScope(
                        targetOwner(nativeScope.target(), nativeScope.accountingIncarnation()), key, payload));
            }
            case TargetNativePolicySnapshot.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var snapshot = TargetNativePolicySnapshot.decode(payload);
                final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(snapshot.policyScopeDigest());
                final var nativeScope = TargetNativePolicyScope.decodeForStore(
                        scopeKey,
                        payload(ColumnFamily.META, scopeKey, TargetNativePolicyScope.VALUE_TYPE),
                        scope.shard());
                result = metadata(TargetQuotaMetadataRecords.nativeSnapshot(
                        targetOwner(nativeScope.target(), nativeScope.accountingIncarnation()),
                        nativeScope,
                        key,
                        payload));
            }
            case CanonicalTargetPartition.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.IDENTITY,
                        CanonicalTargetPartition.decodeForStore(key, payload).id(),
                        key,
                        payload);
            }
            case TargetDispatchCompatibility.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.DISPATCH,
                        TargetDispatchCompatibility.decode(payload).target(),
                        key,
                        payload);
            }
            case TargetControlScope.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.CONTROL,
                        TargetControlScope.decode(payload).target(),
                        key,
                        payload);
            }
            case TargetMembershipGrant.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_GRANT,
                        TargetMembershipGrant.decodeForStore(key, payload, scope.shard())
                                .offered()
                                .target(),
                        key,
                        payload);
            }
            case TargetMembershipPolicy.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                result = shared(
                        TargetQuotaMetadataRecords.SharedRole.MEMBERSHIP_POLICY,
                        TargetMembershipPolicy.decodeForStore(key, payload, scope.shard())
                                .offered()
                                .target(),
                        key,
                        payload);
            }
            case TargetResultRecord.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.DEDUPE);
                final var record = TargetResultRecord.decode(payload);
                record.requireStored(key, value.valueType(), payload);
                final var owner = descriptor(record.primaryIdentity());
                record.requireOwner(owner);
                record.mutation().requireAtOrBefore(operation);
                if (record.kind() != TargetResultRecord.Kind.COMMAND
                        && record.kind() != TargetResultRecord.Kind.SYSTEM) {
                    final byte[] firstKey = Bytes.concat(
                            new byte[] {
                                (byte)
                                        (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                                                ? TargetKeyCodec.RESULT_SYSTEM_TAG
                                                : TargetKeyCodec.RESULT_COMMAND_TAG),
                                1
                            },
                            record.logicalId());
                    record.requireFirst(TargetResultRecord.decode(
                            payload(ColumnFamily.DEDUPE, firstKey, TargetResultRecord.VALUE_TYPE)));
                }
                result = new Charge(owner, resources(record.recordCharge()));
            }
            case TargetQuotaClaimCharge.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var claim = TargetQuotaClaimCharge.decode(payload);
                claim.requireStored(key, value.valueType(), payload);
                final var owner = descriptor(claim.primaryIdentity());
                claim.requireDescriptor(owner);
                claim.creation().requireAtOrBefore(operation);
                result = new Charge(owner, resources(claim.contribution()));
            }
            case TargetQuotaAttemptBudget.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var budget = TargetQuotaAttemptBudget.decodeForStore(key, payload, scope.shard());
                final var owner = descriptor(budget.primaryIdentity());
                owner.requireAttemptBudget(budget);
                budget.mutation().requireAtOrBefore(operation);
                result = new Charge(owner, resources(budget.effectiveCharge().add(root.attemptRecordCharge(budget))));
            }
            case TargetQuotaGrantActivation.VALUE_TYPE -> {
                requireFamily(family, ColumnFamily.META);
                final var grant =
                        TargetQuotaGrantActivation.decodeForStore(key, payload, scope.shard(), scope.tenantScope());
                grant.mutation().requireAtOrBefore(operation);
                if (grant.allocation() != null) {
                    final var owner = descriptor(grant.allocation().identity());
                    if (!owner.allocation().equals(grant.allocation().allocation())
                            || !owner.accounting().equals(grant.allocation().accounting())
                            || !Arrays.equals(
                                    owner.tenantScope(), grant.allocation().tenantScope())
                            || !Arrays.equals(
                                    owner.recoveryLineage(), grant.allocation().recoveryLineage())) {
                        throw new IllegalStateException(
                                "grant activation first allocation differs from its descriptor");
                    }
                }
                return null;
            }
            default ->
                throw new IllegalArgumentException("unhandled business record accounting type: " + value.valueType());
        }
        requireOwner(result.owner());
        return result;
    }

    public TargetQuotaIncarnation descriptor(final TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var descriptor = TargetQuotaIncarnation.decodeForStore(
                key,
                payload(ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!descriptor.identity().equals(identity)) {
            throw new IllegalStateException("descriptor identity mismatch");
        }
        requireOwner(descriptor);
        return descriptor;
    }

    private void requireOwner(final TargetQuotaIncarnation owner) {
        if (!owner.identity().shard().equals(scope.shard())
                || !Arrays.equals(owner.tenantScope(), scope.tenantScope())
                || !Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("record accounting crossed Shard/tenant/lineage");
        }
        owner.latestMutation().requireAtOrBefore(operation);
    }

    private TargetQuotaIncarnation targetOwner(final TargetPartitionId target, final byte[] incarnation) {
        return descriptor(
                new TargetQuotaIdentity(TargetQuotaIdentity.Kind.TARGET, scope.shard(), incarnation, target, null));
    }

    private TargetQuotaPayloadOwner payloadOwner(final DelayMessageId messageId) {
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1}, messageId.bytes());
        return TargetQuotaPayloadOwner.decodeForStore(
                key,
                payload(ColumnFamily.META, key, TargetQuotaPayloadOwner.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
    }

    private TargetMessageRecord message(final DelayMessageId messageId) {
        final byte[] key = TargetKeyCodec.message(messageId);
        return TargetMessageRecord.decodeForStore(
                key, payload(ColumnFamily.ID, key, TargetMessageRecord.VALUE_TYPE), scope.shard());
    }

    private TargetOrderState order(final TargetPartitionId target, final byte[] orderingDomain) {
        final byte[] key = TargetKeyCodec.orderState(target, orderingDomain);
        final var state = TargetOrderState.decode(payload(ColumnFamily.META, key, TargetOrderState.VALUE_TYPE));
        if (!Arrays.equals(key, state.encodedKey())) {
            throw new IllegalStateException("strict accounting key mismatch");
        }
        return state;
    }

    private CanonicalTargetPartition physical(final TargetPartitionId target) {
        final byte[] key = TargetKeyCodec.identity(target);
        return CanonicalTargetPartition.decodeForStore(
                key, payload(ColumnFamily.META, key, CanonicalTargetPartition.VALUE_TYPE));
    }

    private Charge shared(
            final TargetQuotaMetadataRecords.SharedRole role,
            final TargetPartitionId target,
            final byte[] key,
            final byte[] payload) {
        final var targetScope = scope.forTarget(target);
        final byte[] grantKey =
                Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, 1}, targetScope.keySuffix());
        final var grant = TargetQuotaGrantActivation.decodeForStore(
                grantKey,
                payload(ColumnFamily.META, grantKey, TargetQuotaGrantActivation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (grant.allocation() == null) {
            throw new IllegalStateException("shared metadata lacks first allocation origin");
        }
        return metadata(TargetQuotaMetadataRecords.shared(
                role, grant, descriptor(grant.allocation().identity()), physical(target), key, payload));
    }

    private Charge messageCharge(final TargetQuotaPayloadOwner owner, final TargetQuotaMessageRecords.Record record) {
        final var descriptor = descriptor(owner.primaryIdentity());
        descriptor.requirePayloadOwner(owner);
        return new Charge(descriptor, resources(record.charge()));
    }

    private Charge metadata(final TargetQuotaMetadataRecords.Record record) {
        record.requireCurrent(key -> {
            final byte[] raw = reader.projected(ColumnFamily.META, key, overlay);
            if (raw == null) {
                return null;
            }
            final var value = TargetValueEnvelope.decodeAny(raw);
            return new TargetQuotaMetadataRecords.Stored(value.valueType(), value.payload());
        });
        return new Charge(record.owner(), record.contribution());
    }

    private byte[] payload(final ColumnFamily family, final byte[] key, final int type) {
        final byte[] raw = reader.projected(family, key, overlay);
        if (raw == null) {
            throw new IllegalStateException("missing frozen accounting dependency");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static void requireFamily(final ColumnFamily actual, final ColumnFamily expected) {
        if (actual != expected) {
            throw new IllegalArgumentException("business accounting CF/type mismatch");
        }
    }

    public static TargetQuotaUsage resources(final CapacityVector resources) {
        return new TargetQuotaUsage(resources, 0, 0, 0, 0);
    }
}
