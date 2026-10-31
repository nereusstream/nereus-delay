package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact membership history from the same bounded Store view as source control or first binding. */
public final class TargetMembershipStoreAuthority {
    private TargetMembershipStoreAuthority() {}

    /**
     * Existing imported work is authorized by its actually APPLIED migration and exact immutable import graph.
     * The original import/cold authority authenticates full sealed mapping/raw/history; this hot check reads
     * those accepted Native projections, never inventing a normal grant or accepting a proposed digest.
     */
    public static void requireImportedBinding(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope shardScope,
            byte[] lineage,
            TargetScheduleBinding binding) {
        Objects.requireNonNull(binding);
        if (!binding.imported()) {
            return;
        }
        final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
            @Override
            public com.nereusstream.delay.protocol.ShardId shardId() {
                return reader.shardId();
            }

            @Override
            public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("hot migration authority requires actual persisted rows");
                }
                return reader.get(family, key);
            }
        };
        final var root = TargetMigrationStore.requireActivated(
                view, List.of(), shardScope, lineage, reader.aggregate().mutation());
        final var plan = root.plan();
        if (!Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !Arrays.equals(reader.metadata().storeIncarnation(), plan.newStore())
                || !Arrays.equals(reader.metadata().dbIdentity(), plan.newDbIdentity())) {
            throw new IllegalStateException("imported binding changed actual activated Store/plan identity");
        }
        final byte[] bindingKey = binding.encodedKey();
        final byte[] raw =
                Objects.requireNonNull(reader.get(ColumnFamily.ID, bindingKey), "complete retained imported binding");
        if (!Arrays.equals(
                raw, TargetValueEnvelope.encode(TargetScheduleBinding.VALUE_TYPE, binding.canonicalBytes()))) {
            throw new IllegalStateException("imported execution cannot use a caller-proposed binding");
        }
        final var origin = TargetMigrationStore.origin(view, List.of(), plan, ColumnFamily.ID, bindingKey);
        origin.requireRow(ColumnFamily.ID, bindingKey, raw);
        if (!origin.oldRecord().equals(binding.migrationRecord())
                || !Arrays.equals(origin.owner().identity().accountingIncarnation(), binding.accountingIncarnation())
                || !Objects.equals(origin.owner().identity().target(), binding.target())) {
            throw new IllegalStateException(
                    "imported binding changed its complete original conversion/fee-owner closure");
        }
        if (binding.schemaVersion() == 2) {
            if (binding.bindingSource().compareTo(plan.baseline().source()) > 0) {
                throw new IllegalStateException("baseline import binding is later than original C");
            }
        } else {
            final var actual = Objects.requireNonNull(binding.migrationSource(), "actual legacy prefix Source");
            final byte[] prefixKey = TargetMigrationPrefixRecord.keyFor(plan.migrationId(), actual.sequence());
            final byte[] prefixRaw = Objects.requireNonNull(
                    reader.get(ColumnFamily.META, prefixKey), "complete applied migration prefix");
            final var prefix = TargetMigrationPrefixRecord.decode(
                    TargetValueEnvelope.decode(prefixRaw, TargetMigrationPrefixRecord.VALUE_TYPE)
                            .payload());
            prefix.requirePlan(plan);
            if (!Arrays.equals(prefixKey, prefix.key()) || !prefix.at().equals(actual)) {
                throw new IllegalStateException("prefix binding changed exact original applied Source history");
            }
            actual.requireAtOrBefore(reader.aggregate().mutation());
        }
        final byte[] identityKey = TargetKeyCodec.identity(binding.target());
        final var physical = CanonicalTargetPartition.decodeForStore(
                identityKey, importedPayload(reader, view, plan, identityKey, CanonicalTargetPartition.VALUE_TYPE));
        final byte[] requiredKey = TargetKeyCodec.dispatchCompatibility(binding.requiredDispatchRef());
        final byte[] offeredKey = TargetKeyCodec.dispatchCompatibility(binding.offeredDispatchRef());
        final var required = TargetDispatchCompatibility.decodeReferenced(
                binding.requiredDispatchRef(),
                importedPayload(reader, view, plan, requiredKey, TargetDispatchCompatibility.VALUE_TYPE),
                physical);
        final var offered = TargetDispatchCompatibility.decodeReferenced(
                binding.offeredDispatchRef(),
                importedPayload(reader, view, plan, offeredKey, TargetDispatchCompatibility.VALUE_TYPE),
                physical);
        final byte[] controlsKey = TargetKeyCodec.controlScope(binding.controlScopeRef());
        final var controls = TargetControlScope.decodeReferenced(
                binding.controlScopeRef(),
                importedPayload(reader, view, plan, controlsKey, TargetControlScope.VALUE_TYPE),
                binding.target(),
                shardScope.shard());
        required.requireTargetProjection(physical);
        offered.requireTargetProjection(physical);
        if (!offered.canServe(required)
                || !controls.target().equals(binding.target())
                || !controls.sourceShard().equals(shardScope.shard())) {
            throw new IllegalStateException(
                    "imported work changed original mapped dispatch/control/physical authority");
        }
    }

    private static byte[] importedPayload(
            TargetStoreBackend.Reader reader,
            TargetRecordAccounting.View view,
            com.nereusstream.delay.protocol.TargetMigrationPlan plan,
            byte[] key,
            int type) {
        final byte[] raw =
                Objects.requireNonNull(reader.get(ColumnFamily.META, key), "full imported execution material");
        TargetMigrationStore.requireBaselineRow(view, plan, ColumnFamily.META, key, raw);
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    /** Initial migration C allocation is explicit; all later Source/local stamps retain ordinary attribution. */
    public static void requirePayloadOwner(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            com.nereusstream.delay.protocol.TargetQuotaIncarnation descriptor,
            com.nereusstream.delay.protocol.TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding) {
        if (binding.schemaVersion() == 2
                && !owner.mutation().isLocalMutation()
                && owner.mutation().sequence() == descriptor.allocation().sequence()
                && !owner.mutation().equals(descriptor.allocation())) {
            requireImportedBinding(reader, scope, lineage, binding);
            final var root = TargetMigrationStore.decodeRoot(reader.get(
                    ColumnFamily.META, com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard())));
            descriptor.requireImportedPayloadOwner(
                    owner, binding, Objects.requireNonNull(root).plan());
        } else {
            descriptor.requirePayloadOwner(owner);
        }
    }

    public static void requireImportedReservation(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetReservationRecord reservation,
            TargetScheduleBinding binding) {
        if (reservation.migrationOrigin() == null) {
            return;
        }
        requireImportedBinding(reader, scope, lineage, binding);
        final var root = Objects.requireNonNull(TargetMigrationStore.decodeRoot(reader.get(
                ColumnFamily.META, com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(scope.shard()))));
        reservation.migrationOrigin().requirePlan(root.plan(), binding);
        if (reservation.status() == PayloadReservationStatus.RESERVED
                || reservation.mutation().equals(reservation.migrationOrigin().importedAt())) {
            final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
                @Override
                public com.nereusstream.delay.protocol.ShardId shardId() {
                    return reader.shardId();
                }

                @Override
                public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                    if (!overlay.isEmpty()) {
                        throw new IllegalArgumentException("reservation import authority requires persisted rows");
                    }
                    return reader.get(family, key);
                }
            };
            final var locations = new java.util.ArrayList<java.util.Map.Entry<ColumnFamily, byte[]>>();
            locations.add(new java.util.AbstractMap.SimpleImmutableEntry<>(ColumnFamily.ID, reservation.key()));
            locations.add(new java.util.AbstractMap.SimpleImmutableEntry<>(ColumnFamily.ID, reservation.lookupKey()));
            if (reservation.status() == PayloadReservationStatus.RESERVED) {
                locations.add(new java.util.AbstractMap.SimpleImmutableEntry<>(
                        ColumnFamily.ID, reservation.targetIndexKey()));
                locations.add(new java.util.AbstractMap.SimpleImmutableEntry<>(
                        ColumnFamily.TIMELINE, reservation.expiryKey()));
            }
            for (var location : locations) {
                final byte[] raw = Objects.requireNonNull(
                        reader.get(location.getKey(), location.getValue()), "complete imported reservation projection");
                TargetMigrationStore.origin(view, List.of(), root.plan(), location.getKey(), location.getValue())
                        .requireRow(location.getKey(), location.getValue(), raw);
            }
        }
    }

    public static TargetMembershipAuthority.AppliedGrant resolve(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope shardScope,
            final byte[] lineage,
            final byte[] grantRef) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(shardScope, "shardScope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (shardScope.target() != null || !shardScope.shard().equals(reader.shardId())) {
            throw new IllegalArgumentException("membership Store authority needs its exact Shard scope");
        }
        final byte[] grantKey = TargetKeyCodec.membershipGrant(grantRef);
        final byte[] closureKey = TargetKeyCodec.membershipClosure(grantRef);
        final byte[] rawGrant = reader.get(ColumnFamily.META, grantKey);
        final byte[] rawClosure = reader.get(ColumnFamily.META, closureKey);
        if (rawGrant == null) {
            if (rawClosure != null) {
                throw new IllegalStateException("membership closure has no durable grant");
            }
            return null;
        }
        final var grant = TargetMembershipGrant.decodeForStore(
                grantKey,
                TargetValueEnvelope.decode(rawGrant, TargetMembershipGrant.VALUE_TYPE)
                        .payload(),
                shardScope.shard());
        final byte[] policyKey = TargetKeyCodec.membershipPolicy(grant.authorityPolicyRef());
        final byte[] rawPolicy = reader.get(ColumnFamily.META, policyKey);
        if (rawPolicy == null) {
            throw new IllegalStateException("membership grant lacks its durable policy");
        }
        final var policy = TargetMembershipPolicy.decodeForStore(
                policyKey,
                TargetValueEnvelope.decode(rawPolicy, TargetMembershipPolicy.VALUE_TYPE)
                        .payload(),
                shardScope.shard());
        policy.requireGrant(grant);
        final var frontier = reader.source();
        if (!Arrays.equals(grant.tenantScope(), shardScope.tenantScope()) || frontier == null) {
            throw new IllegalStateException("membership grant differs from Store tenant/source frontier");
        }
        final int order = grant.activationSource().compareTo(frontier);
        if (order > 0
                || (order == 0
                        && !Arrays.equals(grant.activationSource().canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("membership grant is ahead of Store source frontier");
        }
        if (rawClosure == null) {
            return new TargetMembershipAuthority.AppliedGrant(grant, null);
        }
        final var closure = TargetMembershipClosureRecord.decodeForStore(
                closureKey,
                TargetValueEnvelope.decode(rawClosure, TargetMembershipClosureRecord.VALUE_TYPE)
                        .payload(),
                shardScope.shard(),
                lineage);
        closure.requireGrant(grant);
        closure.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final int closureOrder = closure.closedAt().compareTo(frontier);
        if (closureOrder > 0
                || (closureOrder == 0
                        && !Arrays.equals(closure.closedAt().canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("membership closure is ahead of Store source frontier");
        }
        return new TargetMembershipAuthority.AppliedGrant(grant, closure.closedAt());
    }
}
