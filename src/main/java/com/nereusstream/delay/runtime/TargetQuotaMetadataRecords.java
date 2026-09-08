package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact META record contributions and bounded attribution read sets; no source, publication or deletion authority. */
public final class TargetQuotaMetadataRecords {
    public static final int MAX_READ_RECORDS = 4;

    private TargetQuotaMetadataRecords() {}

    /** These immutable records lack an incarnation; the first Target allocation owns them for their lifetime. */
    public enum SharedRole {
        IDENTITY,
        DISPATCH,
        CONTROL,
        MEMBERSHIP_GRANT,
        MEMBERSHIP_POLICY
    }

    /** The Store adapter validates the NV envelope before exposing its type and canonical payload. */
    public record Stored(int valueType, byte[] canonicalPayload) {
        public Stored {
            canonicalPayload = Bytes.copy(Objects.requireNonNull(canonicalPayload, "canonicalPayload"));
        }

        @Override
        public byte[] canonicalPayload() {
            return Bytes.copy(canonicalPayload);
        }
    }

    /** Point reads of actual META records under the same Owner/Store/source guard through atomic commit. */
    @FunctionalInterface
    public interface ReadView {
        Stored get(byte[] key);
    }

    private record Point(byte[] key, Stored value) {
        private Point {
            key = Bytes.copy(key);
        }

        private void requireCurrent(final ReadView view) {
            final var actual = view.get(Bytes.copy(key));
            if (actual == null
                    || actual.valueType != value.valueType
                    || !Arrays.equals(actual.canonicalPayload, value.canonicalPayload)) {
                throw new IllegalStateException("metadata record or its frozen attribution read set changed");
            }
        }
    }

    public static final class Record {
        private final Point record;
        private final TargetQuotaIncarnation owner;
        private final TargetQuotaUsage contribution;
        private final List<Point> readSet;

        private Record(
                final int type,
                final byte[] key,
                final byte[] payload,
                final TargetQuotaIncarnation owner,
                final TargetQuotaUsage contribution,
                final Point... dependencies) {
            record = point(type, key, payload);
            this.owner = owner;
            this.contribution = contribution;
            final var points = new ArrayList<Point>();
            points.add(record);
            points.add(point(TargetQuotaIncarnation.VALUE_TYPE, owner.key(), owner.canonicalBytes()));
            for (var dependency : dependencies) {
                final var prior = points.stream()
                        .filter(p -> Arrays.equals(p.key, dependency.key))
                        .findFirst();
                if (prior.isPresent()) {
                    if (prior.get().value.valueType != dependency.value.valueType
                            || !Arrays.equals(prior.get().value.canonicalPayload, dependency.value.canonicalPayload)) {
                        throw new IllegalArgumentException("metadata dependency conflicts with the exact record view");
                    }
                } else {
                    points.add(dependency);
                }
            }
            if (points.size() > MAX_READ_RECORDS) {
                throw new IllegalArgumentException("metadata attribution exceeds its fixed point-read bound");
            }
            readSet = List.copyOf(points);
        }

        public int valueType() {
            return record.value.valueType;
        }

        public byte[] key() {
            return Bytes.copy(record.key);
        }

        public byte[] canonicalPayload() {
            return record.value.canonicalPayload();
        }

        public TargetQuotaIncarnation owner() {
            return owner;
        }

        public TargetQuotaUsage contribution() {
            return contribution;
        }

        public int readSetSize() {
            return readSet.size();
        }

        /** Recheck both the record and every retained ownership dependency; does not enumerate the Store. */
        public void requireCurrent(final ReadView view) {
            Objects.requireNonNull(view, "view");
            for (var point : readSet) {
                point.requireCurrent(view);
            }
        }
    }

    public static Record shared(
            final SharedRole role,
            final TargetQuotaGrantActivation grant,
            final TargetQuotaIncarnation owner,
            final CanonicalTargetPartition physicalIdentity,
            final byte[] key,
            final byte[] payload) {
        requireSharedOrigin(grant, owner);
        requireTarget(owner, physicalIdentity.id());
        final int type;
        switch (Objects.requireNonNull(role, "role")) {
            case IDENTITY -> {
                final var identity = CanonicalTargetPartition.decodeForStore(key, payload);
                if (!Arrays.equals(identity.canonicalBytes(), physicalIdentity.canonicalBytes())) {
                    throw new IllegalArgumentException("metadata physical identity differs from the exact view");
                }
                type = CanonicalTargetPartition.VALUE_TYPE;
            }
            case DISPATCH -> {
                TargetDispatchCompatibility.decodeForStore(key, payload, physicalIdentity);
                type = TargetDispatchCompatibility.VALUE_TYPE;
            }
            case CONTROL -> {
                TargetControlScope.decodeForStore(
                        key, payload, physicalIdentity.id(), owner.identity().shard());
                type = TargetControlScope.VALUE_TYPE;
            }
            case MEMBERSHIP_GRANT -> {
                final var member = TargetMembershipGrant.decodeForStore(
                        key, payload, owner.identity().shard());
                requireTenant(owner, member.tenantScope());
                member.required().requireTargetProjection(physicalIdentity);
                member.offered().requireTargetProjection(physicalIdentity);
                requireLaterSource(owner, member.activationSource());
                type = TargetMembershipGrant.VALUE_TYPE;
            }
            case MEMBERSHIP_POLICY -> {
                final var policy = TargetMembershipPolicy.decodeForStore(
                        key, payload, owner.identity().shard());
                requireTenant(owner, policy.tenantScope());
                policy.offered().requireTargetProjection(physicalIdentity);
                type = TargetMembershipPolicy.VALUE_TYPE;
            }
            default -> throw new IllegalArgumentException("unknown shared metadata role");
        }
        return new Record(
                type,
                key,
                payload,
                owner,
                stateCharge(owner, key, payload),
                point(TargetQuotaGrantActivation.VALUE_TYPE, grant.key(), grant.canonicalBytes()),
                identityPoint(physicalIdentity));
    }

    public static Record queue(
            final TargetQuotaIncarnation owner,
            final CanonicalTargetPartition identity,
            final int maximumDomains,
            final byte[] key,
            final byte[] payload) {
        return new Record(
                TargetQueueState.VALUE_TYPE,
                key,
                payload,
                owner,
                owner.queueContribution(key, payload, identity, maximumDomains),
                identityPoint(identity));
    }

    public static Record strictDomain(final TargetQuotaIncarnation owner, final byte[] key, final byte[] payload) {
        return new Record(
                TargetOrderState.VALUE_TYPE, key, payload, owner, owner.strictDomainContribution(key, payload));
    }

    public static Record channel(final TargetQuotaIncarnation owner, final byte[] key, final byte[] payload) {
        final var channel = TargetChannelIdentity.decodeForStore(
                key, payload, owner.identity().shard());
        requireIncarnation(owner, channel.context().target(), channel.context().accountingIncarnation());
        return new Record(TargetChannelIdentity.VALUE_TYPE, key, payload, owner, stateCharge(owner, key, payload));
    }

    public static Record nativeScope(final TargetQuotaIncarnation owner, final byte[] key, final byte[] payload) {
        final var scope = TargetNativePolicyScope.decodeForStore(
                key, payload, owner.identity().shard());
        requireIncarnation(owner, scope.target(), scope.accountingIncarnation());
        return new Record(TargetNativePolicyScope.VALUE_TYPE, key, payload, owner, stateCharge(owner, key, payload));
    }

    public static Record nativeSnapshot(
            final TargetQuotaIncarnation owner,
            final TargetNativePolicyScope scope,
            final byte[] key,
            final byte[] payload) {
        requireIncarnation(owner, scope.target(), scope.accountingIncarnation());
        TargetNativePolicySnapshot.decodeForStore(
                key, payload, scope, owner.identity().shard());
        return new Record(
                TargetNativePolicySnapshot.VALUE_TYPE,
                key,
                payload,
                owner,
                stateCharge(owner, key, payload),
                point(TargetNativePolicyScope.VALUE_TYPE, scope.encodedKey(), scope.canonicalBytes()));
    }

    private static void requireSharedOrigin(
            final TargetQuotaGrantActivation grant, final TargetQuotaIncarnation owner) {
        Objects.requireNonNull(grant, "grant");
        Objects.requireNonNull(owner, "owner");
        final var origin = grant.allocation();
        if (origin == null
                || owner.identity().target() == null
                || !origin.identity().equals(owner.identity())
                || !origin.allocation().equals(owner.allocation())
                || !Arrays.equals(origin.recoveryLineage(), owner.recoveryLineage())
                || !Arrays.equals(origin.tenantScope(), owner.tenantScope())
                || !Arrays.equals(
                        origin.accounting().canonicalBytes(), owner.accounting().canonicalBytes())) {
            throw new IllegalArgumentException(
                    "shared metadata must retain the complete first Target allocation owner");
        }
    }

    private static void requireTarget(final TargetQuotaIncarnation owner, final TargetPartitionId target) {
        if (!Objects.equals(owner.identity().target(), target)) {
            throw new IllegalArgumentException("metadata belongs to a different Target");
        }
    }

    private static void requireIncarnation(
            final TargetQuotaIncarnation owner, final TargetPartitionId target, final byte[] incarnation) {
        requireTarget(owner, target);
        if (!Arrays.equals(owner.identity().accountingIncarnation(), incarnation)) {
            throw new IllegalArgumentException("metadata cannot move to another incarnation under the same Target");
        }
    }

    private static void requireTenant(final TargetQuotaIncarnation owner, final byte[] tenant) {
        if (!Arrays.equals(owner.tenantScope(), tenant)) {
            throw new IllegalArgumentException("metadata belongs to another tenant routing scope");
        }
    }

    private static void requireLaterSource(final TargetQuotaIncarnation owner, final SourcePosition source) {
        if (source.compareTo(owner.allocation().source()) <= 0) {
            throw new IllegalArgumentException(
                    "membership activation must follow the initial Target allocation source");
        }
    }

    private static TargetQuotaUsage stateCharge(
            final TargetQuotaIncarnation owner, final byte[] key, final byte[] payload) {
        return new TargetQuotaUsage(
                owner.accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length),
                0,
                0,
                0,
                0);
    }

    private static Point point(final int type, final byte[] key, final byte[] payload) {
        return new Point(key, new Stored(type, payload));
    }

    private static Point identityPoint(final CanonicalTargetPartition identity) {
        return point(
                CanonicalTargetPartition.VALUE_TYPE, TargetKeyCodec.identity(identity.id()), identity.canonicalBytes());
    }
}
