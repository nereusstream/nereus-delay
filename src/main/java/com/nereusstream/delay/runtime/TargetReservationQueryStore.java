package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Bounded durable reservation reads. Routing, public query overlays and upload authorization remain caller-owned. */
public final class TargetReservationQueryStore {
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;

    public TargetReservationQueryStore(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null || Arrays.equals(lineage, new byte[16])) {
            throw new IllegalArgumentException("reservation query requires explicit Shard/lineage");
        }
        this.lineage = Bytes.copy(lineage);
    }

    /** No snapshot is exposed until complete validates this exact Store view under the current read authority. */
    public static final class Prepared {
        private final TargetReservationQueryStore owner;
        private final TargetStoreBackend.ReadPlan<Optional<Snapshot>> plan;

        private Prepared(TargetReservationQueryStore owner, TargetStoreBackend.ReadPlan<Optional<Snapshot>> plan) {
            this.owner = owner;
            this.plan = plan;
        }
    }

    public Prepared prepare(BoundedReadBudget budget, byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        final byte[] id = Bytes.copy(reservationId);
        return new Prepared(this, backend.prepareRead(budget, reader -> read(reader, id)));
    }

    /** Local routing preflight; no Store or external authority reads. */
    public void requireShard(com.nereusstream.delay.protocol.ShardId shard) {
        if (!scope.shard().equals(Objects.requireNonNull(shard, "shard"))) {
            throw new IllegalArgumentException("reservation query targets another Shard");
        }
    }

    /** Event-loop read under one authority guard acquired before any projection is loaded. */
    public Optional<Snapshot> read(
            BoundedReadBudget budget, byte[] reservationId, TargetStoreBackend.ReadAuthority authority) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        final byte[] id = Bytes.copy(reservationId);
        return backend.guardedRead(budget, reader -> read(reader, id), authority);
    }

    public Optional<Snapshot> complete(Prepared prepared, TargetStoreBackend.ReadAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign reservation query plan");
        }
        return backend.completeRead(prepared.plan, authority);
    }

    private Optional<Snapshot> read(TargetStoreBackend.Reader reader, byte[] id) {
        if (!reader.shardId().equals(scope.shard()) || reader.source() == null) {
            throw new IllegalStateException("reservation query requires its established Shard source");
        }
        final var aggregate = reader.aggregate();
        if (aggregate.mutation() == null
                || aggregate.mutation().sequence() != reader.sourceSequence()
                || !Arrays.equals(
                        aggregate.mutation().source().canonicalBytes(),
                        reader.source().canonicalBytes())) {
            throw new IllegalStateException("reservation query aggregate differs from the actual source frontier");
        }
        requireOwner(
                reader,
                new TargetQuotaIdentity(
                        TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null));
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_LOOKUP_TAG, 1}, id);
        final byte[] raw = reader.get(ColumnFamily.ID, key);
        if (raw == null) {
            return Optional.empty();
        }
        final var record =
                TargetReservationRecord.decode(TargetValueEnvelope.decode(raw, TargetReservationRecord.VALUE_TYPE)
                        .payload());
        if (!Arrays.equals(key, record.lookupKey())
                || !Arrays.equals(lineage, record.recoveryLineage())
                || !record.locator().messageId().routingId().shardId().equals(scope.shard())
                || !Arrays.equals(
                        record.canonicalBytes(),
                        payload(reader, ColumnFamily.ID, record.key(), TargetReservationRecord.VALUE_TYPE))) {
            throw new IllegalStateException("reservation lookup differs from its full primary identity");
        }
        record.mutation().requireAtOrBefore(aggregate.mutation());
        final byte[] payloadKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, 1},
                record.locator().messageId().bytes());
        final var payloadOwner = TargetQuotaPayloadOwner.decodeForStore(
                payloadKey,
                payload(reader, ColumnFamily.META, payloadKey, TargetQuotaPayloadOwner.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        record.requireOwner(payloadOwner);
        payloadOwner.mutation().requireAtOrBefore(aggregate.mutation());
        requireOwner(reader, payloadOwner.primaryIdentity()).requirePayloadOwner(payloadOwner);
        final byte[] bindingKey =
                TargetKeyCodec.scheduleBinding(record.locator().scheduleBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                payload(reader, ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                scope.shard());
        record.requireBinding(binding);
        payloadOwner.requireInitialBinding(binding);
        final byte[] expiry = reader.get(ColumnFamily.TIMELINE, record.expiryKey());
        if (record.status() == PayloadReservationStatus.RESERVED) {
            if (expiry == null
                    || !Arrays.equals(
                            record.canonicalBytes(),
                            TargetValueEnvelope.decode(expiry, TargetReservationRecord.VALUE_TYPE)
                                    .payload())) {
                throw new IllegalStateException("reserved query lacks its exact expiry index");
            }
        } else if (expiry != null) {
            throw new IllegalStateException("terminal query retains reservation expiry");
        }
        return Optional.of(new Snapshot(
                record,
                PrepareLargeScheduleBody.decode(binding.canonicalBody()),
                payloadOwner.phase(),
                reader.source()));
    }

    private TargetQuotaIncarnation requireOwner(TargetStoreBackend.Reader reader, TargetQuotaIdentity identity) {
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner = TargetQuotaIncarnation.decodeForStore(
                key,
                payload(reader, ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!owner.identity().equals(identity) || !Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("reservation query owner identity/lineage differs");
        }
        owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());
        return owner;
    }

    private static byte[] payload(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key, int type) {
        final byte[] raw = reader.get(family, key);
        if (raw == null) {
            throw new IllegalStateException("reservation query lacks a required durable projection");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    /** Service-owned immutable object location from the pinned Object Store adapter, never a client-selected path. */
    public record ReceiptLocation(ProfileRef profile, byte[] container, byte[] objectKey) {
        public ReceiptLocation {
            Objects.requireNonNull(profile, "profile");
            if (profile.profileKind() != ProfileKind.OBJECT_STORE
                    || container == null
                    || container.length == 0
                    || container.length > 1024
                    || objectKey == null
                    || objectKey.length == 0
                    || objectKey.length > 4096) {
                throw new IllegalArgumentException("invalid reservation receipt location");
            }
            container = Bytes.copy(container);
            objectKey = Bytes.copy(objectKey);
        }

        @Override
        public byte[] container() {
            return Bytes.copy(container);
        }

        @Override
        public byte[] objectKey() {
            return Bytes.copy(objectKey);
        }
    }

    /** A durable point-in-time snapshot, not permission to issue a fresh upload handle or override control overlays. */
    public static final class Snapshot {
        private final TargetReservationRecord record;
        private final PrepareLargeScheduleBody prepare;
        private final TargetQuotaPayloadOwner.Phase payloadPhase;
        private final SourcePosition readSource;

        private Snapshot(
                TargetReservationRecord record,
                PrepareLargeScheduleBody prepare,
                TargetQuotaPayloadOwner.Phase payloadPhase,
                SourcePosition readSource) {
            this.record = record;
            this.prepare = prepare;
            this.payloadPhase = payloadPhase;
            this.readSource = readSource;
        }

        public TargetReservationRecord reservation() {
            return record;
        }

        public TargetQuotaPayloadOwner.Phase payloadPhase() {
            return payloadPhase;
        }

        public SourcePosition readSource() {
            return readSource;
        }

        /** Reconstructs the immutable Prepare receipt; lifecycle changes never change its source or state version. */
        public PayloadReservationReceipt receipt(ReceiptLocation location) {
            Objects.requireNonNull(location, "location");
            if (!location.profile().equals(prepare.objectStoreProfile())
                    || (record.committedPayload() != null
                            && (!Arrays.equals(record.committedPayload().container(), location.container())
                                    || !Arrays.equals(record.committedPayload().objectKey(), location.objectKey())))) {
                throw new IllegalArgumentException("receipt location differs from the pinned object identity");
            }
            return PayloadReservationReceipt.create(
                    record.reservationId(),
                    record.locator().messageId(),
                    record.prepareAnchor().source().shardId(),
                    record.prepareAnchor().source(),
                    1,
                    prepare.objectStoreProfile(),
                    location.container(),
                    location.objectKey(),
                    prepare.expectedPayloadLength(),
                    prepare.payloadSha256(),
                    record.expiryEpochMs(),
                    prepare.trustSet());
        }
    }
}
