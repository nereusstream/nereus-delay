package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Closed Message-family STATE charges derived from validated full record bytes and a frozen payload owner. */
public final class TargetQuotaMessageRecords {
    public static final int MAX_RECORDS_PER_MESSAGE = 6;

    private TargetQuotaMessageRecords() {}

    /** Roles are local inspection results, not new wire or storage discriminators. */
    public enum Role {
        PAYLOAD_OWNER,
        INITIAL_BINDING,
        MESSAGE,
        DUE,
        NATIVE,
        ORDERED,
        ORDER_HEAD,
        EXPIRY
    }

    /**
     * Immutable point-record evidence. The Store adapter must first validate its NV envelope and keep the
     * owner, Message, shared order state and source/permission read set valid through the actual atomic write.
     * The charge is not a grant, source authorization, deletion proof or complete ledger reconstruction.
     */
    public static final class Record {
        private final Role role;
        private final ColumnFamily family;
        private final int valueType;
        private final TargetQuotaPayloadOwner owner;
        private final byte[] key;
        private final byte[] payload;
        private final byte[] messageDigest;
        private final CapacityVector charge;

        private Record(
                final Role role,
                final ColumnFamily family,
                final int valueType,
                final TargetQuotaPayloadOwner owner,
                final byte[] key,
                final byte[] payload,
                final TargetMessageRecord message) {
            this.role = role;
            this.family = family;
            this.valueType = valueType;
            this.owner = owner;
            this.key = Bytes.copy(key);
            this.payload = Bytes.copy(payload);
            messageDigest = message == null ? null : message.digest();
            charge = owner.accounting()
                    .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length);
        }

        public Role role() {
            return role;
        }

        public ColumnFamily family() {
            return family;
        }

        public int valueType() {
            return valueType;
        }

        public TargetQuotaPayloadOwner owner() {
            return owner;
        }

        public byte[] key() {
            return Bytes.copy(key);
        }

        public byte[] canonicalPayload() {
            return Bytes.copy(payload);
        }

        public CapacityVector charge() {
            return charge;
        }

        /** Exact recheck against the actual decoded Store value, not a caller-reported byte count. */
        public void requireStored(
                final ColumnFamily actualFamily,
                final int actualValueType,
                final byte[] actualKey,
                final byte[] actualPayload) {
            if (family != actualFamily
                    || valueType != actualValueType
                    || !Arrays.equals(key, actualKey)
                    || !Arrays.equals(payload, actualPayload)) {
                throw new IllegalStateException("Message record changed or belongs to another storage role");
            }
        }
    }

    public static Record payloadOwner(
            final TargetQuotaPayloadOwner owner, final byte[] key, final byte[] canonicalPayload) {
        final var decoded = TargetQuotaPayloadOwner.decodeForStore(
                key, canonicalPayload, owner.primaryIdentity().shard(), owner.tenantScope());
        if (!Arrays.equals(owner.canonicalBytes(), decoded.canonicalBytes())) {
            throw new IllegalArgumentException("payload owner record is not the exact accounting view");
        }
        return new Record(
                Role.PAYLOAD_OWNER,
                ColumnFamily.META,
                TargetQuotaPayloadOwner.VALUE_TYPE,
                owner,
                key,
                canonicalPayload,
                null);
    }

    public static Record initialBinding(
            final TargetQuotaPayloadOwner owner, final byte[] key, final byte[] canonicalPayload) {
        final var binding = TargetScheduleBinding.decodeForStore(
                key, canonicalPayload, owner.primaryIdentity().shard());
        owner.requireInitialBinding(binding);
        return new Record(
                Role.INITIAL_BINDING,
                ColumnFamily.ID,
                TargetScheduleBinding.VALUE_TYPE,
                owner,
                key,
                canonicalPayload,
                null);
    }

    public static Record message(final TargetQuotaPayloadOwner owner, final byte[] key, final byte[] canonicalPayload) {
        final var message = TargetMessageRecord.decodeForStore(
                key, canonicalPayload, owner.primaryIdentity().shard());
        requireMessage(owner, message);
        return new Record(
                Role.MESSAGE, ColumnFamily.ID, TargetMessageRecord.VALUE_TYPE, owner, key, canonicalPayload, message);
    }

    public static Record timeline(
            final TargetQuotaPayloadOwner owner,
            final TargetMessageRecord message,
            final byte[] key,
            final byte[] canonicalPayload) {
        requireMessage(owner, message);
        message.requireTimelineProjection(key, canonicalPayload);
        final Role role =
                switch (Byte.toUnsignedInt(key[0])) {
                    case TargetKeyCodec.DUE_TAG -> Role.DUE;
                    case TargetKeyCodec.NATIVE_TAG -> Role.NATIVE;
                    case TargetKeyCodec.ORDERED_TAG -> Role.ORDERED;
                    default -> throw new IllegalArgumentException("unsupported Message timeline role");
                };
        return new Record(
                role, ColumnFamily.TIMELINE, TargetTimelineWorkRef.VALUE_TYPE, owner, key, canonicalPayload, message);
    }

    public static Record orderHead(
            final TargetQuotaPayloadOwner owner,
            final TargetMessageRecord message,
            final TargetOrderState orderState,
            final byte[] key,
            final byte[] canonicalPayload) {
        requireMessage(owner, message);
        Objects.requireNonNull(orderState, "orderState").requireServiceableProjection(key, canonicalPayload, message);
        return new Record(
                Role.ORDER_HEAD,
                ColumnFamily.TIMELINE,
                TargetTimelineWorkRef.VALUE_TYPE,
                owner,
                key,
                canonicalPayload,
                message);
    }

    public static Record expiry(
            final TargetQuotaPayloadOwner owner,
            final TargetMessageRecord message,
            final byte[] key,
            final byte[] canonicalPayload) {
        requireMessage(owner, message);
        TargetExpiryRef.decodeForMessage(key, canonicalPayload, message);
        return new Record(
                Role.EXPIRY, ColumnFamily.TIMELINE, TargetExpiryRef.VALUE_TYPE, owner, key, canonicalPayload, message);
    }

    private static void requireMessage(final TargetQuotaPayloadOwner owner, final TargetMessageRecord message) {
        Objects.requireNonNull(owner, "owner").requireMessagePayload(message);
        // Comparison closes kind/Shard/full physical source identity. A later Reschedule need not rewrite the owner.
        if (owner.mutation().source().compareTo(message.scheduleSource()) == 0
                && !Arrays.equals(
                        owner.mutation().source().canonicalBytes(),
                        message.scheduleSource().canonicalBytes())) {
            throw new IllegalArgumentException("same Source position has different Message metadata");
        }
    }

    /**
     * Sums a bounded supplied subset for one exact owner/Message view. Missing records are not inferred absent;
     * C4 derives the actual complete before/after set and separately accounts for all other business families.
     */
    public static CapacityVector total(
            final TargetQuotaPayloadOwner owner, final List<Record> records, final int maximumRecords) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(records, "records");
        if (maximumRecords <= 0 || maximumRecords > MAX_RECORDS_PER_MESSAGE || records.size() > maximumRecords) {
            throw new IllegalArgumentException("Message record inspection exceeds its declared bound");
        }
        final var keys = new HashSet<String>();
        final byte[] ownerBytes = owner.canonicalBytes();
        byte[] messageDigest = null;
        CapacityVector result = CapacityVector.empty();
        for (Record record : records) {
            if (!Arrays.equals(ownerBytes, record.owner.canonicalBytes())
                    || !keys.add(record.family.rocksName() + ":" + Bytes.hex(record.key))) {
                throw new IllegalArgumentException("duplicate storage record or mixed payload owner view");
            }
            if (record.messageDigest != null) {
                if (messageDigest != null && !Arrays.equals(messageDigest, record.messageDigest)) {
                    throw new IllegalArgumentException("Message record subset mixes before/after generation or state");
                }
                messageDigest = record.messageDigest;
            }
            result = result.add(record.charge);
        }
        return result;
    }
}
