package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** One frozen payload accounting owner per Message identity, independent of generation and attempt budgets. */
public final class TargetQuotaPayloadOwner {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 32;
    public static final int MAX_CANONICAL_BYTES = 2
            + 43
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 34
            + 2
            + 11
            + 3 * 34
            + 5
            + TargetPayloadReference.MAX_CANONICAL_BYTES
            + 2
            + 11
            + 4
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 19
            + 2 * 35;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-payload-owner\0");

    public enum Kind {
        INLINE(1),
        OBJECT(2);
        private final int wire;

        Kind(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }
    }

    public enum Phase {
        RESERVED(1),
        ACTIVE(2),
        RETAINED(3),
        RELEASED(4);
        private final int wire;

        Phase(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }
    }

    /**
     * Verifies the exact accepted source body and actual before/after business ledger, grants and protection.
     * For release this includes all remaining attempts/writers, latest reference sources, catalog ancestry,
     * pins, query/replay retention and actual provider/Store deletion. Remains valid through atomic commit.
     * A callback, source digest or Floor DTO is not itself the production implementation of this authority.
     */
    @FunctionalInterface
    public interface TransitionAuthority {
        void requireAuthorized(TargetQuotaPayloadOwner prior, TargetQuotaPayloadOwner next, RecoveryFloorRef floor);
    }

    private final DelayMessageId messageId;
    private final TargetQuotaIdentity owner;
    private final byte[] tenantScope;
    private final TargetQuotaAccounting accounting;
    private final byte[] bindingDigest;
    private final Kind kind;
    private final long length;
    private final byte[] payloadSha256;
    private final byte[] reservationId;
    private final byte[] objectStoreProfileHash;
    private final PayloadReference committed;
    private final Phase phase;
    private final long revision;
    private final TargetQuotaMutation mutation;
    private final byte[] recoveryLineage;
    private final byte[] releaseFloorDigest;
    private final byte[] digest;

    private TargetQuotaPayloadOwner(
            final DelayMessageId messageId,
            final TargetQuotaIdentity owner,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] bindingDigest,
            final Kind kind,
            final long length,
            final byte[] payloadSha256,
            final byte[] reservationId,
            final byte[] objectStoreProfileHash,
            final PayloadReference committed,
            final Phase phase,
            final long revision,
            final TargetQuotaMutation mutation,
            final byte[] recoveryLineage,
            final byte[] releaseFloorDigest) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.tenantScope = TargetCompatibilityCodec.assigned(tenantScope, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.bindingDigest = TargetCompatibilityCodec.assigned(bindingDigest, 32, "initialBindingDigest");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.phase = Objects.requireNonNull(phase, "phase");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        this.recoveryLineage = TargetCompatibilityCodec.assigned(recoveryLineage, 16, "recoveryLineage");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        if (owner.kind() != TargetQuotaIdentity.Kind.TARGET
                || !owner.shard().equals(messageId.routingId().shardId())
                || !owner.shard().equals(mutation.source().shardId())
                || length < 0
                || revision == 0
                || Long.compareUnsigned(revision, mutation.sequence()) > 0) {
            throw new IllegalArgumentException("invalid payload owner identity/length/revision/source");
        }
        if (kind == Kind.INLINE) {
            if (length > TargetScheduleBody.MAX_INLINE_BYTES
                    || phase == Phase.RESERVED
                    || reservationId != null
                    || objectStoreProfileHash != null
                    || committed != null) {
                throw new IllegalArgumentException("invalid inline payload owner branch");
            }
        } else {
            TargetCompatibilityCodec.assigned(reservationId, 32, "reservationId");
            TargetCompatibilityCodec.assigned(objectStoreProfileHash, 32, "objectStoreProfileHash");
            if ((phase == Phase.RESERVED && committed != null) || (phase == Phase.ACTIVE && committed == null)) {
                throw new IllegalArgumentException("object payload owner lacks the correct commit phase");
            }
            if (committed != null) {
                TargetPayloadReference.requireBounded(committed);
                if (length != committed.length()
                        || !Arrays.equals(payloadSha256, committed.payloadSha256())
                        || !Arrays.equals(reservationId, committed.reservationId())
                        || !Arrays.equals(objectStoreProfileHash, committed.objectStoreProfileHash())) {
                    throw new IllegalArgumentException("committed object differs from its frozen reservation");
                }
            }
        }
        if ((phase == Phase.RELEASED) != (releaseFloorDigest != null)) {
            throw new IllegalArgumentException("released payload must retain its Floor digest");
        }
        this.releaseFloorDigest = releaseFloorDigest == null
                ? null
                : TargetCompatibilityCodec.assigned(releaseFloorDigest, 32, "releaseFloorDigest");
        this.length = length;
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.reservationId = reservationId == null ? null : Bytes.copy(reservationId);
        this.objectStoreProfileHash = objectStoreProfileHash == null ? null : Bytes.copy(objectStoreProfileHash);
        this.committed = committed;
        this.revision = revision;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    /** Caller first authenticates the binding and resolves the Message/Command allocation against the Store. */
    public static TargetQuotaPayloadOwner scheduled(
            final TargetScheduleBinding binding,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation stamp) {
        requireInitialBinding(binding, CommandType.SCHEDULE, stamp);
        final var intent = binding.intent();
        final var object =
                intent.hasInlinePayload() ? null : PayloadReference.fromDescriptor(intent.committedPayload());
        return new TargetQuotaPayloadOwner(
                binding.messageId(),
                identity(binding),
                tenantScope,
                accounting,
                binding.digest(),
                object == null ? Kind.INLINE : Kind.OBJECT,
                object == null ? intent.inlinePayload().length : object.length(),
                object == null ? Bytes.sha256(intent.inlinePayload()) : object.payloadSha256(),
                object == null ? null : object.reservationId(),
                object == null ? null : object.objectStoreProfileHash(),
                object,
                Phase.ACTIVE,
                1,
                stamp,
                lineage,
                null);
    }

    /** Reservation ID must come from the authenticated Prepare/receipt authority, not an arbitrary caller value. */
    public static TargetQuotaPayloadOwner reserved(
            final TargetScheduleBinding binding,
            final byte[] reservationId,
            final byte[] tenantScope,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation stamp) {
        requireInitialBinding(binding, CommandType.PREPARE_LARGE_SCHEDULE, stamp);
        final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        return new TargetQuotaPayloadOwner(
                binding.messageId(),
                identity(binding),
                tenantScope,
                accounting,
                binding.digest(),
                Kind.OBJECT,
                prepare.expectedPayloadLength(),
                prepare.payloadSha256(),
                reservationId,
                prepare.objectStoreProfile().semanticHash(),
                null,
                Phase.RESERVED,
                1,
                stamp,
                lineage,
                null);
    }

    private static TargetQuotaIdentity identity(final TargetScheduleBinding binding) {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                binding.messageId().routingId().shardId(),
                binding.accountingIncarnation(),
                binding.target(),
                null);
    }

    private static void requireInitialBinding(
            final TargetScheduleBinding binding, final CommandType type, final TargetQuotaMutation stamp) {
        if (binding.commandType() != type
                || !Arrays.equals(
                        binding.bindingSource().canonicalBytes(), stamp.source().canonicalBytes())) {
            throw new IllegalArgumentException("payload allocation does not match its initial binding source/type");
        }
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public TargetQuotaIdentity primaryIdentity() {
        return owner;
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TENANT_TARGET,
                owner.shard(),
                owner.accountingIncarnation(),
                owner.target(),
                tenantScope);
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenantScope);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public byte[] initialBindingDigest() {
        return Bytes.copy(bindingDigest);
    }

    public Kind kind() {
        return kind;
    }

    public long length() {
        return length;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public byte[] reservationId() {
        return reservationId == null ? null : Bytes.copy(reservationId);
    }

    public byte[] objectStoreProfileHash() {
        return objectStoreProfileHash == null ? null : Bytes.copy(objectStoreProfileHash);
    }

    public PayloadReference committedPayload() {
        return committed;
    }

    public Phase phase() {
        return phase;
    }

    public long revision() {
        return revision;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public byte[] releaseFloorDigest() {
        return releaseFloorDigest == null ? null : Bytes.copy(releaseFloorDigest);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public CapacityVector payloadCharge() {
        return switch (phase) {
            case RESERVED -> TargetQuotaAccounting.reservedPayload(length);
            case ACTIVE -> TargetQuotaAccounting.activePayload(length);
            case RETAINED -> TargetQuotaAccounting.retainedPayload(length);
            case RELEASED -> CapacityVector.empty();
        };
    }

    /** The record has no embedded usage, so its encoded record fee is computed once without recursion. */
    public CapacityVector recordCharge() {
        return accounting.recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, canonicalBytes().length);
    }

    public TargetQuotaPayloadOwner commit(
            final PayloadReference payload, final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RESERVED) {
            throw new IllegalStateException("payload owner is not reserved");
        }
        return transition(Phase.ACTIVE, Objects.requireNonNull(payload, "payload"), stamp, null, authority);
    }

    public TargetQuotaPayloadOwner retain(final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.ACTIVE && phase != Phase.RESERVED) {
            throw new IllegalStateException("payload owner cannot become retained from this phase");
        }
        return transition(Phase.RETAINED, committed, stamp, null, authority);
    }

    public TargetQuotaPayloadOwner replay(final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RETAINED || (kind == Kind.OBJECT && committed == null)) {
            throw new IllegalStateException("only an existing committed/inline retained payload may be replayed");
        }
        return transition(Phase.ACTIVE, committed, stamp, null, authority);
    }

    public TargetQuotaPayloadOwner release(
            final RecoveryFloorRef floor, final TargetQuotaMutation stamp, final TransitionAuthority authority) {
        if (phase != Phase.RETAINED) {
            throw new IllegalStateException("only retained payload may be released");
        }
        Objects.requireNonNull(floor, "floor");
        stamp.requireAfter(mutation);
        TargetSourcePosition.requireBounded(floor.appliedSourcePosition());
        final int order = floor.appliedSourcePosition().compareTo(mutation.source());
        final int sequenceOrder = Long.compareUnsigned(floor.includedMutationSequence(), mutation.sequence());
        if (!Arrays.equals(recoveryLineage, floor.recoveryLineageId())
                || order < 0
                || Integer.signum(order) != Integer.signum(sequenceOrder)
                || (order == 0
                        && !Arrays.equals(
                                floor.appliedSourcePosition().canonicalBytes(),
                                mutation.source().canonicalBytes()))
                || Long.compareUnsigned(floor.includedMutationSequence(), mutation.sequence()) < 0
                || floor.appliedSourcePosition().compareTo(stamp.source()) >= 0
                || Long.compareUnsigned(floor.includedMutationSequence(), stamp.sequence()) >= 0) {
            throw new IllegalStateException("payload release Floor does not cover its latest owner mutation");
        }
        return transition(Phase.RELEASED, committed, stamp, floor, authority);
    }

    private TargetQuotaPayloadOwner transition(
            final Phase nextPhase,
            final PayloadReference payload,
            final TargetQuotaMutation stamp,
            final RecoveryFloorRef floor,
            final TransitionAuthority authority) {
        stamp.requireAfter(mutation);
        final var next = new TargetQuotaPayloadOwner(
                messageId,
                owner,
                tenantScope,
                accounting,
                bindingDigest,
                kind,
                length,
                payloadSha256,
                reservationId,
                objectStoreProfileHash,
                payload,
                nextPhase,
                TargetQuotaMutation.increment(revision),
                stamp,
                recoveryLineage,
                floor == null ? null : floor.floorDigest());
        Objects.requireNonNull(authority, "transitionAuthority").requireAuthorized(this, next, floor);
        return next;
    }

    /** Rebinds decoded owner data to the full retained first Schedule/Prepare, not just its digest. */
    public void requireInitialBinding(final TargetScheduleBinding binding) {
        if (!messageId.equals(binding.messageId())
                || !owner.equals(identity(binding))
                || !Arrays.equals(bindingDigest, binding.digest())
                || binding.bindingSource().compareTo(mutation.source()) > 0
                || (binding.bindingSource().compareTo(mutation.source()) == 0
                        && !Arrays.equals(
                                binding.bindingSource().canonicalBytes(),
                                mutation.source().canonicalBytes()))) {
            throw new IllegalStateException("payload owner differs from its initial full binding");
        }
        if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
            final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
            if (kind != Kind.OBJECT
                    || length != prepare.expectedPayloadLength()
                    || !Arrays.equals(payloadSha256, prepare.payloadSha256())
                    || !Arrays.equals(
                            objectStoreProfileHash, prepare.objectStoreProfile().semanticHash())) {
                throw new IllegalStateException("payload owner differs from its original reservation intent");
            }
        } else {
            final var intent = binding.intent();
            if (intent.hasInlinePayload()
                    ? kind != Kind.INLINE
                            || length != intent.inlinePayload().length
                            || !Arrays.equals(payloadSha256, Bytes.sha256(intent.inlinePayload()))
                    : kind != Kind.OBJECT
                            || !Objects.equals(committed, PayloadReference.fromDescriptor(intent.committedPayload()))) {
                throw new IllegalStateException("payload owner differs from its original Schedule payload");
            }
        }
    }

    /** Checks payload identity; the committer separately validates the current generation, state and grants. */
    public void requireMessagePayload(final TargetMessageRecord message) {
        if (phase == Phase.RESERVED
                || phase == Phase.RELEASED
                || !messageId.equals(message.locator().messageId())
                || !owner.target().equals(message.locator().target())
                || length != message.payloadLength()
                || (kind == Kind.INLINE
                        ? message.inlinePayload() == null
                                || !Arrays.equals(payloadSha256, Bytes.sha256(message.inlinePayload()))
                        : committed == null || !Objects.equals(committed, message.payloadReference()))) {
            throw new IllegalStateException("Message does not refer to the frozen payload owner");
        }
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT}, messageId.bytes());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, messageId.bytes());
            CanonicalProtobuf.bytes(out, 3, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, tenantScope);
            CanonicalProtobuf.bytes(out, 5, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, bindingDigest);
            CanonicalProtobuf.uint32(out, 7, kind.wireValue());
            CanonicalProtobuf.uint64(out, 8, length);
            CanonicalProtobuf.bytes(out, 9, payloadSha256);
            if (reservationId != null) {
                CanonicalProtobuf.bytes(out, 10, reservationId);
            }
            if (objectStoreProfileHash != null) {
                CanonicalProtobuf.bytes(out, 11, objectStoreProfileHash);
            }
            if (committed != null) {
                CanonicalProtobuf.bytes(out, 12, committed.encode());
            }
            CanonicalProtobuf.uint32(out, 13, phase.wireValue());
            CanonicalProtobuf.uint64Bits(out, 14, revision);
            CanonicalProtobuf.bytes(out, 15, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 16, recoveryLineage);
            if (releaseFloorDigest != null) {
                CanonicalProtobuf.bytes(out, 17, releaseFloorDigest);
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 18, digest);
        });
    }

    public static TargetQuotaPayloadOwner decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 18, false, "Target payload owner");
        final byte[][] bytes = new byte[19][];
        final long[] numbers = new long[19];
        final boolean[] present = new boolean[19];
        for (var field : fields) {
            final int n = field.number();
            if (n < 1 || n > 18 || present[n]) {
                throw new IllegalArgumentException("invalid payload owner fields");
            }
            present[n] = true;
            if (n == 1 || n == 7 || n == 13) {
                numbers[n] = QueryCodecSupport.uint32(field, n);
            } else if (n == 8) {
                numbers[n] = QueryCodecSupport.uint(field, n);
            } else if (n == 14) {
                numbers[n] = QueryCodecSupport.uint64Bits(field, n);
            } else {
                bytes[n] = QueryCodecSupport.bytes(field, n);
            }
        }
        for (int n : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14, 15, 16, 18}) {
            if (!present[n]) {
                throw new IllegalArgumentException("missing payload owner field");
            }
        }
        if (numbers[1] != VERSION) {
            throw new IllegalArgumentException("unknown payload owner version");
        }
        final Kind decodedKind =
                switch ((int) numbers[7]) {
                    case 1 -> Kind.INLINE;
                    case 2 -> Kind.OBJECT;
                    default -> throw new IllegalArgumentException("unknown payload owner kind");
                };
        final Phase decodedPhase =
                switch ((int) numbers[13]) {
                    case 1 -> Phase.RESERVED;
                    case 2 -> Phase.ACTIVE;
                    case 3 -> Phase.RETAINED;
                    case 4 -> Phase.RELEASED;
                    default -> throw new IllegalArgumentException("unknown payload owner phase");
                };
        final var value = new TargetQuotaPayloadOwner(
                new DelayMessageId(bytes[2]),
                TargetQuotaIdentity.decode(bytes[3]),
                bytes[4],
                TargetQuotaAccounting.decode(bytes[5]),
                bytes[6],
                decodedKind,
                numbers[8],
                bytes[9],
                bytes[10],
                bytes[11],
                bytes[12] == null ? null : TargetPayloadReference.decode(bytes[12]),
                decodedPhase,
                numbers[14],
                TargetQuotaMutation.decode(bytes[15]),
                bytes[16],
                bytes[17]);
        if (!Arrays.equals(value.digest, bytes[18])) {
            throw new IllegalArgumentException("payload owner digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target payload owner");
        return value;
    }

    public static TargetQuotaPayloadOwner decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard, final byte[] authenticatedTenantRoutingScope) {
        final var value = decode(encoded);
        if (!value.owner.shard().equals(shard)
                || !Arrays.equals(value.tenantScope, authenticatedTenantRoutingScope)
                || !Arrays.equals(key, value.key())) {
            throw new IllegalArgumentException("payload owner key/Shard/tenant mismatch");
        }
        return value;
    }
}
