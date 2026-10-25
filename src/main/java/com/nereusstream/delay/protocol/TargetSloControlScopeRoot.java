package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Pure bounded OPEN/SEALED Scope state. Only the original registry's actual MEMBER CAS admits an input. */
public final class TargetSloControlScopeRoot {
    public static final int VERSION = 1;
    public static final int ROOT_FIXED_RESERVED_BYTES = 64;
    public static final int MAX_CANONICAL_BYTES = TargetSloControlScopeHeader.MAX_CANONICAL_BYTES
            + ROOT_FIXED_RESERVED_BYTES + TargetSloControlScopeHeader.MAX_SLOTS * Slot.RESERVED_BYTES;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-scope-root\0");

    public enum State { OPEN, SEALED }
    public enum SlotState { PENDING, MEMBER }

    /** PENDING is a charged resource reservation, with no immutable version or membership revision. */
    public record Slot(int slotIndex, byte[] operationId, byte[] entryDigest, int entryBytes,
            SlotState state, Long immutableEntryVersion, Long membershipRevision) {
        public static final int MAX_CANONICAL_BYTES = 104;
        // Six bytes cover the enclosing root field tag and maximum length prefix.
        public static final int RESERVED_BYTES = MAX_CANONICAL_BYTES + 6;
        public Slot {
            if (slotIndex < 0 || slotIndex >= TargetSloControlScopeHeader.MAX_SLOTS
                    || entryBytes < 1 || entryBytes > TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES)
                throw new IllegalArgumentException("Scope slot index/entry bytes exceed their closed bounds");
            operationId = TargetCompatibilityCodec.assigned(operationId, 32, "Scope slot original operation ID");
            entryDigest = TargetCompatibilityCodec.assigned(entryDigest, 32, "Scope slot full original entry digest");
            Objects.requireNonNull(state);
            if (state == SlotState.PENDING) {
                if (immutableEntryVersion != null || membershipRevision != null)
                    throw new IllegalArgumentException("PENDING Scope reservation cannot carry an admission version/revision");
            } else if (immutableEntryVersion == null || immutableEntryVersion < 0 || immutableEntryVersion == Long.MAX_VALUE
                    || membershipRevision == null || membershipRevision <= 1 || membershipRevision == Long.MAX_VALUE)
                throw new IllegalArgumentException("MEMBER Scope slot requires its actual immutable version/admission revision");
        }
        @Override public byte[] operationId() { return Bytes.copy(operationId); }
        @Override public byte[] entryDigest() { return Bytes.copy(entryDigest); }
        public void requireInput(TargetSloControlRegisteredInput input) {
            if (input == null || slotIndex != input.slot() || entryBytes != input.entryBytes()
                    || !Arrays.equals(operationId, input.operationId()) || !Arrays.equals(entryDigest, input.digest()))
                throw new IllegalArgumentException("Scope slot changed its exact original operation/entry/size");
        }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, slotIndex); CanonicalProtobuf.bytes(out, 2, operationId);
                CanonicalProtobuf.bytes(out, 3, entryDigest); CanonicalProtobuf.uint32(out, 4, entryBytes);
                CanonicalProtobuf.uint32(out, 5, state == SlotState.PENDING ? 1 : 2);
                if (state == SlotState.MEMBER) {
                    CanonicalProtobuf.uint64(out, 6, immutableEntryVersion); CanonicalProtobuf.uint64(out, 7, membershipRevision);
                }
            });
        }
        public static Slot decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "SLO Control Scope slot");
            if (f.size() != 5 && f.size() != 7) throw new IllegalArgumentException("invalid Scope slot field count");
            QueryCodecSupport.requireNumbers(f, f.size() == 5 ? new int[]{1, 2, 3, 4, 5} : new int[]{1, 2, 3, 4, 5, 6, 7}, "SLO Control Scope slot");
            final var state = switch (QueryCodecSupport.uint32(f.get(4), 5)) {
                case 1 -> SlotState.PENDING; case 2 -> SlotState.MEMBER;
                default -> throw new IllegalArgumentException("unknown Scope slot state");
            };
            final var value = new Slot(QueryCodecSupport.uint32(f.getFirst(), 1), QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.uint32(f.get(3), 4), state,
                    f.size() == 7 ? QueryCodecSupport.uint64Bits(f.get(5), 6) : null,
                    f.size() == 7 ? QueryCodecSupport.uint64Bits(f.get(6), 7) : null);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope slot"); return value;
        }
        @Override public boolean equals(Object other) {
            return other instanceof Slot that && slotIndex == that.slotIndex && entryBytes == that.entryBytes && state == that.state
                    && Objects.equals(immutableEntryVersion, that.immutableEntryVersion) && Objects.equals(membershipRevision, that.membershipRevision)
                    && Arrays.equals(operationId, that.operationId) && Arrays.equals(entryDigest, that.entryDigest);
        }
        @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
    }

    private final TargetSloControlScopeHeader header;
    private final long revision;
    private final State state;
    private final List<Slot> slots;
    private final long entryBytes;
    private final long reservedRootBytes;
    private final byte[] digest;

    private TargetSloControlScopeRoot(TargetSloControlScopeHeader header, long revision, State state, List<Slot> slots) {
        this.header = Objects.requireNonNull(header); this.state = Objects.requireNonNull(state);
        if (revision < 1 || revision == Long.MAX_VALUE || slots == null || slots.size() > header.limits().maximumSlots())
            throw new IllegalArgumentException("Scope root revision/slot count exceeds its original finite bounds");
        this.revision = revision; this.slots = List.copyOf(slots);
        long bytes = 0; int members = 0;
        final var operations = new HashSet<String>(); final var admissions = new HashSet<Long>();
        final var admitted = new ArrayList<Slot>();
        for (int n = 0; n < this.slots.size(); n++) {
            final var slot = Objects.requireNonNull(this.slots.get(n));
            if (slot.slotIndex() != n || !operations.add(Bytes.hex(slot.operationId()))
                    || slot.entryBytes() > header.limits().maximumEntryBytes())
                throw new IllegalArgumentException("Scope slots must be contiguous unique original operation reservations");
            bytes = Math.addExact(bytes, slot.entryBytes());
            if (slot.state() == SlotState.MEMBER) {
                members++; admitted.add(slot);
                if (slot.membershipRevision() < (long) n + 3 || slot.membershipRevision() > revision - (state == State.SEALED ? 1 : 0)
                        || !admissions.add(slot.membershipRevision()))
                    throw new IllegalArgumentException("Scope MEMBER revision is not a unique possible original admission transition");
            }
        }
        if (revision != 1L + this.slots.size() + members + (state == State.SEALED ? 1 : 0))
            throw new IllegalArgumentException("Scope revision does not match its monotonic reserve/admit/seal transitions");
        admitted.sort((a, b) -> Long.compare(a.membershipRevision(), b.membershipRevision()));
        for (int n = 0; n < admitted.size(); n++) {
            final var member = admitted.get(n);
            final long reservationsBefore = member.membershipRevision() - 2L - n;
            if (reservationsBefore <= member.slotIndex() || reservationsBefore > this.slots.size())
                throw new IllegalArgumentException("Scope admission revision precedes its actual possible PENDING reservation");
        }
        entryBytes = bytes; reservedRootBytes = rootReservation(header, this.slots.size());
        if (entryBytes > header.limits().maximumEntriesBytes() || reservedRootBytes > header.limits().maximumRootBytes()
                || maximumRootReservation(header) > header.limits().maximumRootBytes())
            throw new IllegalArgumentException("Scope pending/member/seal storage exceeds its original charged resource bounds");
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > reservedRootBytes) throw new IllegalArgumentException("Scope canonical root exceeds its precharged MEMBER/seal reservation");
    }

    public static TargetSloControlScopeRoot empty(TargetSloControlScopeHeader header) {
        return new TargetSloControlScopeRoot(header, 1, State.OPEN, List.of());
    }
    public TargetSloControlScopeHeader header() { return header; }
    public long revision() { return revision; }
    public State state() { return state; }
    public boolean sealed() { return state == State.SEALED; }
    public List<Slot> slots() { return slots; }
    public List<Slot> members() { return slots.stream().filter(slot -> slot.state() == SlotState.MEMBER).toList(); }
    public List<Slot> pending() { return slots.stream().filter(slot -> slot.state() == SlotState.PENDING).toList(); }
    public long entryBytes() { return entryBytes; }
    /** Includes maximum MEMBER fields and future revision/seal encoding; admission and sealing cannot refund it. */
    public long reservedRootBytes() { return reservedRootBytes; }
    public byte[] digest() { return Bytes.copy(digest); }
    public Slot slot(int index) {
        if (index < 0 || index >= slots.size()) throw new IllegalArgumentException("Scope slot is not reserved");
        return slots.get(index);
    }
    public Slot member(int index) {
        final var value = slot(index);
        if (value.state() != SlotState.MEMBER) throw new IllegalStateException("PENDING reservation is not Control admission");
        return value;
    }
    public Slot pending(int index) {
        final var value = slot(index);
        if (value.state() != SlotState.PENDING) throw new IllegalStateException("Scope slot is already an immutable MEMBER");
        return value;
    }
    public Optional<Slot> find(byte[] operationId) {
        Bytes.requireLength(operationId, 32, "original operation ID");
        return slots.stream().filter(slot -> Arrays.equals(slot.operationId(), operationId)).findFirst();
    }

    /** A charged reservation only; no Source enqueue, actor authority or MEMBER admission is implied. */
    public TargetSloControlScopeRoot reserve(TargetSloControlRegisteredInput input) {
        requireOpen(); Objects.requireNonNull(input).requireHeader(header);
        final var existing = find(input.operationId());
        if (existing.isPresent()) { existing.orElseThrow().requireInput(input); return this; }
        if (input.slot() != slots.size() || slots.size() == header.limits().maximumSlots())
            throw new IllegalStateException("Scope reservation requires its next bounded original slot");
        if (input.entryBytes() > header.limits().maximumEntriesBytes() - entryBytes)
            throw new IllegalStateException("Scope complete entry-byte reservation is full");
        final var next = new ArrayList<>(slots);
        next.add(new Slot(input.slot(), input.operationId(), input.digest(), input.entryBytes(), SlotState.PENDING, null, null));
        return new TargetSloControlScopeRoot(header, Math.incrementExact(revision), state, next);
    }

    /** The surrounding original-root CAS is the unique admission linearization point, after immutable entry put. */
    public TargetSloControlScopeRoot admit(int index, long immutableEntryVersion) {
        requireOpen();
        if (immutableEntryVersion < 0 || immutableEntryVersion == Long.MAX_VALUE)
            throw new IllegalArgumentException("actual immutable entry version is required");
        final var original = slot(index);
        if (original.state() == SlotState.MEMBER) {
            if (original.immutableEntryVersion() != immutableEntryVersion)
                throw new IllegalStateException("original Scope MEMBER immutable version cannot change");
            return this;
        }
        final long nextRevision = Math.incrementExact(revision); final var next = new ArrayList<>(slots);
        next.set(index, new Slot(index, original.operationId(), original.entryDigest(), original.entryBytes(), SlotState.MEMBER,
                immutableEntryVersion, nextRevision));
        return new TargetSloControlScopeRoot(header, nextRevision, state, next);
    }
    public TargetSloControlScopeRoot seal() {
        if (sealed()) return this;
        return new TargetSloControlScopeRoot(header, Math.incrementExact(revision), State.SEALED, slots);
    }
    public Slot requireMember(TargetSloControlRegisteredInput input, long immutableEntryVersion) {
        Objects.requireNonNull(input).requireHeader(header); final var member = member(input.slot()); member.requireInput(input);
        if (member.immutableEntryVersion() != immutableEntryVersion) throw new IllegalArgumentException("Scope MEMBER changed its actual immutable version");
        return member;
    }
    private void requireOpen() { if (sealed()) throw new IllegalStateException("SEALED Scope cannot reserve, admit, reopen or mutate its frozen slots"); }

    public static long maximumRootReservation(TargetSloControlScopeHeader header) {
        return rootReservation(Objects.requireNonNull(header), header.limits().maximumSlots());
    }
    private static long rootReservation(TargetSloControlScopeHeader header, int slots) {
        return Math.addExact(Math.addExact(ROOT_FIXED_RESERVED_BYTES, header.canonicalBytes().length),
                Math.multiplyExact((long) slots, Slot.RESERVED_BYTES));
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, header.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, revision); CanonicalProtobuf.uint32(out, 4, state == State.OPEN ? 1 : 2);
            for (var slot : slots) CanonicalProtobuf.bytes(out, 5, slot.canonicalBytes());
        });
    }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 6, digest); });
    }
    public static TargetSloControlScopeRoot decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES,
                TargetSloControlScopeHeader.MAX_SLOTS + 5, true, "SLO Control Scope root");
        if (f.size() < 5) throw new IllegalArgumentException("incomplete SLO Control Scope root");
        for (int n = 0; n < 4; n++) if (f.get(n).number() != n + 1) throw new IllegalArgumentException("invalid Scope root field order");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope root schema");
        final var header = TargetSloControlScopeHeader.decode(QueryCodecSupport.bytes(f.get(1), 2));
        if (encoded.length > header.limits().maximumRootBytes()) throw new IllegalArgumentException("root exceeds its original Scope byte bound");
        final var state = switch (QueryCodecSupport.uint32(f.get(3), 4)) {
            case 1 -> State.OPEN; case 2 -> State.SEALED; default -> throw new IllegalArgumentException("unknown Scope root state");
        };
        int at = 4; final var slots = new ArrayList<Slot>();
        while (at < f.size() && f.get(at).number() == 5) {
            if (slots.size() == header.limits().maximumSlots()) throw new IllegalArgumentException("Scope slot count exceeds original reservation");
            slots.add(Slot.decode(QueryCodecSupport.bytes(f.get(at++), 5)));
        }
        if (at + 1 != f.size()) throw new IllegalArgumentException("invalid Scope root suffix");
        final var value = new TargetSloControlScopeRoot(header, QueryCodecSupport.uint64Bits(f.get(2), 3), state, slots);
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(at), 6, 32))) throw new IllegalArgumentException("Scope root digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope root"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetSloControlScopeRoot that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
