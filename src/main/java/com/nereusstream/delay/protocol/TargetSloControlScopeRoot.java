package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Pure bounded OPEN/SEALED Scope state. Only the original registry's actual MEMBER CAS admits an input. */
public final class TargetSloControlScopeRoot {
    public static final int LEGACY_VERSION = 1;
    public static final int VERSION = 2;
    public static final int ROOT_FIXED_RESERVED_BYTES = 64;
    public static final int ROOT2_FIXED_RESERVED_BYTES = 128;
    public static final int EXPOSURE_DEBIT_RESERVED_BYTES = 278;
    public static final int LEGACY_MAX_CANONICAL_BYTES = 1_537_724;
    public static final int MAX_CANONICAL_BYTES = TargetSloControlScopeHeader.MAX_CANONICAL_BYTES
            + ROOT2_FIXED_RESERVED_BYTES
            + TargetSloControlScopeHeader.MAX_SLOTS * Slot.RESERVED_BYTES
            + TargetScopeExposurePolicy.MAX_DEBITS * EXPOSURE_DEBIT_RESERVED_BYTES;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-scope-root\0");
    private static final byte[] EXPOSURE_DOMAIN = Bytes.utf8("nereus-delay-target-slo-control-scope-root-2\0");
    private static final byte[] SEALED_MEMBERSHIP_DOMAIN =
            Bytes.utf8("nereus-delay-target-slo-control-scope-sealed-membership-2\0");

    public enum State {
        OPEN,
        SEALED
    }

    public enum SlotState {
        PENDING,
        MEMBER
    }

    /** PENDING is a charged resource reservation, with no immutable version or membership revision. */
    public record Slot(
            int slotIndex,
            byte[] operationId,
            byte[] entryDigest,
            int entryBytes,
            SlotState state,
            Long immutableEntryVersion,
            Long membershipRevision) {
        public static final int MAX_CANONICAL_BYTES = 104;
        // Six bytes cover the enclosing root field tag and maximum length prefix.
        public static final int RESERVED_BYTES = MAX_CANONICAL_BYTES + 6;

        public Slot {
            if (slotIndex < 0
                    || slotIndex >= TargetSloControlScopeHeader.MAX_SLOTS
                    || entryBytes < 1
                    || entryBytes > TargetSloControlRegisteredInput.MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("Scope slot index/entry bytes exceed their closed bounds");
            }
            operationId = TargetCompatibilityCodec.assigned(operationId, 32, "Scope slot original operation ID");
            entryDigest = TargetCompatibilityCodec.assigned(entryDigest, 32, "Scope slot full original entry digest");
            Objects.requireNonNull(state);
            if (state == SlotState.PENDING) {
                if (immutableEntryVersion != null || membershipRevision != null) {
                    throw new IllegalArgumentException(
                            "PENDING Scope reservation cannot carry an admission version/revision");
                }
            } else if (immutableEntryVersion == null
                    || immutableEntryVersion < 0
                    || immutableEntryVersion == Long.MAX_VALUE
                    || membershipRevision == null
                    || membershipRevision <= 1
                    || membershipRevision == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "MEMBER Scope slot requires its actual immutable version/admission revision");
            }
        }

        @Override
        public byte[] operationId() {
            return Bytes.copy(operationId);
        }

        @Override
        public byte[] entryDigest() {
            return Bytes.copy(entryDigest);
        }

        public void requireInput(TargetSloControlRegisteredInput input) {
            if (input == null
                    || slotIndex != input.slot()
                    || entryBytes != input.entryBytes()
                    || !Arrays.equals(operationId, input.operationId())
                    || !Arrays.equals(entryDigest, input.digest())) {
                throw new IllegalArgumentException("Scope slot changed its exact original operation/entry/size");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, slotIndex);
                CanonicalProtobuf.bytes(out, 2, operationId);
                CanonicalProtobuf.bytes(out, 3, entryDigest);
                CanonicalProtobuf.uint32(out, 4, entryBytes);
                CanonicalProtobuf.uint32(out, 5, state == SlotState.PENDING ? 1 : 2);
                if (state == SlotState.MEMBER) {
                    CanonicalProtobuf.uint64(out, 6, immutableEntryVersion);
                    CanonicalProtobuf.uint64(out, 7, membershipRevision);
                }
            });
        }

        public static Slot decode(byte[] encoded) {
            final var f =
                    TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "SLO Control Scope slot");
            if (f.size() != 5 && f.size() != 7) {
                throw new IllegalArgumentException("invalid Scope slot field count");
            }
            QueryCodecSupport.requireNumbers(
                    f,
                    f.size() == 5 ? new int[] {1, 2, 3, 4, 5} : new int[] {1, 2, 3, 4, 5, 6, 7},
                    "SLO Control Scope slot");
            final var state =
                    switch (QueryCodecSupport.uint32(f.get(4), 5)) {
                        case 1 -> SlotState.PENDING;
                        case 2 -> SlotState.MEMBER;
                        default -> throw new IllegalArgumentException("unknown Scope slot state");
                    };
            final var value = new Slot(
                    QueryCodecSupport.uint32(f.getFirst(), 1),
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    QueryCodecSupport.uint32(f.get(3), 4),
                    state,
                    f.size() == 7 ? QueryCodecSupport.uint64Bits(f.get(5), 6) : null,
                    f.size() == 7 ? QueryCodecSupport.uint64Bits(f.get(6), 7) : null);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope slot");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Slot that
                    && slotIndex == that.slotIndex
                    && entryBytes == that.entryBytes
                    && state == that.state
                    && Objects.equals(immutableEntryVersion, that.immutableEntryVersion)
                    && Objects.equals(membershipRevision, that.membershipRevision)
                    && Arrays.equals(operationId, that.operationId)
                    && Arrays.equals(entryDigest, that.entryDigest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    private final int schemaVersion;
    private final TargetSloControlScopeHeader header;
    private final long revision;
    private final long membershipRevision;
    private final State state;
    private final List<Slot> slots;
    private final long sealCut;
    private final List<TargetScopeExposureDebit> exposureDebits;
    private final boolean issuanceClosed;
    private final TargetScopeExposurePolicy.Budget lifetimeExposure;
    private final TargetScopeExposurePolicy.Budget ordinaryLifetimeExposure;
    private final TargetScopeExposurePolicy.Budget windowExposure;
    private final TargetScopeExposurePolicy.Budget ordinaryWindowExposure;
    private final Long exposureWindowIndex;
    private final long entryBytes;
    private final long reservedRootBytes;
    private final byte[] digest;

    private TargetSloControlScopeRoot(
            TargetSloControlScopeHeader header,
            long revision,
            long membershipRevision,
            State state,
            List<Slot> slots,
            long sealCut,
            List<TargetScopeExposureDebit> exposureDebits,
            boolean issuanceClosed) {
        this.header = Objects.requireNonNull(header);
        this.state = Objects.requireNonNull(state);
        schemaVersion = header.schemaVersion();
        if (schemaVersion != LEGACY_VERSION && schemaVersion != VERSION
                || revision < 1
                || revision == Long.MAX_VALUE
                || membershipRevision < 1
                || membershipRevision == Long.MAX_VALUE
                || slots == null
                || slots.size() > header.limits().maximumSlots()
                || exposureDebits == null) {
            throw new IllegalArgumentException("Scope root revision/slot count exceeds its original finite bounds");
        }
        if (schemaVersion == LEGACY_VERSION
                        && (revision != membershipRevision || !exposureDebits.isEmpty() || issuanceClosed)
                || state == State.OPEN && sealCut != 0
                || state == State.SEALED && (sealCut < membershipRevision || sealCut > revision)) {
            throw new IllegalArgumentException("Scope root changed its schema or irreversible seal cut");
        }
        this.revision = revision;
        this.membershipRevision = membershipRevision;
        this.slots = List.copyOf(slots);
        this.sealCut = sealCut;
        this.exposureDebits = List.copyOf(exposureDebits);
        this.issuanceClosed = issuanceClosed;
        long bytes = 0;
        int members = 0;
        final var operations = new HashSet<String>();
        final var admissions = new HashSet<Long>();
        final var admitted = new ArrayList<Slot>();
        for (int n = 0; n < this.slots.size(); n++) {
            final var slot = Objects.requireNonNull(this.slots.get(n));
            if (slot.slotIndex() != n
                    || !operations.add(Bytes.hex(slot.operationId()))
                    || slot.entryBytes() > header.limits().maximumEntryBytes()) {
                throw new IllegalArgumentException(
                        "Scope slots must be contiguous unique original operation reservations");
            }
            bytes = Math.addExact(bytes, slot.entryBytes());
            if (slot.state() == SlotState.MEMBER) {
                members++;
                admitted.add(slot);
                if (slot.membershipRevision() < (long) n + 3
                        || slot.membershipRevision() > membershipRevision - (state == State.SEALED ? 1 : 0)
                        || !admissions.add(slot.membershipRevision())) {
                    throw new IllegalArgumentException(
                            "Scope MEMBER revision is not a unique possible original admission transition");
                }
            }
        }
        if (membershipRevision != 1L + this.slots.size() + members + (state == State.SEALED ? 1 : 0)
                || revision != membershipRevision + this.exposureDebits.size() + (issuanceClosed ? 1L : 0L)) {
            throw new IllegalArgumentException("Scope revisions do not match monotonic member/debit/close transitions");
        }
        admitted.sort((a, b) -> Long.compare(a.membershipRevision(), b.membershipRevision()));
        for (int n = 0; n < admitted.size(); n++) {
            final var member = admitted.get(n);
            final long reservationsBefore = member.membershipRevision() - 2L - n;
            if (reservationsBefore <= member.slotIndex() || reservationsBefore > this.slots.size()) {
                throw new IllegalArgumentException(
                        "Scope admission revision precedes its actual possible PENDING reservation");
            }
        }
        var total = TargetScopeExposurePolicy.Budget.ZERO;
        var ordinary = TargetScopeExposurePolicy.Budget.ZERO;
        var bucket = TargetScopeExposurePolicy.Budget.ZERO;
        var ordinaryBucket = TargetScopeExposurePolicy.Budget.ZERO;
        Long window = null;
        int ordinaryDebits = 0;
        final var attempts = new HashSet<String>();
        final var policy = header.exposurePolicy();
        if (schemaVersion == VERSION && this.exposureDebits.size() > policy.maximumDebits()) {
            throw new IllegalArgumentException(
                    "Scope exposure debit slots exceed their original configured reservation");
        }
        for (int n = 0; n < this.exposureDebits.size(); n++) {
            final var debit = Objects.requireNonNull(this.exposureDebits.get(n));
            debit.requirePolicy(policy);
            requireExposureOrigin(debit);
            if (debit.index() != n
                    || !attempts.add(Bytes.hex(debit.attemptId()))
                    || debit.canonicalBytes().length > EXPOSURE_DEBIT_RESERVED_BYTES - 6) {
                throw new IllegalArgumentException(
                        "Scope exposure attempts require contiguous nonreusable fixed debit slots");
            }
            if (window != null && debit.windowIndex() < window) {
                throw new IllegalArgumentException("Scope exposure cannot regress its qualified UTC bucket");
            }
            if (window == null || debit.windowIndex() != window) {
                window = debit.windowIndex();
                bucket = TargetScopeExposurePolicy.Budget.ZERO;
                ordinaryBucket = TargetScopeExposurePolicy.Budget.ZERO;
            }
            total = total.plus(debit.charge());
            bucket = bucket.plus(debit.charge());
            if (debit.kind() == TargetScopeExposurePolicy.Kind.NEW_PROMISE) {
                ordinaryDebits++;
                ordinary = ordinary.plus(debit.charge());
                ordinaryBucket = ordinaryBucket.plus(debit.charge());
            }
            if (!policy.lifetime().covers(total)
                    || !policy.ordinaryLifetimeLimit().covers(ordinary)
                    || !policy.window().maximum().covers(bucket)
                    || !policy.ordinaryWindowLimit().covers(ordinaryBucket)
                    || ordinaryDebits > policy.ordinaryDebitLimit()) {
                throw new IllegalArgumentException(
                        "Scope exposure exhausted total capacity or the independent closure reserve");
            }
        }
        lifetimeExposure = total;
        ordinaryLifetimeExposure = ordinary;
        windowExposure = bucket;
        ordinaryWindowExposure = ordinaryBucket;
        exposureWindowIndex = window;
        entryBytes = bytes;
        reservedRootBytes = schemaVersion == LEGACY_VERSION
                ? rootReservation(header, this.slots.size())
                : maximumRootReservation(header);
        if (entryBytes > header.limits().maximumEntriesBytes()
                || reservedRootBytes > header.limits().maximumRootBytes()
                || maximumRootReservation(header) > header.limits().maximumRootBytes()) {
            throw new IllegalArgumentException(
                    "Scope pending/member/seal/exposure storage exceeds its original charged bounds");
        }
        digest = Bytes.sha256(schemaVersion == LEGACY_VERSION ? DOMAIN : EXPOSURE_DOMAIN, fields());
        if (canonicalBytes().length > reservedRootBytes) {
            throw new IllegalArgumentException("Scope canonical root exceeds its precharged complete reservation");
        }
    }

    public static TargetSloControlScopeRoot empty(TargetSloControlScopeHeader header) {
        return new TargetSloControlScopeRoot(header, 1, 1, State.OPEN, List.of(), 0, List.of(), false);
    }

    public int schemaVersion() {
        return schemaVersion;
    }

    public TargetSloControlScopeHeader header() {
        return header;
    }

    public long revision() {
        return revision;
    }

    public long membershipRevision() {
        return membershipRevision;
    }

    public State state() {
        return state;
    }

    public boolean sealed() {
        return state == State.SEALED;
    }
    /** The global revision of the irreversible seal; zero only while OPEN. */
    public long sealCut() {
        return sealCut;
    }

    public List<Slot> slots() {
        return slots;
    }

    public List<Slot> members() {
        return slots.stream().filter(slot -> slot.state() == SlotState.MEMBER).toList();
    }

    public List<Slot> pending() {
        return slots.stream().filter(slot -> slot.state() == SlotState.PENDING).toList();
    }

    public List<TargetScopeExposureDebit> exposureDebits() {
        return exposureDebits;
    }

    public boolean issuanceClosed() {
        return issuanceClosed;
    }

    public TargetScopeExposurePolicy.Budget lifetimeExposure() {
        return lifetimeExposure;
    }

    public TargetScopeExposurePolicy.Budget ordinaryLifetimeExposure() {
        return ordinaryLifetimeExposure;
    }

    public TargetScopeExposurePolicy.Budget windowExposure() {
        return windowExposure;
    }

    public TargetScopeExposurePolicy.Budget ordinaryWindowExposure() {
        return ordinaryWindowExposure;
    }

    public OptionalLong exposureWindowIndex() {
        return exposureWindowIndex == null ? OptionalLong.empty() : OptionalLong.of(exposureWindowIndex);
    }

    public long entryBytes() {
        return entryBytes;
    }
    /** Root2 prepays every configured member and attempt slot, including future seal/CLOSED fields. */
    public long reservedRootBytes() {
        return reservedRootBytes;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public Slot slot(int index) {
        if (index < 0 || index >= slots.size()) {
            throw new IllegalArgumentException("Scope slot is not reserved");
        }
        return slots.get(index);
    }

    public Slot member(int index) {
        final var value = slot(index);
        if (value.state() != SlotState.MEMBER) {
            throw new IllegalStateException("PENDING reservation is not Control admission");
        }
        return value;
    }

    public Slot pending(int index) {
        final var value = slot(index);
        if (value.state() != SlotState.PENDING) {
            throw new IllegalStateException("Scope slot is already an immutable MEMBER");
        }
        return value;
    }

    public TargetScopeExposureDebit debit(int index) {
        if (index < 0 || index >= exposureDebits.size()) {
            throw new IllegalArgumentException("Scope exposure slot is not spent");
        }
        return exposureDebits.get(index);
    }

    public Optional<Slot> find(byte[] operationId) {
        Bytes.requireLength(operationId, 32, "original operation ID");
        return slots.stream()
                .filter(slot -> Arrays.equals(slot.operationId(), operationId))
                .findFirst();
    }

    /** A charged reservation only; no Source enqueue, actor authority or MEMBER admission is implied. */
    public TargetSloControlScopeRoot reserve(TargetSloControlRegisteredInput input) {
        requireOpen();
        Objects.requireNonNull(input).requireHeader(header);
        final var existing = find(input.operationId());
        if (existing.isPresent()) {
            existing.orElseThrow().requireInput(input);
            return this;
        }
        if (input.slot() != slots.size() || slots.size() == header.limits().maximumSlots()) {
            throw new IllegalStateException("Scope reservation requires its next bounded original slot");
        }
        if (input.entryBytes() > header.limits().maximumEntriesBytes() - entryBytes) {
            throw new IllegalStateException("Scope complete entry-byte reservation is full");
        }
        final var next = new ArrayList<>(slots);
        next.add(new Slot(
                input.slot(), input.operationId(), input.digest(), input.entryBytes(), SlotState.PENDING, null, null));
        return nextMembership(state, next);
    }

    /** The surrounding original-root CAS is the unique admission linearization point, after immutable entry put. */
    public TargetSloControlScopeRoot admit(int index, long immutableEntryVersion) {
        requireOpen();
        if (immutableEntryVersion < 0 || immutableEntryVersion == Long.MAX_VALUE) {
            throw new IllegalArgumentException("actual immutable entry version is required");
        }
        final var original = slot(index);
        if (original.state() == SlotState.MEMBER) {
            if (original.immutableEntryVersion() != immutableEntryVersion) {
                throw new IllegalStateException("original Scope MEMBER immutable version cannot change");
            }
            return this;
        }
        final var next = new ArrayList<>(slots);
        next.set(
                index,
                new Slot(
                        index,
                        original.operationId(),
                        original.entryDigest(),
                        original.entryBytes(),
                        SlotState.MEMBER,
                        immutableEntryVersion,
                        Math.incrementExact(membershipRevision)));
        return nextMembership(state, next);
    }

    public TargetSloControlScopeRoot seal() {
        if (sealed()) {
            return this;
        }
        return nextMembership(State.SEALED, slots);
    }

    private TargetSloControlScopeRoot nextMembership(State nextState, List<Slot> nextSlots) {
        final long nextRevision = Math.incrementExact(revision);
        return new TargetSloControlScopeRoot(
                header,
                nextRevision,
                Math.incrementExact(membershipRevision),
                nextState,
                nextSlots,
                nextState == State.SEALED ? nextRevision : 0,
                exposureDebits,
                issuanceClosed);
    }

    /** Immutable debit append only; actual Origin/Kind/copy/time qualification and one-shot issuance are external. */
    public TargetSloControlScopeRoot appendExposureDebit(TargetScopeExposureDebit debit) {
        requireExposureSchema();
        if (issuanceClosed) {
            throw new IllegalStateException("Scope exposure issuance is irreversibly CLOSED");
        }
        if (Objects.requireNonNull(debit).index() != exposureDebits.size()) {
            throw new IllegalArgumentException("Scope exposure requires its next nonreusable attempt slot");
        }
        final var next = new ArrayList<>(exposureDebits);
        next.add(debit);
        return new TargetSloControlScopeRoot(
                header, Math.incrementExact(revision), membershipRevision, state, slots, sealCut, next, false);
    }
    /** This CAS blocks new debits only; existing debits, UNKNOWN and outstanding permits remain charged. */
    public TargetSloControlScopeRoot closeIssuance() {
        requireExposureSchema();
        if (issuanceClosed) {
            return this;
        }
        return new TargetSloControlScopeRoot(
                header, Math.incrementExact(revision), membershipRevision, state, slots, sealCut, exposureDebits, true);
    }

    public Slot requireMember(TargetSloControlRegisteredInput input, long immutableEntryVersion) {
        Objects.requireNonNull(input).requireHeader(header);
        final var member = member(input.slot());
        member.requireInput(input);
        if (member.immutableEntryVersion() != immutableEntryVersion) {
            throw new IllegalArgumentException("Scope MEMBER changed its actual immutable version");
        }
        return member;
    }

    private void requireOpen() {
        if (sealed()) {
            throw new IllegalStateException("SEALED Scope cannot reserve, admit, reopen or mutate its frozen slots");
        }
    }

    private void requireExposureSchema() {
        if (schemaVersion != VERSION) {
            throw new IllegalStateException("original Scope format has no exposure issuance domain");
        }
    }

    private void requireExposureOrigin(TargetScopeExposureDebit debit) {
        if (debit.origin() instanceof TargetScopeExposureDebit.ControlMember origin) {
            final var original = member(origin.memberSlot());
            if (!Arrays.equals(original.entryDigest(), origin.registeredInputDigest())) {
                throw new IllegalArgumentException("exposure Control origin changed its exact original MEMBER");
            }
        } else if (debit.origin() instanceof TargetScopeExposureDebit.ServiceWriter origin) {
            if (header.serviceAliases().stream()
                    .noneMatch(alias -> Arrays.equals(
                            Bytes.sha256(alias.canonicalBytes()),
                            origin.exactWriter().writerIdentityDigest()))) {
                throw new IllegalArgumentException("exposure SERVICE writer is outside its original Scope aliases");
            }
        } else {
            throw new IllegalArgumentException("unknown exposure origin");
        }
    }

    /** Full frozen membership, including PENDING orphans. It excludes only later exposure/issuance revisions. */
    public byte[] sealedMembershipBytes() {
        if (!sealed()) {
            throw new IllegalStateException("OPEN Scope has no frozen complete membership");
        }
        if (schemaVersion == LEGACY_VERSION) {
            return canonicalBytes();
        }
        final var memberProjection = CanonicalProtobuf.message(out -> {
            for (var member : members()) {
                CanonicalProtobuf.bytes(out, 1, member.canonicalBytes());
            }
        });
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, header.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, membershipRevision);
            CanonicalProtobuf.uint64(out, 4, sealCut);
            for (var slot : slots) {
                CanonicalProtobuf.bytes(out, 5, slot.canonicalBytes());
            }
            CanonicalProtobuf.uint32(out, 6, members().size());
            CanonicalProtobuf.uint64(out, 7, entryBytes);
            CanonicalProtobuf.bytes(out, 8, Bytes.sha256(SEALED_MEMBERSHIP_DOMAIN, memberProjection));
        });
    }

    public byte[] sealedMembershipDigest() {
        return schemaVersion == LEGACY_VERSION
                ? sealedLegacyDigest()
                : Bytes.sha256(SEALED_MEMBERSHIP_DOMAIN, sealedMembershipBytes());
    }

    private byte[] sealedLegacyDigest() {
        if (!sealed()) {
            throw new IllegalStateException("OPEN Scope has no frozen complete membership");
        }
        return digest();
    }

    public void requireSameSealedMembership(TargetSloControlScopeRoot other) {
        if (other == null
                || schemaVersion != other.schemaVersion
                || !Arrays.equals(sealedMembershipBytes(), other.sealedMembershipBytes())) {
            throw new IllegalStateException(
                    "sealed Scope changed its original header, slots, admission projection or cut");
        }
    }

    /** The actual backend must call this before every whole-root CAS; only one original transition is legal. */
    public void requireTransitionFrom(TargetSloControlScopeRoot original) {
        Objects.requireNonNull(original);
        if (schemaVersion != original.schemaVersion
                || !header.equals(original.header)
                || revision != Math.incrementExact(original.revision)
                || original.issuanceClosed && !issuanceClosed) {
            throw new IllegalArgumentException(
                    "Scope CAS must preserve its exact original header and advance one revision");
        }
        if (original.sealed()) {
            original.requireSameSealedMembership(this);
        }
        final boolean sameDebits = exposureDebits.equals(original.exposureDebits);
        final boolean sameIssuance = issuanceClosed == original.issuanceClosed;
        final boolean memberStep = !original.sealed()
                && membershipRevision == Math.incrementExact(original.membershipRevision)
                && sameDebits
                && sameIssuance;
        if (memberStep && state == State.SEALED && slots.equals(original.slots) && sealCut == revision) {
            return;
        }
        if (memberStep && state == State.OPEN && sealCut == 0) {
            if (slots.size() == original.slots.size() + 1
                    && prefixEquals(slots, original.slots)
                    && slots.getLast().state() == SlotState.PENDING) {
                return;
            }
            if (slots.size() == original.slots.size()) {
                int changed = 0;
                for (int n = 0; n < slots.size(); n++) {
                    final var before = original.slots.get(n);
                    final var after = slots.get(n);
                    if (before.equals(after)) {
                        continue;
                    }
                    if (before.state() != SlotState.PENDING
                            || after.state() != SlotState.MEMBER
                            || after.membershipRevision() != membershipRevision
                            || !sameReservation(before, after)) {
                        throw new IllegalArgumentException("Scope CAS changed an existing reservation or MEMBER");
                    }
                    changed++;
                }
                if (changed == 1) {
                    return;
                }
            }
        }
        if (schemaVersion == VERSION && sameMembershipState(original)) {
            if (sameIssuance
                    && !original.issuanceClosed
                    && exposureDebits.size() == original.exposureDebits.size() + 1
                    && prefixEquals(exposureDebits, original.exposureDebits)) {
                return;
            }
            if (!original.issuanceClosed && issuanceClosed && sameDebits) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Scope CAS is not one legal reserve/admit/seal/debit/issuance-close transition");
    }

    private boolean sameMembershipState(TargetSloControlScopeRoot other) {
        return membershipRevision == other.membershipRevision
                && state == other.state
                && sealCut == other.sealCut
                && slots.equals(other.slots);
    }

    private static boolean sameReservation(Slot first, Slot second) {
        return first.slotIndex() == second.slotIndex()
                && first.entryBytes() == second.entryBytes()
                && Arrays.equals(first.operationId(), second.operationId())
                && Arrays.equals(first.entryDigest(), second.entryDigest());
    }

    private static <T> boolean prefixEquals(List<T> next, List<T> previous) {
        return next.size() >= previous.size()
                && next.subList(0, previous.size()).equals(previous);
    }

    public static long maximumRootReservation(TargetSloControlScopeHeader header) {
        return rootReservation(Objects.requireNonNull(header), header.limits().maximumSlots());
    }

    private static long rootReservation(TargetSloControlScopeHeader header, int slots) {
        final boolean legacy = header.schemaVersion() == LEGACY_VERSION;
        long bytes = Math.addExact(
                Math.addExact(
                        legacy ? ROOT_FIXED_RESERVED_BYTES : ROOT2_FIXED_RESERVED_BYTES,
                        header.canonicalBytes().length),
                Math.multiplyExact((long) slots, Slot.RESERVED_BYTES));
        if (!legacy) {
            bytes = Math.addExact(
                    bytes,
                    Math.multiplyExact(
                            (long) Objects.requireNonNull(header.exposurePolicy())
                                    .maximumDebits(),
                            EXPOSURE_DEBIT_RESERVED_BYTES));
        }
        return bytes;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion);
            CanonicalProtobuf.bytes(out, 2, header.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, revision);
            CanonicalProtobuf.uint32(out, 4, state == State.OPEN ? 1 : 2);
            for (var slot : slots) {
                CanonicalProtobuf.bytes(out, 5, slot.canonicalBytes());
            }
            if (schemaVersion == VERSION) {
                CanonicalProtobuf.uint64(out, 6, membershipRevision);
                for (var debit : exposureDebits) {
                    CanonicalProtobuf.bytes(out, 7, debit.canonicalBytes());
                }
                CanonicalProtobuf.uint32(out, 8, issuanceClosed ? 2 : 1);
                if (sealed()) {
                    CanonicalProtobuf.uint64(out, 9, sealCut);
                }
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, schemaVersion == LEGACY_VERSION ? 6 : 10, digest);
        });
    }

    public static TargetSloControlScopeRoot decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded,
                MAX_CANONICAL_BYTES,
                TargetSloControlScopeHeader.MAX_SLOTS + TargetScopeExposurePolicy.MAX_DEBITS + 9,
                true,
                "SLO Control Scope root");
        if (f.size() < 5) {
            throw new IllegalArgumentException("incomplete SLO Control Scope root");
        }
        for (int n = 0; n < 4; n++) {
            if (f.get(n).number() != n + 1) {
                throw new IllegalArgumentException("invalid Scope root field order");
            }
        }
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (version != LEGACY_VERSION && version != VERSION
                || version == LEGACY_VERSION && encoded.length > LEGACY_MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("unknown or oversized Scope root schema");
        }
        final var header = TargetSloControlScopeHeader.decode(QueryCodecSupport.bytes(f.get(1), 2));
        if (header.schemaVersion() != version
                || encoded.length > header.limits().maximumRootBytes()) {
            throw new IllegalArgumentException("root changed its original header schema or Scope byte bound");
        }
        final var state =
                switch (QueryCodecSupport.uint32(f.get(3), 4)) {
                    case 1 -> State.OPEN;
                    case 2 -> State.SEALED;
                    default -> throw new IllegalArgumentException("unknown Scope root state");
                };
        int at = 4;
        final var slots = new ArrayList<Slot>();
        while (at < f.size() && f.get(at).number() == 5) {
            if (slots.size() == header.limits().maximumSlots()) {
                throw new IllegalArgumentException("Scope slot count exceeds original reservation");
            }
            slots.add(Slot.decode(QueryCodecSupport.bytes(f.get(at++), 5)));
        }
        final long revision = QueryCodecSupport.uint64Bits(f.get(2), 3);
        final TargetSloControlScopeRoot value;
        if (version == LEGACY_VERSION) {
            if (at + 1 != f.size()) {
                throw new IllegalArgumentException("invalid original Scope root suffix");
            }
            value = new TargetSloControlScopeRoot(
                    header, revision, revision, state, slots, state == State.SEALED ? revision : 0, List.of(), false);
        } else {
            if (at + (state == State.SEALED ? 4 : 3) > f.size()) {
                throw new IllegalArgumentException("incomplete exposure Scope root suffix");
            }
            final long membershipRevision = QueryCodecSupport.uint64Bits(f.get(at++), 6);
            final var debits = new ArrayList<TargetScopeExposureDebit>();
            while (at < f.size() && f.get(at).number() == 7) {
                if (debits.size() == header.exposurePolicy().maximumDebits()) {
                    throw new IllegalArgumentException("Scope exposure debit slot reservation is full");
                }
                debits.add(TargetScopeExposureDebit.decode(QueryCodecSupport.bytes(f.get(at++), 7)));
            }
            if (at + (state == State.SEALED ? 3 : 2) != f.size()) {
                throw new IllegalArgumentException("invalid exposure Scope root suffix");
            }
            final boolean closed =
                    switch (QueryCodecSupport.uint32(f.get(at++), 8)) {
                        case 1 -> false;
                        case 2 -> true;
                        default -> throw new IllegalArgumentException("unknown Scope issuance state");
                    };
            final long cut = state == State.SEALED ? QueryCodecSupport.uint64Bits(f.get(at++), 9) : 0;
            value = new TargetSloControlScopeRoot(
                    header, revision, membershipRevision, state, slots, cut, debits, closed);
        }
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(at), version == LEGACY_VERSION ? 6 : 10, 32))) {
            throw new IllegalArgumentException("Scope root digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "SLO Control Scope root");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloControlScopeRoot that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
