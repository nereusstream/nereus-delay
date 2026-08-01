package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical closed values persisted under the five {@code meta/SCHEDULER}
 * key kinds.  The projections are intentionally independent of the in-memory
 * scheduler so recovery can validate each value before rebuilding a ring.
 */
public final class SchedulerProjectionsV1 {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;

    private SchedulerProjectionsV1() {
    }

    public static final class ReadyDiscoveryCursor {
        private final byte[] lastScannedReadyKey;
        private final long wrapGeneration;
        private final long activeRingGeneration;
        private final byte[] digest;

        public ReadyDiscoveryCursor(final byte[] lastScannedReadyKey, final long wrapGeneration,
                                    final long activeRingGeneration) {
            this.lastScannedReadyKey = optionalBytes(lastScannedReadyKey, "lastScannedReadyKey");
            requireNonNegative(wrapGeneration, "wrapGeneration");
            requireNonNegative(activeRingGeneration, "activeRingGeneration");
            this.wrapGeneration = wrapGeneration;
            this.activeRingGeneration = activeRingGeneration;
            this.digest = SchedulerProjectionsV1.digest("nereus-delay-scheduler-ready-discovery-cursor-v1\0",
                    fieldsOneToFive());
        }

        private ReadyDiscoveryCursor(final byte[] lastScannedReadyKey, final long wrapGeneration,
                                     final long activeRingGeneration, final byte[] digest) {
            this.lastScannedReadyKey = optionalBytes(lastScannedReadyKey, "lastScannedReadyKey");
            requireNonNegative(wrapGeneration, "wrapGeneration");
            requireNonNegative(activeRingGeneration, "activeRingGeneration");
            this.wrapGeneration = wrapGeneration;
            this.activeRingGeneration = activeRingGeneration;
            this.digest = fixed(digest, "digest");
        }

        public byte[] lastScannedReadyKey() {
            return optionalBytes(lastScannedReadyKey, "lastScannedReadyKey");
        }

        public long wrapGeneration() {
            return wrapGeneration;
        }

        public long activeRingGeneration() {
            return activeRingGeneration;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                if (lastScannedReadyKey != null) {
                    CanonicalProtobuf.bytes(output, 2, lastScannedReadyKey);
                    CanonicalProtobuf.bytes(output, 3, Bytes.sha256(lastScannedReadyKey));
                }
                CanonicalProtobuf.uint64(output, 4, wrapGeneration);
                CanonicalProtobuf.uint64(output, 5, activeRingGeneration);
                CanonicalProtobuf.bytes(output, 6, digest);
            });
        }

        public static ReadyDiscoveryCursor decode(final byte[] encoded) {
            final var fields = read(encoded, true, "SchedulerReadyDiscoveryCursorV1");
            if (fields.size() < 4 || fields.size() > 6 || number(fields, 0) != 1
                    || number(fields, fields.size() - 2) != 5 || number(fields, fields.size() - 1) != 6) {
                throw new IllegalArgumentException("invalid SchedulerReadyDiscoveryCursorV1 fields");
            }
            int index = 1;
            byte[] key = null;
            if (number(fields, index) == 2) {
                key = bytes(fields.get(index++), 2);
                if (number(fields, index) != 3 || !Arrays.equals(bytes(fields.get(index), 3), Bytes.sha256(key))) {
                    throw new IllegalArgumentException("ready discovery key digest mismatch");
                }
                index++;
            }
            if (number(fields, index) != 4 || number(fields, index + 1) != 5
                    || index + 2 != fields.size() - 1) {
                throw new IllegalArgumentException("invalid ready discovery cursor field order");
            }
            final ReadyDiscoveryCursor result = new ReadyDiscoveryCursor(key, uint(fields.get(index), 4),
                    uint(fields.get(index + 1), 5), fixed(bytes(fields.get(fields.size() - 1), 6), "digest"));
            requireDigest(result.digest, result.fieldsOneToFive(), "nereus-delay-scheduler-ready-discovery-cursor-v1\0");
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerReadyDiscoveryCursorV1");
            return result;
        }

        private byte[] fieldsOneToFive() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                if (lastScannedReadyKey != null) {
                    CanonicalProtobuf.bytes(output, 2, lastScannedReadyKey);
                    CanonicalProtobuf.bytes(output, 3, Bytes.sha256(lastScannedReadyKey));
                }
                CanonicalProtobuf.uint64(output, 4, wrapGeneration);
                CanonicalProtobuf.uint64(output, 5, activeRingGeneration);
            });
        }
    }

    public static final class RingEntry {
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final long observedLaneVersion;

        public RingEntry(final DestinationLaneId laneId, final byte[] laneIncarnation,
                         final long observedLaneVersion) {
            this.laneId = Objects.requireNonNull(laneId, "laneId");
            this.laneIncarnation = fixedLength(laneIncarnation, 16, "laneIncarnation");
            if (observedLaneVersion <= 0) {
                throw new IllegalArgumentException("observedLaneVersion must be positive");
            }
            this.observedLaneVersion = observedLaneVersion;
        }

        public DestinationLaneId laneId() {
            return laneId;
        }

        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        public long observedLaneVersion() {
            return observedLaneVersion;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneId.bytes());
                CanonicalProtobuf.bytes(output, 2, laneIncarnation);
                CanonicalProtobuf.uint64(output, 3, observedLaneVersion);
            });
        }

        public static RingEntry decode(final byte[] encoded) {
            final var fields = read(encoded, false, "SchedulerRingEntryV1");
            requireNumbers(fields, new int[]{1, 2, 3}, "SchedulerRingEntryV1");
            final RingEntry result = new RingEntry(new DestinationLaneId(fixedLength(bytes(fields.get(0), 1),
                    DestinationLaneId.LENGTH, "laneId")), fixedLength(bytes(fields.get(1), 2), 16,
                    "laneIncarnation"), uint(fields.get(2), 3));
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerRingEntryV1");
            return result;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof RingEntry that && observedLaneVersion == that.observedLaneVersion
                    && laneId.equals(that.laneId) && Arrays.equals(laneIncarnation, that.laneIncarnation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(laneId, Arrays.hashCode(laneIncarnation), observedLaneVersion);
        }
    }

    public static final class ActiveRing {
        private final long ringGeneration;
        private final long roundGeneration;
        private final int nextIndex;
        private final List<RingEntry> entries;
        private final byte[] digest;

        public ActiveRing(final long ringGeneration, final long roundGeneration, final int nextIndex,
                          final List<RingEntry> entries) {
            if (ringGeneration <= 0 || roundGeneration < 0) {
                throw new IllegalArgumentException("invalid scheduler ring generations");
            }
            this.ringGeneration = ringGeneration;
            this.roundGeneration = roundGeneration;
            this.entries = uniqueOrdered(entries);
            if (nextIndex < 0 || (!this.entries.isEmpty() && nextIndex >= this.entries.size())
                    || (this.entries.isEmpty() && nextIndex != 0)) {
                throw new IllegalArgumentException("invalid scheduler ring nextIndex");
            }
            this.nextIndex = nextIndex;
            this.digest = SchedulerProjectionsV1.digest("nereus-delay-scheduler-active-ring-v1\0", fieldsOneToFive());
        }

        private ActiveRing(final long ringGeneration, final long roundGeneration, final int nextIndex,
                           final List<RingEntry> entries, final byte[] digest) {
            if (ringGeneration <= 0 || roundGeneration < 0) {
                throw new IllegalArgumentException("invalid scheduler ring generations");
            }
            this.ringGeneration = ringGeneration;
            this.roundGeneration = roundGeneration;
            this.entries = uniqueOrdered(entries);
            if (nextIndex < 0 || (!this.entries.isEmpty() && nextIndex >= this.entries.size())
                    || (this.entries.isEmpty() && nextIndex != 0)) {
                throw new IllegalArgumentException("invalid scheduler ring nextIndex");
            }
            this.nextIndex = nextIndex;
            this.digest = fixed(digest, "digest");
        }

        public long ringGeneration() {
            return ringGeneration;
        }

        public long roundGeneration() {
            return roundGeneration;
        }

        public int nextIndex() {
            return nextIndex;
        }

        public List<RingEntry> entries() {
            return entries;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                output.writeBytes(fieldsOneToFive());
                CanonicalProtobuf.bytes(output, 6, digest);
            });
        }

        public static ActiveRing decode(final byte[] encoded) {
            final var fields = read(encoded, true, "SchedulerActiveRingV1");
            if (fields.size() < 5 || number(fields, 0) != 1 || number(fields, 1) != 2
                    || number(fields, 2) != 3 || number(fields, 3) != 4
                    || number(fields, fields.size() - 1) != 6) {
                throw new IllegalArgumentException("invalid SchedulerActiveRingV1 fields");
            }
            final List<RingEntry> entries = new ArrayList<>();
            for (int index = 4; index < fields.size() - 1; index++) {
                if (number(fields, index) != 5) {
                    throw new IllegalArgumentException("active ring entries must be repeated field 5");
                }
                entries.add(RingEntry.decode(bytes(fields.get(index), 5)));
            }
            final ActiveRing result = new ActiveRing(uint(fields.get(1), 2), uint(fields.get(2), 3),
                    uint32(fields.get(3), 4), entries, bytes(fields.get(fields.size() - 1), 6));
            requireDigest(result.digest, result.fieldsOneToFive(), "nereus-delay-scheduler-active-ring-v1\0");
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerActiveRingV1");
            return result;
        }

        private byte[] fieldsOneToFive() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                CanonicalProtobuf.uint64(output, 2, ringGeneration);
                CanonicalProtobuf.uint64(output, 3, roundGeneration);
                CanonicalProtobuf.uint32(output, 4, nextIndex);
                for (RingEntry entry : entries) {
                    CanonicalProtobuf.bytes(output, 5, entry.canonicalBytes());
                }
            });
        }
    }

    public static final class DeficitEntry {
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final long deficitBytes;
        private final long observedLaneVersion;

        public DeficitEntry(final DestinationLaneId laneId, final byte[] laneIncarnation, final long deficitBytes,
                            final long observedLaneVersion) {
            this.laneId = Objects.requireNonNull(laneId, "laneId");
            this.laneIncarnation = fixedLength(laneIncarnation, 16, "laneIncarnation");
            if (deficitBytes < 0 || observedLaneVersion <= 0) {
                throw new IllegalArgumentException("invalid scheduler deficit entry");
            }
            this.deficitBytes = deficitBytes;
            this.observedLaneVersion = observedLaneVersion;
        }

        public DestinationLaneId laneId() {
            return laneId;
        }

        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        public long deficitBytes() {
            return deficitBytes;
        }

        public long observedLaneVersion() {
            return observedLaneVersion;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneId.bytes());
                CanonicalProtobuf.bytes(output, 2, laneIncarnation);
                CanonicalProtobuf.uint64(output, 3, deficitBytes);
                CanonicalProtobuf.uint64(output, 4, observedLaneVersion);
            });
        }

        public static DeficitEntry decode(final byte[] encoded) {
            final var fields = read(encoded, false, "SchedulerDeficitEntryV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4}, "SchedulerDeficitEntryV1");
            final DeficitEntry result = new DeficitEntry(new DestinationLaneId(fixedLength(bytes(fields.get(0), 1),
                    DestinationLaneId.LENGTH, "laneId")), fixedLength(bytes(fields.get(1), 2), 16,
                    "laneIncarnation"), uint(fields.get(2), 3), uint(fields.get(3), 4));
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerDeficitEntryV1");
            return result;
        }
    }

    public static final class DeficitMap {
        private final List<DeficitEntry> entries;
        private final byte[] digest;

        public DeficitMap(final List<DeficitEntry> entries) {
            this.entries = sortedDeficit(entries);
            this.digest = SchedulerProjectionsV1.digest("nereus-delay-scheduler-deficit-map-v1\0", fieldsOneAndTwo());
        }

        private DeficitMap(final List<DeficitEntry> entries, final byte[] digest) {
            this.entries = sortedDeficit(entries);
            this.digest = fixed(digest, "digest");
        }

        public List<DeficitEntry> entries() {
            return entries;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                output.writeBytes(fieldsOneAndTwo());
                CanonicalProtobuf.bytes(output, 3, digest);
            });
        }

        public static DeficitMap decode(final byte[] encoded) {
            final var fields = read(encoded, true, "SchedulerDeficitMapV1");
            if (fields.size() < 2 || number(fields, 0) != 1 || number(fields, fields.size() - 1) != 3) {
                throw new IllegalArgumentException("invalid SchedulerDeficitMapV1 fields");
            }
            final List<DeficitEntry> entries = new ArrayList<>();
            for (int index = 1; index < fields.size() - 1; index++) {
                if (number(fields, index) != 2) {
                    throw new IllegalArgumentException("deficit entries must be repeated field 2");
                }
                entries.add(DeficitEntry.decode(bytes(fields.get(index), 2)));
            }
            final DeficitMap result = new DeficitMap(entries, bytes(fields.get(fields.size() - 1), 3));
            requireDigest(result.digest, result.fieldsOneAndTwo(), "nereus-delay-scheduler-deficit-map-v1\0");
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerDeficitMapV1");
            return result;
        }

        private byte[] fieldsOneAndTwo() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                for (DeficitEntry entry : entries) {
                    CanonicalProtobuf.bytes(output, 2, entry.canonicalBytes());
                }
            });
        }
    }

    public static final class Round {
        private final long roundGeneration;
        private final OwnerIdentityV1 owner;
        private final boolean recoveryFirstPass;
        private final byte[] digest;

        public Round(final long roundGeneration, final OwnerIdentityV1 owner, final boolean recoveryFirstPass) {
            requireNonNegative(roundGeneration, "roundGeneration");
            this.roundGeneration = roundGeneration;
            this.owner = Objects.requireNonNull(owner, "owner");
            this.recoveryFirstPass = recoveryFirstPass;
            this.digest = SchedulerProjectionsV1.digest("nereus-delay-scheduler-round-v1\0", fieldsOneToFour());
        }

        private Round(final long roundGeneration, final OwnerIdentityV1 owner, final boolean recoveryFirstPass,
                      final byte[] digest) {
            requireNonNegative(roundGeneration, "roundGeneration");
            this.roundGeneration = roundGeneration;
            this.owner = Objects.requireNonNull(owner, "owner");
            this.recoveryFirstPass = recoveryFirstPass;
            this.digest = fixed(digest, "digest");
        }

        public long roundGeneration() {
            return roundGeneration;
        }

        public OwnerIdentityV1 owner() {
            return owner;
        }

        public boolean recoveryFirstPass() {
            return recoveryFirstPass;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                output.writeBytes(fieldsOneToFour());
                CanonicalProtobuf.bytes(output, 5, digest);
            });
        }

        public static Round decode(final byte[] encoded) {
            final var fields = read(encoded, false, "SchedulerRoundV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4, 5}, "SchedulerRoundV1");
            final Round result = new Round(uint(fields.get(1), 2),
                    OwnerIdentityV1.decode(bytes(fields.get(2), 3)), bool(fields.get(3), 4),
                    bytes(fields.get(4), 5));
            requireDigest(result.digest, result.fieldsOneToFour(), "nereus-delay-scheduler-round-v1\0");
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerRoundV1");
            return result;
        }

        private byte[] fieldsOneToFour() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                CanonicalProtobuf.uint64(output, 2, roundGeneration);
                CanonicalProtobuf.bytes(output, 3, owner.canonicalBytes());
                CanonicalProtobuf.uint32(output, 4, recoveryFirstPass ? 1 : 0);
            });
        }
    }

    public static final class LastServedEntry {
        private final DestinationLaneId laneId;
        private final byte[] laneIncarnation;
        private final long lastServedRound;
        private final long serviceGapGeneration;

        public LastServedEntry(final DestinationLaneId laneId, final byte[] laneIncarnation,
                               final long lastServedRound, final long serviceGapGeneration) {
            this.laneId = Objects.requireNonNull(laneId, "laneId");
            this.laneIncarnation = fixedLength(laneIncarnation, 16, "laneIncarnation");
            requireNonNegative(lastServedRound, "lastServedRound");
            requireNonNegative(serviceGapGeneration, "serviceGapGeneration");
            this.lastServedRound = lastServedRound;
            this.serviceGapGeneration = serviceGapGeneration;
        }

        public DestinationLaneId laneId() {
            return laneId;
        }

        public byte[] laneIncarnation() {
            return Bytes.copy(laneIncarnation);
        }

        public long lastServedRound() {
            return lastServedRound;
        }

        public long serviceGapGeneration() {
            return serviceGapGeneration;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneId.bytes());
                CanonicalProtobuf.bytes(output, 2, laneIncarnation);
                CanonicalProtobuf.uint64(output, 3, lastServedRound);
                CanonicalProtobuf.uint64(output, 4, serviceGapGeneration);
            });
        }

        public static LastServedEntry decode(final byte[] encoded) {
            final var fields = read(encoded, false, "SchedulerLastServedEntryV1");
            requireNumbers(fields, new int[]{1, 2, 3, 4}, "SchedulerLastServedEntryV1");
            final LastServedEntry result = new LastServedEntry(new DestinationLaneId(fixedLength(bytes(fields.get(0), 1),
                    DestinationLaneId.LENGTH, "laneId")), fixedLength(bytes(fields.get(1), 2), 16,
                    "laneIncarnation"), uint(fields.get(2), 3), uint(fields.get(3), 4));
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerLastServedEntryV1");
            return result;
        }
    }

    public static final class LastServedMap {
        private final List<LastServedEntry> entries;
        private final byte[] digest;

        public LastServedMap(final List<LastServedEntry> entries) {
            this.entries = sortedLastServed(entries);
            this.digest = SchedulerProjectionsV1.digest("nereus-delay-scheduler-last-served-map-v1\0",
                    fieldsOneAndTwo());
        }

        private LastServedMap(final List<LastServedEntry> entries, final byte[] digest) {
            this.entries = sortedLastServed(entries);
            this.digest = fixed(digest, "digest");
        }

        public List<LastServedEntry> entries() {
            return entries;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                output.writeBytes(fieldsOneAndTwo());
                CanonicalProtobuf.bytes(output, 3, digest);
            });
        }

        public static LastServedMap decode(final byte[] encoded) {
            final var fields = read(encoded, true, "SchedulerLastServedMapV1");
            if (fields.size() < 2 || number(fields, 0) != 1 || number(fields, fields.size() - 1) != 3) {
                throw new IllegalArgumentException("invalid SchedulerLastServedMapV1 fields");
            }
            final List<LastServedEntry> entries = new ArrayList<>();
            for (int index = 1; index < fields.size() - 1; index++) {
                if (number(fields, index) != 2) {
                    throw new IllegalArgumentException("last-served entries must be repeated field 2");
                }
                entries.add(LastServedEntry.decode(bytes(fields.get(index), 2)));
            }
            final LastServedMap result = new LastServedMap(entries, bytes(fields.get(fields.size() - 1), 3));
            requireDigest(result.digest, result.fieldsOneAndTwo(), "nereus-delay-scheduler-last-served-map-v1\0");
            requireCanonical(encoded, result.canonicalBytes(), "SchedulerLastServedMapV1");
            return result;
        }

        private byte[] fieldsOneAndTwo() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.uint32(output, 1, VERSION);
                for (LastServedEntry entry : entries) {
                    CanonicalProtobuf.bytes(output, 2, entry.canonicalBytes());
                }
            });
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final boolean repeated,
                                                              final String name) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, repeated);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return fields;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected,
                                       final String name) {
        if (fields.size() != expected.length) {
            throw new IllegalArgumentException(name + " has an unexpected field count");
        }
        for (int index = 0; index < expected.length; index++) {
            if (number(fields, index) != expected[index]) {
                throw new IllegalArgumentException(name + " has an unexpected field order");
            }
        }
    }

    private static int number(final List<CanonicalProtobuf.Reader.Field> fields, final int index) {
        return fields.get(index).number();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid scheduler bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid scheduler uint field " + number);
        }
        return field.unsignedValue();
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("scheduler uint32 exceeds local range");
        }
        return (int) value;
    }

    private static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value > 1) {
            throw new IllegalArgumentException("scheduler bool must be 0 or 1");
        }
        return value == 1;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        return fixedLength(value, HASH_LENGTH, name);
    }

    private static byte[] fixedLength(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] optionalBytes(final byte[] value, final String name) {
        if (value == null) {
            return null;
        }
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty when present");
        }
        return Bytes.copy(value);
    }

    private static void requireNonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static byte[] digest(final String domain, final byte[] fields) {
        return Bytes.sha256(Bytes.utf8(domain), fields);
    }

    private static void requireDigest(final byte[] actual, final byte[] fields, final String domain) {
        if (!Bytes.constantTimeEquals(actual, digest(domain, fields))) {
            throw new IllegalArgumentException("scheduler projection digest mismatch");
        }
    }

    private static void requireCanonical(final byte[] encoded, final byte[] canonical, final String name) {
        if (!Arrays.equals(encoded, canonical)) {
            throw new IllegalArgumentException("non-canonical " + name);
        }
    }

    private static List<RingEntry> uniqueOrdered(final List<RingEntry> values) {
        Objects.requireNonNull(values, "entries");
        final List<RingEntry> copy = List.copyOf(values);
        for (int left = 0; left < copy.size(); left++) {
            for (int right = left + 1; right < copy.size(); right++) {
                if (sameIdentity(copy.get(left).laneId(), copy.get(left).laneIncarnation(), copy.get(right).laneId(),
                        copy.get(right).laneIncarnation())) {
                    throw new IllegalArgumentException("scheduler ring entries must be unique");
                }
            }
        }
        return copy;
    }

    private static List<DeficitEntry> sortedDeficit(final List<DeficitEntry> values) {
        Objects.requireNonNull(values, "entries");
        final List<DeficitEntry> copy = new ArrayList<>(values);
        copy.sort((left, right) -> compareIdentity(identityLane(left), identityIncarnation(left),
                identityLane(right), identityIncarnation(right)));
        for (int index = 1; index < copy.size(); index++) {
            if (compareIdentity(identityLane(copy.get(index - 1)), identityIncarnation(copy.get(index - 1)),
                    identityLane(copy.get(index)), identityIncarnation(copy.get(index))) == 0) {
                throw new IllegalArgumentException("scheduler map entries must be unique");
            }
        }
        return List.copyOf(copy);
    }

    private static List<LastServedEntry> sortedLastServed(final List<LastServedEntry> values) {
        Objects.requireNonNull(values, "entries");
        final List<LastServedEntry> copy = new ArrayList<>(values);
        copy.sort((left, right) -> compareIdentity(identityLane(left), identityIncarnation(left),
                identityLane(right), identityIncarnation(right)));
        for (int index = 1; index < copy.size(); index++) {
            if (compareIdentity(identityLane(copy.get(index - 1)), identityIncarnation(copy.get(index - 1)),
                    identityLane(copy.get(index)), identityIncarnation(copy.get(index))) == 0) {
                throw new IllegalArgumentException("scheduler map entries must be unique");
            }
        }
        return List.copyOf(copy);
    }

    private static DestinationLaneId identityLane(final DeficitEntry entry) {
        return entry.laneId;
    }

    private static DestinationLaneId identityLane(final LastServedEntry entry) {
        return entry.laneId;
    }

    private static byte[] identityIncarnation(final DeficitEntry entry) {
        return entry.laneIncarnation;
    }

    private static byte[] identityIncarnation(final LastServedEntry entry) {
        return entry.laneIncarnation;
    }

    private static boolean sameIdentity(final DestinationLaneId leftLane, final byte[] leftIncarnation,
                                        final DestinationLaneId rightLane, final byte[] rightIncarnation) {
        return compareIdentity(leftLane, leftIncarnation, rightLane, rightIncarnation) == 0;
    }

    private static int compareIdentity(final DestinationLaneId leftLane, final byte[] leftIncarnation,
                                       final DestinationLaneId rightLane, final byte[] rightIncarnation) {
        final int lane = compareBytes(leftLane.bytes(), rightLane.bytes());
        return lane != 0 ? lane : compareBytes(leftIncarnation, rightIncarnation);
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        for (int index = 0; index < Math.min(left.length, right.length); index++) {
            final int difference = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (difference != 0) {
                return difference;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
