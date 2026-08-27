package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Durable READY projection for one Lane.
 *
 * <p>The six-argument constructor is the legacy ordinary-head shape and is
 * kept byte-compatible for old local stores. A current value may carry one
 * optional native candidate as well. Both heads live in this one value and
 * the physical READY key is ordered by their minimum persistent wake, so a
 * future native candidate can never hide an earlier ordinary due message.</p>
 */
public final class ReadyIndexValue {
    private static final int LEGACY_VERSION = 1;
    private static final int DUAL_SCHEMA_GENERATION = 2;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-dual-ready-index");

    private final DestinationLaneId laneId;
    private final long nextEligibleAtEpochMs;
    private final long laneVersion;
    private final DelayMessageId messageId;
    private final int generation;
    private final byte[] timelineKeySha256;
    private final HandoffPolicyHeadRef policyHeadRef;
    private final ReadyIndexValue nativeHead;
    private final long persistentWakeAtEpochMs;
    private final byte[] stateDigest;

    public ReadyIndexValue(
            final DestinationLaneId laneId,
            final long nextEligibleAtEpochMs,
            final long laneVersion,
            final DelayMessageId messageId,
            final int generation,
            final byte[] timelineKeySha256) {
        this(laneId, nextEligibleAtEpochMs, laneVersion, messageId, generation, timelineKeySha256, null, null);
    }

    private ReadyIndexValue(
            final DestinationLaneId laneId,
            final long nextEligibleAtEpochMs,
            final long laneVersion,
            final DelayMessageId messageId,
            final int generation,
            final byte[] timelineKeySha256,
            final HandoffPolicyHeadRef policyHeadRef,
            final ReadyIndexValue nativeHead) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        if (nextEligibleAtEpochMs < 0 || laneVersion < 0) {
            throw new IllegalArgumentException("invalid READY value");
        }
        this.nextEligibleAtEpochMs = nextEligibleAtEpochMs;
        this.laneVersion = laneVersion;
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = generation;
        Bytes.requireLength(timelineKeySha256, HASH_LENGTH, "timelineKeySha256");
        this.timelineKeySha256 = Bytes.copy(timelineKeySha256);
        this.policyHeadRef = policyHeadRef;
        if (policyHeadRef != null && nativeHead != null) {
            throw new IllegalArgumentException("READY value cannot be both a native head and a dual value");
        }
        if (nativeHead != null) {
            if (!nativeHead.isNativeCandidate()
                    || !nativeHead.laneId().equals(laneId)
                    || nativeHead.policyHeadRef() == null) {
                throw new IllegalArgumentException("invalid nested native READY head");
            }
            if (nativeHead.laneVersion() != laneVersion) {
                throw new IllegalArgumentException("nested native READY head lane version mismatch");
            }
            this.nativeHead = nativeHead;
            this.persistentWakeAtEpochMs = Math.min(nextEligibleAtEpochMs, nativeHead.nextEligibleAtEpochMs());
        } else {
            this.nativeHead = null;
            this.persistentWakeAtEpochMs = nextEligibleAtEpochMs;
        }
        this.stateDigest = nativeHead == null ? null : Bytes.sha256(DIGEST_DOMAIN, dualFieldsWithoutDigest());
    }

    /** Builds a native candidate head suitable for nesting in an ordinary READY value. */
    public static ReadyIndexValue nativeCandidate(
            final DestinationLaneId laneId,
            final long candidateAtEpochMs,
            final long laneVersion,
            final DelayMessageId messageId,
            final int generation,
            final byte[] timelineKeySha256,
            final HandoffPolicyHeadRef policyHeadRef) {
        return new ReadyIndexValue(
                laneId,
                candidateAtEpochMs,
                laneVersion,
                messageId,
                generation,
                timelineKeySha256,
                Objects.requireNonNull(policyHeadRef, "policyHeadRef"),
                null);
    }

    /** Returns an ordinary head carrying the supplied native candidate. */
    public ReadyIndexValue withNativeHead(final ReadyIndexValue nativeCandidate) {
        if (isNativeCandidate()) {
            throw new IllegalStateException("native head cannot own another native head");
        }
        return new ReadyIndexValue(
                laneId,
                nextEligibleAtEpochMs,
                laneVersion,
                messageId,
                generation,
                timelineKeySha256,
                null,
                Objects.requireNonNull(nativeCandidate, "nativeCandidate"));
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    /** Returns this head's ordinary/native candidate time. */
    public long nextEligibleAtEpochMs() {
        return nextEligibleAtEpochMs;
    }

    public long laneVersion() {
        return laneVersion;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public byte[] timelineKeySha256() {
        return Bytes.copy(timelineKeySha256);
    }

    public HandoffPolicyHeadRef policyHeadRef() {
        return policyHeadRef;
    }

    public boolean isNativeCandidate() {
        return policyHeadRef != null;
    }

    public ReadyIndexValue nativeHead() {
        return nativeHead;
    }

    public long persistentWakeAtEpochMs() {
        return persistentWakeAtEpochMs;
    }

    public byte[] stateDigest() {
        return stateDigest == null ? null : Bytes.copy(stateDigest);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ReadyIndexValue that
                && nextEligibleAtEpochMs == that.nextEligibleAtEpochMs
                && laneVersion == that.laneVersion
                && generation == that.generation
                && persistentWakeAtEpochMs == that.persistentWakeAtEpochMs
                && laneId.equals(that.laneId)
                && messageId.equals(that.messageId)
                && Arrays.equals(timelineKeySha256, that.timelineKeySha256)
                && Objects.equals(policyHeadRef, that.policyHeadRef)
                && Objects.equals(nativeHead, that.nativeHead)
                && Arrays.equals(stateDigest, that.stateDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
                laneId,
                nextEligibleAtEpochMs,
                laneVersion,
                messageId,
                generation,
                policyHeadRef,
                nativeHead,
                persistentWakeAtEpochMs);
        result = 31 * result + Arrays.hashCode(timelineKeySha256);
        return 31 * result + Arrays.hashCode(stateDigest);
    }

    public byte[] encode() {
        if (nativeHead == null) {
            return ByteBuffer.allocate(4 + 32 + 8 + 8 + DelayMessageId.LENGTH + 4 + HASH_LENGTH)
                    .putInt(LEGACY_VERSION)
                    .put(laneId.bytes())
                    .putLong(nextEligibleAtEpochMs)
                    .putLong(laneVersion)
                    .put(messageId.bytes())
                    .putInt(generation)
                    .put(timelineKeySha256)
                    .array();
        }
        final byte[] fields = dualFieldsWithoutDigest();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, DUAL_SCHEMA_GENERATION);
            output.writeBytes(fields);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(DIGEST_DOMAIN, fields));
        });
    }

    public static ReadyIndexValue decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length >= Integer.BYTES
                && ByteBuffer.wrap(encoded, 0, Integer.BYTES).getInt() == LEGACY_VERSION) {
            final int expectedLength = 4 + 32 + 8 + 8 + DelayMessageId.LENGTH + 4 + HASH_LENGTH;
            if (encoded.length != expectedLength) {
                throw new IllegalArgumentException("invalid READY value length");
            }
            final ByteBuffer input = ByteBuffer.wrap(encoded);
            input.getInt();
            final byte[] lane = new byte[32];
            input.get(lane);
            final long next = input.getLong();
            final long laneVersion = input.getLong();
            final byte[] message = new byte[DelayMessageId.LENGTH];
            input.get(message);
            final int generation = input.getInt();
            final byte[] hash = new byte[HASH_LENGTH];
            input.get(hash);
            final ReadyIndexValue result = new ReadyIndexValue(
                    new DestinationLaneId(lane), next, laneVersion, new DelayMessageId(message), generation, hash);
            if (!Arrays.equals(encoded, result.encode())) {
                throw new IllegalArgumentException("non-canonical READY value");
            }
            return result;
        }
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded);
        if (fields.size() != 7) {
            throw new IllegalArgumentException("dual READY value has an unexpected field count");
        }
        requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7});
        if (uint(fields.get(0), 1) != DUAL_SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported READY schema generation");
        }
        final DestinationLaneId lane = new DestinationLaneId(bytes(fields.get(1), 2));
        final long laneVersion = uint(fields.get(2), 3);
        final long persistentWake = uint(fields.get(3), 4);
        final ReadyIndexValue ordinary = decodeHead(bytes(fields.get(4), 5), lane, laneVersion, false);
        final ReadyIndexValue nativeHead = decodeHead(bytes(fields.get(5), 6), lane, laneVersion, true);
        final byte[] digest = fixed(fields.get(6), 7);
        if (ordinary.laneVersion != laneVersion || nativeHead.laneVersion != laneVersion) {
            throw new IllegalArgumentException("dual READY lane version mismatch");
        }
        final ReadyIndexValue result = ordinary.withNativeHead(nativeHead);
        if (result.persistentWakeAtEpochMs != persistentWake || !Bytes.constantTimeEquals(result.stateDigest, digest)) {
            throw new IllegalArgumentException("dual READY value digest or projection mismatch");
        }
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical dual READY value");
        }
        return result;
    }

    private byte[] dualFieldsWithoutDigest() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, laneId.bytes());
            CanonicalProtobuf.uint64(output, 3, laneVersion);
            CanonicalProtobuf.uint64(output, 4, persistentWakeAtEpochMs);
            CanonicalProtobuf.bytes(output, 5, encodeHead(this));
            CanonicalProtobuf.bytes(output, 6, encodeHead(nativeHead));
        });
    }

    private static byte[] encodeHead(final ReadyIndexValue head) {
        Objects.requireNonNull(head, "head");
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, head.messageId.bytes());
            CanonicalProtobuf.uint32Bits(output, 2, head.generation);
            CanonicalProtobuf.uint64(output, 3, head.nextEligibleAtEpochMs);
            CanonicalProtobuf.bytes(output, 4, head.timelineKeySha256);
            if (head.policyHeadRef != null) {
                CanonicalProtobuf.bytes(output, 5, head.policyHeadRef.canonicalBytes());
            }
        });
    }

    private static ReadyIndexValue decodeHead(
            final byte[] encoded, final DestinationLaneId lane, final long laneVersion, final boolean nativeHead) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded);
        if (fields.size() != (nativeHead ? 5 : 4)) {
            throw new IllegalArgumentException("READY head has an unexpected field count");
        }
        requireNumbers(fields, nativeHead ? new int[] {1, 2, 3, 4, 5} : new int[] {1, 2, 3, 4});
        final HandoffPolicyHeadRef ref = nativeHead ? HandoffPolicyHeadRef.decode(bytes(fields.get(4), 5)) : null;
        final ReadyIndexValue result = new ReadyIndexValue(
                lane,
                uint(fields.get(2), 3),
                laneVersion,
                new DelayMessageId(fixedMessage(fields.get(0), 1)),
                intBits(fields.get(1), 2),
                fixed(fields.get(3), 4),
                ref,
                null);
        if (!Arrays.equals(encoded, encodeHead(result))) {
            throw new IllegalArgumentException("non-canonical READY head");
        }
        return result;
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static void requireNumbers(final List<CanonicalProtobuf.Reader.Field> fields, final int[] expected) {
        if (fields.size() != expected.length) {
            throw new IllegalArgumentException("READY field count mismatch");
        }
        for (int index = 0; index < expected.length; index++) {
            if (fields.get(index).number() != expected[index]) {
                throw new IllegalArgumentException("READY field order mismatch");
            }
        }
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("READY field is not bytes: " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, HASH_LENGTH, "READY field " + number);
        return value;
    }

    private static byte[] fixedMessage(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, DelayMessageId.LENGTH, "READY messageId");
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("READY field is not uint: " + number);
        }
        return field.unsignedValue();
    }

    private static int intBits(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value > 0xffff_ffffL) {
            throw new IllegalArgumentException("READY generation is outside uint32");
        }
        return (int) value;
    }
}
