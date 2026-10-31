package com.nereusstream.delay.protocol;

/** Source framing and checked inline sizes. Format limits grant no service capacity or Source coverage. */
public final class TargetMigrationArchiveSourceFrame {
    private TargetMigrationArchiveSourceFrame() {}

    public static final int MAX_CONTROL_BODY_BYTES = maximum(
            TargetMigrationControlBody.MAX_CANONICAL_BYTES,
            TargetPayloadSummaryInitializationBody.MAX_CANONICAL_BYTES,
            TargetProtocolActivationBody.MAX_BYTES,
            TargetMembershipControlBody.MAX_CANONICAL_BYTES,
            TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES,
            TargetCloseBody.MAX_CANONICAL_BYTES,
            TargetNativePolicyControlBody.MAX_CANONICAL_BYTES,
            TargetChannelControlBody.MAX_CANONICAL_BYTES,
            TargetSloObservationControlBody.MAX_CANONICAL_BYTES,
            TargetSloWriterControlBody.MAX_CANONICAL_BYTES,
            TargetParentControlCreditBody.MAX_CANONICAL_BYTES,
            TargetScopeControlObligationBody.MAX_CANONICAL_BYTES);
    public static final int MAX_OTHER_BODY_BYTES = maximum(
            TargetTimeFenceBody.MAX_CANONICAL_BYTES,
            TargetExpireGenerationBody.MAX_CANONICAL_BYTES,
            TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES,
            TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES,
            TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES,
            TargetSloObservationEventBody.MAX_CANONICAL_BYTES);
    /** Three exact 32-byte CONTROL hashes, nested under AuthorIdentity's CONTROL branch. */
    public static final int MAX_CONTROL_AUTHOR_BYTES = bytes(2, bytes(1, 32) + bytes(2, 32) + bytes(3, 32));

    public static final int MAX_CONTROL_FRAME_BYTES = systemFrame(MAX_CONTROL_BODY_BYTES, MAX_CONTROL_AUTHOR_BYTES);
    public static final int MAX_OTHER_FRAME_BYTES =
            systemFrame(MAX_OTHER_BODY_BYTES, com.nereusstream.delay.runtime.TargetResultRecord.MAX_AUTHOR_BYTES);
    public static final int MAX_COMMAND_FRAME_BYTES = commandFrame(TargetScheduleBinding.MAX_BODY_BYTES);
    /** Bounds only the explicitly bounded typed subsets above; opaque legacy outcomes are not in this union. */
    public static final int MAX_TYPED_SUBSET_FRAME_BYTES =
            maximum(MAX_CONTROL_FRAME_BYTES, MAX_OTHER_FRAME_BYTES, MAX_COMMAND_FRAME_BYTES);

    public static final int MAX_ENVELOPE_BYTES =
            Integer.MAX_VALUE - ShardLogFrame.HEADER_LENGTH - ShardLogFrame.TRAILER_LENGTH;
    /** NDL1's complete byte[] representation, not a promise that any JVM, loan or Source accepts this size. */
    public static final int MAX_FRAME_BYTES =
            MAX_ENVELOPE_BYTES + ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH;

    public static final int MAX_CANONICAL_BYTES = MAX_FRAME_BYTES;
    public static final int NATIVE_ENVELOPE_BYTES = 12;
    public static final int MAX_NATIVE_CANONICAL_BYTES = Integer.MAX_VALUE - NATIVE_ENVELOPE_BYTES;

    /** Arithmetic for a selected registered body family; it grants no Source or reader permission. */
    public static long controlFrameBytes(long bodyBytes) {
        return systemFrameBytes(bodyBytes, MAX_CONTROL_AUTHOR_BYTES);
    }

    public static long systemFrameBytes(long bodyBytes, long authorBytes) {
        if (bodyBytes < 0 || bodyBytes > Integer.MAX_VALUE || authorBytes < 0 || authorBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Source body/author length is outside its encoded range");
        }
        final long subject = bytes(1, RouteIncarnation.LENGTH) + scalar(2, 5);
        final long mutation = scalar(1, 1)
                + bytes(2, SystemMutation.HASH_LENGTH)
                + bytesLong(3, subject)
                + scalar(4, 1)
                + scalar(5, 10)
                + bytesLong(6, bodyBytes)
                + bytes(7, SystemMutation.HASH_LENGTH)
                + scalar(8, 1)
                + bytesLong(9, authorBytes)
                + scalar(10, 5)
                + bytes(11, SystemMutation.SIGNATURE_LENGTH);
        return Math.addExact(
                ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH, scalar(1, 1) + bytesLong(3, mutation));
    }

    private static int systemFrame(int body, int author) {
        // SystemMutation.canonicalEnvelope: all eleven fields, then the ShardLogEnvelope oneof.
        return Math.toIntExact(systemFrameBytes(body, author));
    }

    private static int commandFrame(int body) {
        // CommandCodec.encodeEnvelope: all eight ClientCommand fields and the outer oneof.
        final int command = scalar(1, 1)
                + bytes(2, CommandId.LENGTH)
                + bytes(3, DelayMessageId.LENGTH)
                + scalar(5, 1)
                + scalar(7, 10)
                + bytes(8, body)
                + bytes(9, 32)
                + scalar(10, 1);
        return Math.addExact(
                ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH, scalar(1, 1) + bytes(2, command));
    }

    static int scalar(int field, int valueBytes) {
        return Math.addExact(varint(field << 3), valueBytes);
    }

    static int bytes(int field, int length) {
        return Math.addExact(Math.addExact(varint((field << 3) | 2), varint(length)), length);
    }

    public static long bytesFieldBytes(int field, long length) {
        if (field <= 0 || field > 0x1fff || length < 0 || length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("inline field cannot be represented by the canonical byte[] codec");
        }
        long value = length;
        int prefix = 1;
        while ((value >>>= 7) != 0) {
            prefix++;
        }
        return Math.addExact(Math.addExact(varint((field << 3) | 2), prefix), length);
    }

    private static long bytesLong(int field, long length) {
        return bytesFieldBytes(field, length);
    }

    public static long scalarFieldBytes(int field, long bits) {
        if (field <= 0 || field > 0x1fff) {
            throw new IllegalArgumentException("canonical field is outside its encoded range");
        }
        int size = 1;
        if (bits < 0) {
            size = 10;
        } else {
            while ((bits >>>= 7) != 0) {
                size++;
            }
        }
        return Math.addExact(varint(field << 3), size);
    }

    public static long frameBytes(long envelopeBytes) {
        if (envelopeBytes < 0 || envelopeBytes > MAX_ENVELOPE_BYTES) {
            throw new IllegalArgumentException("complete Source envelope cannot be represented by NDL1");
        }
        return Math.addExact(envelopeBytes, ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH);
    }

    public static long nativeValueBytes(long canonicalBytes) {
        if (canonicalBytes < 0) {
            throw new IllegalArgumentException("negative Native canonical length");
        }
        return Math.addExact(canonicalBytes, NATIVE_ENVELOPE_BYTES);
    }
    /** Check the whole Native value before copying or assembling a full inline Source field. */
    public static int requireNativeCanonical(long canonicalBytes, String name) {
        if (nativeValueBytes(canonicalBytes) > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " cannot represent its complete Source and Native metadata");
        }
        return Math.toIntExact(canonicalBytes);
    }
    /** Same concatenated preimage, without requiring another frame-sized byte[] merely for hashing. */
    static byte[] digest(byte[] domain, byte[] fields) {
        try {
            final var hash = java.security.MessageDigest.getInstance("SHA-256");
            hash.update(domain);
            return hash.digest(fields);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static int varint(int value) {
        int bytes = 1;
        while ((value >>>= 7) != 0) {
            bytes++;
        }
        return bytes;
    }

    private static int maximum(int... bounds) {
        int result = 0;
        for (int bound : bounds) {
            result = Math.max(result, bound);
        }
        return result;
    }

    public static void requireFrame(byte[] frame, TargetQuotaMutation at) {
        if (frame == null || frame.length < ShardLogFrame.HEADER_LENGTH + ShardLogFrame.TRAILER_LENGTH) {
            throw new IllegalArgumentException("complete archive Source frame is missing its NDL1 representation");
        }
        frameBytes((long) frame.length - ShardLogFrame.HEADER_LENGTH - ShardLogFrame.TRAILER_LENGTH);
        final var decoded = ShardLogFrame.decode(frame);
        final byte[] digest;
        if (decoded.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(frame);
            if (!command.shardId().equals(at.source().shardId())) {
                throw new IllegalArgumentException("archive frame changed actual Source Shard");
            }
            digest = command.canonicalFrameDigest();
        } else {
            final var input = SystemMutation.decodeTargetFrame(frame);
            if (!input.shardId().equals(at.source().shardId())) {
                throw new IllegalArgumentException("archive frame changed actual Source Shard");
            }
            digest = Bytes.sha256(input.canonicalEnvelope());
        }
        if (!java.util.Arrays.equals(digest, at.mutationDigest())) {
            throw new IllegalArgumentException("archive frame changed its actual complete Source digest");
        }
    }

    static int schemaVersion(byte[] encoded, String name) {
        if (encoded == null || encoded.length < 2 || encoded[0] != 8 || (encoded[1] & 0x80) != 0) {
            throw new IllegalArgumentException(name + " is missing its canonical schema");
        }
        return Byte.toUnsignedInt(encoded[1]);
    }
}
