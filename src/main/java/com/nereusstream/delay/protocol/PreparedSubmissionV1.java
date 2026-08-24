package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable two-branch prepared submission; its branch cannot change on retry. */
public final class PreparedSubmissionV1 {
    public static final int VERSION = 1;

    private final byte[] managedFrame;
    private final NativePreparedDeliveryV1 nativePrepared;

    private PreparedSubmissionV1(final byte[] managedFrame, final NativePreparedDeliveryV1 nativePrepared) {
        if ((managedFrame == null) == (nativePrepared == null)) {
            throw new IllegalArgumentException("PreparedSubmissionV1 must select exactly one branch");
        }
        this.managedFrame = managedFrame == null ? null : Bytes.copy(managedFrame);
        this.nativePrepared = nativePrepared;
    }

    public static PreparedSubmissionV1 managed(final byte[] frame) {
        Objects.requireNonNull(frame, "frame");
        final PreparedCommand decoded = CommandCodec.decodeFrameV1(frame);
        if (!Arrays.equals(frame, CommandCodec.encodeFrameV1(decoded))) {
            throw new IllegalArgumentException("managed PreparedSubmission frame is not canonical");
        }
        return new PreparedSubmissionV1(frame, null);
    }

    public static PreparedSubmissionV1 nativePrepared(final NativePreparedDeliveryV1 prepared) {
        return new PreparedSubmissionV1(null, Objects.requireNonNull(prepared, "prepared"));
    }

    public static PreparedSubmissionV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PreparedSubmissionV1");
        if (fields.size() != 2 || fields.get(0).number() != 1 || QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("invalid PreparedSubmissionV1 version/field set");
        }
        final PreparedSubmissionV1 result =
                switch (fields.get(1).number()) {
                    case 2 -> managed(QueryCodecSupport.bytes(fields.get(1), 2));
                    case 3 ->
                        nativePrepared(NativePreparedDeliveryV1.decode(QueryCodecSupport.nested(fields.get(1), 3)));
                    default -> throw new IllegalArgumentException("unknown PreparedSubmissionV1 branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedSubmissionV1");
        return result;
    }

    public boolean isManaged() {
        return managedFrame != null;
    }

    public byte[] managedFrame() {
        return managedFrame == null ? null : Bytes.copy(managedFrame);
    }

    public NativePreparedDeliveryV1 nativePrepared() {
        return nativePrepared;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            if (managedFrame != null) {
                CanonicalProtobuf.bytes(output, 2, managedFrame);
            } else {
                CanonicalProtobuf.bytes(output, 3, nativePrepared.canonicalBytes());
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PreparedSubmissionV1 that
                && Arrays.equals(managedFrame, that.managedFrame)
                && Objects.equals(nativePrepared, that.nativePrepared);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(managedFrame), nativePrepared);
    }
}
