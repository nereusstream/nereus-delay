package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable two-branch prepared submission; its branch cannot change on retry. */
public final class PreparedSubmission {
    public static final int VERSION = 1;

    private final byte[] managedFrame;
    private final NativePreparedDelivery nativePrepared;

    private PreparedSubmission(final byte[] managedFrame, final NativePreparedDelivery nativePrepared) {
        if ((managedFrame == null) == (nativePrepared == null)) {
            throw new IllegalArgumentException("PreparedSubmission must select exactly one branch");
        }
        this.managedFrame = managedFrame == null ? null : Bytes.copy(managedFrame);
        this.nativePrepared = nativePrepared;
    }

    public static PreparedSubmission managed(final byte[] frame) {
        Objects.requireNonNull(frame, "frame");
        final PreparedCommand decoded = CommandCodec.decodeManagedFrame(frame);
        if (!Arrays.equals(frame, CommandCodec.encodeManagedFrame(decoded))) {
            throw new IllegalArgumentException("managed PreparedSubmission frame is not canonical");
        }
        return new PreparedSubmission(frame, null);
    }

    public static PreparedSubmission nativePrepared(final NativePreparedDelivery prepared) {
        return new PreparedSubmission(null, Objects.requireNonNull(prepared, "prepared"));
    }

    public static PreparedSubmission decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PreparedSubmission");
        if (fields.size() != 2 || fields.get(0).number() != 1 || QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("invalid PreparedSubmission version/field set");
        }
        final PreparedSubmission result =
                switch (fields.get(1).number()) {
                    case 2 -> managed(QueryCodecSupport.bytes(fields.get(1), 2));
                    case 3 -> nativePrepared(NativePreparedDelivery.decode(QueryCodecSupport.nested(fields.get(1), 3)));
                    default -> throw new IllegalArgumentException("unknown PreparedSubmission branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PreparedSubmission");
        return result;
    }

    public boolean isManaged() {
        return managedFrame != null;
    }

    public byte[] managedFrame() {
        return managedFrame == null ? null : Bytes.copy(managedFrame);
    }

    public NativePreparedDelivery nativePrepared() {
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
        return other instanceof PreparedSubmission that
                && Arrays.equals(managedFrame, that.managedFrame)
                && Objects.equals(nativePrepared, that.nativePrepared);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(managedFrame), nativePrepared);
    }
}
