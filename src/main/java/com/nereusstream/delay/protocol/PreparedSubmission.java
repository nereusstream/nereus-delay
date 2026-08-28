package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Immutable two-branch prepared submission; its branch cannot change on retry. */
public final class PreparedSubmission {
    public static final int VERSION = 1;
    public static final int NATIVE_RECORD_VERSION = 2;

    private final byte[] managedFrame;
    private final NativePreparedDelivery nativePrepared;
    private final NativePreparedRecordContext nativeRecordContext;

    private PreparedSubmission(
            final byte[] managedFrame,
            final NativePreparedDelivery nativePrepared,
            final NativePreparedRecordContext nativeRecordContext) {
        if ((managedFrame == null) == (nativePrepared == null)) {
            throw new IllegalArgumentException("PreparedSubmission must select exactly one branch");
        }
        if (managedFrame != null && nativeRecordContext != null) {
            throw new IllegalArgumentException("managed PreparedSubmission cannot carry a native record context");
        }
        this.managedFrame = managedFrame == null ? null : Bytes.copy(managedFrame);
        this.nativePrepared = nativePrepared;
        this.nativeRecordContext = nativeRecordContext;
        if (nativeRecordContext != null) {
            NativePreparedRecordBinding.requireExact(nativeRecordContext, nativePrepared);
        }
    }

    public static PreparedSubmission managed(final byte[] frame) {
        Objects.requireNonNull(frame, "frame");
        final PreparedCommand decoded = CommandCodec.decodeManagedFrame(frame);
        if (!Arrays.equals(frame, CommandCodec.encodeManagedFrame(decoded))) {
            throw new IllegalArgumentException("managed PreparedSubmission frame is not canonical");
        }
        return new PreparedSubmission(frame, null, null);
    }

    public static PreparedSubmission nativePrepared(final NativePreparedDelivery prepared) {
        return new PreparedSubmission(null, Objects.requireNonNull(prepared, "prepared"), null);
    }

    /** Creates the current AUTO_FAST branch with its hash-bound record context. */
    public static PreparedSubmission nativePrepared(
            final NativePreparedDelivery prepared, final NativePreparedRecordContext context) {
        return new PreparedSubmission(
                null, Objects.requireNonNull(prepared, "prepared"), Objects.requireNonNull(context, "context"));
    }

    public static PreparedSubmission decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "PreparedSubmission");
        if (fields.isEmpty() || fields.get(0).number() != 1) {
            throw new IllegalArgumentException("invalid PreparedSubmission version/field set");
        }
        final long version = QueryCodecSupport.uint(fields.get(0), 1);
        final PreparedSubmission result;
        if (version == VERSION && fields.size() == 2) {
            result = switch (fields.get(1).number()) {
                case 2 -> managed(QueryCodecSupport.bytes(fields.get(1), 2));
                case 3 -> nativePrepared(NativePreparedDelivery.decode(QueryCodecSupport.nested(fields.get(1), 3)));
                default -> throw new IllegalArgumentException("unknown PreparedSubmission branch");
            };
        } else if (version == NATIVE_RECORD_VERSION
                && fields.size() == 3
                && fields.get(1).number() == 3
                && fields.get(2).number() == 4) {
            result = nativePrepared(
                    NativePreparedDelivery.decode(QueryCodecSupport.nested(fields.get(1), 3)),
                    NativePreparedRecordContext.decode(QueryCodecSupport.nested(fields.get(2), 4)));
        } else {
            throw new IllegalArgumentException("invalid PreparedSubmission version/field set");
        }
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

    public NativePreparedRecordContext nativeRecordContext() {
        return nativeRecordContext;
    }

    public boolean isNativeRecordReady() {
        return nativePrepared != null && nativeRecordContext != null;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, nativeRecordContext == null ? VERSION : NATIVE_RECORD_VERSION);
            if (managedFrame != null) {
                CanonicalProtobuf.bytes(output, 2, managedFrame);
            } else {
                CanonicalProtobuf.bytes(output, 3, nativePrepared.canonicalBytes());
                if (nativeRecordContext != null) {
                    CanonicalProtobuf.bytes(output, 4, nativeRecordContext.canonicalBytes());
                }
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PreparedSubmission that
                && Arrays.equals(managedFrame, that.managedFrame)
                && Objects.equals(nativePrepared, that.nativePrepared)
                && Objects.equals(nativeRecordContext, that.nativeRecordContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(managedFrame), nativePrepared, nativeRecordContext);
    }
}
