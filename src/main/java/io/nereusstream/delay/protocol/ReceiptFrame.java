package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Objects;

/** Fixed NDR1 framing around one canonical receipt payload. */
public final class ReceiptFrame {
    public static final int MAGIC = 0x4e445231;
    public static final int FRAMING_VERSION = 1;
    public static final int HEADER_LENGTH = 12;
    public static final int TRAILER_LENGTH = 4;
    public static final String TEXT_PREFIX = "ndr1_";

    private ReceiptFrame() {
    }

    public static byte[] encode(final ReceiptKind kind, final byte[] payload) {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(payload, "payload");
        final ByteBuffer header = ByteBuffer.allocate(HEADER_LENGTH);
        header.putInt(MAGIC).put((byte) FRAMING_VERSION).put((byte) kind.wireValue()).putShort((short) 0)
                .putInt(payload.length);
        final byte[] body = Bytes.concat(header.array(), payload);
        return Bytes.concat(body, Bytes.crc32cbe(body));
    }

    public static String encodeText(final ReceiptKind kind, final byte[] payload) {
        return TEXT_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(encode(kind, payload));
    }

    public static Decoded decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length < HEADER_LENGTH + TRAILER_LENGTH) {
            throw new IllegalArgumentException("receipt frame is truncated");
        }
        final ByteBuffer header = ByteBuffer.wrap(encoded, 0, HEADER_LENGTH);
        if (header.getInt() != MAGIC) {
            throw new IllegalArgumentException("receipt frame magic mismatch");
        }
        if ((header.get() & 0xff) != FRAMING_VERSION) {
            throw new IllegalArgumentException("unsupported receipt framing version");
        }
        final ReceiptKind kind = ReceiptKind.fromWire(header.get() & 0xff);
        if (header.getShort() != 0) {
            throw new IllegalArgumentException("receipt frame flags must be zero");
        }
        final long payloadLength = Integer.toUnsignedLong(header.getInt());
        if (payloadLength != encoded.length - HEADER_LENGTH - TRAILER_LENGTH) {
            throw new IllegalArgumentException("receipt frame payload length mismatch");
        }
        final long expectedCrc = Bytes.crc32c(encoded, 0, encoded.length - TRAILER_LENGTH);
        final long actualCrc = Bytes.readU32be(encoded, encoded.length - TRAILER_LENGTH);
        if (expectedCrc != actualCrc) {
            throw new IllegalArgumentException("receipt frame CRC mismatch");
        }
        final byte[] payload = java.util.Arrays.copyOfRange(encoded, HEADER_LENGTH,
                encoded.length - TRAILER_LENGTH);
        return new Decoded(kind, payload);
    }

    public static Decoded decodeText(final String encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (!encoded.startsWith(TEXT_PREFIX)) {
            throw new IllegalArgumentException("receipt text prefix mismatch");
        }
        final String text = encoded.substring(TEXT_PREFIX.length());
        if (text.isEmpty()) {
            throw new IllegalArgumentException("receipt text payload is empty");
        }
        try {
            return decode(Base64.getUrlDecoder().decode(text));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("invalid receipt Base64url text", exception);
        }
    }

    public record Decoded(ReceiptKind kind, byte[] payload) {
        public Decoded {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(payload, "payload");
            payload = Bytes.copy(payload);
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }
    }
}
