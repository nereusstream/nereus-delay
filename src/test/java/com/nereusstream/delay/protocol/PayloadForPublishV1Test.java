package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class PayloadForPublishV1Test {
    @Test
    void inlineBranchRoundTripsIncludingIntentionalEmptyPayload() {
        final PayloadForPublishV1 payload = PayloadForPublishV1.inline(new byte[0]);

        final PayloadForPublishV1 decoded = PayloadForPublishV1.decode(payload.canonicalBytes());

        assertEquals(payload, decoded);
        assertTrue(decoded.hasInlinePayload());
        assertFalse(decoded.hasObject());
        assertArrayEquals(new byte[0], decoded.inlinePayload());
        assertThrows(IllegalStateException.class, decoded::object);
    }

    @Test
    void committedObjectBranchRoundTripsAndKeepsDescriptorIdentity() {
        final byte[] bytes = Bytes.utf8("payload");
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(
                new ProfileRefV1(
                        Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("store")), ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                bytes.length,
                Bytes.sha256(bytes),
                nonZero(32, 1),
                nonZero(32, 2));
        final PayloadForPublishV1 payload = PayloadForPublishV1.object(descriptor);

        final PayloadForPublishV1 decoded = PayloadForPublishV1.decode(payload.canonicalBytes());

        assertEquals(payload, decoded);
        assertEquals(descriptor, decoded.object());
        assertEquals(bytes.length, decoded.length());
        assertArrayEquals(Bytes.sha256(bytes), decoded.payloadSha256());
    }

    @Test
    void decoderRejectsDeclaredLengthOrDigestThatDoesNotMatchInlineBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("x")));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("y"));
        });

        assertThrows(IllegalArgumentException.class, () -> PayloadForPublishV1.decode(encoded));
    }

    @Test
    void decoderRejectsObjectDescriptorWithDifferentLengthOrDigest() {
        final byte[] objectBytes = Bytes.utf8("object");
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(
                new ProfileRefV1(
                        Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("store")), ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                objectBytes.length,
                Bytes.sha256(objectBytes),
                nonZero(32, 3),
                nonZero(32, 4));
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, objectBytes.length + 1L);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(objectBytes));
            CanonicalProtobuf.bytes(output, 4, descriptor.canonicalBytes());
        });

        assertThrows(IllegalArgumentException.class, () -> PayloadForPublishV1.decode(encoded));
    }

    private static byte[] nonZero(final int length, final int seed) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) seed);
        return result;
    }
}
