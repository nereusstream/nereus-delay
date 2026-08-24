package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

class PayloadReferenceTest {
    @Test
    void descriptorProjectionPreservesAbsentOptionalEtagAcrossTheLegacyValueCodec() {
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(
                new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 1), ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                7,
                Bytes.sha256(Bytes.utf8("payload")),
                bytes(32, 2),
                bytes(32, 3));
        final PayloadReference reference = PayloadReference.fromDescriptor(descriptor);

        assertNull(reference.etag());
        assertTrue(reference.hasCommitIdentity());
        assertArrayEquals(descriptor.reservationId(), reference.reservationId());
        assertArrayEquals(descriptor.proofId(), reference.proofId());
        assertEquals(reference, PayloadReference.decode(reference.encode()));
        assertNull(PayloadReference.decode(reference.encode()).etag());
    }

    @Test
    void legacyProjectionWithoutCommitIdentityRemainsReadable() {
        final PayloadReference legacy = new PayloadReference(
                bytes(32, 1),
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                7,
                Bytes.sha256(Bytes.utf8("payload")));

        final PayloadReference decoded = PayloadReference.decode(legacy.encode());
        assertEquals(legacy, decoded);
        assertFalse(decoded.hasCommitIdentity());
        assertNull(decoded.reservationId());
        assertNull(decoded.proofId());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
