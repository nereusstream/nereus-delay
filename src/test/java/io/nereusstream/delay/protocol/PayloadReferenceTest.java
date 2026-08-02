package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PayloadReferenceTest {
    @Test
    void descriptorProjectionPreservesAbsentOptionalEtagAcrossTheLegacyValueCodec() {
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(
                new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 1), ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"), Bytes.utf8("object"), Bytes.utf8("version"), null, 7,
                Bytes.sha256(Bytes.utf8("payload")), bytes(32, 2), bytes(32, 3));
        final PayloadReference reference = PayloadReference.fromDescriptor(descriptor);

        assertNull(reference.etag());
        assertEquals(reference, PayloadReference.decode(reference.encode()));
        assertNull(PayloadReference.decode(reference.encode()).etag());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
