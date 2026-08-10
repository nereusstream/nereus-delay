package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueEnvelopeTest {
    @Test
    void registeredPayloadTypeRoundTrips() {
        final byte[] payload = Bytes.utf8("canonical-value");

        final ValueEnvelope.Decoded decoded = ValueEnvelope.decode(
                ValueEnvelope.encode(ValueEnvelope.MAX_REGISTERED_VALUE_TYPE, payload),
                ValueEnvelope.MAX_REGISTERED_VALUE_TYPE);

        assertEquals(ValueEnvelope.MAX_REGISTERED_VALUE_TYPE, decoded.valueType());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void decodeAnyPreservesRegisteredDiscriminatorForSharedKeyBranches() {
        final byte[] payload = Bytes.utf8("retired-identity-branch");

        final ValueEnvelope.Decoded decoded = ValueEnvelope.decodeAny(ValueEnvelope.encode(1, payload));

        assertEquals(1, decoded.valueType());
        assertArrayEquals(payload, decoded.payload());
    }

    @Test
    void unknownPayloadTypeFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.encode(0, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> ValueEnvelope.encode(ValueEnvelope.MAX_REGISTERED_VALUE_TYPE + 1, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> ValueEnvelope.decode(new byte[0], ValueEnvelope.MAX_REGISTERED_VALUE_TYPE + 1));
    }

    @Test
    void crcAndLengthRemainMandatory() {
        final byte[] encoded = ValueEnvelope.encode(1, Bytes.utf8("value"));
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decode(encoded, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ValueEnvelope.decode(new byte[]{0x4e, 0x56, 1, 1, 0, 0, 0, 1}, 1));
    }
}
