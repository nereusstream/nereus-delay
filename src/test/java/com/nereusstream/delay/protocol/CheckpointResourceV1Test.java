package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class CheckpointResourceV1Test {
    @Test
    void roundTripsImmutableManifestIdentity() {
        final CheckpointResourceV1 resource = new CheckpointResourceV1(
                bytes(16, 1),
                bytes(16, 2),
                objectStoreProfile(),
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoints/cp-1/manifest.json"),
                Bytes.utf8("version-7"),
                1234,
                bytes(32, 3));

        assertEquals(resource, CheckpointResourceV1.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.CHECKPOINT, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void rejectsWrongProfileAndTamperedIdentity() {
        final ProfileRefV1 destination =
                new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 4), ProfileKindV1.DESTINATION);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointResourceV1(
                        bytes(16, 1),
                        bytes(16, 2),
                        destination,
                        bytes(1, 1),
                        bytes(1, 2),
                        bytes(1, 3),
                        0,
                        bytes(32, 3)));

        final CheckpointResourceV1 resource = new CheckpointResourceV1(
                bytes(16, 1),
                bytes(16, 2),
                objectStoreProfile(),
                bytes(1, 1),
                bytes(1, 2),
                bytes(1, 3),
                0,
                bytes(32, 3));
        final byte[] tampered = Bytes.concat(resource.canonicalBytes(), new byte[] {0});
        assertThrows(IllegalArgumentException.class, () -> CheckpointResourceV1.decode(tampered));
    }

    private static ProfileRefV1 objectStoreProfile() {
        return new ProfileRefV1(Bytes.utf8("object-store"), 2, bytes(32, 9), ProfileKindV1.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
