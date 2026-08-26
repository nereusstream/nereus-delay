package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class CheckpointResourceTest {
    @Test
    void roundTripsImmutableManifestIdentity() {
        final CheckpointResource resource = new CheckpointResource(
                bytes(16, 1),
                bytes(16, 2),
                objectStoreProfile(),
                Bytes.utf8("bucket"),
                Bytes.utf8("checkpoints/cp-1/manifest.json"),
                Bytes.utf8("version-7"),
                1234,
                bytes(32, 3));

        assertEquals(resource, CheckpointResource.decode(resource.canonicalBytes()));
        final ResourceRetireIntentBody.ExactResourceIdentity identity = ResourceRetireIntentBody.decodeResourceIdentity(
                ResourceKind.CHECKPOINT, resource.exactResourceCanonicalBytes());
        assertArrayEquals(resource.exactResourceCanonicalBytes(), identity.canonicalBytes());
    }

    @Test
    void rejectsWrongProfileAndTamperedIdentity() {
        final ProfileRef destination =
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 4), ProfileKind.DESTINATION);
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointResource(
                        bytes(16, 1),
                        bytes(16, 2),
                        destination,
                        bytes(1, 1),
                        bytes(1, 2),
                        bytes(1, 3),
                        0,
                        bytes(32, 3)));

        final CheckpointResource resource = new CheckpointResource(
                bytes(16, 1),
                bytes(16, 2),
                objectStoreProfile(),
                bytes(1, 1),
                bytes(1, 2),
                bytes(1, 3),
                0,
                bytes(32, 3));
        final byte[] tampered = Bytes.concat(resource.canonicalBytes(), new byte[] {0});
        assertThrows(IllegalArgumentException.class, () -> CheckpointResource.decode(tampered));
    }

    private static ProfileRef objectStoreProfile() {
        return new ProfileRef(Bytes.utf8("object-store"), 2, bytes(32, 9), ProfileKind.OBJECT_STORE);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
