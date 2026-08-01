package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckpointUploadIntentV1Test {
    @Test
    void roundTripsPendingPublishedAndReapingBranches() {
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, null, null);
        assertEquals(pending, CheckpointUploadIntentV1.decode(pending.canonicalBytes()));

        final CheckpointResourceV1 resource = resource();
        final CheckpointUploadIntentV1 published = intent(CheckpointUploadStateV1.PUBLISHED, resource, null);
        assertEquals(published, CheckpointUploadIntentV1.decode(published.canonicalBytes()));

        final CheckpointUploadIntentV1 reaping = intent(CheckpointUploadStateV1.REAPING, null, evidence(2_000));
        assertEquals(reaping, CheckpointUploadIntentV1.decode(reaping.canonicalBytes()));
    }

    @Test
    void enforcesStateBranchAndDigestRules() {
        final CheckpointResourceV1 resource = resource();
        assertThrows(IllegalArgumentException.class,
                () -> intent(CheckpointUploadStateV1.PENDING_UPLOAD, resource, null));
        assertThrows(IllegalArgumentException.class,
                () -> intent(CheckpointUploadStateV1.REAPING, resource, evidence(2_000)));

        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, null, null);
        final byte[] tampered = Bytes.concat(pending.canonicalBytes(), new byte[]{0});
        assertThrows(IllegalArgumentException.class, () -> CheckpointUploadIntentV1.decode(tampered));
    }

    private static CheckpointUploadIntentV1 intent(final CheckpointUploadStateV1 state,
                                                   final CheckpointResourceV1 resource,
                                                   final TrustedUtcIntervalEvidence reaping) {
        return new CheckpointUploadIntentV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 3),
                bytes(16, 2), bytes(16, 3),
                new OwnerIdentityV1(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7), bytes(32, 8), 11,
                bytes(16, 9), bytes(32, 10), objectStoreProfile(), evidence(1_000), 5_000,
                state, 2, resource, reaping);
    }

    private static CheckpointResourceV1 resource() {
        return new CheckpointResourceV1(bytes(16, 2), bytes(16, 3), objectStoreProfile(),
                bytes(4, 11), bytes(8, 12), bytes(8, 13), 42, bytes(32, 14));
    }

    private static ProfileRefV1 objectStoreProfile() {
        return new ProfileRefV1(Bytes.utf8("store"), 1, bytes(32, 15), ProfileKindV1.OBJECT_STORE);
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 16), 1, 2, 3,
                bytes(32, 17), 0, null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
