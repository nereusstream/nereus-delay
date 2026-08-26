package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class CheckpointUploadIntentTest {
    @Test
    void roundTripsPendingPublishedAndReapingBranches() {
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, null, null);
        assertEquals(pending, CheckpointUploadIntent.decode(pending.canonicalBytes()));

        final CheckpointResource resource = resource();
        final CheckpointUploadIntent published = intent(CheckpointUploadState.PUBLISHED, resource, null);
        assertEquals(published, CheckpointUploadIntent.decode(published.canonicalBytes()));

        final CheckpointUploadIntent reaping = intent(CheckpointUploadState.REAPING, null, evidence(2_000));
        assertEquals(reaping, CheckpointUploadIntent.decode(reaping.canonicalBytes()));
    }

    @Test
    void preservesCompleteUnsignedCatalogAndStateRevisionBits() {
        final CheckpointUploadIntent intent =
                intent(CheckpointUploadState.PENDING_UPLOAD, null, null, Long.MIN_VALUE, Long.MIN_VALUE);

        final CheckpointUploadIntent decoded = CheckpointUploadIntent.decode(intent.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.baseCatalogGeneration());
        assertEquals(Long.MIN_VALUE, decoded.stateRevision());
        assertEquals(intent, decoded);
    }

    @Test
    void enforcesStateBranchAndDigestRules() {
        final CheckpointResource resource = resource();
        assertThrows(
                IllegalArgumentException.class, () -> intent(CheckpointUploadState.PENDING_UPLOAD, resource, null));
        assertThrows(
                IllegalArgumentException.class, () -> intent(CheckpointUploadState.REAPING, resource, evidence(2_000)));

        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, null, null);
        final byte[] tampered = Bytes.concat(pending.canonicalBytes(), new byte[] {0});
        assertThrows(IllegalArgumentException.class, () -> CheckpointUploadIntent.decode(tampered));
    }

    private static CheckpointUploadIntent intent(
            final CheckpointUploadState state,
            final CheckpointResource resource,
            final TrustedUtcIntervalEvidence reaping) {
        return intent(state, resource, reaping, 11, 2);
    }

    private static CheckpointUploadIntent intent(
            final CheckpointUploadState state,
            final CheckpointResource resource,
            final TrustedUtcIntervalEvidence reaping,
            final long baseGeneration,
            final long stateRevision) {
        return new CheckpointUploadIntent(
                new ShardSubject(new RouteIncarnation(bytes(16, 1)), 3),
                bytes(16, 2),
                bytes(16, 3),
                new OwnerIdentity(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7),
                bytes(32, 8),
                baseGeneration,
                bytes(16, 9),
                bytes(32, 10),
                objectStoreProfile(),
                evidence(1_000),
                5_000,
                state,
                stateRevision,
                resource,
                reaping);
    }

    private static CheckpointResource resource() {
        return new CheckpointResource(
                bytes(16, 2),
                bytes(16, 3),
                objectStoreProfile(),
                bytes(4, 11),
                bytes(8, 12),
                bytes(8, 13),
                42,
                bytes(32, 14));
    }

    private static ProfileRef objectStoreProfile() {
        return new ProfileRef(Bytes.utf8("store"), 1, bytes(32, 15), ProfileKind.OBJECT_STORE);
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(
                time,
                time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, 16),
                1,
                2,
                3,
                bytes(32, 17),
                0,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
