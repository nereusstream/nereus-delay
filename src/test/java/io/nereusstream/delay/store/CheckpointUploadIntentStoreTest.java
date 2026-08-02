package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointUploadIntentStoreTest {
    @Test
    void createsAndRetriesExactPendingIntentIdempotently() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);

        assertEquals(pending, store.create(pending));
        assertEquals(pending, store.create(pending));
        assertEquals(pending, store.current().orElseThrow());
        assertThrows(IllegalStateException.class,
                () -> store.create(intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, bytes(32, 40), null)));
    }

    @Test
    void publishesOnlyFromExactPendingValueAndIncrementsRevision() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);
        store.create(pending);

        final CheckpointUploadIntentV1 published = store.publish(pending, resource());
        assertEquals(CheckpointUploadStateV1.PUBLISHED, published.state());
        assertEquals(3, published.stateRevision());
        assertEquals(resource(), published.publishedManifest());
        assertEquals(published, store.current().orElseThrow());
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertThrows(IllegalArgumentException.class,
                () -> store.publish(published, resource()));
    }

    @Test
    void reapingCompetesWithPublicationAndRetainsTrustedEvidence() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);
        store.create(pending);
        final TrustedUtcIntervalEvidence reapingEvidence = evidence(5_000);

        final CheckpointUploadIntentV1 reaping = store.beginReaping(pending, reapingEvidence);
        assertEquals(CheckpointUploadStateV1.REAPING, reaping.state());
        assertEquals(3, reaping.stateRevision());
        assertEquals(reapingEvidence, reaping.reapingStartedAt());
        assertFalse(store.current().orElseThrow().state() == CheckpointUploadStateV1.PUBLISHED);
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertEquals(reaping, store.beginReaping(pending, reapingEvidence));
        assertThrows(IllegalStateException.class,
                () -> store.beginReaping(pending, evidence(5_001)));
    }

    @Test
    void reapingRejectsTrustedTimeBeforeUploadDeadline() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);
        store.create(pending);

        assertThrows(IllegalArgumentException.class,
                () -> store.beginReaping(pending, evidence(4_999)));
        assertEquals(CheckpointUploadStateV1.PENDING_UPLOAD, store.current().orElseThrow().state());
    }

    @Test
    void requiresCreationBeforeCasAndRejectsNonPendingCreate() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntentV1 pending = intent(CheckpointUploadStateV1.PENDING_UPLOAD, 2, null, null);
        assertTrue(store.current().isEmpty());
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertThrows(IllegalArgumentException.class,
                () -> store.create(intent(CheckpointUploadStateV1.PUBLISHED, 3, bytes(32, 8), null)));
    }

    private static CheckpointUploadIntentV1 intent(final CheckpointUploadStateV1 state, final long revision,
                                                   final byte[] token, final TrustedUtcIntervalEvidence reaping) {
        final CheckpointResourceV1 resource = state == CheckpointUploadStateV1.PUBLISHED
                ? resource() : null;
        return new CheckpointUploadIntentV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 3),
                bytes(16, 2), bytes(16, 3),
                new OwnerIdentityV1(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7), token == null ? bytes(32, 8) : token, 11,
                bytes(16, 9), bytes(32, 10), objectStoreProfile(), evidence(1_000), 5_000,
                state, revision, resource, reaping);
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
