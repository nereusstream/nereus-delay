package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointUploadIntentStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void createsAndRetriesExactPendingIntentIdempotently() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);

        assertEquals(pending, store.create(pending));
        assertEquals(pending, store.create(pending));
        assertEquals(pending, store.current().orElseThrow());
        assertThrows(
                IllegalStateException.class,
                () -> store.create(intent(CheckpointUploadState.PENDING_UPLOAD, 2, bytes(32, 40), null)));
    }

    @Test
    void publishesOnlyFromExactPendingValueAndIncrementsRevision() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        store.create(pending);

        final CheckpointUploadIntent published = store.publish(pending, resource());
        assertEquals(CheckpointUploadState.PUBLISHED, published.state());
        assertEquals(3, published.stateRevision());
        assertEquals(resource(), published.publishedManifest());
        assertEquals(published, store.current().orElseThrow());
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertThrows(IllegalArgumentException.class, () -> store.publish(published, resource()));
    }

    @Test
    void incrementsUnsignedStateRevisionAcrossSignedHighBitBoundary() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, Long.MIN_VALUE, null, null);
        store.create(pending);

        final CheckpointUploadIntent published = store.publish(pending, resource());
        assertEquals(Long.MIN_VALUE + 1, published.stateRevision());
    }

    @Test
    void reapingCompetesWithPublicationAndRetainsTrustedEvidence() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        store.create(pending);
        final TrustedUtcIntervalEvidence reapingEvidence = evidence(5_000);

        final CheckpointUploadIntent reaping = store.beginReaping(pending, reapingEvidence);
        assertEquals(CheckpointUploadState.REAPING, reaping.state());
        assertEquals(3, reaping.stateRevision());
        assertEquals(reapingEvidence, reaping.reapingStartedAt());
        assertFalse(store.current().orElseThrow().state() == CheckpointUploadState.PUBLISHED);
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertEquals(reaping, store.beginReaping(pending, reapingEvidence));
        assertThrows(IllegalStateException.class, () -> store.beginReaping(pending, evidence(5_001)));
    }

    @Test
    void reapingRejectsTrustedTimeBeforeUploadDeadline() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        store.create(pending);

        assertThrows(IllegalArgumentException.class, () -> store.beginReaping(pending, evidence(4_999)));
        assertEquals(
                CheckpointUploadState.PENDING_UPLOAD,
                store.current().orElseThrow().state());
    }

    @Test
    void requiresCreationBeforeCasAndRejectsNonPendingCreate() {
        final CheckpointUploadIntentStore store = new CheckpointUploadIntentStore();
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        assertTrue(store.current().isEmpty());
        assertThrows(IllegalStateException.class, () -> store.publish(pending, resource()));
        assertThrows(
                IllegalArgumentException.class,
                () -> store.create(intent(CheckpointUploadState.PUBLISHED, 3, bytes(32, 8), null)));
    }

    @Test
    void durableStateSurvivesReopenAndPublicationResponseLossRetry() {
        final Path stateFile = tempDir.resolve("checkpoint-upload.state");
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        final CheckpointUploadIntent published;
        final CheckpointUploadIntentStore first = new CheckpointUploadIntentStore(stateFile);
        first.create(pending);
        published = first.publish(pending, resource());

        final CheckpointUploadIntentStore reopened = new CheckpointUploadIntentStore(stateFile);
        assertEquals(published, reopened.current().orElseThrow());
        assertEquals(published, reopened.currentPublishedFor(pending).orElseThrow());
    }

    @Test
    void durableInstancesShareTheOnDiskCasBoundary() {
        final Path stateFile = tempDir.resolve("checkpoint-upload.state");
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        final CheckpointUploadIntentStore first = new CheckpointUploadIntentStore(stateFile);
        final CheckpointUploadIntentStore second = new CheckpointUploadIntentStore(stateFile);

        first.create(pending);
        final CheckpointUploadIntent published = second.publish(pending, resource());
        assertEquals(published, first.current().orElseThrow());
    }

    @Test
    void durableReapingStateSurvivesReopenAndExactEvidenceRetry() {
        final Path stateFile = tempDir.resolve("checkpoint-upload-reaping.state");
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        final TrustedUtcIntervalEvidence evidence = evidence(5_000);
        final CheckpointUploadIntentStore first = new CheckpointUploadIntentStore(stateFile);
        final CheckpointUploadIntent reaping = first.beginReaping(first.create(pending), evidence);

        final CheckpointUploadIntentStore reopened = new CheckpointUploadIntentStore(stateFile);
        assertEquals(reaping, reopened.current().orElseThrow());
        assertEquals(reaping, reopened.beginReaping(pending, evidence));
        assertThrows(IllegalStateException.class, () -> reopened.beginReaping(pending, evidence(5_001)));
    }

    @Test
    void durableStateChecksumCorruptionFailsClosedBeforeReturningCurrent() throws Exception {
        final Path stateFile = tempDir.resolve("checkpoint-upload.state");
        final CheckpointUploadIntent pending = intent(CheckpointUploadState.PENDING_UPLOAD, 2, null, null);
        new CheckpointUploadIntentStore(stateFile).create(pending);
        final byte[] encoded = Files.readAllBytes(stateFile);
        encoded[encoded.length - 1]++;
        Files.write(stateFile, encoded);

        assertThrows(IllegalStateException.class, () -> new CheckpointUploadIntentStore(stateFile).current());
    }

    @Test
    void rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary() throws Exception {
        final Path parentRoot = tempDir.resolve("upload-intent-parent");
        final Path outside = tempDir.resolve("upload-intent-outside");
        Files.createDirectories(parentRoot);
        Files.createDirectories(outside);
        Files.createSymbolicLink(parentRoot.resolve("nested"), outside);

        final Path stateFile = parentRoot.resolve("nested/state.bin");
        assertThrows(IllegalStateException.class, () -> new CheckpointUploadIntentStore(stateFile));
        assertFalse(Files.exists(outside.resolve("state.bin"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.exists(outside.resolve("state.bin.lock"), java.nio.file.LinkOption.NOFOLLOW_LINKS));
    }

    private static CheckpointUploadIntent intent(
            final CheckpointUploadState state,
            final long revision,
            final byte[] token,
            final TrustedUtcIntervalEvidence reaping) {
        final CheckpointResource resource = state == CheckpointUploadState.PUBLISHED ? resource() : null;
        return new CheckpointUploadIntent(
                new ShardSubject(new RouteIncarnation(bytes(16, 1)), 3),
                bytes(16, 2),
                bytes(16, 3),
                new OwnerIdentity(bytes(8, 4), bytes(8, 5), 9, bytes(32, 6)),
                bytes(16, 7),
                token == null ? bytes(32, 8) : token,
                11,
                bytes(16, 9),
                bytes(32, 10),
                objectStoreProfile(),
                evidence(1_000),
                5_000,
                state,
                revision,
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
