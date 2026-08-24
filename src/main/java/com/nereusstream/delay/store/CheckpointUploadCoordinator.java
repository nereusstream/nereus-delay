package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResourceV1;
import com.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import com.nereusstream.delay.protocol.CheckpointUploadStateV1;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Local, bounded orchestration for publishing one checkpoint upload intent.
 *
 * <p>The caller must have created the exact PENDING_UPLOAD intent in the
 * authoritative intent store. This class inventories the complete local
 * checkpoint before provider I/O, reserves the Worker upload slot, validates
 * the provider's immutable manifest object identity, and only then advances
 * the local intent to PUBLISHED. It does not implement Object Store or Oxia
 * authority.</p>
 */
public final class CheckpointUploadCoordinator {
    private final SharedRocksDbResources resources;
    private final CheckpointUploadIntentAuthority intentStore;
    private final CheckpointManifestLimits limits;

    /** Creates a coordinator over any exact upload-intent CAS authority. */
    public CheckpointUploadCoordinator(
            final SharedRocksDbResources resources, final CheckpointUploadIntentAuthority intentStore) {
        this(resources, intentStore, CheckpointManifestLimits.unbounded());
    }

    /** Creates an upload coordinator with explicit finite inventory limits. */
    public CheckpointUploadCoordinator(
            final SharedRocksDbResources resources,
            final CheckpointUploadIntentAuthority intentStore,
            final CheckpointManifestLimits limits) {
        this.resources = Objects.requireNonNull(resources, "resources");
        this.intentStore = Objects.requireNonNull(intentStore, "intentStore");
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    SharedRocksDbResources resources() {
        return resources;
    }

    CheckpointUploadIntentV1 upload(
            final Path checkpointDirectory,
            final CheckpointUploadIntentV1 pending,
            final CheckpointManifest manifest,
            final long nowEpochMs,
            final CheckpointUploadAdapter adapter) {
        Objects.requireNonNull(checkpointDirectory, "checkpointDirectory");
        Objects.requireNonNull(pending, "pending");
        Objects.requireNonNull(manifest, "manifest");
        Objects.requireNonNull(adapter, "adapter");
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("nowEpochMs must be non-negative");
        }
        if (pending.state() != CheckpointUploadStateV1.PENDING_UPLOAD) {
            throw new IllegalArgumentException("checkpoint upload requires PENDING_UPLOAD intent");
        }
        validateManifestIdentity(pending, manifest);
        manifest.validateLimits(limits);
        final byte[] manifestBytes = manifest.canonicalJsonBytes();
        final var published = intentStore.currentPublishedFor(pending);
        if (published.isPresent()) {
            final CheckpointUploadIntentV1 publishedIntent = published.orElseThrow();
            limits.validateResource(publishedIntent.publishedManifest());
            validatePublishedResource(pending, manifest, publishedIntent.publishedManifest(), manifestBytes);
            return publishedIntent;
        }
        if (nowEpochMs >= pending.uploadDeadlineEpochMs()) {
            throw new IllegalStateException("checkpoint upload intent deadline has expired");
        }
        final var current = intentStore.current(pending);
        if (current.isEmpty() || !current.orElseThrow().equals(pending)) {
            throw new IllegalStateException("checkpoint upload intent is not the current exact pending value");
        }
        validateLocalCheckpoint(checkpointDirectory, manifest, limits);
        final CheckpointUploadRequest request =
                new CheckpointUploadRequest(pending, manifest, checkpointDirectory, manifestBytes);

        boolean slotAcquired = false;
        Throwable primaryFailure = null;
        try {
            resources.acquireCheckpointUploadSlot();
            slotAcquired = true;
            // The upload slot is a process-wide resource, not an intent lock.
            // Another owner/reaper may have advanced the exact intent while
            // this call was waiting for the slot.  Re-read immediately before
            // provider I/O so a stale PENDING_UPLOAD cannot create an orphan
            // object whose later CAS is guaranteed to fail.  A concurrently
            // completed publication is returned idempotently and does not
            // invoke the provider a second time.
            final var publishedAfterSlot = intentStore.currentPublishedFor(pending);
            if (publishedAfterSlot.isPresent()) {
                final CheckpointUploadIntentV1 publishedIntent = publishedAfterSlot.orElseThrow();
                limits.validateResource(publishedIntent.publishedManifest());
                validatePublishedResource(pending, manifest, publishedIntent.publishedManifest(), manifestBytes);
                return publishedIntent;
            }
            final var pendingAfterSlot = intentStore.current(pending);
            if (pendingAfterSlot.isEmpty() || !pendingAfterSlot.orElseThrow().equals(pending)) {
                throw new IllegalStateException("checkpoint upload intent changed before provider I/O");
            }
            final CheckpointResourceV1 resource =
                    Objects.requireNonNull(adapter.upload(request), "checkpoint upload adapter returned null resource");
            limits.validateResource(resource);
            validatePublishedResource(pending, manifest, resource, manifestBytes);
            if (intentStore instanceof CheckpointAtomicPublicationAuthority atomicPublication) {
                return atomicPublication.publishUploadedCheckpointAtomically(
                        pending, resource, manifest, pending.baseCatalogGeneration());
            }
            return intentStore.publish(pending, resource);
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            if (slotAcquired) {
                try {
                    resources.releaseCheckpointUploadSlot();
                } catch (RuntimeException | Error cleanupFailure) {
                    if (primaryFailure != null && cleanupFailure != primaryFailure) {
                        primaryFailure.addSuppressed(cleanupFailure);
                    } else if (primaryFailure == null) {
                        throwUnchecked(cleanupFailure);
                    }
                }
            }
        }
    }

    private static <T> T throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    private static void validateManifestIdentity(
            final CheckpointUploadIntentV1 pending, final CheckpointManifest manifest) {
        if (!pending.shard().shardId().equals(manifest.shardId())
                || !Bytes.constantTimeEquals(pending.recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(pending.checkpointId(), manifest.checkpointId())) {
            throw new IllegalArgumentException("upload intent and checkpoint manifest identity differ");
        }
        final CheckpointManifest.CreatedBy creator = manifest.createdBy();
        if (!Bytes.constantTimeEquals(pending.owner().deploymentId(), creator.deploymentId())
                || !Bytes.constantTimeEquals(pending.owner().workerRunId(), creator.workerRunId())
                || pending.owner().ownerEpoch() != creator.ownerEpoch()) {
            throw new IllegalArgumentException("upload intent owner does not match checkpoint creator");
        }
        if (!Bytes.constantTimeEquals(pending.sourceStoreIncarnation(), uuidBytes(manifest.sourceStoreIncarnation()))) {
            throw new IllegalArgumentException("upload intent store incarnation does not match checkpoint");
        }
        final CheckpointManifest.ParentCheckpoint parent = manifest.parentCheckpoint();
        if (!sameBytes(pending.parentCheckpointId(), parent == null ? null : parent.checkpointId())
                || !sameHashHex(pending.parentManifestSha256(), parent == null ? null : parent.manifestSha256())) {
            throw new IllegalArgumentException("upload intent parent does not match checkpoint manifest");
        }
    }

    private static void validateLocalCheckpoint(
            final Path checkpointDirectory, final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        if (Files.isSymbolicLink(checkpointDirectory)
                || !Files.isDirectory(checkpointDirectory, LinkOption.NOFOLLOW_LINKS)
                || !Files.isRegularFile(checkpointDirectory.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("checkpoint upload source is not a complete RocksDB directory");
        }
        final List<CheckpointFileInventory> actual = CheckpointFileInventory.collect(checkpointDirectory, limits);
        final List<CheckpointManifest.FileEntry> expected = manifest.files();
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("checkpoint upload file count differs from manifest");
        }
        for (int index = 0; index < actual.size(); index++) {
            final CheckpointFileInventory actualFile = actual.get(index);
            final CheckpointManifest.FileEntry expectedFile = expected.get(index);
            if (!actualFile.name().equals(expectedFile.name())
                    || actualFile.length() != expectedFile.length()
                    || !Bytes.constantTimeEquals(actualFile.checksum(), expectedFile.checksum())) {
                throw new IllegalArgumentException(
                        "checkpoint upload file differs from manifest: " + actualFile.name());
            }
        }
        CheckpointControlSnapshotVerifier.validate(checkpointDirectory, manifest);
    }

    private static void validatePublishedResource(
            final CheckpointUploadIntentV1 pending,
            final CheckpointManifest manifest,
            final CheckpointResourceV1 resource,
            final byte[] manifestBytes) {
        if (!Bytes.constantTimeEquals(resource.recoveryLineageId(), pending.recoveryLineageId())
                || !Bytes.constantTimeEquals(resource.checkpointId(), pending.checkpointId())
                || !resource.objectStoreProfile().equals(pending.objectStoreProfile())
                || resource.manifestLength() != manifestBytes.length
                || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("uploaded checkpoint manifest object identity mismatch");
        }
    }

    private static boolean sameBytes(final byte[] left, final byte[] right) {
        return left == null ? right == null : right != null && Bytes.constantTimeEquals(left, right);
    }

    private static boolean sameHashHex(final byte[] left, final String right) {
        return left == null
                ? right == null
                : right != null && java.util.HexFormat.of().formatHex(left).equals(right);
    }

    private static byte[] uuidBytes(final java.util.UUID value) {
        return java.nio.ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }
}
