package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ShardId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * Local bounded orchestration from a catalog-bound checkpoint download to a
 * new Store Incarnation.
 *
 * <p>The provider is asked to materialize only into a private, per-attempt
 * directory. The coordinator then invokes the existing manifest/pin-aware
 * {@link ShardStore} restore protocol and removes the downloaded tree after
 * the Store has installed its own private incarnation. It does not create an
 * Owner Lease, Source Assignment, RecoveryPin, or source-replay authority.</p>
 */
public final class CheckpointRestoreCoordinator {
    private final ShardStoreConfig config;
    private final ShardId shardId;
    private final SharedRocksDbResources resources;
    private final CheckpointDownloadAdapter downloader;
    private final RecoveryCatalogAuthority catalog;
    private final CheckpointManifestLimits limits;

    public CheckpointRestoreCoordinator(final ShardStoreConfig config, final ShardId shardId,
                                        final SharedRocksDbResources resources,
                                        final CheckpointDownloadAdapter downloader,
                                        final RecoveryCatalogAuthority catalog,
                                        final CheckpointManifestLimits limits) {
        this.config = Objects.requireNonNull(config, "config");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.downloader = Objects.requireNonNull(downloader, "downloader");
        this.catalog = catalog;
        this.limits = Objects.requireNonNull(limits, "limits");
    }

    /**
     * Downloads and installs one exact checkpoint. The returned Store remains
     * open; the provider download tree has already been removed.
     */
    public ShardStore restore(final CheckpointDownloadRequest request, final RecoveryPinV1 pin) {
        Objects.requireNonNull(request, "request");
        if (!shardId.equals(request.manifest().shardId())) {
            throw new IllegalArgumentException("checkpoint restore request belongs to another shard");
        }
        if (pin != null && catalog == null) {
            throw new IllegalArgumentException("RecoveryPin requires a catalog authority");
        }
        final Path downloadRoot = config.rootPath().toAbsolutePath().normalize()
                .resolve("checkpoint-download-tmp").normalize();
        final Path attemptRoot = downloadRoot.resolve(UUID.randomUUID().toString()).normalize();
        final Path target = attemptRoot.resolve("db").normalize();
        ensureWithin(downloadRoot, attemptRoot);
        ensureWithin(attemptRoot, target);
        ensureDirectory(downloadRoot);
        if (Files.exists(attemptRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("checkpoint restore attempt directory already exists: " + attemptRoot);
        }

        ShardStore restored = null;
        SharedRocksDbResources.CheckpointDownloadPermit downloadPermit = null;
        try {
            downloadPermit = resources.acquireCheckpointDownloadPermit();
            final Path materialized = downloader.download(request, target);
            if (materialized == null || !materialized.toAbsolutePath().normalize().equals(target)
                    || Files.isSymbolicLink(materialized)
                    || !Files.isDirectory(materialized, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("checkpoint downloader returned an invalid target directory");
            }
            validateDownloadedInventory(materialized, request.manifest());
            restored = ShardStore.restoreFromCheckpointWithDownloadPermit(config, shardId, resources, materialized,
                    request.manifest(), catalog, pin, limits, downloadPermit);
            deleteTree(attemptRoot);
            return restored;
        } catch (RuntimeException | Error failure) {
            if (restored != null) {
                try {
                    restored.close();
                } catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            try {
                deleteTree(attemptRoot);
            } catch (RuntimeException | Error cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        } finally {
            if (downloadPermit != null) {
                downloadPermit.close();
            }
        }
    }

    private void validateDownloadedInventory(final Path directory, final CheckpointManifest manifest) {
        final var actual = CheckpointFileInventory.collect(directory, limits);
        if (actual.size() != manifest.files().size()) {
            throw new IllegalStateException("downloaded checkpoint file count differs from manifest");
        }
        for (int index = 0; index < actual.size(); index++) {
            final var left = actual.get(index);
            final var right = manifest.files().get(index);
            if (!left.name().equals(right.name()) || left.length() != right.length()
                    || !io.nereusstream.delay.protocol.Bytes.constantTimeEquals(left.checksum(), right.checksum())) {
                throw new IllegalStateException("downloaded checkpoint file differs from manifest: " + left.name());
            }
        }
    }

    private static void ensureWithin(final Path parent, final Path child) {
        final Path normalizedParent = parent.toAbsolutePath().normalize();
        final Path normalizedChild = child.toAbsolutePath().normalize();
        if (!normalizedChild.startsWith(normalizedParent)) {
            throw new IllegalArgumentException("checkpoint restore path escapes its boundary: " + child);
        }
    }

    private static void ensureDirectory(final Path directory) {
        try {
            LocalStatePathGuard.ensureRealDirectoryPath(directory, "checkpoint restore staging directory");
        } catch (IOException failure) {
            throw new IllegalStateException("cannot create checkpoint restore staging directory", failure);
        }
    }

    private static void deleteTree(final Path root) {
        if (root == null || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException failure) {
                    throw new IllegalStateException("cannot clean checkpoint restore staging: " + root, failure);
                }
            });
        } catch (IOException failure) {
            throw new IllegalStateException("cannot enumerate checkpoint restore staging: " + root, failure);
        }
    }
}
