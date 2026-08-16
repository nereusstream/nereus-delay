package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Local orchestration for the ordered checkpoint upload/catalog publication
 * boundary.
 *
 * <p>The upload intent is advanced before the catalog publication is
 * attempted.  If the catalog call fails after the intent became
 * {@code PUBLISHED}, retrying with the same pending identity rereads that
 * exact successor and retries the idempotent catalog binding.  The Oxia
 * implementation still has to replace this pair with the single transaction
 * required by V1; this class does not claim cross-record atomicity.</p>
 */
public final class CheckpointPublicationCoordinator {
    private final CheckpointUploadCoordinator uploadCoordinator;
    private final RecoveryCatalogAuthority catalog;

    public CheckpointPublicationCoordinator(final SharedRocksDbResources resources,
                                            final CheckpointUploadIntentAuthority intentAuthority,
                                            final RecoveryCatalogAuthority catalog) {
        this(uploadCoordinator(resources, intentAuthority, catalog), catalog);
    }

    public CheckpointPublicationCoordinator(final SharedRocksDbResources resources,
                                            final CheckpointUploadIntentAuthority intentAuthority,
                                            final CheckpointManifestLimits limits,
                                            final RecoveryCatalogAuthority catalog) {
        this(uploadCoordinator(resources, intentAuthority, limits, catalog), catalog);
    }

    /** Allows callers to share a finite-limit upload coordinator. */
    public CheckpointPublicationCoordinator(final CheckpointUploadCoordinator uploadCoordinator,
                                            final RecoveryCatalogAuthority catalog) {
        this.uploadCoordinator = Objects.requireNonNull(uploadCoordinator, "uploadCoordinator");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    SharedRocksDbResources resources() {
        return uploadCoordinator.resources();
    }

    /**
     * Uploads one exact checkpoint and then binds its published object to the
     * expected catalog generation.  The catalog generation is checked before
     * provider I/O so an obviously stale intent cannot create an orphan.
     */
    CheckpointPublication publish(final Path checkpointDirectory,
                                  final CheckpointUploadIntentV1 pending,
                                  final CheckpointManifest manifest,
                                  final long expectedCatalogGeneration,
                                  final long nowEpochMs,
                                  final CheckpointUploadAdapter adapter) {
        Objects.requireNonNull(pending, "pending");
        if (pending.baseCatalogGeneration() != expectedCatalogGeneration) {
            throw new IllegalStateException("checkpoint intent catalog generation does not match request");
        }
        final CheckpointUploadIntentV1 published = uploadCoordinator.upload(checkpointDirectory, pending, manifest,
                nowEpochMs, adapter);
        final RecoveryCatalog.Publication publication = Objects.requireNonNull(
                catalog.publishUploadedCheckpoint(published, manifest, expectedCatalogGeneration),
                "checkpoint catalog publication");
        return new CheckpointPublication(published, publication);
    }

    public record CheckpointPublication(CheckpointUploadIntentV1 uploadIntent,
                                        RecoveryCatalog.Publication catalogPublication) {
        public CheckpointPublication {
            Objects.requireNonNull(uploadIntent, "uploadIntent");
            Objects.requireNonNull(catalogPublication, "catalogPublication");
        }
    }

    private static CheckpointUploadCoordinator uploadCoordinator(final SharedRocksDbResources resources,
                                                                  final CheckpointUploadIntentAuthority intentAuthority,
                                                                  final RecoveryCatalogAuthority catalog) {
        requireAtomicPublicationPair(intentAuthority, catalog);
        return new CheckpointUploadCoordinator(resources, intentAuthority);
    }

    private static CheckpointUploadCoordinator uploadCoordinator(final SharedRocksDbResources resources,
                                                                  final CheckpointUploadIntentAuthority intentAuthority,
                                                                  final CheckpointManifestLimits limits,
                                                                  final RecoveryCatalogAuthority catalog) {
        requireAtomicPublicationPair(intentAuthority, catalog);
        return new CheckpointUploadCoordinator(resources, intentAuthority, limits);
    }

    private static void requireAtomicPublicationPair(final CheckpointUploadIntentAuthority intentAuthority,
                                                     final RecoveryCatalogAuthority catalog) {
        final boolean intentIsAtomic = intentAuthority instanceof CheckpointAtomicPublicationAuthority;
        final boolean catalogIsAtomic = catalog instanceof CheckpointAtomicPublicationAuthority;
        if ((intentIsAtomic || catalogIsAtomic) && intentAuthority != catalog) {
            throw new IllegalArgumentException(
                    "atomic checkpoint publication intent and catalog authorities must be the same record backend");
        }
    }
}
