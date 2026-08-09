package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryCandidateRefV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryInstallPhaseV1;
import io.nereusstream.delay.protocol.RecoveryInstallStateV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.Checkpoint;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** One independent RocksDB instance for exactly one Delay Shard. */
public final class ShardStore implements AutoCloseable {
    private static final int ACTIVE_MAGIC = 0x41435431;
    private static final int META_STORE_FORMAT = 1;
    private static final int META_SHARD_IDENTITY = 2;
    private static final int META_APPLIED_SOURCE_POSITION = 3;
    private static final int META_INGRESS_FENCE_STATE = 4;
    private static final int META_MUTATION_SEQUENCE = 5;
    private static final int META_EVIDENCE_CURSORS = 6;
    private static final int META_CHECKPOINT_ID = 7;
    private static final int META_OWNER_EPOCH = 8;
    private static final int META_CLEAN_CLOSE_MARKER = 9;
    private static final int META_CONTROL_SNAPSHOT = 10;
    private static final int META_CLAIM_SEQUENCE = 11;
    private static final int META_PAYLOAD_PROOF_CONTROL_STATE = 12;
    private static final int META_PROFILE_CONTROL_STATE = 13;
    private static final int META_RECOVERY_LINEAGE_BASE = 1;
    private static final int META_RECOVERY_LAST_OBSERVED_FLOOR = 2;
    private static final int META_RECOVERY_CATALOG_GENERATION = 3;
    private static final int META_RECOVERY_INSTALL_STATE = 4;
    private static final int META_RECOVERY_VALUE_TYPE = 1;
    private static final int META_FIXED_VALUE_TYPE = 1;
    private static final int META_PAYLOAD_PROOF_VALUE_TYPE = 9;
    private static final int META_PROFILE_VALUE_TYPE = 10;

    static {
        RocksDbNativeLoader.load();
    }

    private final ShardStoreConfig config;
    private final ShardId shardId;
    private final Path dbPath;
    private final SharedRocksDbResources resources;
    private final RocksDB db;
    private final ColumnFamilyHandle defaultColumnFamilyHandle;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyOptions> columnFamilyOptions;
    private final Map<ColumnFamily, ColumnFamilyHandle> handles;
    private final StoreMetadata metadata;
    private final boolean ownsShardSlot;
    private final Supplier<RocksDbUsageSnapshot> physicalUsageSource;
    private final AtomicBoolean closed = new AtomicBoolean();
    /** First close fences all Store operations; native teardown may be retried. */
    private boolean closeStarted;
    private boolean defaultColumnFamilyClosed;
    private boolean dbClosed;
    private boolean dbOptionsClosed;
    private boolean dbSlotReleased;
    private boolean ownedShardSlotReleased;
    private final EnumSet<ColumnFamily> closedColumnFamilyHandles = EnumSet.noneOf(ColumnFamily.class);
    private final boolean[] closedColumnFamilyOptions;
    private boolean cleanCloseAttempted;
    private StoreRuntimeMetadata runtimeMetadata;
    private StoreRecoveryMetadata recoveryMetadata;
    private long closedIngressDeadlineThrough;

    /**
     * A synchronous RocksDB write failed after the batch operation reached the
     * store boundary.  This remains an {@link IllegalStateException} for
     * callers that already treat native-store failures as fatal, but its
     * concrete type lets replay handlers distinguish storage failure from a
     * semantic stale-state rejection.  In particular, a failed WriteBatch must
     * never be converted into a persisted logical result and source advance.
     */
    public static final class RocksDbWriteFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private RocksDbWriteFailure(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    private ShardStore(final ShardStoreConfig config, final ShardId shardId, final Path dbPath,
                        final SharedRocksDbResources resources, final RocksDB db, final DBOptions dbOptions,
                        final ColumnFamilyHandle defaultColumnFamilyHandle,
                        final List<ColumnFamilyOptions> columnFamilyOptions,
                        final Map<ColumnFamily, ColumnFamilyHandle> handles, final StoreMetadata metadata,
                        final boolean ownsShardSlot, final StoreRuntimeMetadata runtimeMetadata,
                        final StoreRecoveryMetadata recoveryMetadata,
                        final long closedIngressDeadlineThrough) {
        this.config = config;
        this.shardId = shardId;
        this.dbPath = dbPath;
        this.resources = resources;
        this.db = db;
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.dbOptions = dbOptions;
        this.columnFamilyOptions = columnFamilyOptions;
        this.handles = handles;
        this.metadata = metadata;
        this.ownsShardSlot = ownsShardSlot;
        this.physicalUsageSource = this::physicalUsage;
        this.closedColumnFamilyOptions = new boolean[columnFamilyOptions.size()];
        this.runtimeMetadata = runtimeMetadata;
        this.recoveryMetadata = recoveryMetadata;
        this.closedIngressDeadlineThrough = closedIngressDeadlineThrough;
    }

    public static ShardStore open(final ShardStoreConfig config, final ShardId shardId,
                                  final SharedRocksDbResources resources) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(resources, "resources");
        try {
            final Path shardRoot = prepareShardRoot(config, shardId);
            final Path dbPath = locateOrCreateDbPath(shardRoot);
            final ShardStore opened = openAtPath(config, shardId, dbPath, resources, null, true);
            try {
                // A fresh/opened Store Incarnation must be durable before the
                // checksummed ACTIVE pointer publishes it.  This also covers
                // recovery of an orphan incarnation when ACTIVE was missing.
                forceIncarnationBeforeActivePointer(dbPath);
                writeActivePointer(shardRoot, storeUuidFromPath(dbPath));
                return opened;
            } catch (IOException exception) {
                closeAfterActivePointerFailure(opened, exception);
                throw exception;
            }
        } catch (IOException | RocksDBException exception) {
            throw new IllegalStateException("cannot open shard DB for " + shardId, exception);
        }
    }

    /**
     * Installs a complete physical checkpoint as a new local Store Incarnation.
     * The source directory is never opened in place and is never merged into a
     * running DB. A failed install leaves only a private restore-tmp orphan.
     */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath) {
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, null, null, null);
    }

    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest) {
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, null, null);
    }

    /**
     * Restores an exact published checkpoint only while the caller still owns
     * the exact session-bound RecoveryPin.  The pin is reread before staging
     * and immediately before installing the new Store Incarnation; a missing
     * or changed pin fails closed and leaves only private restore-tmp state.
     */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest,
                                                   final RecoveryCatalogAuthority catalog,
                                                   final RecoveryPinV1 pin) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(manifest, "manifest");
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, catalog, pin);
    }

    /** Restores a pinned candidate with an explicit finite manifest limit set. */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest,
                                                   final RecoveryCatalogAuthority catalog,
                                                   final RecoveryPinV1 pin,
                                                   final CheckpointManifestLimits limits) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(pin, "pin");
        Objects.requireNonNull(manifest, "manifest");
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, catalog, pin,
                Objects.requireNonNull(limits, "limits"));
    }

    private static ShardStore restoreWithRecoveryGuard(final ShardStoreConfig config, final ShardId shardId,
                                                       final SharedRocksDbResources resources,
                                                       final Path checkpointPath, final CheckpointManifest manifest,
                                                       final RecoveryCatalogAuthority catalog,
                                                       final RecoveryPinV1 pin) {
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, catalog, pin,
                CheckpointManifestLimits.unbounded());
    }

    private static ShardStore restoreWithRecoveryGuard(final ShardStoreConfig config, final ShardId shardId,
                                                       final SharedRocksDbResources resources,
                                                       final Path checkpointPath, final CheckpointManifest manifest,
                                                       final RecoveryCatalogAuthority catalog,
                                                       final RecoveryPinV1 pin,
                                                       final CheckpointManifestLimits limits) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(checkpointPath, "checkpointPath");
        Objects.requireNonNull(limits, "limits");
        if (manifest != null) {
            manifest.validateLimits(limits);
        }
        if (catalog != null) {
            catalog.validatePublishedRestoreCandidate(Objects.requireNonNull(manifest, "manifest"));
            if (pin != null) {
                validateRecoveryPin(shardId, manifest, catalog, pin);
            }
        } else if (pin != null) {
            throw new IllegalArgumentException("RecoveryPin requires a catalog authority");
        }
        final Path shardRoot;
        try {
            shardRoot = prepareShardRoot(config, shardId);
        } catch (IOException exception) {
            throw new IllegalStateException("cannot prepare shard directory", exception);
        }
        final UUID storeUuid = UUID.randomUUID();
        final Path restoreRoot = shardRoot.resolve("restore-tmp").resolve(storeUuid.toString());
        final Path stagedDb = restoreRoot.resolve("db");
        final Path activeDb = shardRoot.resolve("incarnations").resolve(storeUuid.toString()).resolve("db");
        boolean downloadSlotAcquired = false;
        boolean activeDbMoved = false;
        ShardStore staged = null;
        ShardStore prepared = null;
        ShardStore installed = null;
        try {
            resources.acquireCheckpointDownloadSlot();
            downloadSlotAcquired = true;
            if (Files.isSymbolicLink(checkpointPath)
                    || !Files.isDirectory(checkpointPath)
                    || !Files.isRegularFile(checkpointPath.resolve("CURRENT"),
                    java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("checkpoint is not a complete RocksDB directory: " + checkpointPath);
            }
            if (manifest != null) {
                validateCheckpointManifest(shardId, checkpointPath, manifest, limits);
            }
            ensureRealDirectory(shardRoot.resolve("incarnations"));
            ensureRealDirectory(shardRoot.resolve("restore-tmp"));
            ensureRealDirectory(restoreRoot);
            if (hasActiveDb(shardRoot)) {
                throw new IOException("cannot restore while an active shard DB exists: " + shardRoot);
            }
            copyTree(checkpointPath, stagedDb);
            // The source inventory is a pre-copy admission check. Re-inventory
            // the private copy as well so a truncated file, copy failure, or
            // source mutation racing the copy cannot reach RocksDB open/install
            // merely because the copied directory happens to be readable.
            if (manifest != null) {
                validateCheckpointManifest(shardId, stagedDb, manifest, limits);
            }
            staged = openAtPath(config, shardId, stagedDb, resources, null, false);
            if (!staged.shardId().equals(shardId)) {
                throw new IOException("restored DB shard identity mismatch");
            }
            if (manifest != null && (!java.util.Arrays.equals(manifest.dbIdentity(), staged.metadata().dbIdentity())
                    || !manifest.sourceStoreIncarnation().equals(staged.metadata().storeIncarnationUuid()))) {
                throw new IOException("restored DB metadata does not match checkpoint manifest");
            }
            if (manifest != null) {
                validateRestoredRuntimeState(staged, manifest);
                validateRestoredRecoveryState(staged, manifest);
            }
            staged.close();
            staged = null;
            if (pin != null) {
                validateRecoveryPin(shardId, manifest, catalog, pin);
            }
            final RecoveryCandidateRefV1 installedCandidate = manifest == null ? null
                    : new RecoveryCandidateRefV1(
                    io.nereusstream.delay.protocol.RecoveryCandidateKindV1.LOCAL_STORE,
                    manifest.recoveryLineageId(), manifest.checkpointId(), manifest.manifestSha256(),
                    uuidBytes(storeUuid));
            final RecoveryFloorRefV1 observedFloor = pin == null ? null : pin.observedFloor();
            prepared = openAtPath(config, shardId, stagedDb, resources, storeUuid, false);
            if (!prepared.shardId().equals(shardId)) {
                throw new IOException("install-mode DB shard identity mismatch");
            }
            prepared.recordRecoveryMetadata(installedCandidate, observedFloor);
            prepared.close();
            prepared = null;
            ensureRealDirectory(activeDb.getParent());
            Files.move(stagedDb, activeDb, StandardCopyOption.ATOMIC_MOVE);
            activeDbMoved = true;
            // Persist the new Store Incarnation directory entry before the
            // checksummed ACTIVE pointer can publish it.  Without this
            // directory fsync, a crash after the rename could leave ACTIVE
            // pointing at an incarnation whose directory entry was not yet
            // durable, violating the restore install protocol.
            forceDirectory(activeDb.getParent());
            installed = openAtPath(config, shardId, activeDb, resources, null, true);
            if (!installed.shardId().equals(shardId)) {
                throw new IOException("install-mode DB shard identity mismatch");
            }
            forceIncarnationBeforeActivePointer(activeDb);
            deleteTree(restoreRoot);
            writeActivePointer(shardRoot, storeUuid);
            return installed;
        } catch (IOException | RocksDBException exception) {
            cleanupFailedRestore(restoreRoot, activeDb, shardRoot, storeUuid, activeDbMoved, staged, prepared,
                    installed, exception);
            throw new IllegalStateException("cannot restore shard checkpoint", exception);
        } catch (RuntimeException exception) {
            // A failed staged open/metadata validation can surface as a
            // runtime exception after restore-tmp has already been created.
            // Preserve a pre-acquisition concurrency error verbatim, but once
            // the slot is held, clean the private staging tree just like the
            // checked I/O failure path.
            if (!downloadSlotAcquired) {
                throw exception;
            }
            cleanupFailedRestore(restoreRoot, activeDb, shardRoot, storeUuid, activeDbMoved, staged, prepared,
                    installed, exception);
            throw new IllegalStateException("cannot restore shard checkpoint", exception);
        } finally {
            if (downloadSlotAcquired) {
                resources.releaseCheckpointDownloadSlot();
            }
        }
    }

    private static void cleanupFailedRestore(final Path restoreRoot, final Path activeDb, final Path shardRoot,
                                             final UUID storeUuid, final boolean activeDbMoved,
                                             final ShardStore staged, final ShardStore prepared,
                                             final ShardStore installed, final Throwable failure) {
        // A failed close fences the Store but may leave a native handle open
        // for a later retry.  Never delete a directory while one of these
        // restore probes still owns that directory; doing so would turn a
        // recoverable JNI close failure into a use-after-delete corruption
        // window.  The second bounded attempt covers the normal retryable
        // close path (for example, a transient slot-release failure).
        final boolean stagedSafe = closeForRestoreCleanup(staged, failure);
        final boolean preparedSafe = closeForRestoreCleanup(prepared, failure);
        final boolean restoreTreeSafe = stagedSafe && preparedSafe;
        final boolean activeDbSafe = closeForRestoreCleanup(installed, failure);
        if (activeDbMoved && activeDbSafe && canRemoveUnpublishedActiveDb(shardRoot, storeUuid)) {
            try {
                deleteTree(activeDb);
                deleteTree(activeDb.getParent());
            } catch (IOException cleanupException) {
                failure.addSuppressed(cleanupException);
            }
        }
        if (restoreTreeSafe) {
            try {
                deleteTree(restoreRoot);
            } catch (IOException cleanupException) {
                failure.addSuppressed(cleanupException);
            }
        }
    }

    private static boolean closeForRestoreCleanup(final ShardStore store, final Throwable failure) {
        if (store == null) {
            return true;
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                store.close();
                return true;
            } catch (RuntimeException cleanupException) {
                if (cleanupException != failure) {
                    failure.addSuppressed(cleanupException);
                }
            }
        }
        return false;
    }

    /** Only remove an installed DB when ACTIVE cannot already refer to it. */
    private static boolean canRemoveUnpublishedActiveDb(final Path shardRoot, final UUID storeUuid) {
        final Path activePointer = shardRoot.resolve("ACTIVE");
        if (!Files.exists(activePointer, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return true;
        }
        if (Files.isSymbolicLink(activePointer)) {
            return false;
        }
        try {
            return !storeUuid.equals(readActivePointer(activePointer));
        } catch (IOException exception) {
            // An unreadable pointer may already have been atomically replaced;
            // preserve the DB for offline repair rather than delete blindly.
            return false;
        }
    }

    /**
     * Restores only an exact manifest that is already published in the local
     * recovery catalog and is still inside its floor-bounded ancestry.
     *
     * <p>The catalog check is deliberately separate from file verification:
     * the former proves recovery authority, while the latter proves the local
     * physical bytes.  Production wiring replaces the in-memory catalog with
     * the Oxia CAS/catalog read without changing this boundary.</p>
     */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest,
                                                   final RecoveryCatalogAuthority catalog) {
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, catalog, null);
    }

    /** Restores with an explicit finite manifest/file inventory limit set. */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest,
                                                   final RecoveryCatalogAuthority catalog,
                                                   final CheckpointManifestLimits limits) {
        return restoreWithRecoveryGuard(config, shardId, resources, checkpointPath, manifest, catalog, null,
                Objects.requireNonNull(limits, "limits"));
    }

    /**
     * Restores from an object-store/download boundary where the manifest is
     * still raw bytes. Canonical decoding happens before catalog lookup or any
     * local checkpoint files are opened.
     */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final byte[] manifestJson,
                                                   final RecoveryCatalogAuthority catalog) {
        final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(
                Objects.requireNonNull(manifestJson, "manifestJson"));
        return restoreFromCheckpoint(config, shardId, resources, checkpointPath, manifest, catalog);
    }

    /** Decodes and restores a manifest under an explicit finite limit set. */
    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final byte[] manifestJson,
                                                   final RecoveryCatalogAuthority catalog,
                                                   final CheckpointManifestLimits limits) {
        final CheckpointManifestLimits checkedLimits = Objects.requireNonNull(limits, "limits");
        final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(
                Objects.requireNonNull(manifestJson, "manifestJson"), checkedLimits);
        return restoreFromCheckpoint(config, shardId, resources, checkpointPath, manifest, catalog, checkedLimits);
    }

    private static void validateRecoveryPin(final ShardId shardId, final CheckpointManifest manifest,
                                             final RecoveryCatalogAuthority catalog, final RecoveryPinV1 pin) {
        // The pin can remain present while the catalog Floor advances.  A
        // candidate that is no longer in the current Floor-bounded ancestry
        // must be rejected even when the pin bytes themselves are unchanged.
        catalog.validatePublishedRestoreCandidate(manifest);
        if (!new ShardSubjectV1(shardId).equals(pin.shard())
                || !java.util.Arrays.equals(pin.candidate().checkpointId(), manifest.checkpointId())
                || !Bytes.constantTimeEquals(pin.candidate().recoveryLineageId(), manifest.recoveryLineageId())
                || !Bytes.constantTimeEquals(pin.candidate().manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("RecoveryPin does not match the restore candidate");
        }
        final RecoveryPinV1 active = catalog.activeRecoveryPin().orElseThrow(() ->
                new IllegalStateException("RecoveryPin is no longer active"));
        if (!active.equals(pin)) {
            throw new IllegalStateException("RecoveryPin identity/value changed during restore");
        }
    }

    private static void validateCheckpointManifest(final ShardId shardId, final Path checkpointPath,
                                                   final CheckpointManifest manifest,
                                                   final CheckpointManifestLimits limits) throws IOException {
        if (!manifest.shardId().equals(shardId) || manifest.storeFormatVersion() != META_STORE_FORMAT) {
            throw new IOException("checkpoint manifest shard or format mismatch");
        }
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpointPath, limits);
        if (inventory.size() != manifest.files().size()) {
            throw new IOException("checkpoint manifest file count mismatch");
        }
        for (int index = 0; index < inventory.size(); index++) {
            final CheckpointFileInventory actual = inventory.get(index);
            final CheckpointManifest.FileEntry expected = manifest.files().get(index);
            if (!actual.name().equals(expected.name()) || actual.length() != expected.length()
                    || !Bytes.constantTimeEquals(actual.checksum(), expected.checksum())) {
                throw new IOException("checkpoint file checksum mismatch: " + actual.name());
            }
        }
    }

    private static void validateRestoredRuntimeState(final ShardStore staged,
                                                     final CheckpointManifest manifest) throws IOException {
        final byte[] restoredCheckpointId = staged.runtimeMetadata().lastCheckpointId();
        if (restoredCheckpointId == null
                || !Bytes.constantTimeEquals(restoredCheckpointId, manifest.checkpointId())) {
            throw new IOException("restored checkpoint identity does not match checkpoint manifest");
        }
        final SourcePosition restoredPosition = staged.appliedShardLogPosition();
        if (restoredPosition == null
                || !Bytes.constantTimeEquals(restoredPosition.canonicalBytes(),
                manifest.appliedShardLogPosition().canonicalBytes())) {
            throw new IOException("restored source position does not match checkpoint manifest");
        }
        if (staged.shardMutationSequence() != manifest.shardMutationSequence()) {
            throw new IOException("restored mutation sequence does not match checkpoint manifest");
        }
        if (!staged.runtimeMetadata().evidenceCursors().equals(manifest.evidenceCursors())) {
            throw new IOException("restored evidence cursors do not match checkpoint manifest");
        }
    }

    private static void validateRestoredRecoveryState(final ShardStore staged,
                                                      final CheckpointManifest manifest) throws IOException {
        final StoreRecoveryMetadata recovery = staged.recoveryMetadata();
        if (recovery.lineageBase() != null
                && !Bytes.constantTimeEquals(recovery.lineageBase().recoveryLineageId(),
                manifest.recoveryLineageId())) {
            throw new IOException("restored recovery candidate lineage does not match checkpoint manifest");
        }
        if (recovery.lastObservedFloor() != null
                && !Bytes.constantTimeEquals(recovery.lastObservedFloor().recoveryLineageId(),
                manifest.recoveryLineageId())) {
            throw new IOException("restored Recovery Floor lineage does not match checkpoint manifest");
        }
        if (recovery.installState() != null && recovery.lineageBase() != null
                && !java.util.Arrays.equals(recovery.installState().checkpointId(),
                recovery.lineageBase().checkpointId())) {
            throw new IOException("restored recovery install state does not match its base candidate");
        }
    }

    private static Path locateOrCreateDbPath(final Path shardRoot) throws IOException {
        final Path activePointer = shardRoot.resolve("ACTIVE");
        if (Files.exists(activePointer, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(activePointer)) {
                throw new IOException("ACTIVE pointer must not be a symbolic link");
            }
            final UUID activeStore = readActivePointer(activePointer);
            final Path incarnations = shardRoot.resolve("incarnations");
            final Path activeIncarnation = incarnations.resolve(activeStore.toString());
            final Path activeDb = activeIncarnation.resolve("db");
            if (Files.isSymbolicLink(incarnations) || Files.isSymbolicLink(activeIncarnation)
                    || Files.isSymbolicLink(activeDb)
                    || !Files.isRegularFile(activeDb.resolve("CURRENT"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("ACTIVE points to a missing DB: " + activeDb);
            }
            return activeDb;
        }
        final Path incarnations = shardRoot.resolve("incarnations");
        final List<Path> candidates;
        if (!Files.exists(incarnations, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            ensureRealDirectory(incarnations);
            return incarnations.resolve(UUID.randomUUID().toString()).resolve("db");
        }
        if (Files.isSymbolicLink(incarnations)
                || !Files.isDirectory(incarnations, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("incarnations path must be a real directory: " + incarnations);
        }
        final List<Path> found = new ArrayList<>();
        try (var stream = Files.list(incarnations)) {
            for (Path path : stream.sorted(Comparator.comparing(item -> item.getFileName().toString())).toList()) {
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("store incarnation must not be a symbolic link: " + path);
                }
                if (!Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                final Path db = path.resolve("db");
                if (Files.isSymbolicLink(db) || Files.isSymbolicLink(db.resolve("CURRENT"))) {
                    throw new IOException("store DB path must not contain a symbolic link: " + db);
                }
                if (Files.isRegularFile(db.resolve("CURRENT"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    found.add(path);
                }
            }
        }
        candidates = List.copyOf(found);
        if (candidates.size() > 1) {
            throw new IOException("more than one active DB incarnation exists: " + candidates);
        }
        if (candidates.size() == 1) {
            return candidates.get(0).resolve("db");
        }
        return incarnations.resolve(UUID.randomUUID().toString()).resolve("db");
    }

    /**
     * Resolves the fixed local shard path without following a symbolic link in
     * the worker-owned directory components.  The configured root itself may
     * be a deployment symlink, but {@code shards/<route>/<partition>} is the
     * physical ownership boundary and must remain inside that root namespace.
     */
    private static Path prepareShardRoot(final ShardStoreConfig config, final ShardId shardId) throws IOException {
        Files.createDirectories(config.rootPath());
        final Path shardsRoot = config.rootPath().resolve("shards");
        ensureRealDirectory(shardsRoot);
        final Path routeRoot = shardsRoot.resolve(shardId.routeIncarnation().uuid().toString());
        ensureRealDirectory(routeRoot);
        final Path shardRoot = routeRoot.resolve(Integer.toUnsignedString(shardId.partition()));
        ensureRealDirectory(shardRoot);
        return shardRoot;
    }

    private static void ensureRealDirectory(final Path path) throws IOException {
        if (!Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectory(path);
        }
        if (Files.isSymbolicLink(path)
                || !Files.isDirectory(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("shard path component must be a real directory: " + path);
        }
    }

    private static boolean hasActiveDb(final Path shardRoot) throws IOException {
        final Path activePointer = shardRoot.resolve("ACTIVE");
        if (!Files.exists(activePointer, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return false;
        }
        if (Files.isSymbolicLink(activePointer)) {
            throw new IOException("ACTIVE pointer must not be a symbolic link");
        }
        final UUID activeStore = readActivePointer(activePointer);
        final Path incarnations = shardRoot.resolve("incarnations");
        final Path activeIncarnation = incarnations.resolve(activeStore.toString());
        final Path activeDb = activeIncarnation.resolve("db");
        if (Files.isSymbolicLink(incarnations) || Files.isSymbolicLink(activeIncarnation)
                || Files.isSymbolicLink(activeDb)
                || !Files.isDirectory(incarnations, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                || !Files.isDirectory(activeIncarnation, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                || !Files.isDirectory(activeDb, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("active shard path must not contain a symbolic link: " + activeDb);
        }
        if (!Files.isRegularFile(activeDb.resolve("CURRENT"), java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("ACTIVE points to a missing DB: " + activeDb);
        }
        return true;
    }

    private static UUID storeUuidFromPath(final Path dbPath) throws IOException {
        try {
            return UUID.fromString(dbPath.getParent().getFileName().toString());
        } catch (IllegalArgumentException exception) {
            throw new IOException("DB path does not carry a valid Store Incarnation: " + dbPath, exception);
        }
    }

    private static void writeActivePointer(final Path shardRoot, final UUID storeUuid) throws IOException {
        ensureRealDirectory(shardRoot.resolve("incarnations"));
        final byte[] body = java.nio.ByteBuffer.allocate(4 + 16)
                .putInt(ACTIVE_MAGIC).put(uuidBytes(storeUuid)).array();
        final byte[] encoded = Bytes.concat(body, Bytes.crc32cbe(body));
        final Path temporary = shardRoot.resolve("ACTIVE.tmp");
        if (Files.isSymbolicLink(temporary)
                || (Files.exists(temporary, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                && !Files.isRegularFile(temporary, java.nio.file.LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("ACTIVE.tmp must be a regular non-symbolic file: " + temporary);
        }
        Files.write(temporary, encoded, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        Files.move(temporary, shardRoot.resolve("ACTIVE"), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel channel = FileChannel.open(shardRoot, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static UUID readActivePointer(final Path pointer) throws IOException {
        final byte[] encoded = Files.readAllBytes(pointer);
        if (encoded.length != 4 + 16 + 4) {
            throw new IOException("invalid ACTIVE pointer length");
        }
        final long expected = Bytes.crc32c(encoded, 0, encoded.length - 4);
        final long actual = Bytes.readU32be(encoded, encoded.length - 4);
        if (expected != actual || java.nio.ByteBuffer.wrap(encoded).getInt() != ACTIVE_MAGIC) {
            throw new IOException("invalid ACTIVE pointer checksum or magic");
        }
        final java.nio.ByteBuffer input = java.nio.ByteBuffer.wrap(encoded, 4, 16);
        return new UUID(input.getLong(), input.getLong());
    }

    private static ShardStore openAtPath(final ShardStoreConfig config, final ShardId shardId, final Path dbPath,
                                         final SharedRocksDbResources resources,
                                         final UUID restoreStoreIncarnation,
                                         final boolean acquireOwnedSlot) throws IOException, RocksDBException {
        boolean acquireSlotAcquired = false;
        boolean ownedSlotAcquired = false;
        boolean dbSlotAcquired = false;
        try {
            resources.acquireShardAcquireSlot();
            acquireSlotAcquired = true;
            if (acquireOwnedSlot) {
                resources.acquireOwnedShardSlot(shardId);
                ownedSlotAcquired = true;
            }
            resources.acquireDbSlot();
            dbSlotAcquired = true;
            final ShardStore opened = openAtPathWithSlot(config, shardId, dbPath, resources,
                    restoreStoreIncarnation, acquireOwnedSlot);
            resources.releaseShardAcquireSlot();
            acquireSlotAcquired = false;
            return opened;
        } catch (IOException | RocksDBException | RuntimeException exception) {
            // The DB slot is acquired after the owned slot.  Release only the
            // slots that this invocation actually acquired.
            if (dbSlotAcquired) {
                resources.releaseDbSlot();
            }
            if (ownedSlotAcquired) {
                resources.releaseOwnedShardSlot(shardId);
            }
            if (acquireSlotAcquired) {
                resources.releaseShardAcquireSlot();
            }
            throw exception;
        }
    }

    // RocksDB JNI deprecates the explicit split setters in favor of the single
    // max-background-jobs knob, but V1 requires a nonzero flush reserve and a
    // separate compaction ceiling, so keep the registered split at this boundary.
    @SuppressWarnings("deprecation")
    private static ShardStore openAtPathWithSlot(final ShardStoreConfig config, final ShardId shardId,
                                                 final Path dbPath, final SharedRocksDbResources resources,
                                                 final UUID restoreStoreIncarnation,
                                                 final boolean ownsShardSlot)
            throws IOException, RocksDBException {
        if (Files.isSymbolicLink(dbPath)
                || (Files.exists(dbPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                && !Files.isDirectory(dbPath, java.nio.file.LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("DB path must be a real directory: " + dbPath);
        }
        final Path current = dbPath.resolve("CURRENT");
        if (Files.isSymbolicLink(current)) {
            throw new IOException("RocksDB CURRENT must not be a symbolic link: " + current);
        }
        Files.createDirectories(dbPath);
        final boolean existing = Files.isRegularFile(current, java.nio.file.LinkOption.NOFOLLOW_LINKS);
        final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
        final List<ColumnFamilyDescriptor> descriptors = descriptors(config, resources, existing, cfOptions, dbPath);
        final DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setParanoidChecks(true)
                .setMaxOpenFiles(config.maxOpenFilesPerDb())
                .setEnv(resources.env())
                .setMaxBackgroundJobs(config.maxBackgroundJobsPerDb())
                .setMaxBackgroundFlushes(config.reservedFlushJobs())
                .setMaxBackgroundCompactions(config.maxCompactionJobs())
                .setWriteBufferManager(resources.writeBufferManager())
                .setRateLimiter(resources.rateLimiter());
        final List<ColumnFamilyHandle> openedHandles = new ArrayList<>();
        final RocksDB db;
        try {
            db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, openedHandles);
        } catch (RocksDBException exception) {
            RuntimeException cleanupFailure = closeQuietly(cfOptions);
            try {
                dbOptions.close();
            } catch (RuntimeException closeException) {
                cleanupFailure = appendCloseFailure(cleanupFailure, closeException);
            }
            if (cleanupFailure != null) {
                exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
        boolean keepOpen = false;
        Throwable primaryFailure = null;
        try {
            final Map<ColumnFamily, ColumnFamilyHandle> handles = new EnumMap<>(ColumnFamily.class);
            for (int index = 0; index < ColumnFamily.values().length; index++) {
                handles.put(ColumnFamily.values()[index], openedHandles.get(index + 1));
            }
            if (hasDefaultColumnFamilyData(db, openedHandles.get(0))) {
                throw new IllegalStateException("default RocksDB column family must remain empty");
            }
            final byte[] encodedIdentity = db.get(handles.get(ColumnFamily.META),
                    KeyCodec.metaFixed(META_SHARD_IDENTITY));
            final byte[] identityBytes = encodedIdentity == null ? null
                    : ValueEnvelope.decode(encodedIdentity, META_FIXED_VALUE_TYPE).payload();
            StoreMetadata metadata;
            StoreRuntimeMetadata runtimeMetadata;
            if (identityBytes == null) {
                if (existing) {
                    throw new IllegalStateException("existing shard DB is missing meta_cf shard identity");
                }
                final UUID storeUuid;
                try {
                    storeUuid = UUID.fromString(dbPath.getParent().getFileName().toString());
                } catch (IllegalArgumentException exception) {
                    throw new IllegalStateException("DB path does not carry a valid store incarnation: " + dbPath,
                            exception);
                }
                final byte[] storeIncarnation = java.nio.ByteBuffer.allocate(16)
                        .putLong(storeUuid.getMostSignificantBits()).putLong(storeUuid.getLeastSignificantBits())
                        .array();
                final StoreMetadata created = new StoreMetadata(1, shardId, storeIncarnation, Bytes.sha256(
                        Bytes.concat(Bytes.utf8("nereus-delay-db-identity-v1\0"), storeIncarnation,
                                shardId.routeIncarnation().bytes(), Bytes.u32beBits(shardId.partition()))));
                try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
                    batch.put(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_STORE_FORMAT),
                            ValueEnvelope.encode(META_FIXED_VALUE_TYPE, Bytes.u32be(1)));
                    batch.put(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_SHARD_IDENTITY),
                            ValueEnvelope.encode(META_FIXED_VALUE_TYPE, created.encode()));
                    db.write(writeOptions, batch);
                }
                metadata = created;
            } else {
                metadata = StoreMetadata.decode(identityBytes);
                if (!metadata.shardId().equals(shardId)) {
                    throw new IllegalStateException("shard identity mismatch: expected " + shardId + " got "
                            + metadata.shardId());
                }
                final UUID pathStoreIncarnation = incarnationFromPath(dbPath);
                if (pathStoreIncarnation != null && !pathStoreIncarnation.equals(metadata.storeIncarnationUuid())) {
                    throw new IllegalStateException("store incarnation does not match DB path: expected "
                            + pathStoreIncarnation + " got " + metadata.storeIncarnationUuid());
                }
            }
            if (restoreStoreIncarnation != null && identityBytes != null) {
                final byte[] storeIncarnation = uuidBytes(restoreStoreIncarnation);
                final StoreMetadata restored = new StoreMetadata(metadata.storeFormatVersion(), shardId,
                        storeIncarnation, metadata.dbIdentity());
                try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
                    batch.put(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_SHARD_IDENTITY),
                            ValueEnvelope.encode(META_FIXED_VALUE_TYPE, restored.encode()));
                    db.write(writeOptions, batch);
                }
                metadata = restored;
            }
            final byte[] format = optionalFixedValue(db, handles.get(ColumnFamily.META), META_STORE_FORMAT);
            if (format == null || format.length != Integer.BYTES || Bytes.readU32be(format, 0) != 1) {
                throw new IllegalStateException("missing or unsupported store format marker");
            }
            if (db.get(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_CONTROL_SNAPSHOT)) != null) {
                throw new IllegalStateException("meta/FIXED control snapshot is not supported by this store version");
            }
            validateFixedMetadata(db, handles.get(ColumnFamily.META), shardId);
            final RuntimeMetadataRead runtimeRead = readRuntimeMetadata(db, handles.get(ColumnFamily.META));
            runtimeMetadata = runtimeRead.metadata();
            StoreRecoveryMetadata recoveryMetadata = readRecoveryMetadata(db, handles.get(ColumnFamily.META),
                    metadata, restoreStoreIncarnation != null);
            final long closedIngressDeadlineThrough = runtimeRead.closedIngressDeadlineThrough();
            if (runtimeMetadata.cleanCloseMarker()) {
                runtimeMetadata = runtimeMetadata.withCleanCloseMarker(false);
            }
            final RecoveryInstallPhaseV1 openPhase = restoreStoreIncarnation == null
                    ? RecoveryInstallPhaseV1.OPEN : RecoveryInstallPhaseV1.INSTALLED;
            recoveryMetadata = recoveryMetadata.withInstallState(new RecoveryInstallStateV1(openPhase,
                    uuidBytes(metadata.storeIncarnationUuid()), recoveryMetadata.checkpointId()));
            try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
                putRuntimeMetadata(batch, handles.get(ColumnFamily.META), runtimeMetadata,
                        closedIngressDeadlineThrough);
                putRecoveryMetadata(batch, handles.get(ColumnFamily.META), recoveryMetadata);
                db.write(writeOptions, batch);
            }
            final ShardStore result = new ShardStore(config, shardId, dbPath, resources, db, dbOptions,
                    openedHandles.get(0), cfOptions, handles, metadata, ownsShardSlot, runtimeMetadata,
                    recoveryMetadata, closedIngressDeadlineThrough);
            resources.registerPhysicalUsage(shardId, result.physicalUsageSource);
            keepOpen = true;
            return result;
        } catch (IOException | RocksDBException | RuntimeException | Error exception) {
            primaryFailure = exception;
            throw exception;
        } finally {
            if (!keepOpen) {
                final RuntimeException cleanupFailure = closeHandles(db, openedHandles, cfOptions, dbOptions);
                if (cleanupFailure != null) {
                    if (primaryFailure != null) {
                        primaryFailure.addSuppressed(cleanupFailure);
                    } else {
                        throw cleanupFailure;
                    }
                }
            }
        }
    }

    private static RuntimeMetadataRead readRuntimeMetadata(final RocksDB db,
                                                           final ColumnFamilyHandle metaHandle)
            throws RocksDBException {
        final byte[] ingressFenceBytes = optionalFixedValue(db, metaHandle, META_INGRESS_FENCE_STATE);
        final IngressFenceState ingressFence = ingressFenceBytes == null
                ? new IngressFenceState(IngressFenceState.OPEN, null)
                : IngressFenceState.decode(ingressFenceBytes);
        final byte[] evidenceBytes = optionalFixedValue(db, metaHandle, META_EVIDENCE_CURSORS);
        final byte[] checkpointId = optionalFixedValue(db, metaHandle, META_CHECKPOINT_ID);
        final byte[] ownerBytes = optionalFixedValue(db, metaHandle, META_OWNER_EPOCH);
        final byte[] cleanBytes = optionalFixedValue(db, metaHandle, META_CLEAN_CLOSE_MARKER);
        final long ownerEpoch;
        if (ownerBytes == null) {
            ownerEpoch = 0;
        } else {
            if (ownerBytes.length != Long.BYTES) {
                throw new IllegalArgumentException("invalid persisted owner epoch");
            }
            ownerEpoch = Bytes.readU64be(ownerBytes, 0);
        }
        final boolean cleanClose;
        if (cleanBytes == null) {
            cleanClose = false;
        } else {
            if (cleanBytes.length != 1 || (cleanBytes[0] != 0 && cleanBytes[0] != 1)) {
                throw new IllegalArgumentException("invalid clean-close marker");
            }
            cleanClose = cleanBytes[0] == 1;
        }
        final List<EvidenceCursorV1> evidenceCursors = evidenceBytes == null
                ? List.of() : StoreRuntimeMetadata.decodeEvidenceCursors(evidenceBytes);
        return new RuntimeMetadataRead(new StoreRuntimeMetadata(ingressFence.proofId(), checkpointId, ownerEpoch,
                cleanClose, evidenceCursors), ingressFence.closedThroughEpochMs());
    }

    private static StoreRecoveryMetadata readRecoveryMetadata(final RocksDB db,
                                                              final ColumnFamilyHandle metaHandle,
                                                              final StoreMetadata metadata,
                                                              final boolean installMode)
            throws RocksDBException {
        final byte[] lineageBytes = optionalRecoveryValue(db, metaHandle, META_RECOVERY_LINEAGE_BASE);
        final RecoveryCandidateRefV1 lineageBase = lineageBytes == null
                ? null : RecoveryCandidateRefV1.decode(lineageBytes);
        if (!installMode && lineageBase != null
                && lineageBase.kind() == io.nereusstream.delay.protocol.RecoveryCandidateKindV1.LOCAL_STORE
                && !java.util.Arrays.equals(lineageBase.storeIncarnation(), metadata.storeIncarnation())) {
            throw new IllegalStateException("local recovery candidate store incarnation does not match DB identity");
        }
        final byte[] floorBytes = optionalRecoveryValue(db, metaHandle, META_RECOVERY_LAST_OBSERVED_FLOOR);
        final RecoveryFloorRefV1 floor = floorBytes == null ? null : RecoveryFloorRefV1.decode(floorBytes);
        if (floor != null && !metadata.shardId().equals(floor.appliedSourcePosition().shardId())) {
            throw new IllegalStateException("persisted Recovery Floor belongs to another shard");
        }
        final byte[] generationBytes = optionalRecoveryValue(db, metaHandle, META_RECOVERY_CATALOG_GENERATION);
        final long catalogGeneration;
        if (generationBytes == null) {
            catalogGeneration = 0;
        } else {
            if (generationBytes.length != Long.BYTES) {
                throw new IllegalArgumentException("invalid persisted recovery catalog generation");
            }
            catalogGeneration = Bytes.readU64be(generationBytes, 0);
            if (catalogGeneration == 0) {
                throw new IllegalArgumentException("persisted recovery catalog generation must be nonzero");
            }
        }
        final byte[] installBytes = optionalRecoveryValue(db, metaHandle, META_RECOVERY_INSTALL_STATE);
        final RecoveryInstallStateV1 installState = installBytes == null
                ? null : RecoveryInstallStateV1.decode(installBytes);
        if (installState != null && lineageBase != null
                && !java.util.Arrays.equals(installState.checkpointId(), lineageBase.checkpointId())) {
            throw new IllegalStateException("recovery install state checkpoint does not match lineage base");
        }
        if (installState != null && lineageBase == null && installState.checkpointId() != null) {
            throw new IllegalStateException("recovery install state has checkpoint without lineage base");
        }
        if (!installMode && installState != null
                && !java.util.Arrays.equals(installState.storeIncarnation(), metadata.storeIncarnation())) {
            throw new IllegalStateException("recovery install state store incarnation does not match DB identity");
        }
        return new StoreRecoveryMetadata(lineageBase, floor, catalogGeneration, installState);
    }

    private static byte[] optionalRecoveryValue(final RocksDB db, final ColumnFamilyHandle metaHandle,
                                                final int recoveryKeyKind) throws RocksDBException {
        final byte[] encoded = db.get(metaHandle, KeyCodec.metaRecovery(recoveryKeyKind));
        return encoded == null ? null : ValueEnvelope.decode(encoded, META_RECOVERY_VALUE_TYPE).payload();
    }

    private static void validateFixedMetadata(final RocksDB db, final ColumnFamilyHandle metaHandle,
                                              final ShardId shardId) throws RocksDBException {
        final byte[] sourceBytes = optionalFixedValue(db, metaHandle, META_APPLIED_SOURCE_POSITION);
        if (sourceBytes != null) {
            final SourcePosition position = SourcePositionCodec.decode(sourceBytes);
            if (!shardId.equals(position.shardId())) {
                throw new IllegalStateException("persisted source position belongs to another shard");
            }
        }
        validateUnsignedSequence(optionalFixedValue(db, metaHandle, META_MUTATION_SEQUENCE),
                "persisted shard mutation sequence");
        validateUnsignedSequence(optionalFixedValue(db, metaHandle, META_CLAIM_SEQUENCE),
                "persisted Claim sequence");
        validateOptionalFixedEnvelope(db, metaHandle, META_PAYLOAD_PROOF_CONTROL_STATE,
                META_PAYLOAD_PROOF_VALUE_TYPE);
        validateOptionalFixedEnvelope(db, metaHandle, META_PROFILE_CONTROL_STATE, META_PROFILE_VALUE_TYPE);
    }

    private static void validateOptionalFixedEnvelope(final RocksDB db, final ColumnFamilyHandle metaHandle,
                                                      final int fixedKeyKind, final int valueType)
            throws RocksDBException {
        final byte[] encoded = db.get(metaHandle, KeyCodec.metaFixed(fixedKeyKind));
        if (encoded != null) {
            final byte[] payload = ValueEnvelope.decode(encoded, valueType).payload();
            if (payload.length == 0) {
                throw new IllegalArgumentException("empty fixed metadata payload: " + fixedKeyKind);
            }
        }
    }

    private static void validateUnsignedSequence(final byte[] payload, final String name) {
        if (payload == null) {
            return;
        }
        if (payload.length != Long.BYTES) {
            throw new IllegalArgumentException(name + " is invalid");
        }
    }

    private static byte[] optionalFixedValue(final RocksDB db, final ColumnFamilyHandle metaHandle,
                                             final int fixedKeyKind) throws RocksDBException {
        final byte[] encoded = db.get(metaHandle, KeyCodec.metaFixed(fixedKeyKind));
        if (encoded == null) {
            return null;
        }
        return ValueEnvelope.decode(encoded, META_FIXED_VALUE_TYPE).payload();
    }

    private static void putRuntimeMetadata(final WriteBatch batch, final ColumnFamilyHandle metaHandle,
                                           final StoreRuntimeMetadata next,
                                           final long closedIngressDeadlineThrough) throws RocksDBException {
        if (closedIngressDeadlineThrough < IngressFenceState.OPEN) {
            throw new IllegalArgumentException("closed ingress deadline must be -1 or non-negative");
        }
        final IngressFenceState ingressFence = new IngressFenceState(closedIngressDeadlineThrough,
                next.lastIngressFenceProofId());
        if (ingressFence.closedThroughEpochMs() == IngressFenceState.OPEN && ingressFence.proofId() == null) {
            batch.delete(metaHandle, KeyCodec.metaFixed(META_INGRESS_FENCE_STATE));
        } else {
            batch.put(metaHandle, KeyCodec.metaFixed(META_INGRESS_FENCE_STATE),
                    ValueEnvelope.encode(META_FIXED_VALUE_TYPE, ingressFence.canonicalBytes()));
        }
        batch.put(metaHandle, KeyCodec.metaFixed(META_EVIDENCE_CURSORS),
                ValueEnvelope.encode(META_FIXED_VALUE_TYPE, next.evidenceCursorArrayCanonicalBytes()));
        putOptionalFixedValue(batch, metaHandle, META_CHECKPOINT_ID, next.lastCheckpointId());
        batch.put(metaHandle, KeyCodec.metaFixed(META_OWNER_EPOCH),
                ValueEnvelope.encode(META_FIXED_VALUE_TYPE, Bytes.u64beBits(next.lastOpenedOwnerEpoch())));
        batch.put(metaHandle, KeyCodec.metaFixed(META_CLEAN_CLOSE_MARKER),
                ValueEnvelope.encode(META_FIXED_VALUE_TYPE,
                        Bytes.u8(next.cleanCloseMarker() ? 1 : 0)));
    }

    private static void putRecoveryMetadata(final WriteBatch batch, final ColumnFamilyHandle metaHandle,
                                            final StoreRecoveryMetadata next) throws RocksDBException {
        putOptionalRecoveryValue(batch, metaHandle, META_RECOVERY_LINEAGE_BASE,
                next.lineageBase() == null ? null : next.lineageBase().canonicalBytes());
        putOptionalRecoveryValue(batch, metaHandle, META_RECOVERY_LAST_OBSERVED_FLOOR,
                next.lastObservedFloor() == null ? null : next.lastObservedFloor().canonicalBytes());
        putOptionalRecoveryValue(batch, metaHandle, META_RECOVERY_CATALOG_GENERATION,
                next.catalogGeneration() == 0 ? null : Bytes.u64beBits(next.catalogGeneration()));
        putOptionalRecoveryValue(batch, metaHandle, META_RECOVERY_INSTALL_STATE,
                next.installState() == null ? null : next.installState().canonicalBytes());
    }

    private static void putOptionalRecoveryValue(final WriteBatch batch, final ColumnFamilyHandle metaHandle,
                                                 final int recoveryKeyKind, final byte[] payload)
            throws RocksDBException {
        final byte[] key = KeyCodec.metaRecovery(recoveryKeyKind);
        if (payload == null) {
            batch.delete(metaHandle, key);
        } else {
            batch.put(metaHandle, key, ValueEnvelope.encode(META_RECOVERY_VALUE_TYPE, payload));
        }
    }

    private static void putOptionalFixedValue(final WriteBatch batch, final ColumnFamilyHandle metaHandle,
                                              final int fixedKeyKind, final byte[] payload) throws RocksDBException {
        final byte[] key = KeyCodec.metaFixed(fixedKeyKind);
        if (payload == null) {
            batch.delete(metaHandle, key);
        } else {
            batch.put(metaHandle, key, ValueEnvelope.encode(META_FIXED_VALUE_TYPE, payload));
        }
    }

    private static IngressFenceState readIngressFenceState(final RocksDB db,
                                                           final ColumnFamilyHandle metaHandle)
            throws RocksDBException {
        final byte[] encoded = db.get(metaHandle, KeyCodec.metaFixed(META_INGRESS_FENCE_STATE));
        if (encoded == null) {
            return new IngressFenceState(IngressFenceState.OPEN, null);
        }
        return IngressFenceState.decode(ValueEnvelope.decode(encoded, META_FIXED_VALUE_TYPE).payload());
    }

    private record RuntimeMetadataRead(StoreRuntimeMetadata metadata, long closedIngressDeadlineThrough) {
    }

    private static byte[] uuidBytes(final UUID uuid) {
        return java.nio.ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits()).array();
    }

    private static UUID incarnationFromPath(final Path dbPath) throws IOException {
        final Path incarnation = dbPath.getParent();
        final Path container = incarnation == null ? null : incarnation.getParent();
        if (container == null || !"incarnations".equals(container.getFileName().toString())) {
            return null;
        }
        try {
            return UUID.fromString(incarnation.getFileName().toString());
        } catch (IllegalArgumentException exception) {
            throw new IOException("DB path has an invalid store incarnation: " + dbPath, exception);
        }
    }

    private static void copyTree(final Path source, final Path target) throws IOException {
        final List<Path> copiedDirectories = new ArrayList<>();
        try (var paths = Files.walk(source)) {
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
                final Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("checkpoint contains a symbolic link: " + path);
                }
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(destination);
                    ensureRealDirectory(destination);
                    copiedDirectories.add(destination);
                } else if (Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination, LinkOption.NOFOLLOW_LINKS);
                    forceFile(destination);
                } else {
                    throw new IOException("checkpoint contains a non-regular file: " + path);
                }
            }
        }
        // Persist children before their parent directories, so a later ACTIVE
        // pointer publication cannot expose a directory whose copied DB files
        // still exist only in the page cache.
        for (int index = copiedDirectories.size() - 1; index >= 0; index--) {
            forceDirectory(copiedDirectories.get(index));
        }
    }

    private static void forceFile(final Path file) throws IOException {
        if (Files.isSymbolicLink(file)
                || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("file must be a real regular file: " + file);
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private static void forceIncarnationBeforeActivePointer(final Path dbPath) throws IOException {
        forceDirectory(dbPath);
        forceDirectory(dbPath.getParent());
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attributes) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path directory, final IOException exception)
                    throws IOException {
                if (exception != null) {
                    throw exception;
                }
                Files.deleteIfExists(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static List<ColumnFamilyDescriptor> descriptors(final ShardStoreConfig config,
                                                             final SharedRocksDbResources resources,
                                                             final boolean existing, final List<ColumnFamilyOptions> options,
                                                             final Path dbPath) throws IOException, RocksDBException {
        final List<byte[]> existingNames;
        if (existing) {
            try (Options listOptions = new Options()) {
                existingNames = RocksDB.listColumnFamilies(listOptions, dbPath.toString());
            }
            final Set<String> names = new HashSet<>();
            for (byte[] name : existingNames) {
                names.add(new String(name, java.nio.charset.StandardCharsets.UTF_8));
            }
            final Set<String> expected = new HashSet<>();
            expected.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, java.nio.charset.StandardCharsets.UTF_8));
            for (ColumnFamily family : ColumnFamily.values()) {
                expected.add(family.rocksName());
            }
            if (!names.equals(expected)) {
                throw new IOException("DB column families differ from V1 set: " + names);
            }
        }
        final List<ColumnFamilyDescriptor> result = new ArrayList<>();
        result.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                familyOptions(resources, config.maxWriteBufferBytesPerDb())));
        options.add(result.get(0).getOptions());
        for (ColumnFamily family : ColumnFamily.values()) {
            final ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    family.rocksName().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    familyOptions(resources, config.maxWriteBufferBytesPerDb()));
            result.add(descriptor);
            options.add(descriptor.getOptions());
        }
        return result;
    }

    private static ColumnFamilyOptions familyOptions(final SharedRocksDbResources resources,
                                                     final long maxWriteBufferBytesPerDb) {
        final BlockBasedTableConfig table = new BlockBasedTableConfig().setBlockCache(resources.blockCache());
        return new ColumnFamilyOptions().setTableFormatConfig(table)
                .setWriteBufferSize(maxWriteBufferBytesPerDb);
    }

    private static RuntimeException closeQuietly(final List<ColumnFamilyOptions> options) {
        RuntimeException failure = null;
        for (ColumnFamilyOptions option : options) {
            try {
                option.close();
            } catch (RuntimeException closeException) {
                failure = appendCloseFailure(failure, closeException);
            }
        }
        return failure;
    }

    private static RuntimeException closeHandles(final RocksDB db, final List<ColumnFamilyHandle> handles,
                                                 final List<ColumnFamilyOptions> options, final DBOptions dbOptions) {
        RuntimeException failure = null;
        for (ColumnFamilyHandle handle : handles) {
            try {
                handle.close();
            } catch (RuntimeException closeException) {
                failure = appendCloseFailure(failure, closeException);
            }
        }
        try {
            db.close();
        } catch (RuntimeException closeException) {
            failure = appendCloseFailure(failure, closeException);
        }
        final RuntimeException optionsFailure = closeQuietly(options);
        failure = appendCloseFailure(failure, optionsFailure);
        try {
            dbOptions.close();
        } catch (RuntimeException closeException) {
            failure = appendCloseFailure(failure, closeException);
        }
        return failure;
    }

    private static void closeAfterActivePointerFailure(final ShardStore opened, final IOException failure) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                opened.close();
                return;
            } catch (RuntimeException cleanupException) {
                failure.addSuppressed(cleanupException);
            }
        }
    }

    private static boolean hasDefaultColumnFamilyData(final RocksDB db, final ColumnFamilyHandle defaultHandle) {
        try (RocksIterator iterator = db.newIterator(defaultHandle)) {
            iterator.seekToFirst();
            iterator.status();
            return iterator.isValid();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("cannot validate default column family", exception);
        }
    }

    public ShardId shardId() {
        return shardId;
    }

    public Path dbPath() {
        return dbPath;
    }

    public StoreMetadata metadata() {
        return metadata;
    }

    /** Returns the process-wide resource envelope that owns this DB slot. */
    public SharedRocksDbResources sharedResources() {
        return resources;
    }

    /** Returns a checked point-in-time physical usage observation for this DB. */
    public synchronized RocksDbUsageSnapshot physicalUsage() {
        ensureOpen();
        return RocksDbUsageSnapshot.collect(shardId, db, dbPath);
    }

    /** Applies a per-DB usage guard against the filesystem containing this store. */
    public synchronized void requirePhysicalUsageWithin(final RocksDbUsageLimits limits) {
        ensureOpen();
        Objects.requireNonNull(limits, "limits").validate(List.of(physicalUsage()), config.rootPath());
    }

    /** Returns whether Store close has fenced operations and may need a retry. */
    public synchronized boolean isCloseStarted() {
        return closeStarted;
    }

    /** Returns whether every native handle and Worker slot has been released. */
    public synchronized boolean isClosed() {
        return closed.get();
    }

    /** Returns the local mutable metadata projection; it is not remote authority. */
    public synchronized StoreRuntimeMetadata runtimeMetadata() {
        return runtimeMetadata;
    }

    /** Returns the local recovery projection; it is not Oxia or catalog authority. */
    public synchronized StoreRecoveryMetadata recoveryMetadata() {
        return recoveryMetadata;
    }

    /**
     * Returns whether this DB contains the minimum local facts needed before
     * an external catalog can consider local recovery reuse.  The catalog must
     * still prove ancestry/Floor coverage; this method never does so itself.
     */
    public synchronized boolean hasReusableRecoveryProof() {
        return recoveryMetadata.hasReusableProof();
    }

    /**
     * Atomically records the local lineage/base and observed Floor projection
     * for this Store Incarnation.  A null Floor clears the local reuse proof.
     */
    public synchronized void recordRecoveryMetadata(final RecoveryCandidateRefV1 lineageBase,
                                                     final RecoveryFloorRefV1 lastObservedFloor) {
        ensureOpen();
        if (lastObservedFloor != null
                && !shardId.equals(lastObservedFloor.appliedSourcePosition().shardId())) {
            throw new IllegalArgumentException("Recovery Floor belongs to another shard");
        }
        if (lineageBase != null
                && lineageBase.kind() == io.nereusstream.delay.protocol.RecoveryCandidateKindV1.LOCAL_STORE
                && !java.util.Arrays.equals(lineageBase.storeIncarnation(), metadata.storeIncarnation())) {
            throw new IllegalArgumentException("local recovery candidate must identify this Store Incarnation");
        }
        final RecoveryInstallStateV1 state = new RecoveryInstallStateV1(RecoveryInstallPhaseV1.OPEN,
                metadata.storeIncarnation(), lineageBase == null ? null : lineageBase.checkpointId());
        final StoreRecoveryMetadata next = new StoreRecoveryMetadata(lineageBase, lastObservedFloor,
                lastObservedFloor == null ? 0 : lastObservedFloor.catalogGeneration(), state);
        write(batch -> batch.putRecoveryMetadata(next));
    }

    /** Returns the exact source position persisted by the authoritative shard WriteBatch. */
    public synchronized SourcePosition appliedShardLogPosition() {
        final ValueEnvelope.Decoded value = getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), 1);
        return value == null ? null : SourcePositionCodec.decode(value.payload());
    }

    /** Returns the persisted source-ordered mutation sequence used by checkpoint barriers. */
    public synchronized long shardMutationSequence() {
        final ValueEnvelope.Decoded value = getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_MUTATION_SEQUENCE), 1);
        if (value == null) {
            return 0;
        }
        final byte[] payload = value.payload();
        if (payload.length != Long.BYTES) {
            throw new IllegalStateException("invalid persisted shard mutation sequence");
        }
        return java.nio.ByteBuffer.wrap(payload).getLong();
    }

    /** Persists the latest authenticated ingress fence proof identity. */
    public synchronized void recordLastIngressFenceProofId(final byte[] proofId) {
        ensureOpen();
        persistRuntimeMetadata(new StoreRuntimeMetadata(proofId, runtimeMetadata.lastCheckpointId(),
                runtimeMetadata.lastOpenedOwnerEpoch(), false, runtimeMetadata.evidenceCursors()));
    }

    /** Persists the last checkpoint identity represented by this Store. */
    public synchronized void recordLastCheckpointId(final byte[] checkpointId) {
        ensureOpen();
        persistRuntimeMetadata(new StoreRuntimeMetadata(runtimeMetadata.lastIngressFenceProofId(), checkpointId,
                runtimeMetadata.lastOpenedOwnerEpoch(), false, runtimeMetadata.evidenceCursors()));
    }

    /** Persists a non-decreasing Owner Epoch observed at Store open. */
    public synchronized void recordOpenedOwnerEpoch(final long ownerEpoch) {
        ensureOpen();
        if (ownerEpoch == 0 || Long.compareUnsigned(ownerEpoch, runtimeMetadata.lastOpenedOwnerEpoch()) < 0) {
            throw new IllegalArgumentException("owner epoch regressed or is zero");
        }
        persistRuntimeMetadata(new StoreRuntimeMetadata(runtimeMetadata.lastIngressFenceProofId(),
                runtimeMetadata.lastCheckpointId(), ownerEpoch, false, runtimeMetadata.evidenceCursors()));
    }

    /** Persists the complete, canonically ordered evidence cursor projection. */
    public synchronized void recordEvidenceCursors(final List<EvidenceCursorV1> cursors) {
        ensureOpen();
        persistRuntimeMetadata(new StoreRuntimeMetadata(runtimeMetadata.lastIngressFenceProofId(),
                runtimeMetadata.lastCheckpointId(), runtimeMetadata.lastOpenedOwnerEpoch(), false, cursors));
    }

    public synchronized byte[] get(final ColumnFamily family, final byte[] key) {
        ensureOpen();
        try {
            return db.get(handles.get(family), key);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("RocksDB read failed", exception);
        }
    }

    public ValueEnvelope.Decoded getValue(final ColumnFamily family, final byte[] key, final int valueType) {
        final byte[] value = get(family, key);
        return value == null ? null : ValueEnvelope.decode(value, valueType);
    }

    /** Returns a bounded snapshot of one column family in RocksDB key order. */
    public synchronized List<KeyValue> scan(final ColumnFamily family, final byte[] lowerInclusive,
                                            final byte[] upperExclusive, final int limit) {
        ensureOpen();
        Objects.requireNonNull(family, "family");
        if (limit <= 0) {
            throw new IllegalArgumentException("scan limit must be positive");
        }
        final List<KeyValue> result = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(handles.get(family))) {
            if (lowerInclusive == null) {
                iterator.seekToFirst();
            } else {
                iterator.seek(lowerInclusive);
            }
            while (iterator.isValid() && result.size() < limit) {
                final byte[] key = iterator.key();
                if (upperExclusive != null && compareUnsigned(key, upperExclusive) >= 0) {
                    break;
                }
                result.add(new KeyValue(key, iterator.value()));
                iterator.next();
            }
            iterator.status();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("RocksDB scan failed", exception);
        }
        return List.copyOf(result);
    }

    public synchronized void write(final BatchOperation operation) {
        ensureOpen();
        Objects.requireNonNull(operation, "operation");
        try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
            final Batch pending = new Batch(this, batch, handles, closedIngressDeadlineThrough, runtimeMetadata);
            operation.apply(pending);
            db.write(writeOptions, batch);
            if (pending.runtimeMetadata != null) {
                runtimeMetadata = pending.runtimeMetadata;
            }
            if (pending.recoveryMetadata != null) {
                recoveryMetadata = pending.recoveryMetadata;
            }
            try {
                closedIngressDeadlineThrough = readIngressFenceState(db, handles.get(ColumnFamily.META))
                        .closedThroughEpochMs();
            } catch (RocksDBException exception) {
                throw new RocksDbWriteFailure("cannot reread ingress fence state after write", exception);
            }
        } catch (RocksDBException exception) {
            throw new RocksDbWriteFailure("RocksDB write failed", exception);
        }
    }

    /** Flushes all column families and synchronizes the WAL before a drain/close boundary. */
    public synchronized void flushAndSync() {
        ensureOpen();
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions);
            db.syncWal();
        } catch (RocksDBException exception) {
            throw new IllegalStateException("RocksDB flush/sync failed", exception);
        }
    }

    public synchronized Path createCheckpoint(final Path checkpointPath) {
        return createCheckpoint(checkpointPath, null);
    }

    /**
     * Creates a physical checkpoint whose local runtime metadata carries the
     * exact 16-byte checkpoint identity inside the copied DB image.
     *
     * <p>The identity is written before RocksDB snapshots the files, so a
     * restored image can prove which checkpoint it represents.  If physical
     * creation fails, the previous local projection is synchronously restored;
     * a failed attempt must not leave a live DB claiming a checkpoint that was
     * never produced.</p>
     */
    public synchronized Path createCheckpoint(final Path checkpointPath, final byte[] checkpointId) {
        Objects.requireNonNull(checkpointPath, "checkpointPath");
        if (checkpointId != null) {
            Bytes.requireLength(checkpointId, 16, "checkpointId");
            boolean nonZero = false;
            for (byte value : checkpointId) {
                nonZero |= value != 0;
            }
            if (!nonZero) {
                throw new IllegalArgumentException("checkpointId must not be all zero");
            }
        }
        ensureOpen();
        final StoreRuntimeMetadata previousMetadata = runtimeMetadata;
        if (checkpointId != null) {
            recordLastCheckpointId(checkpointId);
        }
        boolean slotAcquired = false;
        Path temporary = null;
        Path installedTarget = null;
        try {
            resources.acquireCheckpointCreateSlot();
            slotAcquired = true;
            if (Files.exists(checkpointPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                    || Files.isSymbolicLink(checkpointPath)) {
                throw new IOException("checkpoint target already exists: " + checkpointPath);
            }
            final Path absoluteTarget = checkpointPath.toAbsolutePath();
            final Path parent = absoluteTarget.getParent();
            if (parent == null) {
                throw new IOException("checkpoint target has no parent: " + checkpointPath);
            }
            Files.createDirectories(parent);
            final Path stagingRoot = parent.resolve("checkpoint-tmp");
            ensureRealDirectory(parent);
            ensureRealDirectory(stagingRoot);
            temporary = stagingRoot.resolve(UUID.randomUUID().toString());
            try (Checkpoint checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(temporary.toString());
            }
            forceDirectory(temporary);
            try {
                Files.move(temporary, absoluteTarget, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.FileAlreadyExistsException exception) {
                throw new IOException("checkpoint target appeared during creation: " + absoluteTarget, exception);
            }
            installedTarget = absoluteTarget;
            temporary = null;
            forceDirectory(parent);
            return checkpointPath;
        } catch (RocksDBException | IOException | RuntimeException exception) {
            if (installedTarget != null) {
                try {
                    deleteTree(installedTarget);
                } catch (IOException cleanupException) {
                    exception.addSuppressed(cleanupException);
                }
            }
            if (checkpointId != null) {
                try {
                    persistRuntimeMetadata(previousMetadata);
                } catch (RuntimeException rollbackFailure) {
                    exception.addSuppressed(rollbackFailure);
                }
            }
            throw new IllegalStateException("cannot create RocksDB checkpoint", exception);
        } finally {
            if (temporary != null) {
                try {
                    deleteTree(temporary);
                } catch (IOException cleanupException) {
                    // Preserve the creation failure; an orphan is still kept
                    // under the bounded checkpoint-tmp namespace for repair.
                }
            }
            if (slotAcquired) {
                resources.releaseCheckpointCreateSlot();
            }
        }
    }

    private static void forceDirectory(final Path directory) throws IOException {
        if (Files.isSymbolicLink(directory)
                || !Files.isDirectory(directory, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("directory must be a real directory: " + directory);
        }
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    public synchronized long latestSequenceNumber() {
        ensureOpen();
        return db.getLatestSequenceNumber();
    }

    @Override
    public synchronized void close() {
        if (closed.get()) {
            return;
        }
        RuntimeException closeFailure = null;
        if (!cleanCloseAttempted) {
            cleanCloseAttempted = true;
            try {
                // Keep the store open while the clean-close marker is written
                // so the lifecycle guard on write() does not reject its own
                // close protocol. The marker is attempted at most once: if it
                // fails, the native DB may still be torn down safely and the
                // next open will conservatively treat the store as unclean.
                final StoreRuntimeMetadata cleanRuntime = runtimeMetadata.withCleanCloseMarker(true);
                final StoreRecoveryMetadata cleanRecovery = recoveryMetadata.withInstallState(
                        new RecoveryInstallStateV1(RecoveryInstallPhaseV1.CLOSED_CLEAN,
                                metadata.storeIncarnation(), recoveryMetadata.checkpointId()));
                write(batch -> {
                    batch.putRuntimeMetadata(cleanRuntime);
                    batch.putRecoveryMetadata(cleanRecovery);
                });
            } catch (RuntimeException exception) {
                closeFailure = appendCloseFailure(closeFailure, exception);
            }
        }
        // The marker write above is the final allowed Store operation. Once
        // it has been attempted, fence all public operations even if native
        // teardown below needs a later retry.
        closeStarted = true;
        // Every native close and every worker-slot release is attempted even
        // when an earlier JNI close reports a runtime failure.  Losing the
        // release in that path would permanently consume maxOpenShardDbs or
        // maxOwnedShards and make a healthy worker reject future ownership.
        resources.unregisterPhysicalUsage(shardId, physicalUsageSource);
        if (!defaultColumnFamilyClosed) {
            try {
                defaultColumnFamilyHandle.close();
                defaultColumnFamilyClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        for (ColumnFamily family : ColumnFamily.values()) {
            if (closedColumnFamilyHandles.contains(family)) {
                continue;
            }
            try {
                handles.get(family).close();
                closedColumnFamilyHandles.add(family);
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (!dbClosed) {
            try {
                db.close();
                dbClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        for (int index = 0; index < columnFamilyOptions.size(); index++) {
            if (closedColumnFamilyOptions[index]) {
                continue;
            }
            try {
                columnFamilyOptions.get(index).close();
                closedColumnFamilyOptions[index] = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        if (!dbOptionsClosed) {
            try {
                dbOptions.close();
                dbOptionsClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = appendCloseFailure(closeFailure, failure);
            }
        }
        // A DB slot represents a live native DB, so do not release it while
        // any handle/options teardown is still unfinished. Otherwise a
        // shared-resource close could pass its in-flight check and destroy
        // the cache/rate limiter while this Store still owns a native handle.
        if (nativeTeardownComplete()) {
            if (!dbSlotReleased) {
                try {
                    resources.releaseDbSlot();
                    dbSlotReleased = true;
                } catch (RuntimeException failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
            if (dbSlotReleased && ownsShardSlot && !ownedShardSlotReleased) {
                try {
                    resources.releaseOwnedShardSlot(shardId);
                    ownedShardSlotReleased = true;
                } catch (RuntimeException failure) {
                    closeFailure = appendCloseFailure(closeFailure, failure);
                }
            }
        }
        if (closeFailure == null && defaultColumnFamilyClosed && dbClosed && dbOptionsClosed
                && dbSlotReleased && (!ownsShardSlot || ownedShardSlotReleased)
                && closedColumnFamilyHandles.size() == ColumnFamily.values().length
                && allOptionsClosed()) {
            closed.set(true);
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private boolean allOptionsClosed() {
        for (boolean closedOption : closedColumnFamilyOptions) {
            if (!closedOption) {
                return false;
            }
        }
        return true;
    }

    private boolean nativeTeardownComplete() {
        return defaultColumnFamilyClosed && dbClosed && dbOptionsClosed
                && closedColumnFamilyHandles.size() == ColumnFamily.values().length && allOptionsClosed();
    }

    private static RuntimeException appendCloseFailure(final RuntimeException first,
                                                       final RuntimeException failure) {
        if (failure == null) {
            return first;
        }
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private void ensureOpen() {
        if (closed.get() || closeStarted) {
            throw new IllegalStateException("shard store is closed");
        }
    }

    private void persistRuntimeMetadata(final StoreRuntimeMetadata next) {
        write(batch -> batch.putRuntimeMetadata(next));
    }

    @FunctionalInterface
    public interface BatchOperation {
        void apply(Batch batch) throws RocksDBException;
    }

    public record KeyValue(byte[] key, byte[] value) {
        public KeyValue {
            key = Bytes.copy(key);
            value = Bytes.copy(value);
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] value() {
            return Bytes.copy(value);
        }
    }

    public static final class Batch {
        private final ShardStore owner;
        private final WriteBatch batch;
        private final Map<ColumnFamily, ColumnFamilyHandle> handles;
        private final StoreRuntimeMetadata currentRuntimeMetadata;
        private StoreRuntimeMetadata runtimeMetadata;
        private StoreRecoveryMetadata recoveryMetadata;
        private long closedIngressDeadlineThrough;

        private Batch(final ShardStore owner, final WriteBatch batch,
                      final Map<ColumnFamily, ColumnFamilyHandle> handles,
                      final long closedIngressDeadlineThrough,
                      final StoreRuntimeMetadata currentRuntimeMetadata) {
            this.owner = Objects.requireNonNull(owner, "owner");
            this.batch = batch;
            this.handles = handles;
            this.closedIngressDeadlineThrough = closedIngressDeadlineThrough;
            this.currentRuntimeMetadata = currentRuntimeMetadata;
        }

        /** Returns whether this batch belongs to the supplied open ShardStore. */
        boolean belongsTo(final ShardStore candidate) {
            return owner == candidate;
        }

        public void put(final ColumnFamily family, final byte[] key, final byte[] value) throws RocksDBException {
            batch.put(handle(family), key, value);
        }

        public void putValue(final ColumnFamily family, final int valueType, final byte[] key,
                             final byte[] payload) throws RocksDBException {
            put(family, key, ValueEnvelope.encode(valueType, payload));
        }

        /** Adds the Store runtime projection to this same atomic WriteBatch. */
        public void putRuntimeMetadata(final StoreRuntimeMetadata next) throws RocksDBException {
            Objects.requireNonNull(next, "next");
            if (runtimeMetadata != null) {
                throw new IllegalStateException("Store runtime metadata may be written once per batch");
            }
            ShardStore.putRuntimeMetadata(batch, handle(ColumnFamily.META), next, closedIngressDeadlineThrough);
            runtimeMetadata = next;
        }

        /** Adds the Store recovery projection to this same atomic WriteBatch. */
        public void putRecoveryMetadata(final StoreRecoveryMetadata next) throws RocksDBException {
            Objects.requireNonNull(next, "next");
            if (recoveryMetadata != null) {
                throw new IllegalStateException("Store recovery metadata may be written once per batch");
            }
            ShardStore.putRecoveryMetadata(batch, handle(ColumnFamily.META), next);
            recoveryMetadata = next;
        }

        /** Advances the source-ordered ingress fence in the same atomic WriteBatch. */
        public void putIngressFenceDeadline(final long closeThroughEpochMs) throws RocksDBException {
            if (closeThroughEpochMs < 0) {
                throw new IllegalArgumentException("closeThroughEpochMs must be non-negative");
            }
            if (closeThroughEpochMs < closedIngressDeadlineThrough) {
                throw new IllegalArgumentException("ingress fence deadline regressed");
            }
            closedIngressDeadlineThrough = closeThroughEpochMs;
            final byte[] proofId = runtimeMetadata == null
                    ? currentRuntimeMetadata.lastIngressFenceProofId() : runtimeMetadata.lastIngressFenceProofId();
            final IngressFenceState state = new IngressFenceState(closedIngressDeadlineThrough, proofId);
            batch.put(handle(ColumnFamily.META), KeyCodec.metaFixed(META_INGRESS_FENCE_STATE),
                    ValueEnvelope.encode(META_FIXED_VALUE_TYPE, state.canonicalBytes()));
        }

        public void delete(final ColumnFamily family, final byte[] key) throws RocksDBException {
            batch.delete(handle(family), key);
        }

        private ColumnFamilyHandle handle(final ColumnFamily family) {
            return handles.get(Objects.requireNonNull(family, "family"));
        }
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Integer.compare(Byte.toUnsignedInt(left[index]), Byte.toUnsignedInt(right[index]));
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
