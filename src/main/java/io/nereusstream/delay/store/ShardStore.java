package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ShardId;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.Checkpoint;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** One independent RocksDB instance for exactly one Delay Shard. */
public final class ShardStore implements AutoCloseable {
    private static final int ACTIVE_MAGIC = 0x41435431;
    private static final int META_STORE_FORMAT = 1;
    private static final int META_SHARD_IDENTITY = 2;

    static {
        RocksDbNativeLoader.load();
    }

    private final ShardStoreConfig config;
    private final ShardId shardId;
    private final Path dbPath;
    private final SharedRocksDbResources resources;
    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyOptions> columnFamilyOptions;
    private final Map<ColumnFamily, ColumnFamilyHandle> handles;
    private final StoreMetadata metadata;
    private final boolean ownsShardSlot;
    private final AtomicBoolean closed = new AtomicBoolean();

    private ShardStore(final ShardStoreConfig config, final ShardId shardId, final Path dbPath,
                        final SharedRocksDbResources resources, final RocksDB db, final DBOptions dbOptions,
                        final List<ColumnFamilyOptions> columnFamilyOptions,
                        final Map<ColumnFamily, ColumnFamilyHandle> handles, final StoreMetadata metadata,
                        final boolean ownsShardSlot) {
        this.config = config;
        this.shardId = shardId;
        this.dbPath = dbPath;
        this.resources = resources;
        this.db = db;
        this.dbOptions = dbOptions;
        this.columnFamilyOptions = columnFamilyOptions;
        this.handles = handles;
        this.metadata = metadata;
        this.ownsShardSlot = ownsShardSlot;
    }

    public static ShardStore open(final ShardStoreConfig config, final ShardId shardId,
                                  final SharedRocksDbResources resources) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(resources, "resources");
        try {
            final Path shardRoot = config.rootPath().resolve("shards")
                    .resolve(shardId.routeIncarnation().uuid().toString())
                    .resolve(Integer.toString(shardId.partition()));
            Files.createDirectories(shardRoot);
            final Path dbPath = locateOrCreateDbPath(shardRoot);
            final ShardStore opened = openAtPath(config, shardId, dbPath, resources, null, true);
            try {
                writeActivePointer(shardRoot, storeUuidFromPath(dbPath));
                return opened;
            } catch (IOException exception) {
                opened.close();
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
        return restoreFromCheckpoint(config, shardId, resources, checkpointPath, null);
    }

    public static ShardStore restoreFromCheckpoint(final ShardStoreConfig config, final ShardId shardId,
                                                   final SharedRocksDbResources resources,
                                                   final Path checkpointPath, final CheckpointManifest manifest) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(resources, "resources");
        Objects.requireNonNull(checkpointPath, "checkpointPath");
        final Path shardRoot = config.rootPath().resolve("shards")
                .resolve(shardId.routeIncarnation().uuid().toString())
                .resolve(Integer.toString(shardId.partition()));
        final UUID storeUuid = UUID.randomUUID();
        final Path restoreRoot = shardRoot.resolve("restore-tmp").resolve(storeUuid.toString());
        final Path stagedDb = restoreRoot.resolve("db");
        final Path activeDb = shardRoot.resolve("incarnations").resolve(storeUuid.toString()).resolve("db");
        boolean downloadSlotAcquired = false;
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
                validateCheckpointManifest(shardId, checkpointPath, manifest);
            }
            Files.createDirectories(shardRoot);
            if (hasActiveDb(shardRoot)) {
                throw new IOException("cannot restore while an active shard DB exists: " + shardRoot);
            }
            copyTree(checkpointPath, stagedDb);
            try (ShardStore staged = openAtPath(config, shardId, stagedDb, resources, null, false)) {
                if (!staged.shardId().equals(shardId)) {
                    throw new IOException("restored DB shard identity mismatch");
                }
                if (manifest != null && (!java.util.Arrays.equals(manifest.dbIdentity(), staged.metadata().dbIdentity())
                        || !manifest.sourceStoreIncarnation().equals(staged.metadata().storeIncarnationUuid()))) {
                    throw new IOException("restored DB metadata does not match checkpoint manifest");
                }
            }
            try (ShardStore installed = openAtPath(config, shardId, stagedDb, resources, storeUuid, false)) {
                if (!installed.shardId().equals(shardId)) {
                    throw new IOException("install-mode DB shard identity mismatch");
                }
            }
            Files.createDirectories(activeDb.getParent());
            Files.move(stagedDb, activeDb, StandardCopyOption.ATOMIC_MOVE);
            writeActivePointer(shardRoot, storeUuid);
            deleteTree(restoreRoot);
            return openAtPath(config, shardId, activeDb, resources, null, true);
        } catch (IOException | RocksDBException exception) {
            try {
                deleteTree(restoreRoot);
            } catch (IOException cleanupException) {
                exception.addSuppressed(cleanupException);
            }
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
            try {
                deleteTree(restoreRoot);
            } catch (IOException cleanupException) {
                exception.addSuppressed(cleanupException);
            }
            throw new IllegalStateException("cannot restore shard checkpoint", exception);
        } finally {
            if (downloadSlotAcquired) {
                resources.releaseCheckpointDownloadSlot();
            }
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
        Objects.requireNonNull(catalog, "catalog");
        catalog.validatePublishedRestoreCandidate(Objects.requireNonNull(manifest, "manifest"));
        return restoreFromCheckpoint(config, shardId, resources, checkpointPath, manifest);
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

    private static void validateCheckpointManifest(final ShardId shardId, final Path checkpointPath,
                                                   final CheckpointManifest manifest) throws IOException {
        if (!manifest.shardId().equals(shardId) || manifest.storeFormatVersion() != META_STORE_FORMAT) {
            throw new IOException("checkpoint manifest shard or format mismatch");
        }
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpointPath);
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
                || Files.isSymbolicLink(activeDb)) {
            throw new IOException("active shard path must not contain a symbolic link: " + activeDb);
        }
        return Files.isRegularFile(activeDb.resolve("CURRENT"), java.nio.file.LinkOption.NOFOLLOW_LINKS);
    }

    private static UUID storeUuidFromPath(final Path dbPath) throws IOException {
        try {
            return UUID.fromString(dbPath.getParent().getFileName().toString());
        } catch (IllegalArgumentException exception) {
            throw new IOException("DB path does not carry a valid Store Incarnation: " + dbPath, exception);
        }
    }

    private static void writeActivePointer(final Path shardRoot, final UUID storeUuid) throws IOException {
        Files.createDirectories(shardRoot.resolve("incarnations"));
        final byte[] body = java.nio.ByteBuffer.allocate(4 + 16)
                .putInt(ACTIVE_MAGIC).put(uuidBytes(storeUuid)).array();
        final byte[] encoded = Bytes.concat(body, Bytes.crc32cbe(body));
        final Path temporary = shardRoot.resolve("ACTIVE.tmp");
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
        boolean ownedSlotAcquired = false;
        boolean dbSlotAcquired = false;
        try {
            if (acquireOwnedSlot) {
                resources.acquireOwnedShardSlot();
                ownedSlotAcquired = true;
            }
            resources.acquireDbSlot();
            dbSlotAcquired = true;
            return openAtPathWithSlot(config, shardId, dbPath, resources, restoreStoreIncarnation,
                    acquireOwnedSlot);
        } catch (IOException | RocksDBException | RuntimeException exception) {
            // The DB slot is acquired after the owned slot.  Release only the
            // slots that this invocation actually acquired.
            if (dbSlotAcquired) {
                resources.releaseDbSlot();
            }
            if (ownedSlotAcquired) {
                resources.releaseOwnedShardSlot();
            }
            throw exception;
        }
    }

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
                .setMaxBackgroundJobs(config.maxBackgroundJobs())
                .setWriteBufferManager(resources.writeBufferManager())
                .setRateLimiter(resources.rateLimiter());
        final List<ColumnFamilyHandle> openedHandles = new ArrayList<>();
        final RocksDB db;
        try {
            db = RocksDB.open(dbOptions, dbPath.toString(), descriptors, openedHandles);
        } catch (RocksDBException exception) {
            closeQuietly(cfOptions);
            dbOptions.close();
            throw exception;
        }
        boolean keepOpen = false;
        try {
            final Map<ColumnFamily, ColumnFamilyHandle> handles = new EnumMap<>(ColumnFamily.class);
            for (int index = 0; index < ColumnFamily.values().length; index++) {
                handles.put(ColumnFamily.values()[index], openedHandles.get(index + 1));
            }
            if (hasDefaultColumnFamilyData(db, openedHandles.get(0))) {
                throw new IllegalStateException("default RocksDB column family must remain empty");
            }
            final byte[] identityBytes = db.get(handles.get(ColumnFamily.META),
                    KeyCodec.metaFixed(META_SHARD_IDENTITY));
            StoreMetadata metadata;
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
                                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()))));
                try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
                    batch.put(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_STORE_FORMAT),
                            java.nio.ByteBuffer.allocate(4).putInt(1).array());
                    batch.put(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_SHARD_IDENTITY),
                            created.encode());
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
                            restored.encode());
                    db.write(writeOptions, batch);
                }
                metadata = restored;
            }
            final byte[] format = db.get(handles.get(ColumnFamily.META), KeyCodec.metaFixed(META_STORE_FORMAT));
            if (format == null || format.length != 4 || java.nio.ByteBuffer.wrap(format).getInt() != 1) {
                throw new IllegalStateException("missing or unsupported store format marker");
            }
            final ShardStore result = new ShardStore(config, shardId, dbPath, resources, db, dbOptions, cfOptions,
                    handles, metadata, ownsShardSlot);
            keepOpen = true;
            return result;
        } finally {
            if (!keepOpen) {
                closeHandles(db, openedHandles, cfOptions, dbOptions);
            }
        }
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
        try (var paths = Files.walk(source)) {
            for (Path path : paths.toList()) {
                final Path destination = target.resolve(source.relativize(path).toString());
                if (Files.isSymbolicLink(path)) {
                    throw new IOException("checkpoint contains a symbolic link: " + path);
                }
                if (Files.isDirectory(path)) {
                    Files.createDirectories(destination);
                } else if (Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(destination.getParent());
                    Files.copy(path, destination);
                }
            }
        }
    }

    private static void deleteTree(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
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
        result.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions(resources)));
        options.add(result.get(0).getOptions());
        for (ColumnFamily family : ColumnFamily.values()) {
            final ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(
                    family.rocksName().getBytes(java.nio.charset.StandardCharsets.UTF_8), familyOptions(resources));
            result.add(descriptor);
            options.add(descriptor.getOptions());
        }
        return result;
    }

    private static ColumnFamilyOptions familyOptions(final SharedRocksDbResources resources) {
        final BlockBasedTableConfig table = new BlockBasedTableConfig().setBlockCache(resources.blockCache());
        return new ColumnFamilyOptions().setTableFormatConfig(table);
    }

    private static void closeQuietly(final List<ColumnFamilyOptions> options) {
        for (ColumnFamilyOptions option : options) {
            option.close();
        }
    }

    private static void closeHandles(final RocksDB db, final List<ColumnFamilyHandle> handles,
                                     final List<ColumnFamilyOptions> options, final DBOptions dbOptions) {
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        db.close();
        closeQuietly(options);
        dbOptions.close();
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

    public byte[] get(final ColumnFamily family, final byte[] key) {
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
    public List<KeyValue> scan(final ColumnFamily family, final byte[] lowerInclusive, final byte[] upperExclusive,
                               final int limit) {
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

    public void write(final BatchOperation operation) {
        Objects.requireNonNull(operation, "operation");
        try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(true)) {
            operation.apply(new Batch(batch, handles));
            db.write(writeOptions, batch);
        } catch (RocksDBException exception) {
            throw new IllegalStateException("RocksDB write failed", exception);
        }
    }

    public Path createCheckpoint(final Path checkpointPath) {
        Objects.requireNonNull(checkpointPath, "checkpointPath");
        resources.acquireCheckpointCreateSlot();
        Path temporary = null;
        try {
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
            Files.createDirectories(stagingRoot);
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
            temporary = null;
            forceDirectory(parent);
            return checkpointPath;
        } catch (RocksDBException | IOException exception) {
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
            resources.releaseCheckpointCreateSlot();
        }
    }

    private static void forceDirectory(final Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    public long latestSequenceNumber() {
        return db.getLatestSequenceNumber();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (ColumnFamilyHandle handle : handles.values()) {
                handle.close();
            }
            db.close();
            closeQuietly(columnFamilyOptions);
            dbOptions.close();
            resources.releaseDbSlot();
            if (ownsShardSlot) {
                resources.releaseOwnedShardSlot();
            }
        }
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
        private final WriteBatch batch;
        private final Map<ColumnFamily, ColumnFamilyHandle> handles;

        private Batch(final WriteBatch batch, final Map<ColumnFamily, ColumnFamilyHandle> handles) {
            this.batch = batch;
            this.handles = handles;
        }

        public void put(final ColumnFamily family, final byte[] key, final byte[] value) throws RocksDBException {
            batch.put(handle(family), key, value);
        }

        public void putValue(final ColumnFamily family, final int valueType, final byte[] key,
                             final byte[] payload) throws RocksDBException {
            put(family, key, ValueEnvelope.encode(valueType, payload));
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
