package io.nereusstream.delay.store;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durable two-process evidence for local RocksDB storage failure boundaries.
 *
 * <p>The test is opt-in because each phase intentionally leaves a persistent
 * artifact for the next JVM. The shell child supplies the cell and phase and
 * independently audits the resulting before/after dumps.</p>
 */
class LocalStorageDurableChaosTest {
    private static final String ARTIFACT_ENV = "NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR";
    private static final String CELL_ENV = "NEREUS_DELAY_STORAGE_CHAOS_CELL";
    private static final String PHASE_ENV = "NEREUS_DELAY_STORAGE_CHAOS_PHASE";
    private static final String HOLD_ENV = "NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE";
    private static final String ROOT_ENV = "NEREUS_DELAY_STORAGE_CHAOS_ROOT";
    private static final String HEADROOM_ENV = "NEREUS_DELAY_STORAGE_CHAOS_HEADROOM_FILE";
    private static final String SCHEMA = "nereus-delay-storage-chaos-durable-state-dump-v1";
    private static final String BEFORE_PHASE = "BEFORE_FRESH_PROCESS_RECOVERY";
    private static final String AFTER_PHASE = "RECOVERED_AFTER_FRESH_PROCESS";
    private static final String FSYNC_CELL = "fsync-error";
    private static final String SST_CELL = "sst-corruption";
    private static final String DISASTER_CELL = "disaster-host-fault";
    private static final String ENOSPC_CELL = "enospc";
    private static final String FSYNC_FAULT = "FSYNC_ERROR";
    private static final String SST_FAULT = "SST_CORRUPTION";
    private static final String DISASTER_FAULT = "DISASTER_HOST_FAULT";
    private static final String ENOSPC_FAULT = "ENOSPC";
    private static final String VALUE_TEXT = "durable-storage-chaos-value-v1";
    private static final RouteIncarnation ROUTE = RouteIncarnation.fromUuid(
            UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
    private static final ShardId SHARD = new ShardId(ROUTE, 17);
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    @Test
    void localStorageFailureSurvivesFreshProcessRecovery() throws Exception {
        final String cell = System.getenv(CELL_ENV);
        final String phase = System.getenv(PHASE_ENV);
        Assumptions.assumeTrue(cell != null && !cell.isBlank()
                        && phase != null && !phase.isBlank(),
                "local storage chaos is opt-in");
        final Path artifact = artifactDirectory();
        Files.createDirectories(artifact);
        switch (cell) {
            case FSYNC_CELL -> runFsync(artifact, phase);
            case SST_CELL -> runSstCorruption(artifact, phase);
            case DISASTER_CELL -> runDisaster(artifact, phase);
            case ENOSPC_CELL -> runEnospc(artifact, phase);
            default -> throw new IllegalArgumentException("unsupported local storage chaos cell: " + cell);
        }
    }

    private static void runFsync(final Path artifact, final String phase) throws Exception {
        switch (phase) {
            case "before" -> writeFsyncBefore(artifact);
            case "after" -> writeFsyncAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported fsync phase: " + phase);
        }
    }

    private static void writeFsyncBefore(final Path artifact) throws Exception {
        final Path storeRoot = artifact.resolve("store");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final byte[] key = key();
        final byte[] value = value();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key, value));
            final ValueEnvelope.Decoded written = store.getValue(ColumnFamily.META, key, 3);
            assertNotNull(written);
            assertArrayEquals(value, written.payload());
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> store.flushAndSync(() -> {
                        throw new RocksDBException("synthetic directory fsync failure");
                    }));
            assertEquals("RocksDB flush/sync failed", failure.getMessage());
            assertTrue(store.isWriteOutcomeUncertain());
            assertThrows(IllegalStateException.class, () -> store.getValue(ColumnFamily.META, key, 3));

            final JsonObject dump = commonDump(FSYNC_CELL, BEFORE_PHASE, FSYNC_FAULT,
                    storeRoot, true, "FSYNC_FAILURE_FENCED_STORE_REOPEN_REQUIRED");
            dump.addProperty("flush_sync_failure_observed", true);
            dump.addProperty("write_outcome_uncertain", store.isWriteOutcomeUncertain());
            dump.addProperty("value_written_before_fault", true);
            writeJson(artifact.resolve("before.json"), dump);
        }
    }

    private static void writeFsyncAfter(final Path artifact) throws Exception {
        final Path storeRoot = artifact.resolve("store");
        final JsonObject before = readJson(artifact.resolve("before.json"));
        requireBefore(before, FSYNC_CELL, FSYNC_FAULT);
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            final ValueEnvelope.Decoded recovered = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(recovered);
            assertArrayEquals(value(), recovered.payload());
            store.flushAndSync();
            final JsonObject dump = commonDump(FSYNC_CELL, AFTER_PHASE, FSYNC_FAULT,
                    storeRoot, true, "FRESH_PROCESS_REOPENED_AFTER_FSYNC_FAILURE");
            dump.addProperty("flush_sync_failure_observed", true);
            dump.addProperty("write_outcome_uncertain", false);
            dump.addProperty("value_recovered_exactly", true);
            requireDifferentProcess(before, dump);
            writeJson(artifact.resolve("after.json"), dump);
        }
    }

    private static void runSstCorruption(final Path artifact, final String phase) throws Exception {
        switch (phase) {
            case "before" -> writeSstBefore(artifact);
            case "after" -> writeSstAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported SST phase: " + phase);
        }
    }

    private static void writeSstBefore(final Path artifact) throws Exception {
        final Path storeRoot = artifact.resolve("source-store");
        final Path checkpoint = artifact.resolve("clean-checkpoint");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key(), value()));
            store.flushAndSync();
            final ValueEnvelope.Decoded written = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(written);
            assertArrayEquals(value(), written.payload());
            store.createCheckpoint(checkpoint);
            final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(checkpoint);
            assertFalse(inventory.isEmpty());
            final JsonObject dump = commonDump(SST_CELL, BEFORE_PHASE, SST_FAULT,
                    storeRoot, true, "CLEAN_CHECKPOINT_PUBLISHED_BEFORE_COPIED_SST_CORRUPTION");
            dump.addProperty("clean_checkpoint_present", true);
            dump.addProperty("clean_checkpoint_inventory_sha256", inventoryDigest(inventory));
            dump.addProperty("clean_checkpoint_file_count", inventory.size());
            dump.addProperty("value_in_source_store", true);
            writeJson(artifact.resolve("before.json"), dump);
        }
    }

    private static void writeSstAfter(final Path artifact) throws Exception {
        final JsonObject before = readJson(artifact.resolve("before.json"));
        requireBefore(before, SST_CELL, SST_FAULT);
        final Path cleanCheckpoint = artifact.resolve("clean-checkpoint");
        final Path corruptCheckpoint = artifact.resolve("corrupt-checkpoint");
        copyTree(cleanCheckpoint, corruptCheckpoint);
        final Path sst = firstSst(corruptCheckpoint);
        flipFirstByte(sst);
        final List<CheckpointFileInventory> corruptedInventory = CheckpointFileInventory.collect(corruptCheckpoint);
        final String cleanDigest = before.get("clean_checkpoint_inventory_sha256").getAsString();
        final String corruptedDigest = inventoryDigest(corruptedInventory);
        assertNotEquals(cleanDigest, corruptedDigest);

        boolean corruptionRejected = false;
        final Path corruptRestoreRoot = artifact.resolve("corrupt-restore");
        final ShardStoreConfig corruptConfig = ShardStoreConfig.defaults(corruptRestoreRoot);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(corruptConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(corruptConfig, SHARD, resources,
                     corruptCheckpoint)) {
            final ValueEnvelope.Decoded decoded = restored.getValue(ColumnFamily.META, key(), 3);
            if (decoded == null || !Arrays.equals(value(), decoded.payload())) {
                corruptionRejected = true;
            }
        } catch (RuntimeException rejected) {
            corruptionRejected = true;
        }
        assertTrue(corruptionRejected, "corrupted SST must be rejected by restore or exact read");

        final Path restoreRoot = artifact.resolve("clean-restore");
        final ShardStoreConfig restoreConfig = ShardStoreConfig.defaults(restoreRoot);
        boolean exactRecovery;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(restoreConfig);
             ShardStore restored = ShardStore.restoreFromCheckpoint(restoreConfig, SHARD, resources,
                     cleanCheckpoint)) {
            final ValueEnvelope.Decoded decoded = restored.getValue(ColumnFamily.META, key(), 3);
            exactRecovery = decoded != null && Arrays.equals(value(), decoded.payload());
            assertTrue(exactRecovery);
        }
        final JsonObject dump = commonDump(SST_CELL, AFTER_PHASE, SST_FAULT,
                restoreRoot, exactRecovery, "FRESH_PROCESS_REJECTED_CORRUPT_SST_AND_RESTORED_CLEAN_CHECKPOINT");
        dump.addProperty("clean_checkpoint_inventory_sha256", cleanDigest);
        dump.addProperty("corrupt_checkpoint_inventory_sha256", corruptedDigest);
        dump.addProperty("corruption_rejected", corruptionRejected);
        dump.addProperty("clean_restore_exact", exactRecovery);
        requireDifferentProcess(before, dump);
        writeJson(artifact.resolve("after.json"), dump);
    }

    private static void runDisaster(final Path artifact, final String phase) throws Exception {
        switch (phase) {
            case "before" -> writeDisasterBefore(artifact);
            case "after" -> writeDisasterAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported disaster phase: " + phase);
        }
    }

    private static void writeDisasterBefore(final Path artifact) throws Exception {
        final Path storeRoot = artifact.resolve("store");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final Path hold = requiredPath(HOLD_ENV);
        Files.createDirectories(hold.getParent());
        if (!Files.exists(hold, LinkOption.NOFOLLOW_LINKS)) {
            writeTextAtomically(hold, "hold\n");
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key(), value()));
            store.flushAndSync();
            final ValueEnvelope.Decoded durable = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(durable);
            assertArrayEquals(value(), durable.payload());
            final JsonObject dump = commonDump(DISASTER_CELL, BEFORE_PHASE, DISASTER_FAULT,
                    storeRoot, true, "DURABLE_STORE_READ_BEFORE_HOST_PROCESS_TERMINATION");
            dump.addProperty("host_fault_pending", true);
            dump.addProperty("store_left_open_for_host_fault", true);
            writeJson(artifact.resolve("before.json"), dump);
            writeTextAtomically(artifact.resolve("ready"), "ready\n");
            while (Files.exists(hold, LinkOption.NOFOLLOW_LINKS)) {
                Thread.sleep(100L);
            }
        }
    }

    private static void writeDisasterAfter(final Path artifact) throws Exception {
        final JsonObject before = readJson(artifact.resolve("before.json"));
        requireBefore(before, DISASTER_CELL, DISASTER_FAULT);
        final Path storeRoot = artifact.resolve("store");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            final ValueEnvelope.Decoded recovered = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(recovered);
            assertArrayEquals(value(), recovered.payload());
            store.flushAndSync();
            final JsonObject dump = commonDump(DISASTER_CELL, AFTER_PHASE, DISASTER_FAULT,
                    storeRoot, true, "FRESH_PROCESS_REOPENED_AFTER_SIGKILL_AND_REPLAYED_DURABLE_STORE");
            dump.addProperty("host_fault_pending", false);
            dump.addProperty("host_fault_signal", "SIGKILL");
            dump.addProperty("value_recovered_exactly", true);
            requireDifferentProcess(before, dump);
            writeJson(artifact.resolve("after.json"), dump);
        }
    }

    private static void runEnospc(final Path artifact, final String phase) throws Exception {
        switch (phase) {
            case "before" -> writeEnospcBefore(artifact);
            case "after" -> writeEnospcAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported ENOSPC phase: " + phase);
        }
    }

    private static void writeEnospcBefore(final Path artifact) throws Exception {
        final Path storeRoot = requiredPath(ROOT_ENV);
        final Path headroom = requiredPath(HEADROOM_ENV);
        assertTrue(Files.isRegularFile(headroom, LinkOption.NOFOLLOW_LINKS));
        Files.createDirectories(storeRoot);
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final long filesystemTotal = Files.getFileStore(storeRoot).getTotalSpace();
        final long filesystemUsableBefore = Files.getFileStore(storeRoot).getUsableSpace();
        int attemptedFillerRecords = 0;
        RuntimeException noSpaceFailure = null;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key(), value()));
            store.flushAndSync();
            final ValueEnvelope.Decoded durable = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(durable);
            assertArrayEquals(value(), durable.payload());

            for (int index = 0; index < 256; index++) {
                attemptedFillerRecords++;
                final byte[] fillerKey = Bytes.utf8("enospc-filler-" + index);
                final byte[] filler = new byte[1024 * 1024];
                new Random(0x5eed_0000L + index).nextBytes(filler);
                try {
                    store.write(batch -> batch.putValue(ColumnFamily.META, 3, fillerKey, filler));
                } catch (RuntimeException failure) {
                    if (!isNoSpace(failure)) {
                        throw failure;
                    }
                    noSpaceFailure = failure;
                    break;
                }
            }
            assertNotNull(noSpaceFailure, "the bounded filesystem fixture must return ENOSPC");
            assertTrue(store.isWriteOutcomeUncertain());
            final JsonObject dump = commonDump(ENOSPC_CELL, BEFORE_PHASE, ENOSPC_FAULT,
                    storeRoot, true, "ENOSPC_FENCED_STORE_AFTER_DURABLE_KEY");
            dump.addProperty("enospc_observed", true);
            dump.addProperty("enospc_status", "NO_SPACE_LEFT_ON_DEVICE");
            dump.addProperty("headroom_file_present", true);
            dump.addProperty("filler_records_attempted", attemptedFillerRecords);
            dump.addProperty("filesystem_total_bytes", filesystemTotal);
            dump.addProperty("filesystem_usable_before_bytes", filesystemUsableBefore);
            dump.addProperty("write_outcome_uncertain", store.isWriteOutcomeUncertain());
            writeJson(artifact.resolve("before.json"), dump);
        }
    }

    private static void writeEnospcAfter(final Path artifact) throws Exception {
        final JsonObject before = readJson(artifact.resolve("before.json"));
        requireBefore(before, ENOSPC_CELL, ENOSPC_FAULT);
        final Path storeRoot = requiredPath(ROOT_ENV);
        final Path headroom = requiredPath(HEADROOM_ENV);
        assertFalse(Files.exists(headroom, LinkOption.NOFOLLOW_LINKS));
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, SHARD, resources)) {
            final ValueEnvelope.Decoded recovered = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(recovered);
            assertArrayEquals(value(), recovered.payload());
            store.flushAndSync();
            final JsonObject dump = commonDump(ENOSPC_CELL, AFTER_PHASE, ENOSPC_FAULT,
                    storeRoot, true, "FRESH_PROCESS_REOPENED_AFTER_ENOSPC_AND_HEADROOM_RELEASE");
            dump.addProperty("enospc_recovered", true);
            dump.addProperty("space_released_before_recovery", true);
            dump.addProperty("value_recovered_exactly", true);
            dump.addProperty("write_outcome_uncertain", false);
            requireDifferentProcess(before, dump);
            writeJson(artifact.resolve("after.json"), dump);
        }
    }

    private static JsonObject commonDump(final String cell, final String phase, final String fault,
                                         final Path storeRoot, final boolean durableRead,
                                         final String recoveryAction) {
        final JsonObject dump = new JsonObject();
        dump.addProperty("schema", SCHEMA);
        dump.addProperty("cell", cell);
        dump.addProperty("phase", phase);
        dump.addProperty("fault", fault);
        dump.addProperty("dump_forced", true);
        dump.addProperty("durable_store_read", durableRead);
        dump.addProperty("process_pid", ProcessHandle.current().pid());
        dump.addProperty("store_root", storeRoot.toAbsolutePath().normalize().toString());
        dump.addProperty("shard", SHARD.toString());
        dump.addProperty("key_sha256", digest(key()));
        dump.addProperty("value_sha256", digest(value()));
        dump.addProperty("recovery_action", recoveryAction);
        return dump;
    }

    private static void requireBefore(final JsonObject before, final String cell, final String fault) {
        assertEquals(SCHEMA, before.get("schema").getAsString());
        assertEquals(cell, before.get("cell").getAsString());
        assertEquals(BEFORE_PHASE, before.get("phase").getAsString());
        assertEquals(fault, before.get("fault").getAsString());
        assertTrue(before.get("dump_forced").getAsBoolean());
        assertTrue(before.get("durable_store_read").getAsBoolean());
        assertEquals(digest(key()), before.get("key_sha256").getAsString());
        assertEquals(digest(value()), before.get("value_sha256").getAsString());
    }

    private static void requireDifferentProcess(final JsonObject before, final JsonObject after) {
        assertTrue(after.get("dump_forced").getAsBoolean());
        assertTrue(after.get("durable_store_read").getAsBoolean());
        assertNotEquals(before.get("process_pid").getAsLong(), after.get("process_pid").getAsLong());
    }

    private static void copyTree(final Path source, final Path target) throws IOException {
        if (Files.isSymbolicLink(source) || !Files.isDirectory(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("checkpoint source is not a real directory: " + source);
        }
        Files.createDirectories(target);
        try (var paths = Files.walk(source)) {
            for (Path sourcePath : paths.toList()) {
                if (Files.isSymbolicLink(sourcePath)) {
                    throw new IOException("checkpoint contains a symbolic link: " + sourcePath);
                }
                final Path targetPath = target.resolve(source.relativize(sourcePath).toString());
                if (Files.isDirectory(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                    Files.createDirectories(targetPath);
                } else if (Files.isRegularFile(sourcePath, LinkOption.NOFOLLOW_LINKS)) {
                    Files.copy(sourcePath, targetPath, StandardCopyOption.COPY_ATTRIBUTES);
                } else {
                    throw new IOException("checkpoint contains a non-regular path: " + sourcePath);
                }
            }
        }
    }

    private static Path firstSst(final Path checkpoint) throws IOException {
        try (var paths = Files.walk(checkpoint)) {
            return paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
                    .filter(path -> path.getFileName().toString().endsWith(".sst"))
                    .findFirst()
                    .orElseThrow(() -> new IOException("checkpoint has no SST file: " + checkpoint));
        }
    }

    private static void flipFirstByte(final Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(1);
            if (channel.read(byteBuffer, 0) != 1) {
                throw new IOException("cannot read SST first byte: " + file);
            }
            final byte original = byteBuffer.array()[0];
            byteBuffer.clear();
            byteBuffer.put((byte) (original ^ 0x01)).flip();
            while (byteBuffer.hasRemaining()) {
                channel.write(byteBuffer, 0);
            }
            channel.force(true);
        }
        forceDirectory(file.getParent());
    }

    private static String inventoryDigest(final List<CheckpointFileInventory> inventory) {
        final StringBuilder canonical = new StringBuilder();
        for (CheckpointFileInventory file : inventory) {
            canonical.append(file.name()).append('\n').append(file.length()).append('\n')
                    .append(Bytes.hex(file.checksum())).append('\n');
        }
        return digest(Bytes.utf8(canonical.toString()));
    }

    private static void writeJson(final Path target, final JsonObject value) throws IOException {
        writeTextAtomically(target, GSON.toJson(value) + "\n");
    }

    private static void writeTextAtomically(final Path target, final String value) throws IOException {
        final Path parent = target.toAbsolutePath().normalize().getParent();
        if (parent == null) {
            throw new IOException("artifact target has no parent: " + target);
        }
        Files.createDirectories(parent);
        final Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IOException("atomic durable artifact move is unavailable", unsupported);
        }
        forceDirectory(parent);
    }

    private static void forceDirectory(final Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static JsonObject readJson(final Path path) throws IOException {
        return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
    }

    private static Path artifactDirectory() {
        return requiredPath(ARTIFACT_ENV);
    }

    private static Path requiredPath(final String environment) {
        final String value = System.getenv(environment);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(environment + " is required");
        }
        return Path.of(value).toAbsolutePath().normalize();
    }

    private static byte[] key() {
        return Bytes.utf8("local-storage-chaos-key-v1");
    }

    private static byte[] value() {
        return Bytes.utf8(VALUE_TEXT);
    }

    private static String digest(final byte[] bytes) {
        return Bytes.hex(Bytes.sha256(bytes));
    }

    private static boolean isNoSpace(final Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            final String message = current.getMessage();
            if (message != null) {
                final String normalized = message.toLowerCase(java.util.Locale.ROOT);
                if (normalized.contains("no space left on device") || normalized.contains("enospc")) {
                    return true;
                }
            }
        }
        return false;
    }
}
