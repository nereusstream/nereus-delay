package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.AdmissionGate;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.RuntimeReadiness;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Real JVM long-GC pause and fresh-process scheduler admission evidence. */
class LongGcDurableChaosTest {
    private static final String ARTIFACT_ENV = "NEREUS_DELAY_LONG_GC_ARTIFACT_DIR";
    private static final String PHASE_ENV = "NEREUS_DELAY_LONG_GC_PHASE";
    private static final String SCHEMA = "nereus-delay-long-gc-durable-state-dump";
    private static final String BEFORE_PHASE = "BEFORE_FRESH_PROCESS_RECOVERY";
    private static final String AFTER_PHASE = "RECOVERED_AFTER_FRESH_PROCESS";
    private static final String FAULT = "LONG_GC";
    private static final String VALUE_TEXT = "long-gc-durable-store-value";
    private static final long MIN_LONG_GC_PAUSE_MS = 50;
    private static final RouteIncarnation ROUTE =
            RouteIncarnation.fromUuid(UUID.fromString("01234567-89ab-cdef-0123-456789abcdef"));
    private static final ShardId SHARD = new ShardId(ROUTE, 19);
    private static final DestinationLaneId LANE = DestinationLaneId.derive(Bytes.utf8("long-gc-lane"));
    private static final UUID MESSAGE_UUID = UUID.fromString("018f0000-7000-7000-8000-000000000001");
    private static final DelayMessageId MESSAGE_ID = new DelayMessageId(
            SelfRoutingId.fromLogicalUuid(SHARD, MESSAGE_UUID).bytes());
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static volatile Object gcHold;

    @Test
    void realLongGcPausePreservesDurableDueAdmission() throws Exception {
        final String phase = System.getenv(PHASE_ENV);
        Assumptions.assumeTrue(phase != null && !phase.isBlank(), "long-GC chaos is opt-in");
        final Path artifact = requiredPath(ARTIFACT_ENV);
        Files.createDirectories(artifact);
        switch (phase) {
            case "before" -> writeBefore(artifact);
            case "after" -> writeAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported long-GC phase: " + phase);
        }
    }

    private static void writeBefore(final Path artifact) throws Exception {
        final Path storeRoot = artifact.resolve("store");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final Fixture fixture = fixtureWithDueItem();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, SHARD, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 3, key(), value()));
            store.flushAndSync();
            final ValueEnvelope.Decoded durable = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(durable);
            assertArrayEquals(value(), durable.payload());
            assertEquals(1, fixture.laneScheduler().pendingItems(LANE));

            final GcObservation observation = induceLongGc();
            assertTrue(
                    observation.collectionCountDelta() > 0,
                    () -> "no GC collection observed; maxHeap=" + observation.memoryMaxBytes());
            assertTrue(
                    observation.collectionTimeDeltaMs() >= MIN_LONG_GC_PAUSE_MS,
                    () -> "GC MXBean reported only " + observation.collectionTimeDeltaMs() + "ms; maxHeap="
                            + observation.memoryMaxBytes());
            final JsonObject dump = commonDump(
                    BEFORE_PHASE, storeRoot, true, "REAL_HEAP_PRESSURE_AND_FULL_GC_AT_DUE_ADMISSION_BOUNDARY");
            dump.addProperty("gc_pause_observed", true);
            dump.addProperty("gc_collection_count_delta", observation.collectionCountDelta());
            dump.addProperty("gc_collection_time_delta_ms", observation.collectionTimeDeltaMs());
            dump.addProperty("gc_wall_elapsed_ms", observation.wallPauseMs());
            dump.addProperty("gc_memory_max_bytes", observation.memoryMaxBytes());
            dump.addProperty("scheduler_pending_before", fixture.laneScheduler().pendingItems(LANE));
            dump.addProperty("due_item_sha256", Bytes.hex(Bytes.sha256(MESSAGE_ID.bytes())));
            writeJson(artifact.resolve("before.json"), dump);
        }
    }

    private static void writeAfter(final Path artifact) throws Exception {
        final JsonObject before = readJson(artifact.resolve("before.json"));
        assertEquals(SCHEMA, before.get("schema").getAsString());
        assertEquals(BEFORE_PHASE, before.get("phase").getAsString());
        assertEquals(FAULT, before.get("fault").getAsString());
        assertTrue(before.get("gc_pause_observed").getAsBoolean());
        assertTrue(before.get("gc_collection_count_delta").getAsLong() > 0);
        assertTrue(before.get("gc_collection_time_delta_ms").getAsLong() >= MIN_LONG_GC_PAUSE_MS);
        assertEquals(1, before.get("scheduler_pending_before").getAsInt());
        assertEquals(
                Bytes.hex(Bytes.sha256(MESSAGE_ID.bytes())),
                before.get("due_item_sha256").getAsString());

        final Path storeRoot = artifact.resolve("store");
        final ShardStoreConfig config = ShardStoreConfig.defaults(storeRoot);
        final Fixture fixture = fixtureWithDueItem();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, SHARD, resources)) {
            final ValueEnvelope.Decoded recovered = store.getValue(ColumnFamily.META, key(), 3);
            assertNotNull(recovered);
            assertArrayEquals(value(), recovered.payload());
            final List<ScheduleWorkItem> served =
                    fixture.worker().poll(1_000, new SchedulerBudget(1, 1, 1_000_000_000));
            assertEquals(1, served.size());
            assertEquals(MESSAGE_ID, served.get(0).messageId());
            assertEquals(0, fixture.laneScheduler().pendingItems(LANE));
            store.flushAndSync();
            final JsonObject dump = commonDump(
                    AFTER_PHASE, storeRoot, true, "FRESH_PROCESS_REOPENED_AFTER_LONG_GC_AND_SERVED_DUE_ITEM");
            dump.addProperty("gc_pause_observed", false);
            dump.addProperty("durable_store_value_recovered_exactly", true);
            dump.addProperty("scheduler_item_served", true);
            dump.addProperty("scheduler_pending_after", fixture.laneScheduler().pendingItems(LANE));
            dump.addProperty("due_item_sha256", Bytes.hex(Bytes.sha256(MESSAGE_ID.bytes())));
            assertNotEquals(
                    before.get("process_pid").getAsLong(),
                    ProcessHandle.current().pid());
            writeJson(artifact.resolve("after.json"), dump);
        }
    }

    private static Fixture fixtureWithDueItem() {
        final WorkerScheduler worker = new WorkerScheduler(1, 4);
        final LaneScheduler laneScheduler = LaneScheduler.defaults();
        worker.registerShard(SHARD, 1, laneScheduler);
        worker.registerLane(
                SHARD, new LaneRecord(LANE, new byte[16], 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0));
        worker.offer(new ScheduleWorkItem(LANE, MESSAGE_ID, 1, 0, 1));
        return new Fixture(worker, laneScheduler);
    }

    private static GcObservation induceLongGc() {
        final List<GarbageCollectorMXBean> collectors = ManagementFactory.getGarbageCollectorMXBeans();
        final long beforeCount = collectionCount(collectors);
        final long beforeTime = collectionTime(collectors);
        long longestPauseMs = 0;
        for (int attempt = 0; attempt < 8; attempt++) {
            final int blocks = allocationBlocks();
            final List<byte[]> live = new ArrayList<>(blocks);
            for (int index = 0; index < blocks; index++) {
                final byte[] block = new byte[1024 * 1024];
                block[index % block.length] = (byte) (attempt + index);
                live.add(block);
            }
            gcHold = live;
            for (int index = 0; index < blocks; index++) {
                final byte[] garbage = new byte[1024 * 1024];
                garbage[index % garbage.length] = (byte) index;
            }
            final long started = System.nanoTime();
            gcHold = null;
            System.gc();
            System.gc();
            final long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            longestPauseMs = Math.max(longestPauseMs, elapsedMs);
            final long countDelta = difference(collectionCount(collectors), beforeCount);
            final long collectionTimeDelta = difference(collectionTime(collectors), beforeTime);
            if (countDelta > 0 && collectionTimeDelta >= MIN_LONG_GC_PAUSE_MS) {
                break;
            }
        }
        final long afterCount = collectionCount(collectors);
        final long afterTime = collectionTime(collectors);
        return new GcObservation(
                difference(afterCount, beforeCount),
                difference(afterTime, beforeTime),
                longestPauseMs,
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
    }

    private static int allocationBlocks() {
        final long maxMemory =
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        final long maxMegabytes = maxMemory / (1024 * 1024);
        return (int) Math.max(16, Math.min(512, Math.max(1, maxMegabytes / 2)));
    }

    private static long collectionCount(final List<GarbageCollectorMXBean> collectors) {
        long result = 0;
        for (GarbageCollectorMXBean collector : collectors) {
            if (collector.getCollectionCount() < 0) {
                throw new IllegalStateException("GC collection count is unavailable: " + collector.getName());
            }
            result = Math.addExact(result, collector.getCollectionCount());
        }
        if (collectors.isEmpty()) {
            throw new IllegalStateException("no GarbageCollectorMXBean is available");
        }
        return result;
    }

    private static long collectionTime(final List<GarbageCollectorMXBean> collectors) {
        long result = 0;
        for (GarbageCollectorMXBean collector : collectors) {
            if (collector.getCollectionTime() < 0) {
                throw new IllegalStateException("GC collection time is unavailable: " + collector.getName());
            }
            result = Math.addExact(result, collector.getCollectionTime());
        }
        return result;
    }

    private static long difference(final long after, final long before) {
        if (after < before) {
            throw new IllegalStateException("GC counter moved backwards");
        }
        return after - before;
    }

    private static JsonObject commonDump(
            final String phase, final Path storeRoot, final boolean durableRead, final String recoveryAction) {
        final JsonObject dump = new JsonObject();
        dump.addProperty("schema", SCHEMA);
        dump.addProperty("cell", "long-gc");
        dump.addProperty("phase", phase);
        dump.addProperty("fault", FAULT);
        dump.addProperty("dump_forced", true);
        dump.addProperty("durable_store_read", durableRead);
        dump.addProperty("process_pid", ProcessHandle.current().pid());
        dump.addProperty("store_root", storeRoot.toAbsolutePath().normalize().toString());
        dump.addProperty("shard", SHARD.toString());
        dump.addProperty("recovery_action", recoveryAction);
        dump.addProperty("key_sha256", Bytes.hex(Bytes.sha256(key())));
        dump.addProperty("value_sha256", Bytes.hex(Bytes.sha256(value())));
        return dump;
    }

    private static void writeJson(final Path target, final JsonObject value) throws IOException {
        final Path parent = target.toAbsolutePath().normalize().getParent();
        Files.createDirectories(parent);
        final Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        final byte[] bytes = (GSON.toJson(value) + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(
                temporary, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IOException("atomic durable long-GC artifact move is unavailable", unsupported);
        }
        try (FileChannel directory = FileChannel.open(parent, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private static JsonObject readJson(final Path path) throws IOException {
        return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
    }

    private static Path requiredPath(final String environment) {
        final String value = System.getenv(environment);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(environment + " is required");
        }
        return Path.of(value).toAbsolutePath().normalize();
    }

    private static byte[] key() {
        return Bytes.utf8("long-gc-chaos-key");
    }

    private static byte[] value() {
        return Bytes.utf8(VALUE_TEXT);
    }

    private record Fixture(WorkerScheduler worker, LaneScheduler laneScheduler) {}

    private record GcObservation(
            long collectionCountDelta, long collectionTimeDeltaMs, long wallPauseMs, long memoryMaxBytes) {}
}
