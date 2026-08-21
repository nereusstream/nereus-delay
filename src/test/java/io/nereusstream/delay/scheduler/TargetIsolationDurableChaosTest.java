package io.nereusstream.delay.scheduler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.runtime.AdmissionGate;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Durable two-JVM proof for the scheduler's target-isolation boundary.
 *
 * <p>The first JVM injects a blocked target A while target B remains READY and
 * persists the exact scheduler projection. The second JVM reopens the same
 * manifest, restores the projection and proves that B continues to make
 * progress while A's pending work remains untouched.</p>
 */
class TargetIsolationDurableChaosTest {
    private static final String ARTIFACT_ENV = "NEREUS_DELAY_TARGET_ISOLATION_ARTIFACT_DIR";
    private static final String PHASE_ENV = "NEREUS_DELAY_TARGET_ISOLATION_PHASE";
    private static final String BEFORE_FILE = "before.json";
    private static final String AFTER_FILE = "after.json";
    private static final String MANIFEST_FILE = "manifest.json";
    private static final String SCHEMA = "nereus-delay-target-isolation-durable-state-dump-v1";
    private static final String FAULT = "TARGET_ISOLATION";
    private static final String BEFORE_PHASE = "BEFORE_FRESH_PROCESS_RECOVERY";
    private static final String AFTER_PHASE = "RECOVERED_AFTER_FRESH_PROCESS";
    private static final int PENDING_BAD = 8;
    private static final int PROGRESS_BEFORE = 8;

    private static final RouteIncarnation ROUTE = new RouteIncarnation(new byte[]{
            0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef,
            0x10, 0x32, 0x54, 0x76, (byte) 0x98, (byte) 0xba, (byte) 0xdc, (byte) 0xfe
    });
    private static final ShardId BAD_SHARD = new ShardId(ROUTE, 11);
    private static final ShardId HEALTHY_SHARD = new ShardId(ROUTE, 12);
    private static final DestinationLaneId BAD_LANE = lane("target-isolation-bad");
    private static final DestinationLaneId HEALTHY_LANE = lane("target-isolation-healthy");
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    @Test
    void targetIsolationSurvivesFreshProcessRecovery() throws Exception {
        final Path artifact = artifactDirectory();
        final String phase = required(PHASE_ENV);
        Files.createDirectories(artifact);
        switch (phase) {
            case "before" -> writeBefore(artifact);
            case "after" -> writeAfter(artifact);
            default -> throw new IllegalArgumentException("unsupported target isolation phase: " + phase);
        }
    }

    private static void writeBefore(final Path artifact) throws Exception {
        final Fixture fixture = newFixture();
        final WorkerScheduler worker = fixture.worker();
        for (int index = 0; index < PENDING_BAD; index++) {
            worker.offer(item(BAD_SHARD, BAD_LANE, index + 1));
            worker.offer(item(HEALTHY_SHARD, HEALTHY_LANE, index + 1));
        }
        worker.markShardBlocked(BAD_SHARD);

        int healthyProgress = 0;
        for (int index = 0; index < PROGRESS_BEFORE; index++) {
            final List<ScheduleWorkItem> served = worker.poll(1_000,
                    new SchedulerBudget(1, 1, 1_000_000_000));
            assertEquals(1, served.size());
            assertEquals(HEALTHY_LANE, served.get(0).laneId());
            healthyProgress++;
            worker.offer(item(HEALTHY_SHARD, HEALTHY_LANE, PENDING_BAD + index + 1));
        }

        final WorkerScheduler.WorkerSnapshot snapshot = worker.snapshot();
        final JsonObject manifest = manifest();
        writeAtomically(artifact.resolve(MANIFEST_FILE), manifest);
        final String manifestHash = sha256(Files.readAllBytes(artifact.resolve(MANIFEST_FILE)));
        final JsonObject dump = dump(BEFORE_PHASE, ProcessHandle.current().pid(), manifestHash,
                snapshot, fixture.badScheduler().pendingItems(BAD_LANE),
                fixture.healthyScheduler().pendingItems(HEALTHY_LANE), healthyProgress,
                "TARGET_A_BLOCKED_HEALTHY_B_PROGRESS_CONTINUES");
        assertEquals(PENDING_BAD, dump.get("bad_pending").getAsInt());
        assertEquals(PENDING_BAD, dump.get("healthy_pending").getAsInt());
        assertEquals(PROGRESS_BEFORE, dump.get("healthy_progress").getAsInt());
        writeAtomically(artifact.resolve(BEFORE_FILE), dump);
    }

    private static void writeAfter(final Path artifact) throws Exception {
        final JsonObject before = read(artifact.resolve(BEFORE_FILE));
        final JsonObject manifest = read(artifact.resolve(MANIFEST_FILE));
        final String manifestHash = sha256(Files.readAllBytes(artifact.resolve(MANIFEST_FILE)));
        assertEquals(SCHEMA, before.get("schema").getAsString());
        assertEquals(BEFORE_PHASE, before.get("phase").getAsString());
        assertEquals(FAULT, before.get("fault").getAsString());
        assertEquals(manifestHash, before.get("manifest_sha256").getAsString());
        assertEquals(manifest.get("bad_pending").getAsInt(), before.get("bad_pending").getAsInt());
        assertTrue(before.get("healthy_progress").getAsInt() >= PROGRESS_BEFORE);

        final Fixture fixture = newFixture();
        final WorkerScheduler worker = fixture.worker();
        for (int index = 0; index < before.get("bad_pending").getAsInt(); index++) {
            worker.offer(item(BAD_SHARD, BAD_LANE, index + 1));
        }
        worker.offer(item(HEALTHY_SHARD, HEALTHY_LANE, PENDING_BAD + PROGRESS_BEFORE + 1));
        worker.restore(snapshot(before));

        final List<ScheduleWorkItem> served = worker.poll(1_000,
                new SchedulerBudget(1, 1, 1_000_000_000));
        assertEquals(1, served.size());
        assertEquals(HEALTHY_LANE, served.get(0).laneId());
        assertEquals(before.get("bad_pending").getAsInt(), fixture.badScheduler().pendingItems(BAD_LANE));
        assertNotEquals(before.get("process_pid").getAsLong(), ProcessHandle.current().pid());

        final JsonObject dump = dump(AFTER_PHASE, ProcessHandle.current().pid(), manifestHash, worker.snapshot(),
                fixture.badScheduler().pendingItems(BAD_LANE), fixture.healthyScheduler().pendingItems(HEALTHY_LANE),
                before.get("healthy_progress").getAsInt() + 1,
                "FRESH_PROCESS_REOPENED_AND_HEALTHY_PROGRESS_CONTINUED");
        writeAtomically(artifact.resolve(AFTER_FILE), dump);
    }

    private static Fixture newFixture() {
        final WorkerScheduler worker = new WorkerScheduler(1, 4);
        final LaneScheduler badScheduler = LaneScheduler.defaults();
        final LaneScheduler healthyScheduler = LaneScheduler.defaults();
        worker.registerShard(BAD_SHARD, 1, badScheduler);
        worker.registerShard(HEALTHY_SHARD, 1, healthyScheduler);
        worker.registerLane(BAD_SHARD, laneRecord(BAD_LANE));
        worker.registerLane(HEALTHY_SHARD, laneRecord(HEALTHY_LANE));
        return new Fixture(worker, badScheduler, healthyScheduler);
    }

    private static WorkerScheduler.WorkerSnapshot snapshot(final JsonObject dump) {
        return new WorkerScheduler.WorkerSnapshot(dump.get("cursor").getAsInt(),
                dump.get("round_generation").getAsLong(), List.of(
                new WorkerScheduler.ShardSnapshot(BAD_SHARD, 1, dump.get("bad_deficit").getAsLong(),
                        dump.get("bad_last_served_round").getAsLong(), true),
                new WorkerScheduler.ShardSnapshot(HEALTHY_SHARD, 1, dump.get("healthy_deficit").getAsLong(),
                        dump.get("healthy_last_served_round").getAsLong(), false)));
    }

    private static JsonObject manifest() {
        final JsonObject manifest = new JsonObject();
        manifest.addProperty("schema", SCHEMA);
        manifest.addProperty("fault", FAULT);
        manifest.addProperty("bad_target", "A");
        manifest.addProperty("healthy_target", "B");
        manifest.addProperty("bad_pending", PENDING_BAD);
        manifest.addProperty("healthy_progress_before", PROGRESS_BEFORE);
        return manifest;
    }

    private static JsonObject dump(final String phase, final long processPid, final String manifestHash,
                                   final WorkerScheduler.WorkerSnapshot snapshot, final int badPending,
                                   final int healthyPending, final int healthyProgress, final String recoveryAction) {
        final JsonObject dump = new JsonObject();
        dump.addProperty("schema", SCHEMA);
        dump.addProperty("cell", "target-isolation");
        dump.addProperty("phase", phase);
        dump.addProperty("fault", FAULT);
        dump.addProperty("dump_forced", true);
        dump.addProperty("durable_store_read", true);
        dump.addProperty("process_pid", processPid);
        dump.addProperty("manifest_sha256", manifestHash);
        dump.addProperty("bad_target_state", "BLOCKED");
        dump.addProperty("healthy_target_state", "READY");
        dump.addProperty("bad_pending", badPending);
        dump.addProperty("healthy_pending", healthyPending);
        dump.addProperty("healthy_progress", healthyProgress);
        dump.addProperty("cursor", snapshot.cursor());
        dump.addProperty("round_generation", snapshot.roundGeneration());
        final WorkerScheduler.ShardSnapshot bad = shard(snapshot, BAD_SHARD);
        final WorkerScheduler.ShardSnapshot healthy = shard(snapshot, HEALTHY_SHARD);
        dump.addProperty("bad_deficit", bad.deficit());
        dump.addProperty("bad_last_served_round", bad.lastServedRound());
        dump.addProperty("healthy_deficit", healthy.deficit());
        dump.addProperty("healthy_last_served_round", healthy.lastServedRound());
        dump.addProperty("recovery_action", recoveryAction);
        return dump;
    }

    private static WorkerScheduler.ShardSnapshot shard(final WorkerScheduler.WorkerSnapshot snapshot,
                                                        final ShardId shardId) {
        return snapshot.shards().stream().filter(shard -> shard.shardId().equals(shardId)).findFirst()
                .orElseThrow(() -> new IllegalStateException("missing shard snapshot: " + shardId));
    }

    private static LaneRecord laneRecord(final DestinationLaneId lane) {
        return new LaneRecord(lane, new byte[16], 1, 0, AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
    }

    private static ScheduleWorkItem item(final ShardId shard, final DestinationLaneId lane, final int generation) {
        return new ScheduleWorkItem(lane, DelayMessageId.random(shard), generation, 0, 1);
    }

    private static DestinationLaneId lane(final String value) {
        return DestinationLaneId.derive(value.getBytes(StandardCharsets.UTF_8));
    }

    private static Path artifactDirectory() {
        return Path.of(required(ARTIFACT_ENV));
    }

    private static String required(final String name) {
        final String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static JsonObject read(final Path path) throws IOException {
        return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
    }

    private static String sha256(final byte[] bytes) {
        return Bytes.hex(Bytes.sha256(bytes));
    }

    private static void writeAtomically(final Path target, final JsonObject value) throws IOException {
        final Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        final byte[] bytes = (GSON.toJson(value) + "\n").getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IOException("atomic durable target-isolation move is unavailable", unsupported);
        }
        try (FileChannel directory = FileChannel.open(target.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private record Fixture(WorkerScheduler worker, LaneScheduler badScheduler, LaneScheduler healthyScheduler) {
    }
}
