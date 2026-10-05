package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.LegacyCheckpointTailReplayer;
import com.nereusstream.delay.ownership.ReplayTurnBudget;
import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayCursor;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplayOutcome;
import com.nereusstream.delay.ownership.SourceReplayRecord;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.transport.PulsarClientArtifactRecoverySourceCursor;
import com.nereusstream.delay.transport.PulsarClientArtifactRecoverySourcePositioner;
import com.nereusstream.delay.transport.PulsarClientArtifactSourceConsumerFactory;
import com.nereusstream.delay.transport.PulsarClientArtifactSourceRecordConsumer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TopicResourceGuard;

/** Real Pulsar verification of bounded legacy checkpoint-tail replay into an isolated Store copy. */
public final class PulsarLegacyCheckpointReplaySmoke {
    private static final CheckpointManifestLimits MANIFEST_LIMITS =
            new CheckpointManifestLimits(1_024, 1L << 30, 1L << 28, 4_096, 1 << 20, 256, 4_096);

    private PulsarLegacyCheckpointReplaySmoke() {}

    public static void run(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final PreparedCommand checkpointCommand,
            final PreparedCommand cutCommand,
            final PreparedCommand postCutCommand)
            throws Exception {
        final String activeSubscription = "nereus-delay-b6-active-" + UUID.randomUUID();
        final Path temporaryRoot = Files.createTempDirectory("nereus-delay-b6-pulsar-replay-");
        try {
            final ShardStoreConfig config = ShardStoreConfig.defaults(temporaryRoot.resolve("store-root"));
            final Path checkpointPath = temporaryRoot.resolve("legacy-checkpoint");
            final byte[] checkpointId = uuidBytes(UUID.randomUUID());
            final Path activePointer = config.rootPath()
                    .resolve("shards")
                    .resolve(shard.routeIncarnation().uuid().toString())
                    .resolve(Integer.toUnsignedString(shard.partition()))
                    .resolve("ACTIVE");
            final CheckpointManifest manifest;
            final PulsarSourcePosition checkpointPosition;
            final PulsarSourcePosition sourceCutPosition;
            final byte[] activePointerBefore;
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                    ShardStore originalStore = ShardStore.open(config, shard, resources);
                    PulsarClientArtifactSourceRecordConsumer activeSource =
                            new PulsarClientArtifactSourceRecordConsumer(
                                    PulsarClientArtifactSourceConsumerFactory.create(
                                            client, guard, physicalTopic, activeSubscription),
                                    guard,
                                    shard,
                                    physicalTopic,
                                    Duration.ofMillis(250))) {
                final DelayShard applier = new DelayShard(originalStore, DelayShardConfig.defaults());
                final SourceRecordConsumer.PolledSourceRecord checkpointRecord =
                        pollExpected(activeSource, checkpointCommand);
                checkpointPosition = position(sourceRecord(checkpointRecord));
                applyThenAcknowledge(applier, checkpointRecord, "Pulsar B6 checkpoint record");
                originalStore.createCheckpoint(checkpointPath, checkpointId);
                manifest = manifestFor(checkpointPath, shard, originalStore, checkpointId, checkpointPosition);
                activePointerBefore = Files.readAllBytes(activePointer);

                final SourceRecordConsumer.PolledSourceRecord cutRecord = pollExpected(activeSource, cutCommand);
                sourceCutPosition = position(sourceRecord(cutRecord));
                final SourceReplaySuccessor successor = strictSameLedgerSuccessor();
                successor.validate(checkpointPosition, sourceCutPosition);
                applyThenAcknowledge(applier, cutRecord, "Pulsar B6 source cut record");
                final SourceRecordConsumer.CheckpointCut protectedCut = activeSource.checkpointCut(sourceCutPosition);
                protectedCut.requireCurrent();

                replayToCut(
                        client,
                        guard,
                        physicalTopic,
                        shard,
                        checkpointPath,
                        manifest,
                        config,
                        resources,
                        checkpointPosition,
                        sourceCutPosition,
                        successor,
                        protectedCut);
                activeSource.checkpointCut(sourceCutPosition).requireCurrent();
            }

            final LegacyCheckpointImageInspector.ImageProof imageProof =
                    LegacyCheckpointImageInspector.inspect(checkpointPath, shard, manifest, MANIFEST_LIMITS);
            if (!samePosition(imageProof.appliedSourcePosition(), checkpointPosition)
                    || !Arrays.equals(activePointerBefore, Files.readAllBytes(activePointer))) {
                throw new IllegalStateException("Pulsar B6 replay changed the original checkpoint or ACTIVE pointer");
            }
            final PulsarSourcePosition postCutPosition =
                    receiveAfter(client, guard, physicalTopic, shard, sourceCutPosition, postCutCommand);
            if (postCutPosition.compareTo(sourceCutPosition) <= 0) {
                throw new IllegalStateException("Pulsar B6 post-cut record did not follow the protected cut");
            }
            System.out.println("Pulsar B6 legacy checkpoint replay passed: checkpoint="
                    + checkpointPosition.ledgerId() + "/" + checkpointPosition.entryId()
                    + ", sourceCut=" + sourceCutPosition.ledgerId() + "/" + sourceCutPosition.entryId()
                    + ", postCut=" + postCutPosition.ledgerId() + "/" + postCutPosition.entryId()
                    + ", recordsApplied=1");
        } finally {
            deleteTree(temporaryRoot);
        }
    }

    private static void replayToCut(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final Path checkpointPath,
            final CheckpointManifest manifest,
            final ShardStoreConfig config,
            final SharedRocksDbResources resources,
            final PulsarSourcePosition checkpointPosition,
            final PulsarSourcePosition sourceCutPosition,
            final SourceReplaySuccessor successor,
            final SourceRecordConsumer.CheckpointCut protectedCut)
            throws Exception {
        final GuardedConsumer<byte[]> recoveryNative = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-b6-recovery-" + UUID.randomUUID());
        boolean cursorOwnsNative = false;
        try {
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof positionedProof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                            recoveryNative,
                            guard,
                            physicalTopic,
                            shard,
                            Optional.of(checkpointPosition),
                            Duration.ofSeconds(5));
            final SourceAssignment assignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("pulsar-b6-replay-assignment"), Bytes.utf8(physicalTopic)),
                    1,
                    PulsarActivationBarrier.empty(
                            shard,
                            guard.resourceIncarnation(),
                            physicalTopic,
                            positionedProof.connectionGeneration(),
                            positionedProof.attestationDigest()));
            try (ShardStore.LegacyCheckpointReplayCopy replayCopy = ShardStore.openLegacyCheckpointReplayCopy(
                            config, shard, resources, checkpointPath, manifest, MANIFEST_LIMITS);
                    PulsarClientArtifactRecoverySourceCursor recovery =
                            new PulsarClientArtifactRecoverySourceCursor(
                                    recoveryNative, guard, assignment, physicalTopic, Duration.ofMillis(250))) {
                cursorOwnsNative = true;
                final var result = LegacyCheckpointTailReplayer.replay(
                        replayCopy,
                        assignment,
                        successor,
                        protectedCut,
                        recovery,
                        store -> new DelayShard(store, DelayShardConfig.defaults()),
                        KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic(),
                        System::currentTimeMillis,
                        new ReplayTurnBudget(8, 1 << 20, TimeUnit.SECONDS.toNanos(30)));
                if (result.status() != LegacyCheckpointTailReplayer.Status.EXACT_CUT_REACHED
                        || result.recordsApplied() != 1
                        || !samePosition(result.checkpointPosition(), checkpointPosition)
                        || !samePosition(result.sourceCut(), sourceCutPosition)
                        || !samePosition(result.appliedThrough(), sourceCutPosition)
                        || !samePosition(replayCopy.store().appliedShardLogPosition(), sourceCutPosition)) {
                    throw new IllegalStateException("Pulsar B6 replay did not reach the exact protected cut");
                }
                protectedCut.requireCurrent();
            }
        } finally {
            if (!cursorOwnsNative) {
                closeNative(recoveryNative);
            }
        }
    }

    private static PulsarSourcePosition receiveAfter(
            final PulsarClient client,
            final TopicResourceGuard guard,
            final String physicalTopic,
            final ShardId shard,
            final PulsarSourcePosition lastApplied,
            final PreparedCommand expected)
            throws Exception {
        final GuardedConsumer<byte[]> nativeConsumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, guard, physicalTopic, "nereus-delay-b6-post-cut-" + UUID.randomUUID());
        boolean cursorOwnsNative = false;
        try {
            final PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof proof =
                    PulsarClientArtifactRecoverySourcePositioner.seekAfter(
                            nativeConsumer,
                            guard,
                            physicalTopic,
                            shard,
                            Optional.of(lastApplied),
                            Duration.ofSeconds(5));
            final SourceAssignment assignment = new SourceAssignment(
                    shard,
                    Bytes.sha256(Bytes.utf8("pulsar-b6-post-cut-assignment"), Bytes.utf8(physicalTopic)),
                    1,
                    PulsarActivationBarrier.empty(
                            shard,
                            guard.resourceIncarnation(),
                            physicalTopic,
                            proof.connectionGeneration(),
                            proof.attestationDigest()));
            try (PulsarClientArtifactRecoverySourceCursor cursor = new PulsarClientArtifactRecoverySourceCursor(
                    nativeConsumer, guard, assignment, physicalTopic, Duration.ofSeconds(2))) {
                cursorOwnsNative = true;
                final SourceReplayEntry next = SourceReplayCursor.of(cursor).peek();
                if (!(next instanceof SourceReplayRecord record) || !record.command().equals(expected)) {
                    throw new IllegalStateException("Pulsar B6 post-cut record was not available after the cut");
                }
                return position(record);
            }
        } finally {
            if (!cursorOwnsNative) {
                closeNative(nativeConsumer);
            }
        }
    }

    private static SourceReplaySuccessor strictSameLedgerSuccessor() {
        return (previous, current) -> {
            if (!(previous instanceof PulsarSourcePosition previousPulsar)
                    || !(current instanceof PulsarSourcePosition currentPulsar)
                    || previousPulsar.ledgerId() != currentPulsar.ledgerId()) {
                return false;
            }
            if (previousPulsar.entryId() == currentPulsar.entryId()) {
                return previousPulsar.entryKind() == PulsarSourcePosition.EntryKind.BATCH
                        && currentPulsar.entryKind() == PulsarSourcePosition.EntryKind.BATCH
                        && previousPulsar.batchSize() == currentPulsar.batchSize()
                        && previousPulsar.normalizedBatchIndex() != previousPulsar.batchSize() - 1
                        && currentPulsar.normalizedBatchIndex() == previousPulsar.normalizedBatchIndex() + 1;
            }
            return previousPulsar.entryId() != Long.MAX_VALUE
                    && currentPulsar.entryId() == previousPulsar.entryId() + 1
                    && previousPulsar.normalizedBatchIndex() == previousPulsar.batchSize() - 1
                    && currentPulsar.normalizedBatchIndex() == 0;
        };
    }

    private static SourceRecordConsumer.PolledSourceRecord pollExpected(
            final PulsarClientArtifactSourceRecordConsumer source, final PreparedCommand expected) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            final Optional<SourceRecordConsumer.PolledSourceRecord> polled = source.poll();
            if (polled.isPresent()) {
                if (!(polled.get().entry() instanceof SourceReplayRecord replay)
                        || !replay.command().equals(expected)) {
                    throw new IllegalStateException("Pulsar B6 source returned an unexpected command");
                }
                return polled.get();
            }
        }
        throw new IllegalStateException("Pulsar B6 source record did not become visible");
    }

    private static void applyThenAcknowledge(
            final DelayShard applier,
            final SourceRecordConsumer.PolledSourceRecord polled,
            final String label) {
        final SourceReplayRecord replay = sourceRecord(polled);
        final var result = applier.apply(replay.command(), replay.position());
        final SourceAcknowledgement.AcknowledgementResult acknowledged = polled.acknowledgement()
                .acknowledge(replay, SourceReplayOutcome.command(replay.position(), result));
        if (acknowledged.disposition() != SourceAcknowledgement.Disposition.ACKED) {
            throw new IllegalStateException(label + " did not receive a durable Pulsar ACK", acknowledged.failure());
        }
    }

    private static CheckpointManifest manifestFor(
            final Path image,
            final ShardId shard,
            final ShardStore store,
            final byte[] checkpointId,
            final PulsarSourcePosition sourcePosition) {
        final List<CheckpointManifest.FileEntry> files = CheckpointFileInventory.collect(image, MANIFEST_LIMITS)
                .stream()
                .map(file -> new CheckpointManifest.FileEntry(
                        file.name(),
                        file.length(),
                        file.checksum(),
                        Bytes.utf8("legacy/" + file.name()),
                        Bytes.utf8("version-1"),
                        null))
                .toList();
        return new CheckpointManifest(
                checkpointId,
                uuidBytes(UUID.randomUUID()),
                1,
                null,
                null,
                new CheckpointManifest.CreatedBy(randomBytes(8), randomBytes(8), 1),
                new CheckpointManifest.CreatedAt(
                        900,
                        1_000,
                        "CERTIFIED_HOST_CLOCK",
                        randomBytes(8),
                        1,
                        2,
                        3,
                        Bytes.sha256(Bytes.utf8("pulsar-b6-legacy-checkpoint")),
                        0,
                        null),
                shard,
                store.metadata().dbIdentity(),
                store.metadata().storeIncarnationUuid(),
                1,
                store.shardMutationSequence(),
                sourcePosition,
                new byte[32],
                Bytes.sha256(Bytes.utf8("pulsar-b6-legacy-checkpoint-semantics")),
                List.of(),
                files);
    }

    private static SourceReplayRecord sourceRecord(final SourceRecordConsumer.PolledSourceRecord polled) {
        return (SourceReplayRecord) polled.entry();
    }

    private static PulsarSourcePosition position(final SourceReplayRecord entry) {
        return (PulsarSourcePosition) entry.position();
    }

    private static boolean samePosition(final SourcePosition left, final SourcePosition right) {
        return left != null && right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static void closeNative(final GuardedConsumer<byte[]> consumer) {
        try {
            consumer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar B6 recovery consumer close failed", failure);
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

    private static byte[] uuidBytes(final UUID value) {
        return ByteBuffer.allocate(16)
                .putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits())
                .array();
    }

    private static byte[] randomBytes(final int length) {
        return Arrays.copyOf(uuidBytes(UUID.randomUUID()), length);
    }

}
