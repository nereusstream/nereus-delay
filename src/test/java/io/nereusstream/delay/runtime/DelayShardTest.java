package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityGrantKindV1;
import io.nereusstream.delay.protocol.CapacityGrantV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.CommittedPayloadDescriptorV1;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.ControlAuthorV1;
import io.nereusstream.delay.protocol.ControlOperationRequestV1;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.ControlTargetKindV1;
import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneControlTargetV1;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.LaneRetirementProgressV1;
import io.nereusstream.delay.protocol.LaneTerminalGuardV1;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProof;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.PayloadProofIssuanceClosePayloadV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetActivatePayloadV1;
import io.nereusstream.delay.protocol.ControlReasonKindV1;
import io.nereusstream.delay.protocol.ControlReasonV1;
import io.nereusstream.delay.protocol.ProfileAcceptanceV1;
import io.nereusstream.delay.protocol.ProfileBindingActivatePayloadV1;
import io.nereusstream.delay.protocol.ProfileNewBindingClosePayloadV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishAdmissionBodyTest.Fixture;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.RetryPolicySemanticV1;
import io.nereusstream.delay.protocol.UncertainPolicyV1;
import io.nereusstream.delay.protocol.DlqExportModeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardCapacityEnvelopeV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.RecoveryFloor;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelayShardTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsNegativePersistedShardSequences() {
        for (final int metadataKey : List.of(5, 11)) {
            final ShardStoreConfig config = ShardStoreConfig.defaults(
                    tempDir.resolve("negative-sequence-" + metadataKey));
            final ShardId shardId = new ShardId(RouteIncarnation.random(), 40 + metadataKey);
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                 ShardStore store = ShardStore.open(config, shardId, resources)) {
                final byte[] negativeSequence = ByteBuffer.allocate(Long.BYTES).putLong(-1).array();
                store.write(batch -> batch.putValue(ColumnFamily.META, 1,
                        KeyCodec.metaFixed(metadataKey), negativeSequence));
            }
            try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
                final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                        () -> ShardStore.open(config, shardId, resources));
                assertEquals(metadataKey == 5
                                ? "persisted shard mutation sequence is invalid"
                                : "persisted Claim sequence is invalid",
                        exception.getMessage());
            }
        }
    }

    @Test
    void mutationSequenceExhaustionFailsClosedBeforeCommandMutation() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("mutation-sequence-exhaustion"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 52);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("mutation-sequence-exhaustion-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("mutation-sequence-exhaustion")), 9_000);
        final SourcePosition sourcePosition = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(5),
                    Bytes.u64be(Long.MAX_VALUE)));
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());

            assertEquals(Long.MAX_VALUE, shard.mutationSequence());
            assertThrows(ArithmeticException.class, () -> shard.apply(command, sourcePosition));
            assertNull(shard.getMessage(command.delayMessageId()));
            assertNull(shard.lastAppliedSourcePosition());
            assertEquals(Long.MAX_VALUE, shard.mutationSequence());
            assertEquals(Long.MAX_VALUE, ByteBuffer.wrap(store.getValue(ColumnFamily.META,
                    KeyCodec.metaFixed(5), 1).payload()).getLong());
        }
    }

    @Test
    void terminalGenerationLookupRejectsKeyValueIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("terminal-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 53);
        final DelayMessageId keyMessageId = DelayMessageId.random(shardId);
        final DelayMessageId valueMessageId = DelayMessageId.random(shardId);
        final SourcePosition source = position(shardId, 0, 1_000);
        final TerminalGenerationRecord misplaced = new TerminalGenerationRecord(valueMessageId, 0,
                MessageStatus.CANCELED, StableCode.CANCELED, 1, source.canonicalBytes(), false);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(keyMessageId, 0), misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getTerminalGeneration(keyMessageId, 0));
        }
    }

    @Test
    void systemMutationResultLookupRejectsKeyValueIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-result-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 56);
        final byte[] keyMutationId = Bytes.sha256(Bytes.utf8("system-result-key"));
        final byte[] valueMutationId = Bytes.sha256(Bytes.utf8("system-result-value"));
        final byte[] mutationHash = Bytes.sha256(Bytes.utf8("system-result-hash"));
        final byte[] author = AuthorIdentity.service(Bytes.utf8("system-result-service"),
                Bytes.utf8("run-1"), 1).canonicalBytes();
        final SystemMutationResult misplaced = new SystemMutationResult(valueMutationId, mutationHash,
                SystemMutationType.TIME_FENCE, 9_000, author, ApplyStatus.APPLIED, StableCode.OK,
                position(shardId, 0, 1_000).canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.DEDUPE, SystemMutationResult.VALUE_TYPE,
                    KeyCodec.dedupeSystemMutation(keyMutationId), misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getSystemMutationResult(keyMutationId));
        }
    }

    @Test
    void messageLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("message-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 59);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 60);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("message-source-shard-mismatch-lane"));
        final MessageRecord misplaced = new MessageRecord(MessageStatus.SCHEDULED, 0, 1, 2_000, 5_000, lane,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"), position(otherShardId, 0, 1_000).canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId),
                    misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getMessage(messageId));
        }
    }

    @Test
    void routeKeyLookupsRejectForeignMessageShardBeforeMissingRead() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("missing-foreign-message-shard"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 57);
        final ShardId foreignShardId = new ShardId(RouteIncarnation.random(), 58);
        final DelayMessageId foreignMessageId = DelayMessageId.random(foreignShardId);
        final CommandId foreignCommandId = CommandId.random(foreignShardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertThrows(IllegalStateException.class, () -> shard.getMessage(foreignMessageId));
            assertThrows(IllegalStateException.class,
                    () -> shard.findClaimForMessage(foreignMessageId));
            assertThrows(IllegalStateException.class,
                    () -> shard.getTerminalGeneration(foreignMessageId, 0));
            assertThrows(IllegalStateException.class,
                    () -> shard.getDlqExportRecord(foreignMessageId, 0));
            assertThrows(IllegalStateException.class,
                    () -> shard.getCommandResult(foreignCommandId));
        }
    }

    @Test
    void commandResultLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("command-result-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 62);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 63);
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("command-result-source-shard-mismatch-lane")),
                        2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 9_000);
        final CommandResult misplaced = new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND, -1, 0,
                null, position(otherShardId, 0, 1_000).canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.DEDUPE, 2, KeyCodec.dedupeResult(command.commandId()),
                    misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getCommandResult(command.commandId()));
        }
    }

    @Test
    void commandDedupeLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("command-dedupe-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 61);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 62);
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("command-dedupe-source-shard-mismatch-lane")),
                        2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 9_000);
        final CommandResult misplaced = new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND, -1, 0,
                null, position(otherShardId, 0, 1_000).canonicalBytes());
        final CommandDedupeRecord record = new CommandDedupeRecord(command.commandHash(), misplaced);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.DEDUPE, 1, KeyCodec.dedupeCommand(command.commandId()),
                    record.encode()));

            assertThrows(IllegalStateException.class,
                    () -> shard.apply(command, position(shardId, 0, 1_001)));
        }
    }

    @Test
    void terminalGenerationLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("terminal-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 64);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 65);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final TerminalGenerationRecord misplaced = new TerminalGenerationRecord(messageId, 0,
                MessageStatus.CANCELED, StableCode.CANCELED, 1,
                position(otherShardId, 0, 1_000).canonicalBytes(), false);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(messageId, 0), misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getTerminalGeneration(messageId, 0));
        }
    }

    @Test
    void publishAttemptLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("attempt-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 66);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 67);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("attempt-source-shard-mismatch-lane"));
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("attempt-source-shard-mismatch-attempt"));
        final byte[] claimId = Bytes.sha256(Bytes.utf8("attempt-source-shard-mismatch-claim"));
        final PublishAttemptLedger misplaced = PublishAttemptLedger.publishing(messageId, 0, attemptId, claimId, 1,
                1, lane, new byte[16], Bytes.sha256(Bytes.utf8("attempt-owner")), new byte[16],
                Bytes.sha256(Bytes.utf8("attempt-prepared")), Bytes.utf8("attempt-admission"),
                position(otherShardId, 0, 1_000).canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE,
                    misplaced.encodedKey(), misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getPublishAttempt(attemptId, 1));
        }
    }

    @Test
    void claimLookupRejectsForeignMessageShard() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-message-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 75);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 76);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-message-shard-mismatch-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(otherShardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-message-shard-mismatch")), 9_000);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("claim-mismatch-deployment"),
                Bytes.utf8("claim-mismatch-worker"), 1, Bytes.sha256(Bytes.utf8("claim-mismatch-lease")));
        final ClaimRecord claim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, otherShardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, position(otherShardId, 0, 1_000)).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000, new byte[0], chargeVector());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.INFLIGHT, ClaimRecord.VALUE_TYPE,
                    claim.encodedKey(), claim.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getClaim(claim.claimId(), claim.ownerEpoch()));
        }
    }

    @Test
    void activationRejectsForeignAppliedSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("applied-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 68);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 69);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(3),
                    position(otherShardId, 0, 1_000).canonicalBytes()));

            assertThrows(IllegalStateException.class, () -> new DelayShard(store, DelayShardConfig.defaults()));
        }
    }

    @Test
    void activationRejectsForeignSourcePositionInProfileControlState() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("profile-control-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 70);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 71);
        final ProfileRefV1 profile = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2), ProfileKindV1.DESTINATION);
        final io.nereusstream.delay.protocol.ProfileBindingControlState state =
                io.nereusstream.delay.protocol.ProfileBindingControlState.empty()
                        .activate(profile, position(otherShardId, 0, 1_000));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 10, KeyCodec.metaFixed(13),
                    state.canonicalBytes()));

            assertThrows(IllegalStateException.class, () -> new DelayShard(store, DelayShardConfig.defaults()));
        }
    }

    @Test
    void laneTerminalGuardLookupRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-guard-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 72);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 73);
        final byte[] tuple = Bytes.utf8("lane-guard-source-shard-mismatch-tuple");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final ProfileRefV1 destination = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2),
                ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(bytes(4, 3), 1, bytes(32, 4),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final LaneTerminalGuardV1 misplaced = new LaneTerminalGuardV1(bytes(16, 5), 1,
                position(otherShardId, 0, 1_000), destination, capability, tuple, bytes(32, 6), 1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                    LaneRecordEnvelopeV1.terminal(misplaced).canonicalBytes()));

            assertThrows(IllegalStateException.class, () -> shard.getLaneTerminalGuard(lane));
        }
    }

    @Test
    void laneLookupRejectsKeyValueIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 57);
        final DestinationLaneId keyLane = DestinationLaneId.derive(Bytes.utf8("lane-key"));
        final DestinationLaneId valueLane = DestinationLaneId.derive(Bytes.utf8("lane-value"));
        final LaneRecord misplaced = new LaneRecord(valueLane, new byte[16], 1, 0,
                AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 0);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(keyLane),
                    LaneRecordEnvelopeV1.active(misplaced.encode()).canonicalBytes()));

            assertThrows(IllegalStateException.class, () -> shard.getLane(keyLane));
        }
    }

    @Test
    void laneCloseCursorLookupRejectsKeyValueIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("close-cursor-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 58);
        final DestinationLaneId keyLane = DestinationLaneId.derive(Bytes.utf8("close-cursor-key"));
        final DestinationLaneId valueLane = DestinationLaneId.derive(Bytes.utf8("close-cursor-value"));
        final LaneRecord lane = new LaneRecord(keyLane, new byte[16], 1, 0,
                AdmissionGate.CLOSED, RuntimeReadiness.BLOCKED, 1, 0);
        final byte[] closeSource = position(shardId, 0, 1_000).canonicalBytes();
        final LaneCloseMaterializationCursor misplaced = new LaneCloseMaterializationCursor(valueLane,
                new byte[16], 1, closeSource, LaneCloseMaterializationCursor.Phase.MESSAGES, null,
                0, 0, 0, 0);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(keyLane),
                        LaneRecordEnvelopeV1.active(lane.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.TIMELINE, LaneCloseMaterializationCursor.VALUE_TYPE,
                        KeyCodec.timelineSystem(LaneCloseMaterializationCursor.SYSTEM_WORK_KIND, 0,
                                keyLane.bytes(), 1), misplaced.canonicalBytes());
            });

            assertThrows(IllegalStateException.class, () -> shard.getLaneCloseCursor(keyLane));
        }
    }

    @Test
    void laneCloseMaterializationDiscoveryRejectsForeignSourcePosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("close-discovery-source-shard-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 79);
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 80);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("close-discovery-source-shard-mismatch"));
        final LaneRecord closed = new LaneRecord(lane, new byte[16], 1, 0,
                AdmissionGate.CLOSED, RuntimeReadiness.BLOCKED, 1, 0);
        final LaneCloseMaterializationCursor misplaced = new LaneCloseMaterializationCursor(lane,
                closed.laneIncarnation(), closed.laneControlVersion(), position(otherShardId, 0, 1_000).canonicalBytes(),
                LaneCloseMaterializationCursor.Phase.MESSAGES, null, 0, 0, 0, 0);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(lane),
                        LaneRecordEnvelopeV1.active(closed.encode()).canonicalBytes());
                batch.putValue(ColumnFamily.TIMELINE, LaneCloseMaterializationCursor.VALUE_TYPE,
                        KeyCodec.timelineSystem(LaneCloseMaterializationCursor.SYSTEM_WORK_KIND, 0,
                                lane.bytes(), closed.laneControlVersion()), misplaced.canonicalBytes());
            });

            assertThrows(IllegalStateException.class, () -> shard.discoverLaneCloseMaterialization(1));
        }
    }

    @Test
    void admissionBudgetConfigRequiresAPositiveTotalAndSmallerUncertainBudget() {
        assertThrows(IllegalArgumentException.class,
                () -> new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                        3, 100, 10_000, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                        3, 100, 10_000, 1, 1));
    }

    @Test
    void cancelAndRescheduleRemainTooLateWhenUncertainObligationSurvivesCurrentWorkProjection() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("uncertain-control-guard"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 27);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("uncertain-control-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-control")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] attemptId = Bytes.sha256(Bytes.utf8("uncertain-control-attempt"));
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("uncertain-control-claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("uncertain-control-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-control-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            shard.applyUnknownPublishOutcome(attemptId, 42, Bytes.utf8("unknown-outcome"),
                    Bytes.utf8("unknown-evidence"), unknownPosition);

            final MessageRecord uncertain = shard.getMessage(schedule.delayMessageId());
            final MessageRecord scheduledProjection = new MessageRecord(MessageStatus.SCHEDULED,
                    uncertain.generation(), uncertain.stateVersion() + 1, uncertain.deliverAtEpochMs(),
                    uncertain.expireAtEpochMs(), uncertain.laneId(), uncertain.orderingMode(), uncertain.payload(),
                    uncertain.scheduleSourcePosition(), uncertain.payloadReference(), uncertain.retryEligibilityAtEpochMs())
                    .withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN,
                            uncertain.runtimeIndex().attemptObligations(), uncertain.runtimeIndex().admissionsUsed(),
                            uncertain.runtimeIndex().uncertainRetryAdmissionsUsed(),
                            uncertain.runtimeIndex().possibleDestinationDuplicate(),
                            uncertain.runtimeIndex().runtimeRevision() + 1));
            store.write(batch -> batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()),
                    scheduledProjection.encode()));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.TOO_LATE,
                    shard.apply(cancel, position(shardId, 3, 1_003)).stableCode());
            final PreparedCommand reschedule = PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0,
                    3_000, 6_000, 9_000);
            assertEquals(StableCode.TOO_LATE,
                    shard.apply(reschedule, position(shardId, 4, 1_004)).stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().attemptObligations().size());
        }
    }

    @Test
    void appliesScheduleCancelAndRescheduleAtomicallyAndReplaysIdempotently() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-a"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("hello")), 9_000);
        final KafkaSourcePosition position0 = position(shardId, 0, 1_000);
        final KafkaSourcePosition position1 = position(shardId, 1, 1_100);
        final KafkaSourcePosition position2 = position(shardId, 2, 1_200);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final CommandResult scheduled = shard.apply(schedule, position0);
            assertEquals(StableCode.SCHEDULED, scheduled.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(GenerationAggregateState.SCHEDULED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.INITIAL_SCHEDULE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().timeline().workKind());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, position0.sourceOrderToken(), schedule.delayMessageId(), 0), 1));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
            assertEquals(0, shard.discoverDue(1_999, 10).size());
            assertEquals(1, shard.discoverDue(2_000, 10).size());
            assertEquals(1, shard.discoverExpiry(5_000, 10).size());

            assertEquals(scheduled, shard.apply(schedule, position0));

            final PreparedCommand reschedule = PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0,
                    3_000, 6_000, 9_000);
            final CommandResult superseded = shard.apply(reschedule, position1);
            assertEquals(StableCode.SUPERSEDED, superseded.stableCode());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).generation());
            assertEquals(MessageStatus.SUPERSEDED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertEquals(2, shard.getTerminalGeneration(schedule.delayMessageId(), 0).stateVersion());

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 1, 9_000);
            final CommandResult canceled = shard.apply(cancel, position2);
            assertEquals(StableCode.CANCELED, canceled.stableCode());
            assertEquals(MessageStatus.CANCELED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(MessageStatus.CANCELED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 1).status());
            assertEquals(3, shard.getTerminalGeneration(schedule.delayMessageId(), 1).stateVersion());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 3_000, position1.sourceOrderToken(), schedule.delayMessageId(), 1), 1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1), 1));
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(position2, reopened.lastAppliedSourcePosition());
            assertEquals(MessageStatus.CANCELED, reopened.getMessage(schedule.delayMessageId()).status());
        }
    }

    @Test
    void timelineDiscoveryRejectsOrphanDueAndExpiryEntries() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("timeline-discovery-orphan"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 81);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("timeline-discovery-orphan-lane"));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final KafkaSourcePosition sourcePosition = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineDue(lane, 2_000, sourcePosition.sourceOrderToken(), messageId, 0),
                        new TimelineEntry(messageId, 0).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineExpiry(5_000, lane, messageId, 0),
                        new TimelineEntry(messageId, 0).encode());
            });
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertThrows(IllegalStateException.class, () -> shard.discoverDue(2_000, 1));
            assertThrows(IllegalStateException.class, () -> shard.discoverExpiry(5_000, 1));
        }
    }

    @Test
    void messageGenerationAndStateVersionOverflowFailClosedBeforeMutation() {
        final ShardId stateVersionShardId = new ShardId(RouteIncarnation.random(), 41);
        final DestinationLaneId stateVersionLane = DestinationLaneId.derive(Bytes.utf8("state-version-overflow"));
        final PreparedCommand stateVersionSchedule = PreparedCommand.schedule(stateVersionShardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(stateVersionLane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("state-version-overflow")), 9_000);
        final KafkaSourcePosition stateVersionSchedulePosition = position(stateVersionShardId, 0, 1_000);
        final KafkaSourcePosition stateVersionCancelPosition = position(stateVersionShardId, 1, 1_001);
        final ShardStoreConfig stateVersionConfig = ShardStoreConfig.defaults(
                tempDir.resolve("state-version-overflow"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(stateVersionConfig);
             ShardStore store = ShardStore.open(stateVersionConfig, stateVersionShardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(stateVersionSchedule, stateVersionSchedulePosition).stableCode());
            final MessageRecord exhausted = new MessageRecord(MessageStatus.SCHEDULED, 0, Long.MAX_VALUE,
                    2_000, 5_000, stateVersionLane, OrderingMode.BEST_EFFORT, Bytes.utf8("state-version-overflow"),
                    stateVersionSchedulePosition.canonicalBytes());
            store.write(batch -> batch.putValue(ColumnFamily.ID, 1,
                    KeyCodec.idMessage(stateVersionSchedule.delayMessageId()), exhausted.encode()));

            final PreparedCommand cancel = PreparedCommand.cancel(stateVersionShardId,
                    stateVersionSchedule.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.INVALID_COMMAND,
                    shard.apply(cancel, stateVersionCancelPosition).stableCode());
            assertEquals(exhausted, shard.getMessage(stateVersionSchedule.delayMessageId()));
            assertEquals(stateVersionCancelPosition, shard.lastAppliedSourcePosition());
        }

        final ShardId generationShardId = new ShardId(RouteIncarnation.random(), 42);
        final DestinationLaneId generationLane = DestinationLaneId.derive(Bytes.utf8("generation-overflow"));
        final PreparedCommand generationSchedule = PreparedCommand.schedule(generationShardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(generationLane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("generation-overflow")), 9_000);
        final KafkaSourcePosition generationSchedulePosition = position(generationShardId, 0, 1_000);
        final KafkaSourcePosition generationReschedulePosition = position(generationShardId, 1, 1_001);
        final ShardStoreConfig generationConfig = ShardStoreConfig.defaults(tempDir.resolve("generation-overflow"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(generationConfig);
             ShardStore store = ShardStore.open(generationConfig, generationShardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(generationSchedule, generationSchedulePosition).stableCode());
            final MessageRecord exhausted = new MessageRecord(MessageStatus.SCHEDULED, Integer.MAX_VALUE, 1,
                    2_000, 5_000, generationLane, OrderingMode.BEST_EFFORT, Bytes.utf8("generation-overflow"),
                    generationSchedulePosition.canonicalBytes());
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1,
                        KeyCodec.idMessage(generationSchedule.delayMessageId()), exhausted.encode());
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineDue(generationLane, 2_000,
                        generationSchedulePosition.sourceOrderToken(), generationSchedule.delayMessageId(), 0));
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineExpiry(5_000, generationLane,
                        generationSchedule.delayMessageId(), 0));
            });

            final PreparedCommand reschedule = PreparedCommand.reschedule(generationShardId,
                    generationSchedule.delayMessageId(), Integer.MAX_VALUE, 3_000, 6_000, 9_000);
            assertEquals(StableCode.INVALID_COMMAND,
                    shard.apply(reschedule, generationReschedulePosition).stableCode());
            assertEquals(exhausted, shard.getMessage(generationSchedule.delayMessageId()));
            assertEquals(generationReschedulePosition, shard.lastAppliedSourcePosition());
        }
    }

    @Test
    void sameKafkaOffsetWithDifferentCanonicalMetadataCannotReplayAsAnotherPosition() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("same-kafka-offset"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 32);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("same-kafka-offset-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("same-kafka-offset")), 9_000);
        final UUID topic = UUID.randomUUID();
        final KafkaSourcePosition first = new KafkaSourcePosition(shardId, "cluster", topic, 7, 3, 1_000);
        final KafkaSourcePosition conflicting = new KafkaSourcePosition(shardId, "cluster", topic, 7, 4, 1_001);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, first).stableCode());
            assertThrows(IllegalStateException.class, () -> shard.apply(schedule, conflicting));
            assertEquals(first, shard.lastAppliedSourcePosition());
        }
    }

    @Test
    void appliesRegistryCancelAndRescheduleWithGenerationAndStateVersionPreconditions() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("registry-cancel-reschedule"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("registry-cancel-reschedule-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("registry-body")), 9_000);
        final KafkaSourcePosition position0 = position(shardId, 0, 1_000);
        final KafkaSourcePosition position1 = position(shardId, 1, 1_001);
        final KafkaSourcePosition position2 = position(shardId, 2, 1_002);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, position0).stableCode());

            final PreparedCommand reschedule = PreparedCommand.rescheduleV1(shardId, schedule.delayMessageId(),
                    new MessagePreconditionV1(0L, 1L), 3_000, 6_000, 9_000);
            assertEquals(StableCode.SUPERSEDED, shard.apply(reschedule, position1).stableCode());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).generation());
            assertEquals(2, shard.getMessage(schedule.delayMessageId()).stateVersion());

            final PreparedCommand staleCancel = PreparedCommand.cancelV1(shardId, schedule.delayMessageId(),
                    new MessagePreconditionV1(0L, 1L), 9_000);
            assertEquals(StableCode.VERSION_CONFLICT,
                    shard.apply(staleCancel, position2).stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());

            final PreparedCommand cancel = PreparedCommand.cancelV1(shardId, schedule.delayMessageId(),
                    new MessagePreconditionV1(1L, 2L), 9_000);
            assertEquals(StableCode.CANCELED,
                    shard.apply(cancel, position(shardId, 3, 1_003)).stableCode());
            assertEquals(MessageStatus.CANCELED, shard.getMessage(schedule.delayMessageId()).status());
        }
    }

    @Test
    void registryScheduleAndPrepareRequireAndUseExplicitLaneResolver() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("registry-schedule-resolver"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 32);
        final ScheduleIntentV1 scheduleIntent = ScheduleIntentV1.create(
                new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 1),
                        ProfileKindV1.DESTINATION),
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 2)),
                2_000, 5_000, io.nereusstream.delay.protocol.DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT, Bytes.utf8("ordering"), Bytes.utf8("v1-payload"), null,
                io.nereusstream.delay.protocol.AdapterMetadataV1.kafka(
                        new io.nereusstream.delay.protocol.KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shardId, scheduleIntent, 9_000);
        final KafkaSourcePosition position0 = position(shardId, 0, 1_000);
        final KafkaSourcePosition position1 = position(shardId, 1, 1_001);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard withoutResolver = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    withoutResolver.apply(schedule, position0).stableCode());
            assertNull(withoutResolver.getMessage(schedule.delayMessageId()));
        }

        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            private final byte[] tuple = Bytes.utf8("canonical-lane-tuple-v1");
            private final DestinationLaneId lane = DestinationLaneId.derive(tuple);

            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 intent,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, intent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
        final ScheduleIntentV1 prepareIntent = ScheduleIntentV1.forPrepare(scheduleIntent.profile(),
                scheduleIntent.retryPolicy(), 3_000, 6_000, scheduleIntent.deliveryMode(),
                OrderingMode.BEST_EFFORT, scheduleIntent.orderingKey(), scheduleIntent.adapterMetadata(), null, null);
        final PreparedCommand prepare = PreparedCommand.prepareLargeV1(shardId, prepareIntent, 2_000_000,
                Bytes.sha256(Bytes.utf8("large-v1")), 1_000,
                new PayloadProofTrustSetRefV1(1, bytes(32, 7)), 9_000);
        final ShardStoreConfig resolverConfig =
                ShardStoreConfig.defaults(tempDir.resolve("registry-schedule-resolver-enabled"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(resolverConfig);
             ShardStore store = ShardStore.open(resolverConfig, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, position0).stableCode());
            final MessageRecord message = shard.getMessage(schedule.delayMessageId());
            assertEquals(DestinationLaneId.derive(Bytes.utf8("canonical-lane-tuple-v1")), message.laneId());
            assertArrayEquals(Bytes.utf8("v1-payload"), message.payload());
            assertEquals(StableCode.OK, shard.apply(prepare, position1).stableCode());
            final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                    prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
            assertEquals(PayloadReservationStatus.RESERVED, shard.getReservation(reservationId).status());
            assertEquals(prepare.delayMessageId(), shard.getReservation(reservationId).delayMessageId());
            assertEquals(io.nereusstream.delay.protocol.CommandType.SCHEDULE,
                    shard.getV1ScheduleBinding(schedule.delayMessageId()).commandType());
            assertArrayEquals(schedule.canonicalBody(),
                    shard.getV1ScheduleBinding(schedule.delayMessageId()).canonicalBody());
            assertEquals(io.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE,
                    shard.getV1ScheduleBinding(prepare.delayMessageId()).commandType());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(resolverConfig);
             ShardStore store = ShardStore.open(resolverConfig, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertNotNull(reopened.getV1ScheduleBinding(schedule.delayMessageId()));
            assertNotNull(reopened.getV1ScheduleBinding(prepare.delayMessageId()));
        }
    }

    @Test
    void scheduleBindingLookupRejectsForeignMessageShard() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("binding-foreign-shard"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 84);
        final ShardId foreignShardId = new ShardId(RouteIncarnation.random(), 85);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertThrows(IllegalStateException.class,
                    () -> shard.getV1ScheduleBinding(DelayMessageId.random(foreignShardId)));
        }
    }

    @Test
    void registryScheduleRequiresSourceVisibleRetryPolicySemantic() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("retry-policy-catalog"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 31),
                io.nereusstream.delay.protocol.ProfileKindV1.DESTINATION);
        final RetryPolicySemanticV1 policy = new RetryPolicySemanticV1(Bytes.utf8("retry"), 1,
                100, 10_000, 5, 60_000, UncertainPolicyV1.HOLD_FOR_EVIDENCE, 0,
                DlqExportModeV1.NOT_CONFIGURED, 0, 0, 0, 0, false, bytes(32, 32));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(profile, policy.ref(), 2_000, 5_000,
                io.nereusstream.delay.protocol.DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT,
                Bytes.utf8("ordering"), Bytes.utf8("retry-policy"), null,
                io.nereusstream.delay.protocol.AdapterMetadataV1.kafka(
                        new io.nereusstream.delay.protocol.KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shardId, intent, 9_000);
        final byte[] tuple = Bytes.utf8("retry-policy-lane");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 scheduleIntent,
                                                     final SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, scheduleIntent.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final PrepareLargeScheduleBodyV1 body,
                                                  final SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
        final KafkaSourcePosition source = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final InMemoryRetryPolicyCatalog catalog = new InMemoryRetryPolicyCatalog();
            catalog.publish(policy, source);
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver, null,
                    catalog);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, source).stableCode());
        }

        final ShardStoreConfig blockedConfig = ShardStoreConfig.defaults(tempDir.resolve("retry-policy-blocked"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(blockedConfig);
             ShardStore store = ShardStore.open(blockedConfig, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver, null,
                    (reference, sourcePosition) -> null);
            assertEquals(StableCode.RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    shard.apply(PreparedCommand.scheduleV1(shardId, intent, 9_000), source).stableCode());
            assertNull(shard.getMessage(schedule.delayMessageId()));
        }
    }

    @Test
    void profileBindingControlsGateNewRegistryBindingsBySourcePosition() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("profile-binding-controls"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 34);
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 21),
                ProfileKindV1.DESTINATION);
        final io.nereusstream.delay.protocol.RetryPolicyRefV1 retryPolicy =
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 22));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(profile, retryPolicy, 2_000, 5_000,
                io.nereusstream.delay.protocol.DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT,
                Bytes.utf8("ordering"), Bytes.utf8("profile-gated"), null,
                io.nereusstream.delay.protocol.AdapterMetadataV1.kafka(
                        new io.nereusstream.delay.protocol.KafkaMetadataV1(null, List.of())), null, null);
        final byte[] tuple = Bytes.utf8("profile-gated-lane");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 schedule,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, schedule.inlinePayload(), null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedPrepare(lane, tuple);
            }
        };
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("profile-control-actor")),
                Bytes.sha256(Bytes.utf8("profile-control-roles")), Bytes.sha256(Bytes.utf8("profile-control-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            final ControlRef activateRef = new ControlRef(Bytes.sha256(Bytes.utf8("profile-activate-op")),
                    Bytes.sha256(Bytes.utf8("profile-activate-request")), 1);
            final byte[] activateBody = profileControlBody(shardId, activateRef, 2, profile,
                    new ProfileBindingActivatePayloadV1(profile).canonicalBytes());
            final SystemMutation activate = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, activateRef.logicalOperationIdentity(2), activateBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(activate, position(shardId, 0, 1_000),
                    keyPair.getPublic()).stableCode());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(PreparedCommand.scheduleV1(shardId, intent, 9_000), position(shardId, 1, 1_001))
                            .stableCode());

            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("profile-close-op")),
                    Bytes.sha256(Bytes.utf8("profile-close-request")), 2);
            final ProfileNewBindingClosePayloadV1 closePayload = new ProfileNewBindingClosePayloadV1(profile,
                    new ControlReasonV1(ControlReasonKindV1.POLICY_CHANGE,
                            Bytes.sha256(Bytes.utf8("profile-close-ticket")), null));
            final byte[] closeBody = profileControlBody(shardId, closeRef, 3, profile,
                    closePayload.canonicalBytes());
            final SystemMutation close = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, closeRef.logicalOperationIdentity(3), closeBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(close, position(shardId, 2, 1_002),
                    keyPair.getPublic()).stableCode());
            final PreparedCommand later = PreparedCommand.scheduleV1(shardId, intent, 9_000);
            assertEquals(StableCode.PROFILE_DEPRECATED_FOR_NEW_USE,
                    shard.apply(later, position(shardId, 3, 1_003)).stableCode());
            assertEquals(ProfileAcceptanceV1.CLOSED_FOR_FIRST_BINDING,
                    shard.profileBindingControlState().firstBindingAcceptance(profile,
                            position(shardId, 3, 1_003)));
        }
    }

    @Test
    void registryCommittedScheduleResolverPreservesOptionalEtag() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("registry-committed-schedule"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 33);
        final ProfileRefV1 objectStore = new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 4),
                ProfileKindV1.OBJECT_STORE);
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(objectStore,
                Bytes.utf8("bucket"), Bytes.utf8("object"), Bytes.utf8("version"), null, 7,
                Bytes.sha256(Bytes.utf8("committed")), bytes(32, 5), bytes(32, 6));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(
                new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 7), ProfileKindV1.DESTINATION),
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 8)),
                2_000, 5_000, io.nereusstream.delay.protocol.DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT, new byte[0], null, descriptor,
                io.nereusstream.delay.protocol.AdapterMetadataV1.kafka(
                        new io.nereusstream.delay.protocol.KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand command = PreparedCommand.scheduleV1(shardId, intent, 9_000);
        final byte[] tuple = Bytes.utf8("committed-lane-tuple");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 resolvedIntent,
                                                     final io.nereusstream.delay.protocol.SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, null,
                        io.nereusstream.delay.protocol.PayloadReference.fromDescriptor(
                                resolvedIntent.committedPayload()));
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final io.nereusstream.delay.protocol.SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(command, position(shardId, 0, 1_000)).stableCode());
            assertEquals(io.nereusstream.delay.protocol.PayloadReference.fromDescriptor(descriptor),
                    shard.getMessage(command.delayMessageId()).payloadReference());
            assertNull(shard.getMessage(command.delayMessageId()).payloadReference().etag());
        }
    }

    @Test
    void rejectsWindowAndCommandIdentityConflictWithoutChangingMessageState() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final KafkaSourcePosition position0 = position(shardId, 0, 10_000);
        final PreparedCommand invalid = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(
                        DestinationLaneId.derive(Bytes.utf8("lane-b")), 1, 2, OrderingMode.BEST_EFFORT, new byte[0]),
                20_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.INVALID_DELIVERY_WINDOW, shard.apply(invalid, position0).stableCode());
            assertNull(shard.getMessage(invalid.delayMessageId()));

            final PreparedCommand conflicting = PreparedCommand.create(shardId, invalid.commandId(),
                    invalid.delayMessageId(), invalid.type(), invalid.retryUntilEpochMs(),
                    io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("different")), 11_000, 12_000,
                            OrderingMode.BEST_EFFORT, new byte[0])));
            final CommandResult conflict = shard.apply(conflicting, position(shardId, 1, 10_001));
            assertEquals(StableCode.COMMAND_ID_CONFLICT, conflict.stableCode());
            assertEquals(conflict, shard.apply(conflicting, position(shardId, 1, 10_001)));
        }
    }

    @Test
    void boundedQueryProjectionSeparatesActiveAndTerminalState() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("query-projection"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("query-projection-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("query-payload")), 9_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            final MessageQuerySnapshot active = shard.queryMessageSnapshot(schedule.delayMessageId());
            assertEquals(GenerationAggregateState.SCHEDULED, active.state());
            assertEquals(PayloadAvailability.INLINE_RETAINED, active.payloadAvailability());
            assertNull(active.terminalCode());
            assertEquals(2_000, active.deliverAtEpochMs());
            assertEquals(5_000, active.expireAtEpochMs());

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.CANCELED,
                    shard.apply(cancel, position(shardId, 1, 1_001)).stableCode());
            final MessageQuerySnapshot terminal = shard.queryMessageSnapshot(schedule.delayMessageId());
            assertEquals(GenerationAggregateState.CANCELED, terminal.state());
            assertEquals(StableCode.CANCELED, terminal.terminalCode());
            assertTrue(terminal.terminal());
            assertEquals(PayloadAvailability.INLINE_RETAINED, terminal.payloadAvailability());
        }
    }

    @Test
    void fifoScheduleUsesOrderedTimelineNamespace() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("fifo"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("fifo-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.DELIVERY_TIME_FIFO, Bytes.utf8("fifo")), 9_000);
        final KafkaSourcePosition position = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, position).stableCode());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineOrdered(lane, 2_000, position.sourceOrderToken(), command.delayMessageId(), 0),
                    1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, position.sourceOrderToken(), command.delayMessageId(), 0), 1));
        }
    }

    @Test
    void readyIndexTracksLaneVersionAndCanBeDeterministicallyRebuilt() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 5);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("ready-lane"));
        final PreparedCommand later = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 3_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("later")), 9_000);
        final PreparedCommand earlier = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("earlier")), 9_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(later, position(shardId, 0, 1_000)).stableCode());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(earlier, position(shardId, 1, 1_001)).stableCode());
            assertEquals(0, shard.discoverReady(10_000, 10).size());

            final LaneRecord readyLane = shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(2_000, readyLane.nextEligibleAtEpochMs());
            assertEquals(1, shard.discoverReady(10_000, 10).size());
            assertEquals(earlier.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(2_000, lane, readyLane.laneVersion()), 3));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, earlier.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.CANCELED,
                    shard.apply(cancel, position(shardId, 2, 1_002)).stableCode());
            final LaneRecord afterCancel = shard.getLane(lane);
            assertEquals(3_000, afterCancel.nextEligibleAtEpochMs());
            assertEquals(later.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(2_000, lane, readyLane.laneVersion()), 3));

            store.write(batch -> batch.delete(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(3_000, lane, afterCancel.laneVersion())));
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertEquals(1, shard.rebuildReadyIndexes());
            assertEquals(later.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());

            final LaneRecord paused = shard.updateLaneGate(lane, afterCancel.laneControlVersion(),
                    AdmissionGate.ADMIN_PAUSED);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertThrows(IllegalStateException.class,
                    () -> shard.updateLaneGate(lane, afterCancel.laneControlVersion(), AdmissionGate.OPEN));
            final LaneRecord resumed = shard.updateLaneGate(lane, paused.laneControlVersion(), AdmissionGate.OPEN);
            assertEquals(RuntimeReadiness.BLOCKED, resumed.runtimeReadiness());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(1, shard.discoverReady(10_000, 10).size());

            shard.updateLaneReadiness(lane, RuntimeReadiness.BLOCKED);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            shard.updateLaneReadiness(lane, RuntimeReadiness.RECOVERING_EVIDENCE);
            assertEquals(0, shard.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void readyRebuildRejectsTimelineKeyThatDiffersFromCurrentMessage() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 82);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("ready-key-mismatch-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("ready-key-mismatch")), 9_000);
        final KafkaSourcePosition sourcePosition = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, sourcePosition).stableCode());
            final byte[] correctKey = KeyCodec.timelineDue(lane, 2_000, sourcePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            final byte[] wrongKey = KeyCodec.timelineDue(lane, 2_001, sourcePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            store.write(batch -> {
                batch.delete(ColumnFamily.TIMELINE, correctKey);
                batch.putValue(ColumnFamily.TIMELINE, 1, wrongKey,
                        new TimelineEntry(schedule.delayMessageId(), 0).encode());
            });
            assertThrows(IllegalStateException.class, () -> shard.rebuildReadyIndexes());
        }
    }

    @Test
    void readyDiscoveryRejectsMissingTimelineEntry() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready-missing-timeline"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 83);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("ready-missing-timeline-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("ready-missing-timeline")), 9_000);
        final KafkaSourcePosition sourcePosition = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, sourcePosition).stableCode());
            final LaneRecord readyLane = shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            store.write(batch -> batch.delete(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, sourcePosition.sourceOrderToken(), schedule.delayMessageId(), 0)));
            assertThrows(IllegalStateException.class, () -> shard.discoverReady(10_000, 1));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(2_000, lane, readyLane.laneVersion()), 3));
        }
    }

    @Test
    void readyRebuildRejectsTimelineCandidateScanOverflow() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("ready-candidate-overflow"));
        final DelayShardConfig shardConfig = new DelayShardConfig(
                365L * 24 * 60 * 60 * 1000, 1, 365L * 24 * 60 * 60 * 1000,
                1, 1L << 20, 10, 1L << 20, 1L << 32, 24L * 60 * 60 * 1000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 86);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("ready-candidate-overflow-lane"));
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("first")), 9_000);
        final KafkaSourcePosition firstPosition = position(shardId, 0, 1_000);
        final DelayMessageId secondMessageId = DelayMessageId.random(shardId);
        final KafkaSourcePosition secondPosition = position(shardId, 1, 1_001);
        final MessageRecord second = new MessageRecord(MessageStatus.SCHEDULED, 0, 1, 3_000, 5_000, lane,
                OrderingMode.BEST_EFFORT, Bytes.utf8("second"), secondPosition.canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(first, firstPosition).stableCode());
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(secondMessageId), second.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineDue(lane, 3_000, secondPosition.sourceOrderToken(), secondMessageId, 0),
                        new TimelineEntry(secondMessageId, 0).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineExpiry(5_000, lane, secondMessageId, 0),
                        new TimelineEntry(secondMessageId, 0).encode());
            });
            assertThrows(IllegalStateException.class, shard::rebuildReadyIndexes);
        }
    }

    @Test
    void sourceOrderedLaneControlPausesAndResumesWithClaimRollback() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-control"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 6);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("source-ordered-lane-control"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("lane-control")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("lane-control-deployment"),
                Bytes.utf8("lane-control-worker"), 7, Bytes.sha256(Bytes.utf8("lane-control-fence")));
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("lane-control-actor")),
                Bytes.sha256(Bytes.utf8("lane-control-roles")), Bytes.sha256(Bytes.utf8("lane-control-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 2_500,
                    new byte[0], chargeVector());
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());

            final LaneRecord beforePause = shard.getLane(lane);
            final ControlRef pauseRef = new ControlRef(Bytes.sha256(Bytes.utf8("lane-control-pause-op")),
                    Bytes.sha256(Bytes.utf8("lane-control-pause-request")), 1);
            final byte[] pauseBody = applyShardControlBody(shardId, pauseRef, 8, lane,
                    beforePause.laneIncarnation(), beforePause.laneControlVersion());
            final SystemMutation pause = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, pauseRef.logicalOperationIdentity(8), pauseBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            final SystemMutationResult pausedResult = shard.applySystemMutation(pause,
                    position(shardId, 1, 1_001), keyPair.getPublic());
            assertEquals(StableCode.OK, pausedResult.stableCode());
            assertEquals(AdmissionGate.ADMIN_PAUSED, shard.getLane(lane).admissionGate());
            assertEquals(RuntimeReadiness.BLOCKED, shard.getLane(lane).runtimeReadiness());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertEquals(pausedResult, shard.applySystemMutation(pause, position(shardId, 1, 1_001),
                    keyPair.getPublic()));

            final LaneRecord beforeResume = shard.getLane(lane);
            final ControlRef resumeRef = new ControlRef(Bytes.sha256(Bytes.utf8("lane-control-resume-op")),
                    Bytes.sha256(Bytes.utf8("lane-control-resume-request")), 2);
            final byte[] resumeBody = applyShardControlBody(shardId, resumeRef, 9, lane,
                    beforeResume.laneIncarnation(), beforeResume.laneControlVersion());
            final SystemMutation resume = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, resumeRef.logicalOperationIdentity(9), resumeBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            final SystemMutationResult resumedResult = shard.applySystemMutation(resume,
                    position(shardId, 2, 1_002), keyPair.getPublic());
            assertEquals(StableCode.OK, resumedResult.stableCode());
            assertEquals(AdmissionGate.OPEN, shard.getLane(lane).admissionGate());
            assertEquals(RuntimeReadiness.BLOCKED, shard.getLane(lane).runtimeReadiness());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(1, shard.discoverReady(10_000, 10).size());
            assertEquals(schedule.delayMessageId(), shard.discoverReady(10_000, 10).get(0).messageId());
        }
    }

    @Test
    void configuredControlRegistrationRejectsUnregisteredMarkerBeforeHandler() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("unregistered-control-marker"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 66);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("unregistered-control-marker-lane"));
        final byte[] laneIncarnation = bytes(16, 91);
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("unregistered-control-operation")),
                Bytes.sha256(Bytes.utf8("unregistered-control-request")), 0);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("unregistered-control-actor")),
                Bytes.sha256(Bytes.utf8("unregistered-control-roles")),
                Bytes.sha256(Bytes.utf8("unregistered-control-scope")));
        final byte[] body = applyShardControlBody(shardId, controlRef, 8, lane, laneIncarnation, 1);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                9_000, controlRef.logicalOperationIdentity(8), body, control.canonicalBytes(), 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, null, null, null,
                    new InMemoryControlTargetRegistrationAuthority());

            final SystemMutationResult result = shard.applySystemMutation(mutation, position(shardId, 0, 1_000),
                    keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, result.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, result.stableCode());
            assertNull(shard.getLane(lane));
        }
    }

    @Test
    void configuredControlRegistrationAppliesExactRegisteredMarker() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("registered-control-marker"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 67);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("registered-control-marker-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("registered-control-marker")), 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("registered-control-actor")),
                Bytes.sha256(Bytes.utf8("registered-control-roles")),
                Bytes.sha256(Bytes.utf8("registered-control-scope")));
        final InMemoryControlTargetRegistrationAuthority authority =
                new InMemoryControlTargetRegistrationAuthority();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, null, null, null,
                    authority);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            final LaneRecord laneRecord = shard.getLane(lane);
            final ControlReasonV1 reason = new ControlReasonV1(ControlReasonKindV1.OPERATOR_REQUEST, null, null);
            final ControlOperationRequestV1 request = ControlOperationRequestV1.pauseDestinationLane(reason);
            final byte[] operationId = Bytes.sha256(Bytes.utf8("registered-control-operation"));
            final ControlRef controlRef = new ControlRef(operationId,
                    PreparedControlOperationV1.requestHash(request.kind(), request), 0);
            final byte[] body = applyShardControlBody(shardId, controlRef, 8, lane,
                    laneRecord.laneIncarnation(), laneRecord.laneControlVersion());
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, controlRef.logicalOperationIdentity(8), body, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.LANE,
                    new LaneControlTargetV1(lane.bytes(), laneRecord.laneIncarnation(),
                            laneRecord.laneControlVersion()), mutation.systemMutationId(), mutation.mutationHash());
            final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(operationId,
                    request.kind(), new ControlAuthorV1(Bytes.sha256(Bytes.utf8("registered-control-author")),
                            Bytes.sha256(Bytes.utf8("registered-control-author-roles")),
                            Bytes.sha256(Bytes.utf8("registered-control-author-scope"))), request, List.of(target),
                    1, 9_000, 1, keyPair.getPrivate());
            assertEquals(io.nereusstream.delay.ownership.ControlTargetRegistrationAuthority.RegistrationResult.RECORDED,
                    authority.register(prepared));

            final SystemMutationResult result = shard.applySystemMutation(mutation, position(shardId, 1, 1_001),
                    keyPair.getPublic());
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(AdmissionGate.ADMIN_PAUSED, shard.getLane(lane).admissionGate());
        }
    }

    @Test
    void sourceOrderedLaneBreakAndCloseRequireOrderLossAcknowledgements() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-break-close"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("source-ordered-break-close"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.DELIVERY_TIME_FIFO, Bytes.utf8("strict-lane-control")), 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("break-close-actor")),
                Bytes.sha256(Bytes.utf8("break-close-roles")), Bytes.sha256(Bytes.utf8("break-close-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final LaneRecord beforeBreak = shard.getLane(lane);
            final ControlRef breakRef = new ControlRef(Bytes.sha256(Bytes.utf8("break-op")),
                    Bytes.sha256(Bytes.utf8("break-request")), 1);
            final byte[] breakBody = applyShardControlBody(shardId, breakRef, 10, lane,
                    beforeBreak.laneIncarnation(), beforeBreak.laneControlVersion());
            final SystemMutation breakMutation = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000, breakRef.logicalOperationIdentity(10),
                    breakBody, control.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(breakMutation,
                    position(shardId, 1, 1_001), keyPair.getPublic()).stableCode());
            assertEquals(AdmissionGate.ORDERING_BROKEN, shard.getLane(lane).admissionGate());
            assertEquals(0, shard.discoverReady(10_000, 10).size());

            final LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("close-op")),
                    Bytes.sha256(Bytes.utf8("close-request")), 2);
            final byte[] closeBody = applyShardControlBody(shardId, closeRef, 11, lane,
                    beforeClose.laneIncarnation(), beforeClose.laneControlVersion());
            final SystemMutation closeMutation = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000, closeRef.logicalOperationIdentity(11),
                    closeBody, control.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(closeMutation,
                    position(shardId, 2, 1_002), keyPair.getPublic()).stableCode());
            assertEquals(AdmissionGate.CLOSED, shard.getLane(lane).admissionGate());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-close-materialization"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 68);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-close-materialization"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("close-me")), 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("close-materialize-actor")),
                Bytes.sha256(Bytes.utf8("close-materialize-roles")),
                Bytes.sha256(Bytes.utf8("close-materialize-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            assertEquals(1, shard.quota().pendingMessages());
            final LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("close-materialize-op")),
                    Bytes.sha256(Bytes.utf8("close-materialize-request")), 0);
            final byte[] closeBody = applyShardControlBody(shardId, closeRef, 11, lane,
                    beforeClose.laneIncarnation(), beforeClose.laneControlVersion());
            final SystemMutation closeMutation = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000,
                    closeRef.logicalOperationIdentity(11), closeBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(closeMutation,
                    position(shardId, 1, 1_001), keyPair.getPublic()).stableCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertNotNull(shard.getLaneCloseCursor(lane));
            final List<DelayShard.LaneCloseMaterializationWork> discovered =
                    shard.discoverLaneCloseMaterialization(1);
            assertEquals(1, discovered.size());
            assertEquals(lane, discovered.get(0).laneId());
            assertEquals(0, discovered.get(0).nextEligibleAtEpochMs());
            assertThrows(IllegalArgumentException.class, () -> shard.discoverLaneCloseMaterialization(0));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.ALREADY_DEAD_LETTERED,
                    shard.apply(cancel, position(shardId, 2, 1_002)).stableCode());

            final LaneCloseMaterializer materializer = new LaneCloseMaterializer();
            final LaneCloseMaterializer.TurnResult messageTurn = materializer.runTurn(shard, 1, 1);
            assertEquals(1, messageTurn.materializedMessages());
            assertFalse(messageTurn.complete());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.LANE_CLOSED_BEFORE_ADMISSION,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertNotNull(shard.getLaneCloseCursor(lane));

            final LaneCloseMaterializer.TurnResult reservationTurn = materializer.runTurn(shard, 1, 1);
            assertEquals(0, reservationTurn.materializedReservations());
            assertTrue(reservationTurn.complete());
            assertNull(shard.getLaneCloseCursor(lane));
            assertTrue(shard.discoverLaneCloseMaterialization(1).isEmpty());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.DEAD_LETTER, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.LANE_CLOSED_BEFORE_ADMISSION,
                    reopened.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertNull(reopened.getLaneCloseCursor(lane));
        }
    }

    @Test
    void closeAbandonsUncommittedReservationWithoutReleasingQuotaTwice() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-close-reservation"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 69);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-close-reservation"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("lane-close-reservation-payload")),
                4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final PayloadProofTrustSet trustSet = new PayloadProofTrustSet(9, Map.of(2, keyPair.getPublic()));
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("close-reservation-actor")),
                Bytes.sha256(Bytes.utf8("close-reservation-roles")),
                Bytes.sha256(Bytes.utf8("close-reservation-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig, trustSet);
            assertEquals(StableCode.OK, shard.apply(prepare, position(shardId, 0, 1_000)).stableCode());
            assertEquals(1, shard.quota().reservationMessages());
            final LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("close-reservation-op")),
                    Bytes.sha256(Bytes.utf8("close-reservation-request")), 0);
            final byte[] closeBody = applyShardControlBody(shardId, closeRef, 11, lane,
                    beforeClose.laneIncarnation(), beforeClose.laneControlVersion());
            final SystemMutation closeMutation = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000,
                    closeRef.logicalOperationIdentity(11), closeBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(closeMutation,
                    position(shardId, 1, 1_001), keyPair.getPublic()).stableCode());
            assertEquals(0, shard.quota().reservationMessages());
            assertEquals(StableCode.PAYLOAD_RESERVATION_CLOSED,
                    shard.apply(PreparedCommand.commitLarge(shardId, prepare.delayMessageId(),
                                    PayloadCommitProof.signed(9, 2, shardId.routeIncarnation().bytes(),
                                            shardId.partition(), prepare.delayMessageId(), reservationId,
                                            Bytes.sha256(Bytes.utf8("close-reservation-profile")),
                                            Bytes.utf8("bucket"), Bytes.utf8("key"), Bytes.utf8("v1"), new byte[0],
                                            intent.expectedPayloadLength(), intent.payloadSha256(), 5_000,
                                            keyPair.getPrivate()), 9_000),
                            position(shardId, 2, 1_002)).stableCode());
            final DelayShard.LaneCloseMaterializationResult phase = shard.materializeClosedLane(lane, 1);
            assertFalse(phase.complete());
            assertEquals(LaneCloseMaterializationCursor.Phase.RESERVATIONS,
                    shard.getLaneCloseCursor(lane).phase());
            assertEquals(0, shard.quota().reservationMessages());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig, trustSet);
            assertEquals(LaneCloseMaterializationCursor.Phase.RESERVATIONS,
                    reopened.getLaneCloseCursor(lane).phase());
            final DelayShard.LaneCloseMaterializationResult done = reopened.materializeClosedLane(lane, 1);
            assertTrue(done.complete());
            assertEquals(PayloadReservationStatus.ABANDONED, reopened.getReservation(reservationId).status());
            assertEquals(0, reopened.quota().reservationMessages());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.reservationExpiry(intent.reservationTtlMs() + 1_000, reservationId), 5));
            assertNull(reopened.getLaneCloseCursor(lane));
        }
    }

    @Test
    void sourceOrderedTrustSetControlsPersistMarkersAndCloseFirstSeenIssuance() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("trust-set-controls"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final PayloadProofVerifierKeyV1 verifier = PayloadProofVerifierKeyV1.fromPublicKey(
                7, keyPair.getPublic(), 0, 10_000);
        final PayloadProofTrustSetSemanticV1 semantic = new PayloadProofTrustSetSemanticV1(4,
                List.of(verifier));
        final PayloadProofTrustSetControlCatalog catalog = reference -> reference.equals(semantic.ref())
                ? semantic : null;
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("trust-control-actor")),
                Bytes.sha256(Bytes.utf8("trust-control-roles")), Bytes.sha256(Bytes.utf8("trust-control-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, null, catalog);
            final ControlRef activateRef = new ControlRef(Bytes.sha256(Bytes.utf8("trust-activate-op")),
                    Bytes.sha256(Bytes.utf8("trust-activate-request")), 1);
            final byte[] activateBody = trustSetControlBody(shardId, activateRef, 12, semantic.ref(),
                    new PayloadProofTrustSetActivatePayloadV1(semantic.ref()).canonicalBytes());
            final SystemMutation activate = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, activateRef.logicalOperationIdentity(12), activateBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(activate, position(shardId, 1, 1_001),
                    keyPair.getPublic()).stableCode());
            assertTrue(shard.payloadProofTrustSetControlState().activatedAt(semantic.ref(),
                    position(shardId, 1, 1_001)));
            assertTrue(shard.payloadProofTrustSetControlState().firstSeenIssuanceOpen(semantic.ref(), 7,
                    position(shardId, 1, 1_001)));

            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("trust-close-op")),
                    Bytes.sha256(Bytes.utf8("trust-close-request")), 2);
            final PayloadProofIssuanceClosePayloadV1 closePayload = new PayloadProofIssuanceClosePayloadV1(
                    semantic.ref(), 7, new ControlReasonV1(ControlReasonKindV1.INCIDENT,
                    Bytes.sha256(Bytes.utf8("incident")), null));
            final byte[] closeBody = trustSetControlBody(shardId, closeRef, 13, semantic.ref(),
                    closePayload.canonicalBytes());
            final SystemMutation close = SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL,
                    9_000, closeRef.logicalOperationIdentity(13), closeBody, control.canonicalBytes(), 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(close, position(shardId, 2, 1_002),
                    keyPair.getPublic()).stableCode());
            assertFalse(shard.payloadProofTrustSetControlState().firstSeenIssuanceOpen(semantic.ref(), 7,
                    position(shardId, 3, 1_003)));
            assertTrue(shard.payloadProofTrustSetControlState().historicalVerificationAllowed(semantic.ref(), 7,
                    position(shardId, 3, 1_003)));
            assertNotNull(store.getValue(ColumnFamily.META, KeyCodec.metaFixed(12), 9));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults(), null, null, null, catalog);
            assertFalse(reopened.payloadProofTrustSetControlState().firstSeenIssuanceOpen(semantic.ref(), 7,
                    position(shardId, 3, 1_003)));
            assertTrue(reopened.payloadProofTrustSetControlState().historicalVerificationAllowed(semantic.ref(), 7,
                    position(shardId, 3, 1_003)));
        }
    }

    @Test
    void laneRetirementAtomicallyReplacesActiveValueWithTerminalGuard() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-terminal-guard"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final byte[] tuple = Bytes.utf8("terminal-lane-tuple");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("terminal-lane")), 9_000);
        final KafkaSourcePosition source = position(shardId, 0, 1_000);
        final ProfileRefV1 destination = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2),
                ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(bytes(4, 3), 1, bytes(32, 4),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final byte[] retirementId = bytes(32, 6);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, source).stableCode());
            // Simulate the floor-protected GC phase: no current message or
            // timeline work remains, but the lane metadata is retained.
            store.write(batch -> {
                batch.delete(ColumnFamily.ID, KeyCodec.idMessage(schedule.delayMessageId()));
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineDue(lane, 2_000,
                        source.sourceOrderToken(), schedule.delayMessageId(), 0));
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineExpiry(5_000, lane,
                        schedule.delayMessageId(), 0));
            });
            final LaneRecord closed = shard.updateLaneGate(lane, 1, AdmissionGate.CLOSED);
            final LaneTerminalGuardV1 guard = new LaneTerminalGuardV1(closed.laneIncarnation(),
                    closed.laneControlVersion(), source, destination, capability, tuple, retirementId, 1);
            final LaneRetirementProgressV1 progress = new LaneRetirementProgressV1(retirementId, 1, source);
            final KafkaSourcePosition conflictingSource = new KafkaSourcePosition(shardId, "cluster-a",
                    UUID.nameUUIDFromBytes(Bytes.utf8("topic")), source.offset(), 7,
                    source.brokerLogAppendTimeEpochMs() + 1);
            final LaneTerminalGuardV1 conflictingGuard = new LaneTerminalGuardV1(closed.laneIncarnation(),
                    closed.laneControlVersion(), conflictingSource, destination, capability, tuple, retirementId, 1);
            final LaneRetirementProgressV1 conflictingProgress = new LaneRetirementProgressV1(retirementId, 1,
                    conflictingSource);
            assertThrows(IllegalStateException.class,
                    () -> shard.retireLaneWithTerminalGuard(lane, closed.laneControlVersion(), conflictingProgress,
                            conflictingGuard));
            assertEquals(guard, shard.retireLaneWithTerminalGuard(lane, closed.laneControlVersion(), progress,
                    guard));
            assertEquals(AdmissionGate.RETIRED, shard.getLane(lane).admissionGate());
            assertEquals(guard, shard.getLaneTerminalGuard(lane));
            assertThrows(IllegalStateException.class,
                    () -> shard.updateLaneGate(lane, closed.laneControlVersion(), AdmissionGate.OPEN));
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertNotNull(reopened.getLaneTerminalGuard(lane));
            final PreparedCommand replacement = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_100, 5_100,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("must-not-reopen")), 9_000);
            assertEquals(StableCode.LANE_TERMINALLY_CLOSED,
                    reopened.apply(replacement, position(shardId, 1, 1_001)).stableCode());
        }
    }

    @Test
    void laneRetirementRejectsInflightKeyValueMismatchBeforeRetiring() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("lane-retirement-inflight-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 74);
        final byte[] tuple = Bytes.utf8("lane-retirement-inflight-mismatch-tuple");
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("lane-retirement-inflight-mismatch")), 9_000);
        final KafkaSourcePosition source = position(shardId, 0, 1_000);
        final ProfileRefV1 destination = new ProfileRefV1(bytes(4, 1), 1, bytes(32, 2),
                ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(bytes(4, 3), 1, bytes(32, 4),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final byte[] retirementId = bytes(32, 6);
        final byte[] valueAttemptId = Bytes.sha256(Bytes.utf8("lane-retirement-value-attempt"));
        final byte[] keyAttemptId = Bytes.sha256(Bytes.utf8("lane-retirement-key-attempt"));
        final PublishAttemptLedger misplaced = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                valueAttemptId, Bytes.sha256(Bytes.utf8("lane-retirement-claim")), 1, 1, lane, new byte[16],
                Bytes.sha256(Bytes.utf8("lane-retirement-owner")), new byte[16],
                Bytes.sha256(Bytes.utf8("lane-retirement-prepared")), Bytes.utf8("lane-retirement-admission"),
                source.canonicalBytes());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, source).stableCode());
            store.write(batch -> {
                batch.delete(ColumnFamily.ID, KeyCodec.idMessage(schedule.delayMessageId()));
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineDue(lane, 2_000,
                        source.sourceOrderToken(), schedule.delayMessageId(), 0));
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.timelineExpiry(5_000, lane,
                        schedule.delayMessageId(), 0));
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE,
                        KeyCodec.inflight((byte) 2, 1, keyAttemptId), misplaced.encode());
            });
            final LaneRecord closed = shard.updateLaneGate(lane, 1, AdmissionGate.CLOSED);
            final LaneTerminalGuardV1 guard = new LaneTerminalGuardV1(closed.laneIncarnation(),
                    closed.laneControlVersion(), source, destination, capability, tuple, retirementId, 1);
            final LaneRetirementProgressV1 progress = new LaneRetirementProgressV1(retirementId, 1, source);

            final IllegalStateException exception = assertThrows(IllegalStateException.class,
                    () -> shard.retireLaneWithTerminalGuard(lane, closed.laneControlVersion(), progress, guard));
            assertEquals("open publish attempt key/value mismatch", exception.getMessage());
        }
    }

    @Test
    void hardQuotaRejectsNewScheduleAndReleasesOnCancel() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("quota"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 1, 3, 1,
                1, 1_000, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("quota-lane"));
        final PreparedCommand first = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("abc")), 9_000);
        final PreparedCommand second = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_100, 5_100,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("d")), 9_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(first, position(shardId, 0, 1_000)).stableCode());
            assertEquals(1, shard.quota().pendingMessages());
            assertEquals(3, shard.quota().pendingBytes());
            assertEquals(StableCode.HARD_QUOTA_EXCEEDED,
                    shard.apply(second, position(shardId, 1, 1_001)).stableCode());
            final PreparedCommand cancel = PreparedCommand.cancel(shardId, first.delayMessageId(), 0, 9_000);
            assertEquals(StableCode.CANCELED, shard.apply(cancel, position(shardId, 2, 1_002)).stableCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.quota().pendingBytes());
            final PreparedCommand retry = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_100, 5_100,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("d")), 9_000);
            assertEquals(StableCode.SCHEDULED, shard.apply(retry, position(shardId, 3, 1_003)).stableCode());
        }
    }

    @Test
    void largePayloadPrepareCommitUsesReservationQuotaAndObjectReference() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("large"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("large-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("large")), 4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final PayloadProofTrustSet trustSet = new PayloadProofTrustSet(9, Map.of(2, keyPair.getPublic()));
        final KafkaSourcePosition preparePosition = position(shardId, 0, 1_000);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig, trustSet);
            assertEquals(StableCode.OK, shard.apply(prepare, preparePosition).stableCode());
            assertEquals(PayloadReservationStatus.RESERVED, shard.getReservation(reservationId).status());
            assertEquals(1, shard.quota().reservationMessages());
            assertEquals(8, shard.quota().reservationBytes());

            final PreparedCommand abandonedPrepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
            assertEquals(StableCode.OK,
                    shard.apply(abandonedPrepare, position(shardId, 1, 1_001)).stableCode());
            final PreparedCommand abandon = PreparedCommand.cancel(shardId, abandonedPrepare.delayMessageId(), 0,
                    9_000);
            assertEquals(StableCode.PAYLOAD_RESERVATION_ABANDONED,
                    shard.apply(abandon, position(shardId, 2, 1_002)).stableCode());
            assertEquals(1, shard.quota().reservationMessages());

            final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("object-store"), 1,
                    Bytes.sha256(Bytes.utf8("profile")), ProfileKindV1.OBJECT_STORE);
            final PayloadCommitProofV1 proof = PayloadCommitProofV1.signed(reservationId,
                    Bytes.sha256(Bytes.utf8("tenant-scope")), shardId.routeIncarnation().bytes(), shardId.partition(),
                    prepare.delayMessageId(), profile, 9, 2, Bytes.utf8("bucket"), Bytes.utf8("key"),
                    Bytes.utf8("v1"), new byte[0], intent.expectedPayloadLength(), intent.payloadSha256(), 5_000,
                    keyPair.getPrivate());
            final PreparedCommand commit = PreparedCommand.commitLargeV1(shardId, prepare.delayMessageId(),
                    reservationId, proof, 9_000);
            assertEquals(StableCode.SCHEDULED,
                    shard.apply(commit, position(shardId, 3, 1_003)).stableCode());
            final MessageRecord message = shard.getMessage(prepare.delayMessageId());
            assertNotNull(message.payloadReference());
            assertEquals(8, message.payloadLength());
            assertEquals(PayloadReservationStatus.COMMITTED, shard.getReservation(reservationId).status());
            assertEquals(1, shard.quota().pendingMessages());
            assertEquals(0, shard.quota().reservationMessages());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig, trustSet);
            assertEquals(8, reopened.getMessage(prepare.delayMessageId()).payloadLength());
            assertEquals(0, reopened.quota().reservationMessages());
        }
    }

    @Test
    void reservationLookupRejectsKeyValueIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reservation-key-mismatch"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 61);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("reservation-key-mismatch-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("reservation-key-mismatch-payload")),
                4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        final byte[] misplacedId = Bytes.sha256(Bytes.utf8("misplaced-reservation-key"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.OK, shard.apply(prepare, position(shardId, 0, 1_000)).stableCode());
            final PayloadReservation reservation = shard.getReservation(reservationId);
            store.write(batch -> {
                batch.delete(ColumnFamily.ID, KeyCodec.idReservation(reservationId));
                batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(misplacedId), reservation.encode());
            });

            assertThrows(IllegalStateException.class, () -> shard.getReservation(misplacedId));
            final PreparedCommand cancel = PreparedCommand.cancel(shardId, prepare.delayMessageId(), 0, 9_000);
            assertThrows(IllegalStateException.class,
                    () -> shard.apply(cancel, position(shardId, 1, 1_001)));
        }
    }

    @Test
    void cancelRejectsMultipleReservationsForOneMessage() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("duplicate-reservations"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 62);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("duplicate-reservations-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("duplicate-reservations-payload")),
                4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        final byte[] duplicateId = Bytes.sha256(Bytes.utf8("duplicate-reservation-id"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.OK, shard.apply(prepare, position(shardId, 0, 1_000)).stableCode());
            final PayloadReservation reservation = shard.getReservation(reservationId);
            final PayloadReservation duplicate = new PayloadReservation(shardId, duplicateId,
                    reservation.commandId(), reservation.delayMessageId(), reservation.commandHash(), reservation.intent(),
                    reservation.reservationExpiryEpochMs(), PayloadReservationStatus.RESERVED,
                    reservation.stateVersion(), reservation.sourcePosition(), null);
            store.write(batch -> batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(duplicateId),
                    duplicate.encode()));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, prepare.delayMessageId(), 0, 9_000);
            assertThrows(IllegalStateException.class,
                    () -> shard.apply(cancel, position(shardId, 1, 1_001)));
        }
    }

    @Test
    void reservationExpiryDiscoveryRejectsStaleIdProjection() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("stale-reservation-expiry"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 63);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("stale-reservation-expiry-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("stale-reservation-expiry-payload")),
                4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.OK, shard.apply(prepare, position(shardId, 0, 1_000)).stableCode());
            final PayloadReservation reservation = shard.getReservation(reservationId);
            final PayloadReservation stale = new PayloadReservation(shardId, reservation.reservationId(),
                    reservation.commandId(), reservation.delayMessageId(), reservation.commandHash(), reservation.intent(),
                    reservation.reservationExpiryEpochMs(), PayloadReservationStatus.ABANDONED,
                    Math.addExact(reservation.stateVersion(), 1), reservation.sourcePosition(), null);
            store.write(batch -> batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(reservationId),
                    stale.encode()));

            assertThrows(IllegalStateException.class, () -> shard.discoverReservationExpiry(5_000, 10));
        }
    }

    @Test
    void timeFenceOverlaysReservedPayloadAndBoundedCursorMaterializesExpiry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reservation-fence"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("reservation-fence-lane"));
        final LargeScheduleIntent intent = new LargeScheduleIntent(lane, 2_000, 5_000,
                OrderingMode.BEST_EFFORT, 8, Bytes.sha256(Bytes.utf8("reservation-fence-payload")), 4_000, 9);
        final PreparedCommand prepare = PreparedCommand.prepareLarge(shardId, intent, 9_000);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final PayloadProofTrustSet trustSet = new PayloadProofTrustSet(9, Map.of(2, keyPair.getPublic()));
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"),
                prepare.commandId().bytes(), prepare.delayMessageId().bytes(), prepare.commandHash());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig, trustSet);
            assertEquals(StableCode.OK, shard.apply(prepare, position(shardId, 0, 1_000)).stableCode());
            assertEquals(PayloadReservationStatus.RESERVED, shard.getReservation(reservationId).status());
            final ReservationQuerySnapshot reserved = shard.queryReservationSnapshot(reservationId);
            assertEquals(PayloadReservationStatus.RESERVED, reserved.status());
            assertEquals(PayloadAvailability.UPLOAD_PENDING, reserved.payloadAvailability());

            final TrustedUtcIntervalEvidence fenceProof = new TrustedUtcIntervalEvidence(5_000, 5_000,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("reservation-fence-clock"),
                    1, 1, 1, Bytes.sha256(Bytes.utf8("reservation-fence-proof")), 0, null);
            final byte[] proofId = Bytes.sha256(Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                    shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()), Bytes.i64be(5_000),
                    Bytes.u32be(1), Bytes.lp32(fenceProof.canonicalBytes()));
            final AuthorIdentity fenceAuthor = AuthorIdentity.fence(Bytes.utf8("reservation-fence-writer"), 1);
            final SystemMutation fence = SystemMutation.signed(shardId, SystemMutationType.TIME_FENCE, 9_000,
                    proofId, timeFenceBody(shardId, 5_000, 1, proofId, fenceProof.canonicalBytes()),
                    fenceAuthor.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(fence,
                    position(shardId, 1, 1_001), keyPair.getPublic()).stableCode());
            assertEquals(PayloadReservationStatus.EXPIRED, shard.getReservation(reservationId).status());
            assertEquals(PayloadReservationStatus.EXPIRED,
                    shard.queryReservationSnapshot(reservationId).status());
            assertEquals(PayloadAvailability.NOT_APPLICABLE,
                    shard.queryReservationSnapshot(reservationId).payloadAvailability());
            assertEquals(1, shard.quota().reservationMessages());
            assertEquals(1, shard.discoverReservationExpiry(5_000, 10).size());

            final PayloadCommitProof proof = PayloadCommitProof.signed(9, 2, shardId.routeIncarnation().bytes(),
                    shardId.partition(), prepare.delayMessageId(), reservationId,
                    Bytes.sha256(Bytes.utf8("reservation-fence-profile")), Bytes.utf8("bucket"),
                    Bytes.utf8("reservation-fence-key"), Bytes.utf8("v1"), new byte[0],
                    intent.expectedPayloadLength(), intent.payloadSha256(), 5_000, keyPair.getPrivate());
            final PreparedCommand commit = PreparedCommand.commitLarge(shardId, prepare.delayMessageId(), proof,
                    9_000);
            assertEquals(StableCode.RESERVATION_EXPIRED,
                    shard.apply(commit, position(shardId, 2, 1_002)).stableCode());

            final PayloadReservation materialized = shard.materializeReservationExpiry(reservationId);
            assertEquals(PayloadReservationStatus.EXPIRED, materialized.status());
            assertEquals(0, shard.quota().reservationMessages());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.reservationExpiry(5_000, reservationId), 5));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig, trustSet);
            assertEquals(PayloadReservationStatus.EXPIRED, reopened.getReservation(reservationId).status());
            assertEquals(0, reopened.quota().reservationMessages());
        }
    }

    @Test
    void publishAdmissionAndUnknownOutcomeMoveOneAttemptAcrossInflightKeysAtomically() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("attempt-ledger"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("attempt-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("attempt")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("attempt-id"));
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-id"));
        final byte[] ownerIdentity = Bytes.sha256(Bytes.utf8("owner"));
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("prepared-publish"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(command.delayMessageId(), 0,
                    attemptId, claimId, 42, 1, lane, new byte[16], ownerIdentity,
                    store.metadata().storeIncarnation(), preparedHash, Bytes.utf8("admission-body"),
                    admissionPosition.canonicalBytes());

            assertEquals(admission, shard.admitPublishAttempt(admission, admissionPosition));
            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(command.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.PUBLISHING,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(1, shard.getMessage(command.delayMessageId()).runtimeIndex().attemptObligations().size());
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(), command.delayMessageId(), 0),
                    1));
            assertNotNull(store.getValue(ColumnFamily.INFLIGHT, admission.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertEquals(admission.obligationRef(),
                    AttemptObligationRef.decode(admission.obligationRef().canonicalBytes()));
            assertEquals(admission, PublishAttemptLedger.decode(admission.encode()));

            final PublishAttemptLedger uncertain = shard.applyUnknownPublishOutcome(attemptId, 42,
                    Bytes.utf8("unknown-outcome"), Bytes.utf8("timeout-evidence"), outcomePosition);
            assertEquals(AttemptLedgerState.UNCERTAIN, uncertain.state());
            assertEquals(uncertain, PublishAttemptLedger.decode(uncertain.encode()));
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(uncertain, shard.findOpenPublishAttempt(attemptId));
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(command.delayMessageId()).status());
            assertEquals(GenerationAggregateState.UNCERTAIN,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.NONE,
                    shard.getMessage(command.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(1, shard.getMessage(command.delayMessageId()).runtimeIndex().attemptObligations().size());
            assertNull(store.getValue(ColumnFamily.INFLIGHT, admission.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertNotNull(store.getValue(ColumnFamily.INFLIGHT, uncertain.encodedKey(), PublishAttemptLedger.VALUE_TYPE));
            assertArrayEquals(outcomePosition.canonicalBytes(), uncertain.sourcePosition());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.findOpenPublishAttempt(attemptId).state());
            assertEquals(outcomePosition, reopened.lastAppliedSourcePosition());
        }
    }

    @Test
    void verifiedPublishSuccessClosesPublishingLedgerAndRetainsTerminalHistory() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("attempt-success"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("success-lane"));
        final PreparedCommand command = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("success")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("success-attempt"));
        final PublishAttemptLedger[] holder = new PublishAttemptLedger[1];
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(command, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            holder[0] = PublishAttemptLedger.publishing(command.delayMessageId(), 0, attemptId,
                    Bytes.sha256(Bytes.utf8("success-claim")), 7, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("success-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("success-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(holder[0], admissionPosition);
            assertEquals(MessageStatus.PUBLISHED,
                    shard.applyPublishedPublishOutcome(attemptId, 7, outcomePosition).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(command.delayMessageId()).status());
            assertEquals(MessageStatus.PUBLISHED,
                    shard.getTerminalGeneration(command.delayMessageId(), 0).status());
            assertEquals(0, shard.quota().pendingMessages());
        }
    }

    @Test
    void sourceOrderedExpireMutationAtomicallyClosesScheduledGenerationAndDedupes() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-expiry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 12);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("expiry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("expiry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition expiryPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition duplicatePosition = position(shardId, 2, 1_002);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(5_000, 5_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("expiry-proof")), 0, null);
        final byte[] body = expiryBody(shardId, schedule.delayMessageId(), 0, 5_000, proof.canonicalBytes());
        final byte[] author = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.EXPIRE_GENERATION, 9_000,
                Bytes.sha256(Bytes.utf8("expiry-operation")), body, author, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(mutation, expiryPosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.EXPIRED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.ALREADY_EXPIRED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
            final SystemMutationResult duplicate = shard.applySystemMutation(mutation, duplicatePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, duplicate.stableCode());
            assertEquals(duplicate, shard.applySystemMutation(mutation, duplicatePosition, keyPair.getPublic()));
            assertEquals(duplicatePosition, shard.lastAppliedSourcePosition());
            assertArrayEquals(expiryPosition.canonicalBytes(), shard.getSystemMutationResult(mutation.systemMutationId())
                    .appliedSourcePosition());
            assertThrows(IllegalStateException.class,
                    () -> shard.applySystemMutation(mutation, position(shardId, 2, 1_003), keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.EXPIRED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.OK,
                    reopened.getSystemMutationResult(mutation.systemMutationId()).stableCode());
        }
    }

    @Test
    void sourceOrderedPublishOutcomeAtomicallyMovesAttemptToUncertainAndRetainsDedupe() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-unknown"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 13);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("outcome-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("outcome")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-outcome-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("outcome-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-outcome-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(mutation, outcomePosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.findOpenPublishAttempt(attemptId).state());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.getSystemMutationResult(mutation.systemMutationId()).stableCode());
        }
    }

    @Test
    void sourceOrderedUnknownOutcomeMaterializesPinnedPolicyUncertainRetry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-uncertain-retry"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("uncertain-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("policy-uncertain-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 5, 5,
                Bytes.sha256(Bytes.utf8("uncertain-retry-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);
        assertEquals(2, parsed.retryDecision().kind());
        assertEquals(3_000, parsed.retryDecision().nextRetryAt());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("policy-uncertain-retry-operation")), body, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("policy-uncertain-retry-claim")), 42, 1, lane,
                    new byte[16], Bytes.sha256(Bytes.utf8("policy-uncertain-retry-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("policy-uncertain-retry-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, result.stableCode());
            final MessageRecord retry = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(GenerationAggregateState.UNCERTAIN, retry.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE, retry.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.PINNED_POLICY,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertEquals(2, retry.runtimeIndex().timeline().candidateAttemptNo());
            assertEquals(3_000, retry.retryEligibilityAtEpochMs());
            assertEquals(1, retry.runtimeIndex().admissionsUsed());
            assertEquals(0, retry.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(0, shard.discoverDue(2_999, 10).size());
            assertEquals(1, shard.discoverDue(3_000, 10).size());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord retry = reopened.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.PINNED_POLICY,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.getPublishAttempt(attemptId, 42).state());
            assertEquals(1, reopened.discoverDue(3_000, 10).size());
        }
    }

    @Test
    void sourceOrderedPublishedEvidenceRemovesUncertainRetryWorkAndTerminalizes() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("system-published-evidence-with-retry-work"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 29);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("published-evidence-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("published-evidence-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolvePosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("published-evidence-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 5, 5,
                Bytes.sha256(Bytes.utf8("published-evidence-retry-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("published-evidence-retry-unknown")), unknownBody,
                owner.canonicalBytes(), 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("published-evidence-retry-claim")), 42, 1, lane,
                    laneIncarnation, Bytes.sha256(Bytes.utf8("published-evidence-retry-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("published-evidence-retry-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            assertEquals(CurrentSendWorkKind.TIMELINE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());

            final ControlRef controlRef = new ControlRef(
                    Bytes.sha256(Bytes.utf8("published-evidence-retry-operation")),
                    Bytes.sha256(Bytes.utf8("published-evidence-retry-request")), 1);
            final byte[] resolveBody = resolveUncertainEvidenceBody(shardId, controlRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, 1, publishEvidence(attemptId, true, StableCode.OK));
            final SystemMutation resolve = SystemMutation.signed(shardId,
                    SystemMutationType.RESOLVE_UNCERTAIN, 9_000,
                    controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), resolveBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertTrue(shard.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertNull(shard.getPublishAttempt(attemptId, 42));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            assertEquals(MessageStatus.PUBLISHED, reopened.getMessage(schedule.delayMessageId()).status());
            assertTrue(reopened.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertNull(reopened.getPublishAttempt(attemptId, 42));
            assertEquals(0, reopened.quota().pendingMessages());
        }
    }

    @Test
    void sourceOrderedResolveUncertainNotPublishedEvidenceNormalizesDefinitiveRetry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("system-not-published-evidence-retry"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 35);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("not-published-evidence-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("not-published-evidence")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolvePosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("not-published-evidence-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 9, 9,
                Bytes.sha256(Bytes.utf8("not-published-evidence-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("not-published-evidence-unknown")), unknownBody,
                owner.canonicalBytes(), 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("not-published-evidence-claim")), 42, 1, lane,
                    laneIncarnation, Bytes.sha256(Bytes.utf8("not-published-evidence-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("not-published-evidence-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            assertEquals(CurrentSendWorkKind.NONE,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());

            final ControlRef controlRef = new ControlRef(
                    Bytes.sha256(Bytes.utf8("not-published-evidence-operation")),
                    Bytes.sha256(Bytes.utf8("not-published-evidence-request")), 7);
            final byte[] resolveBody = resolveUncertainEvidenceBody(shardId, controlRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, 2,
                    publishEvidence(attemptId, false, StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED));
            final SystemMutation resolve = SystemMutation.signed(shardId,
                    SystemMutationType.RESOLVE_UNCERTAIN, 9_000,
                    controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), resolveBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord retry = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(GenerationAggregateState.RETRY_WAIT, retry.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE, retry.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(2_000, retry.retryEligibilityAtEpochMs());
            assertNull(shard.getPublishAttempt(attemptId, 42));
            assertEquals(1, shard.quota().pendingMessages());
            assertTrue(shard.quota().pendingBytes() > 0);
            assertEquals(1, shard.discoverDue(2_000, 10).size());
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord retry = reopened.getMessage(schedule.delayMessageId());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(1, reopened.discoverDue(2_000, 10).size());
            assertNull(reopened.getPublishAttempt(attemptId, 42));
        }
    }

    @Test
    void sourceOrderedResolveUncertainRetryMaterializesControlOverrideTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-control-uncertain-retry"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 0);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("control-uncertain-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("control-uncertain-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolvePosition = position(shardId, 3, 2_001);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("control-uncertain-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 7, 7,
                Bytes.sha256(Bytes.utf8("control-uncertain-retry-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("control-uncertain-retry-unknown")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("resolve-operation")),
                Bytes.sha256(Bytes.utf8("resolve-request")), 4);
        final byte[] acknowledgementHash = Bytes.sha256(Bytes.utf8("possible-duplicate-ack"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("control-uncertain-retry-claim")), 42, 1, lane,
                    laneIncarnation, Bytes.sha256(Bytes.utf8("control-uncertain-retry-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("control-uncertain-retry-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final byte[] body = resolveUncertainRetryBody(shardId, controlRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, acknowledgementHash);
            final SystemMutation resolve = SystemMutation.signed(shardId, SystemMutationType.RESOLVE_UNCERTAIN,
                    9_000, controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), body,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord retry = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, retry.status());
            assertEquals(GenerationAggregateState.UNCERTAIN, retry.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE, retry.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.CONTROL_OVERRIDE,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertArrayEquals(controlRef.canonicalBytes(), retry.runtimeIndex().timeline().uncertainRetryControl());
            assertArrayEquals(resolvePosition.canonicalBytes(),
                    retry.runtimeIndex().timeline().uncertainRetryControlPosition());
            assertEquals(2_001, retry.retryEligibilityAtEpochMs());
            assertEquals(2, retry.runtimeIndex().timeline().candidateAttemptNo());
            assertEquals(1, retry.runtimeIndex().admissionsUsed());
            assertEquals(0, retry.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(0, shard.discoverDue(2_000, 10).size());
            assertEquals(1, shard.discoverDue(2_001, 10).size());
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord retry = reopened.getMessage(schedule.delayMessageId());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, retry.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.CONTROL_OVERRIDE,
                    retry.runtimeIndex().timeline().uncertainRetryAuthority());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.getPublishAttempt(attemptId, 42).state());
        }
    }

    @Test
    void sourceOrderedNotPublishedEvidenceRevokesClaimAndNormalizesDefinitiveRetry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("system-not-published-evidence-claimed"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 0);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 36);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("not-published-evidence-claimed-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("not-published-evidence-claimed")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition retryPosition = position(shardId, 3, 2_001);
        final KafkaSourcePosition evidencePosition = position(shardId, 4, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("not-published-evidence-claimed-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 10, 10,
                Bytes.sha256(Bytes.utf8("not-published-evidence-claimed-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("not-published-evidence-claimed-unknown")), unknownBody,
                owner.canonicalBytes(), 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("not-published-evidence-claimed-claim")), 42, 1, lane,
                    laneIncarnation, owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("not-published-evidence-claimed-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final ControlRef retryRef = new ControlRef(Bytes.sha256(Bytes.utf8("claimed-retry-operation")),
                    Bytes.sha256(Bytes.utf8("claimed-retry-request")), 8);
            final byte[] retryBody = resolveUncertainRetryBody(shardId, retryRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, Bytes.sha256(Bytes.utf8("claimed-retry-ack")));
            final SystemMutation retry = SystemMutation.signed(shardId, SystemMutationType.RESOLVE_UNCERTAIN,
                    9_000, retryRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), retryBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(retry, retryPosition, keyPair.getPublic()).stableCode());

            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY.wireValue(),
                    ClaimResultBody.decodePrecondition(claim.preconditionBytes()).sourceWorkKind());
            assertEquals(CurrentSendWorkKind.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertNotNull(shard.getClaim(claim.claimId(), owner.generation()));

            final ControlRef evidenceRef = new ControlRef(
                    Bytes.sha256(Bytes.utf8("claimed-not-published-operation")),
                    Bytes.sha256(Bytes.utf8("claimed-not-published-request")), 9);
            final byte[] evidenceBody = resolveUncertainEvidenceBody(shardId, evidenceRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, 2,
                    publishEvidence(attemptId, false, StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED));
            final SystemMutation evidence = SystemMutation.signed(shardId,
                    SystemMutationType.RESOLVE_UNCERTAIN, 9_000,
                    evidenceRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), evidenceBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(evidence, evidencePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord next = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, next.status());
            assertEquals(GenerationAggregateState.RETRY_WAIT, next.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.TIMELINE, next.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, next.runtimeIndex().timeline().workKind());
            assertEquals(2_002, next.retryEligibilityAtEpochMs());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(shard.getPublishAttempt(attemptId, owner.generation()));
            assertEquals(result, shard.applySystemMutation(evidence, evidencePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord next = reopened.getMessage(schedule.delayMessageId());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, next.runtimeIndex().timeline().workKind());
            assertEquals(2_002, next.retryEligibilityAtEpochMs());
            assertEquals(1, reopened.discoverDue(2_002, 10).size());
        }
    }

    @Test
    void sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("system-not-published-evidence-remaining-uncertain"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 37);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("not-published-evidence-remaining")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition firstAdmissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition firstUnknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition secondAdmissionPosition = position(shardId, 3, 3_001);
        final KafkaSourcePosition secondUnknownPosition = position(shardId, 4, 3_002);
        final KafkaSourcePosition retryPosition = position(shardId, 5, 4_001);
        final KafkaSourcePosition evidencePosition = position(shardId, 6, 4_002);
        final byte[] firstAttemptId = Bytes.sha256(Bytes.utf8("not-published-evidence-remaining-first"));
        final byte[] secondAttemptId = Bytes.sha256(Bytes.utf8("attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                Bytes.sha256(Bytes.utf8("lease")));
        final TrustedUtcIntervalEvidence firstObservedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 11, 11,
                Bytes.sha256(Bytes.utf8("remaining-first-proof")), 0, null);
        final TrustedUtcIntervalEvidence secondObservedAt = new TrustedUtcIntervalEvidence(3_002, 3_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 12, 12,
                Bytes.sha256(Bytes.utf8("remaining-second-proof")), 0, null);
        final byte[] firstUnknownBody = publishOutcomeBody(shardId, firstAttemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], firstObservedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final byte[] secondUnknownBody = publishOutcomeBody(shardId, secondAttemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], secondObservedAt.canonicalBytes());
        final SystemMutation firstUnknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME,
                9_000, Bytes.sha256(Bytes.utf8("remaining-first-unknown")), firstUnknownBody,
                owner.canonicalBytes(), 1, keyPair.getPrivate());
        final SystemMutation secondUnknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME,
                9_000, Bytes.sha256(Bytes.utf8("remaining-second-unknown")), secondUnknownBody,
                owner.canonicalBytes(), 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger firstAdmission = PublishAttemptLedger.publishing(schedule.delayMessageId(),
                    0, firstAttemptId, Bytes.sha256(Bytes.utf8("remaining-first-claim")), owner.generation(), 1,
                    lane, laneIncarnation, owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("remaining-first-prepared")), Bytes.utf8("admission"),
                    firstAdmissionPosition.canonicalBytes());
            shard.admitPublishAttempt(firstAdmission, firstAdmissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(firstUnknown, firstUnknownPosition, keyPair.getPublic()).stableCode());

            final MessageRecord firstRetry = shard.getMessage(schedule.delayMessageId());
            final Fixture secondFixture = Fixture.createForSource(shardId, schedule.delayMessageId(),
                    laneIncarnation, firstRetry.runtimeIndex().timeline().encodedTimelineKey(), 3, 1, 0,
                    GenerationRuntimeIndex.obligationSetDigest(firstRetry.runtimeIndex().attemptObligations()),
                    firstRetry.runtimeIndex().timeline().semanticWorkDigest(), 2, firstRetry.stateVersion());
            final SystemMutation secondAdmission = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_ADMISSION, 9_000,
                    Bytes.sha256(Bytes.utf8("remaining-second-admission")), secondFixture.body(), secondFixture.owner(),
                    1, keyPair.getPrivate());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(secondAdmission, secondAdmissionPosition, keyPair.getPublic())
                            .stableCode());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(secondUnknown, secondUnknownPosition, keyPair.getPublic()).stableCode());
            assertEquals(2, shard.getMessage(schedule.delayMessageId()).runtimeIndex().attemptObligations().size());

            final ControlRef retryRef = new ControlRef(Bytes.sha256(Bytes.utf8("remaining-retry-operation")),
                    Bytes.sha256(Bytes.utf8("remaining-retry-request")), 10);
            final byte[] retryBody = resolveUncertainRetryBody(shardId, retryRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, secondAttemptId, Bytes.sha256(Bytes.utf8("remaining-retry-ack")));
            final SystemMutation retry = SystemMutation.signed(shardId, SystemMutationType.RESOLVE_UNCERTAIN,
                    9_000, retryRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), retryBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")), Bytes.sha256(Bytes.utf8("role")),
                            Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(retry, retryPosition, keyPair.getPublic()).stableCode());

            final TimelineWorkRef claimedWork = shard.getMessage(schedule.delayMessageId()).runtimeIndex().timeline();
            final ClaimRecord firstClaim = shard.claimForPublish(schedule.delayMessageId(), owner, 4_500,
                    new byte[0], chargeVector());
            final MessageRecord revoked = shard.revokeClaim(firstClaim.claimId(), owner.generation());
            assertEquals(MessageStatus.SCHEDULED, revoked.status());
            assertEquals(CurrentSendWorkKind.TIMELINE, revoked.runtimeIndex().currentWorkKind());
            assertEquals(TimelineWorkKind.UNCERTAIN_RETRY, revoked.runtimeIndex().timeline().workKind());
            assertEquals(UncertainRetryAuthority.CONTROL_OVERRIDE,
                    revoked.runtimeIndex().timeline().uncertainRetryAuthority());
            assertArrayEquals(claimedWork.semanticWorkDigest(),
                    revoked.runtimeIndex().timeline().semanticWorkDigest());
            assertArrayEquals(claimedWork.uncertainRetryControl(),
                    revoked.runtimeIndex().timeline().uncertainRetryControl());
            assertArrayEquals(claimedWork.uncertainRetryControlPosition(),
                    revoked.runtimeIndex().timeline().uncertainRetryControlPosition());
            assertNull(shard.getClaim(firstClaim.claimId(), owner.generation()));

            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 4_500,
                    new byte[0], chargeVector());
            final long claimedStateVersion = shard.getMessage(schedule.delayMessageId()).stateVersion();
            assertEquals(CurrentSendWorkKind.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertNotNull(shard.getClaim(claim.claimId(), owner.generation()));

            final ControlRef evidenceRef = new ControlRef(
                    Bytes.sha256(Bytes.utf8("remaining-not-published-operation")),
                    Bytes.sha256(Bytes.utf8("remaining-not-published-request")), 11);
            final byte[] evidenceBody = resolveUncertainEvidenceBody(shardId, evidenceRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, firstAttemptId, 2,
                    publishEvidence(firstAttemptId, false, StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED));
            final SystemMutation evidence = SystemMutation.signed(shardId,
                    SystemMutationType.RESOLVE_UNCERTAIN, 9_000,
                    evidenceRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), evidenceBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")), Bytes.sha256(Bytes.utf8("role")),
                            Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult result = shard.applySystemMutation(evidence, evidencePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord uncertain = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.UNCERTAIN, uncertain.status());
            assertEquals(Math.addExact(claimedStateVersion, 1), uncertain.stateVersion());
            assertEquals(GenerationAggregateState.UNCERTAIN, uncertain.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.NONE, uncertain.runtimeIndex().currentWorkKind());
            assertEquals(1, uncertain.runtimeIndex().attemptObligations().size());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(shard.getPublishAttempt(firstAttemptId, owner.generation()));
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(secondAttemptId,
                    owner.generation()).state());
            assertEquals(result, shard.applySystemMutation(evidence, evidencePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            assertEquals(MessageStatus.UNCERTAIN, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.NONE,
                    reopened.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertEquals(1, reopened.getMessage(schedule.delayMessageId()).runtimeIndex().attemptObligations().size());
            assertEquals(AttemptLedgerState.UNCERTAIN,
                    reopened.getPublishAttempt(secondAttemptId, owner.generation()).state());
        }
    }

    @Test
    void sourceOrderedResolveUncertainPublishedEvidenceSettlesExactObligation() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-control-published-evidence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 34);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("control-published-evidence-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("control-published-evidence")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolvePosition = position(shardId, 3, 2_001);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("control-published-evidence-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 8, 8,
                Bytes.sha256(Bytes.utf8("published-evidence-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("published-evidence-unknown")), unknownBody, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("published-evidence-claim")), 42, 1, lane,
                    laneIncarnation, Bytes.sha256(Bytes.utf8("published-evidence-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("published-evidence-prepared")),
                    Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("published-evidence-operation")),
                    Bytes.sha256(Bytes.utf8("published-evidence-request")), 6);
            final byte[] evidence = publishEvidence(attemptId, true, StableCode.OK);
            final byte[] body = resolveUncertainEvidenceBody(shardId, controlRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, 1, evidence);
            final SystemMutation resolve = SystemMutation.signed(shardId, SystemMutationType.RESOLVE_UNCERTAIN,
                    9_000, controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), body,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getPublishAttempt(attemptId, 42));
            assertEquals(MessageStatus.PUBLISHED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.PUBLISHED, reopened.getMessage(schedule.delayMessageId()).status());
            assertNull(reopened.getPublishAttempt(attemptId, 42));
            assertEquals(0, reopened.quota().pendingMessages());
        }
    }

    @Test
    void sourceOrderedResolveUncertainTerminalizesPossibleDeliveryAndRetainsObligation() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-control-terminal"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 33);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("control-terminal-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("control-terminal")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolvePosition = position(shardId, 3, 2_100);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("control-terminal-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 7, 7,
                Bytes.sha256(Bytes.utf8("control-terminal-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("control-terminal-unknown")), unknownBody, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final byte[] laneIncarnation = shard.getLane(lane).laneIncarnation();
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("control-terminal-claim")), 42, 1, lane, laneIncarnation,
                    Bytes.sha256(Bytes.utf8("control-terminal-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("control-terminal-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("terminal-operation")),
                    Bytes.sha256(Bytes.utf8("terminal-request")), 5);
            final byte[] body = resolvePossibleDeliveryTerminalBody(shardId, controlRef, lane, laneIncarnation,
                    schedule.delayMessageId(), 0, attemptId, Bytes.sha256(Bytes.utf8("terminal-ack")));
            final SystemMutation resolve = SystemMutation.signed(shardId, SystemMutationType.RESOLVE_UNCERTAIN,
                    9_000, controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), body,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
            final MessageRecord terminal = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.DEAD_LETTER, terminal.status());
            assertEquals(CurrentSendWorkKind.NONE, terminal.runtimeIndex().currentWorkKind());
            assertTrue(terminal.runtimeIndex().possibleDestinationDuplicate());
            assertEquals(1, terminal.runtimeIndex().attemptObligations().size());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(1, shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations().size());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverExpiry(5_000, 10).size());
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.DEAD_LETTER, reopened.getMessage(schedule.delayMessageId()).status());
            assertTrue(reopened.getMessage(schedule.delayMessageId()).runtimeIndex()
                    .possibleDestinationDuplicate());
            assertEquals(AttemptLedgerState.UNCERTAIN, reopened.getPublishAttempt(attemptId, 42).state());
            assertEquals(0, reopened.quota().pendingMessages());
        }
    }

    @Test
    void sourceOrderedAdmissionConsumesUncertainRetryBudgetOnlyForTheNewAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-uncertain-admission"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 3, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 29);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("uncertain-admission")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition retryAdmissionPosition = position(shardId, 3, 3_001);
        final byte[] firstAttemptId = Bytes.sha256(Bytes.utf8("uncertain-admission-first"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 6, 6,
                Bytes.sha256(Bytes.utf8("uncertain-admission-proof")), 0, null);
        final byte[] outcomeBody = publishOutcomeBody(shardId, firstAttemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes(),
                unknownRetryDecision(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, 3_000));
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("uncertain-admission-unknown")), outcomeBody, owner, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger firstAdmission = PublishAttemptLedger.publishing(schedule.delayMessageId(),
                    0, firstAttemptId, Bytes.sha256(Bytes.utf8("uncertain-admission-claim")), 42, 1, lane,
                    LaneRecord.initial(lane, schedulePosition).laneIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-admission-owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("uncertain-admission-prepared")), Bytes.utf8("admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(firstAdmission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final MessageRecord uncertainRetry = shard.getMessage(schedule.delayMessageId());
            final byte[] retryTimelineKey = uncertainRetry.runtimeIndex().timeline().encodedTimelineKey();
            final TimelineWorkRef retryWork = uncertainRetry.runtimeIndex().timeline();
            final Fixture fixture = Fixture.createForSource(shardId,
                    schedule.delayMessageId(), LaneRecord.initial(lane, schedulePosition).laneIncarnation(),
                    retryTimelineKey, 3, 1, 0,
                    GenerationRuntimeIndex.obligationSetDigest(uncertainRetry.runtimeIndex().attemptObligations()),
                    retryWork.semanticWorkDigest(), 2, uncertainRetry.stateVersion());
            final SystemMutation retryAdmission = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_ADMISSION, 9_000,
                    Bytes.sha256(Bytes.utf8("uncertain-admission-retry")), fixture.body(), fixture.owner(), 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(retryAdmission, retryAdmissionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            final MessageRecord publishing = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.PUBLISHING, publishing.status());
            assertEquals(GenerationAggregateState.UNCERTAIN, publishing.runtimeIndex().aggregateState());
            assertEquals(CurrentSendWorkKind.PUBLISHING, publishing.runtimeIndex().currentWorkKind());
            assertEquals(2, publishing.runtimeIndex().admissionsUsed());
            assertEquals(1, publishing.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(2, publishing.runtimeIndex().attemptObligations().size());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(firstAttemptId, 42).state());
            final PublishAdmissionBody admitted = PublishAdmissionBody.decode(fixture.body());
            assertEquals(AttemptLedgerState.PUBLISHING,
                    shard.getPublishAttempt(admitted.publishAttemptId(), 7).state());
            assertEquals(result, shard.getSystemMutationResult(retryAdmission.systemMutationId()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig);
            final MessageRecord publishing = reopened.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.PUBLISHING, publishing.status());
            assertEquals(2, publishing.runtimeIndex().admissionsUsed());
            assertEquals(1, publishing.runtimeIndex().uncertainRetryAdmissionsUsed());
            assertEquals(2, publishing.runtimeIndex().attemptObligations().size());
        }
    }

    @Test
    void sourceOrderedEvidenceResolutionClosesUncertainAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-evidence-resolution"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("evidence-resolution-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("evidence")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolutionPosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-resolution-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence unknownObserved = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("unknown-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], unknownObserved.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("resolution-unknown-operation")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final byte[] resolutionBody = evidenceResolutionBody(shardId, attemptId, StableCode.OK, 1, 0,
                new TrustedUtcIntervalEvidence(1_003, 1_003,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                        Bytes.sha256(Bytes.utf8("resolution-proof")), 0, null).canonicalBytes());
        final byte[] service = AuthorIdentity.service(Bytes.utf8("evidence-service"), Bytes.utf8("run"), 1)
                .canonicalBytes();
        final SystemMutation resolution = SystemMutation.signed(shardId, SystemMutationType.EVIDENCE_RESOLUTION,
                9_000, Bytes.sha256(Bytes.utf8("resolution-operation")), resolutionBody, service, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(schedule.delayMessageId()).status());

            final SystemMutationResult result = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(MessageStatus.PUBLISHED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(result, shard.getSystemMutationResult(resolution.systemMutationId()));
        }
    }

    @Test
    void sourceOrderedEvidenceResolutionRequeuesVerifiedNotPublishedAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-evidence-retry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("evidence-retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("evidence-retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition resolutionPosition = position(shardId, 3, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-resolution-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence unknownObserved = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 2,
                Bytes.sha256(Bytes.utf8("unknown-retry-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], unknownObserved.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("resolution-retry-unknown-operation")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final byte[] resolutionBody = evidenceResolutionBody(shardId, attemptId,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2, 1,
                new TrustedUtcIntervalEvidence(2_002, 2_002,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                        Bytes.sha256(Bytes.utf8("resolution-retry-proof")), 0, null).canonicalBytes());
        final byte[] service = AuthorIdentity.service(Bytes.utf8("evidence-service"), Bytes.utf8("run"), 1)
                .canonicalBytes();
        final SystemMutation resolution = SystemMutation.signed(shardId, SystemMutationType.EVIDENCE_RESOLUTION,
                9_000, Bytes.sha256(Bytes.utf8("resolution-retry-operation")), resolutionBody, service, 1,
                keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());

            final SystemMutationResult result = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(1, shard.discoverDue(2_002, 10).size());
        }
    }

    @Test
    void sourceOrderedPublishSuccessUsesEvidenceAndClosesAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-published"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 14);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("published-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("published")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 1_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-published-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 7,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 3, 3,
                Bytes.sha256(Bytes.utf8("published-proof")), 0, null);
        final byte[] body = publishOutcomeBody(shardId, attemptId, 1, 0, StableCode.OK,
                nestedPlaceholder(), observedAt.canonicalBytes());
        final PublishOutcomeBody parsedOutcome = PublishOutcomeBody.decode(body);
        assertEquals(1, parsedOutcome.retryDecision().kind());
        assertEquals(1, parsedOutcome.retryDecision().completedAttemptNo());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-published-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 7, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(StableCode.OK,
                    shard.applySystemMutation(mutation, outcomePosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.findOpenPublishAttempt(attemptId));
        }
    }

    @Test
    void sourceOrderedNotPublishedRetriableClosesAttemptAndRequeuesBestEffortWork() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-retry"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 16);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("retry-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("retry")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-retry-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2_002);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-retry-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().admissionsUsed());

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).runtimeIndex().admissionsUsed());
            assertEquals(2, shard.getMessage(schedule.delayMessageId()).runtimeIndex().timeline()
                    .candidateAttemptNo());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(0, shard.discoverDue(2_001, 10).size());
            assertEquals(1, shard.discoverDue(2_002, 10).size());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void closedLaneNotPublishedOutcomeTerminalizesWithoutRetry() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-closed-lane"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 70);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("closed-outcome-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("closed-outcome")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition closePosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition outcomePosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("closed-outcome-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] outcomeBody = publishNotPublishedBody(shardId, attemptId, 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2_002);
        final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("closed-outcome-operation")), outcomeBody, owner, 1,
                keyPair.getPrivate());
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("closed-actor")),
                Bytes.sha256(Bytes.utf8("closed-roles")), Bytes.sha256(Bytes.utf8("closed-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("closed-close-op")),
                    Bytes.sha256(Bytes.utf8("closed-close-request")), 0);
            final byte[] closeBody = applyShardControlBody(shardId, closeRef, 11, lane,
                    beforeClose.laneIncarnation(), beforeClose.laneControlVersion());
            final SystemMutation close = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000, closeRef.logicalOperationIdentity(11),
                    closeBody, control.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(close, closePosition, keyPair.getPublic())
                    .stableCode());

            final SystemMutationResult result = shard.applySystemMutation(outcome, outcomePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
        }
    }

    @Test
    void closedLaneUnknownOutcomeKeepsUncertainWithoutRetryTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-unknown-closed-lane"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 71);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("closed-unknown-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("closed-unknown")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition closePosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition outcomePosition = position(shardId, 3, 1_003);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("closed-unknown-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_003, 1_003,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 6, 6,
                Bytes.sha256(Bytes.utf8("closed-unknown-proof")), 0, null);
        final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
        final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("closed-unknown-operation")), unknownBody, owner, 1,
                keyPair.getPrivate());
        final AuthorIdentity control = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("unknown-closed-actor")),
                Bytes.sha256(Bytes.utf8("unknown-closed-roles")), Bytes.sha256(Bytes.utf8("unknown-closed-scope")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final LaneRecord beforeClose = shard.getLane(lane);
            final ControlRef closeRef = new ControlRef(Bytes.sha256(Bytes.utf8("unknown-close-op")),
                    Bytes.sha256(Bytes.utf8("unknown-close-request")), 0);
            final byte[] closeBody = applyShardControlBody(shardId, closeRef, 11, lane,
                    beforeClose.laneIncarnation(), beforeClose.laneControlVersion());
            final SystemMutation close = SystemMutation.signed(shardId,
                    SystemMutationType.APPLY_SHARD_CONTROL, 9_000, closeRef.logicalOperationIdentity(11),
                    closeBody, control.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.OK, shard.applySystemMutation(close, closePosition, keyPair.getPublic())
                    .stableCode());

            final SystemMutationResult result = shard.applySystemMutation(unknown, outcomePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, result.stableCode());
            assertEquals(MessageStatus.UNCERTAIN, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.NONE, shard.getMessage(schedule.delayMessageId()).runtimeIndex()
                    .currentWorkKind());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(attemptId, 42).state());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
        }
    }

    @Test
    void sourceOrderedNotPublishedPermanentClosesGenerationAndReleasesQuota() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-permanent"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("permanent-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("permanent")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-permanent-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 2,
                StableCode.DESTINATION_DEFINITIVE_PERMANENT, -1);
        final PublishOutcomeBody parsed = PublishOutcomeBody.decode(body);
        assertEquals(3, parsed.retryDecision().kind());
        assertEquals(1, parsed.retryDecision().completedAttemptNo());
        assertEquals(5_000, parsed.retryDecision().retryDeadline());
        assertFalse(parsed.retryDecision().hasNextRetryAt());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-permanent-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_PERMANENT, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.DESTINATION_DEFINITIVE_PERMANENT,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
        }
    }

    @Test
    void sourceOrderedNotPublishedLaneUnavailableBlocksLaneAndKeepsRetryTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-outcome-lane"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("lane-unavailable"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("lane")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("system-lane-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] body = publishNotPublishedBody(shardId, attemptId, 3,
                StableCode.CAPABILITY_UNAVAILABLE, 2_002);
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("system-lane-operation")), body, owner, 1, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);

            final SystemMutationResult result = shard.applySystemMutation(mutation, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CAPABILITY_UNAVAILABLE, result.stableCode());
            assertEquals(RuntimeReadiness.BLOCKED, shard.getLane(lane).runtimeReadiness());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(2_002, shard.getMessage(schedule.delayMessageId()).retryEligibilityAtEpochMs());
            assertNull(shard.findOpenPublishAttempt(attemptId));
            assertEquals(1, shard.discoverDue(2_002, 10).size());
        }
    }

    @Test
    void sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-admission"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 15);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId, io.nereusstream.delay.protocol.CommandId.random(shardId),
                messageId, io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("hello"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()), sourceWork.semanticWorkDigest());
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final java.security.KeyPairGenerator keyPairGenerator = java.security.KeyPairGenerator.getInstance("Ed25519");
        final java.security.KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-operation")), fixture.body(), fixture.owner(), 1,
                keyPair.getPrivate());
        final PublishAdmissionBody parsed = PublishAdmissionBody.decode(fixture.body());

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertNull(shard.getClaim(parsed.claimId(), 1));

            final SystemMutationResult result = shard.applySystemMutation(mutation, admissionPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(messageId).status());
            final PublishAttemptLedger ledger = shard.findOpenPublishAttempt(parsed.publishAttemptId());
            assertNotNull(ledger);
            assertEquals(AttemptLedgerState.PUBLISHING, ledger.state());
            assertEquals(parsed.descriptor().attemptNo(), ledger.attemptNo());
            assertArrayEquals(fixture.body(), ledger.admissionBytes());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
            assertEquals(result, shard.applySystemMutation(mutation, admissionPosition, keyPair.getPublic()));
        }
    }

    @Test
    void admissionOutcomeReserveGatesAndThenReleasesFromDurableUsage() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("admission-outcome-reserve"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 10, 0, 1, 1);
        assertEquals(1, shardConfig.maxOutcomeReserveBytes());
        assertEquals(1, shardConfig.maxOutcomeReserveRecords());
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 16);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("reserve"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(List.of()), sourceWork.semanticWorkDigest());
        final byte[] chargedBody = replaceAdmissionCharge(fixture.body(), 2, 2);
        assertEquals(2, PublishAdmissionBody.decode(chargedBody).chargeVector().resultRecords());
        assertEquals(2, PublishAdmissionBody.decode(chargedBody).chargeVector().resultBytes());
        assertFalse(OutcomeReserveUsage.empty().fits(
                OutcomeReserveUsage.from(PublishAdmissionBody.decode(chargedBody).chargeVector()), 1, 1));
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-outcome-reserve")), chargedBody, fixture.owner(), 1,
                keyPair.getPrivate());
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final SystemMutationResult gated = shard.applySystemMutation(mutation, admissionPosition,
                    keyPair.getPublic());
            assertEquals(StableCode.ADMISSION_CAPACITY_GATED, gated.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(messageId).status());
            assertNull(shard.findOpenPublishAttempt(PublishAdmissionBody.decode(chargedBody).publishAttemptId()));
            assertEquals(OutcomeReserveUsage.empty(), shard.outcomeReserve());
            assertEquals(admissionPosition, shard.lastAppliedSourcePosition());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            assertEquals(OutcomeReserveUsage.empty(), new DelayShard(store, shardConfig).outcomeReserve());
        }
    }

    @Test
    void admissionOutcomeReserveGateRevokesLiveClaimWithoutConsumingAttempt() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("admission-outcome-claim-gate"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 10, 0, 1, 1);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 18);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("claim-gate"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(List.of()), sourceWork.semanticWorkDigest());
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final AuthorIdentity owner = AuthorIdentity.decode(fixture.owner());
            final PublishAdmissionBody fixtureAdmission = PublishAdmissionBody.decode(fixture.body());
            final ClaimRecord claim = shard.claimForPublish(messageId, owner, 3_000,
                    fixtureAdmission.descriptor().materializationBytes(), chargeVector());
            final byte[] claimBody = replaceAdmissionClaim(fixture.body(), claim, store.metadata().storeIncarnation());
            final byte[] chargedBody = replaceAdmissionCharge(claimBody, 2, 2);
            final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(chargedBody);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION,
                    9_000, Bytes.sha256(Bytes.utf8("admission-outcome-claim-gate")), chargedBody,
                    fixture.owner(), 1, keyPair.getPrivate());

            assertEquals(MessageStatus.CLAIMED, shard.getMessage(messageId).status());
            assertEquals(StableCode.ADMISSION_CAPACITY_GATED,
                    shard.applySystemMutation(mutation, admissionPosition, keyPair.getPublic()).stableCode());
            final MessageRecord restored = shard.getMessage(messageId);
            assertEquals(MessageStatus.SCHEDULED, restored.status());
            assertEquals(0, restored.runtimeIndex().admissionsUsed());
            assertEquals(CurrentSendWorkKind.TIMELINE, restored.runtimeIndex().currentWorkKind());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(shard.findOpenPublishAttempt(admissionBody.publishAttemptId()));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(), messageId, 0), 1));
            assertEquals(1, shard.discoverReady(10_000, 10).size());
            assertEquals(OutcomeReserveUsage.empty(), shard.outcomeReserve());
        }
    }

    @Test
    void publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("admission-timing-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(
                        new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                                OrderingMode.BEST_EFFORT, Bytes.utf8("timing-claim"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final Fixture fixture = Fixture.createForSource(shardId, messageId,
                    shard.getLane(lane).laneIncarnation(),
                    KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(), messageId, 0),
                    1, 0, 0, Bytes.sha256(Bytes.utf8("obligations")), Bytes.sha256(Bytes.utf8("semantic")),
                    1, 1, 1_500);
            final PublishAdmissionBody fixtureAdmission = PublishAdmissionBody.decode(fixture.body());
            final AuthorIdentity owner = AuthorIdentity.decode(fixture.owner());
            final ClaimRecord claim = shard.claimForPublish(messageId, owner, 3_000,
                    fixtureAdmission.descriptor().materializationBytes(), chargeVector());
            final byte[] claimBody = replaceAdmissionClaim(fixture.body(), claim, store.metadata().storeIncarnation());
            final PublishAdmissionBody admissionBody = PublishAdmissionBody.decode(claimBody);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION,
                    9_000, Bytes.sha256(Bytes.utf8("admission-timing-claim")), claimBody, fixture.owner(), 1,
                    keyPair.getPrivate());

            assertEquals(MessageStatus.CLAIMED, shard.getMessage(fixture.messageId()).status());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION,
                    shard.applySystemMutation(mutation, admissionPosition, keyPair.getPublic()).stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(fixture.messageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(shard.findOpenPublishAttempt(admissionBody.publishAttemptId()));
            assertEquals(1, shard.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void admissionOutcomeReserveChargeIsReleasedByVerifiedPublish() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("admission-outcome-release"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 10, 0, 10, 10);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 17);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("reserve-release"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(List.of()), sourceWork.semanticWorkDigest());
        final byte[] chargedBody = replaceAdmissionCharge(fixture.body(), 2, 2);
        final PublishAdmissionBody parsed = PublishAdmissionBody.decode(chargedBody);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation admission = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-outcome-release")), chargedBody, fixture.owner(), 1,
                keyPair.getPrivate());
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final byte[] outcomeBody = publishOutcomeBody(shardId, parsed.publishAttemptId(), 1, 0, StableCode.OK,
                nestedPlaceholder(), new TrustedUtcIntervalEvidence(2_002, 2_002,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("outcome-clock"),
                        1, 1, 1, Bytes.sha256(Bytes.utf8("outcome-proof")), 0, null).canonicalBytes());
        final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("outcome-outcome-release")), outcomeBody, fixture.owner(), 1,
                keyPair.getPrivate());
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(admission, admissionPosition, keyPair.getPublic()).stableCode());
            assertEquals(new OutcomeReserveUsage(2, 2), shard.outcomeReserve());
            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(messageId).status());
            assertNotNull(shard.findOpenPublishAttempt(parsed.publishAttemptId()));
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(outcome, outcomePosition, keyPair.getPublic()).stableCode());
            assertEquals(OutcomeReserveUsage.empty(), shard.outcomeReserve());
            assertEquals(MessageStatus.PUBLISHED, shard.getMessage(messageId).status());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            assertEquals(OutcomeReserveUsage.empty(), new DelayShard(store, shardConfig).outcomeReserve());
        }
    }

    @Test
    void boundCapacityEnvelopeChargesExactOutcomeVectorAndRejectsIdentityDrift() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("bound-capacity-envelope"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 10, 0, 10, 10);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 19);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(new io.nereusstream.delay.protocol.ScheduleIntent(
                        lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8("bound-capacity"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final Fixture fixture = Fixture.createForSource(shardId, messageId,
                LaneRecord.initial(lane, schedulePosition).laneIncarnation(), sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(List.of()), sourceWork.semanticWorkDigest());
        final byte[] chargedBody = replaceAdmissionCharge(fixture.body(), 1, 1);
        final PublishAdmissionBody parsed = PublishAdmissionBody.decode(chargedBody);
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final SystemMutation admission = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("bound-capacity-admission")), chargedBody, fixture.owner(), 1,
                keyPair.getPrivate());
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final ShardCapacityEnvelopeV1 envelope = capacityEnvelope(1);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig, null, envelope);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(admission, admissionPosition, keyPair.getPublic()).stableCode());
            assertEquals(1, shard.outcomeReserveVector().amount(CapacityDimensionV1.RESULT_RECORDS));
            assertEquals(1, shard.outcomeReserveVector().amount(CapacityDimensionV1.RESULT_BYTES));
            assertEquals(envelope, shard.capacityEnvelope());
            assertEquals(shard.outcomeReserveVector(), CapacityVectorV1.decode(store.getValue(ColumnFamily.META,
                    KeyCodec.metaControlReserve(2, envelope.outcomeReserve().grantId()), 8).payload()));
            assertNotNull(shard.findOpenPublishAttempt(parsed.publishAttemptId()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, shardConfig, null, envelope);
            assertEquals(1, reopened.outcomeReserveVector().amount(CapacityDimensionV1.RESULT_RECORDS));
            assertEquals(1, reopened.outcomeReserveVector().amount(CapacityDimensionV1.RESULT_BYTES));
            assertThrows(IllegalStateException.class,
                    () -> new DelayShard(store, shardConfig, null, capacityEnvelope(2)));
            final long[] staleReserve = new long[CapacityDimensionV1.COUNT];
            staleReserve[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = 1;
            store.write(batch -> batch.putValue(ColumnFamily.META, 8,
                    KeyCodec.metaControlReserve(3, envelope.nonOutcomeControl().grantId()),
                    new CapacityVectorV1(staleReserve).canonicalBytes()));
            assertThrows(IllegalStateException.class, () -> new DelayShard(store, shardConfig, null, envelope));
        }
    }

    @Test
    void mutableControlReserveProjectionUsesGrantBoundedCheckedArithmetic() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("control-reserve-accounting"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final ShardCapacityEnvelopeV1 envelope = capacityEnvelopeWithNonOutcomeReserve();
        final CapacityVectorV1 oneReserveByte = capacityVector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, envelope);
            assertEquals(CapacityVectorV1.empty(), shard.controlReserveUsage(3));
            assertEquals(oneReserveByte, shard.reserveControlCapacity(3, oneReserveByte));
            assertEquals(oneReserveByte, shard.controlReserveUsage(3));
            assertThrows(IllegalStateException.class,
                    () -> shard.reserveControlCapacity(3, new CapacityVectorV1(
                            capacityAmounts(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 4))));
            assertEquals(CapacityVectorV1.empty(), shard.releaseControlCapacity(3, oneReserveByte));
            assertNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaControlReserve(3, envelope.nonOutcomeControl().grantId()), 8));
            assertEquals(CapacityVectorV1.empty(), shard.controlReserveUsage(6));
        }
    }

    @Test
    void systemWriterReserveProjectionIsPartitionedAndPersistsAcrossReopen() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-writer-reserve-accounting"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final ShardCapacityEnvelopeV1 envelope = capacityEnvelopeWithSystemWriterReserve();
        final CapacityVectorV1 oneWriterRecord = capacityVector(
                CapacityDimensionV1.SYSTEM_WRITER_RESERVED_RECORDS, 1);
        final CapacityVectorV1 oneWriterByte = capacityVector(
                CapacityDimensionV1.SYSTEM_WRITER_RESERVED_BYTES, 1);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, envelope);
            assertEquals(CapacityVectorV1.empty(), shard.controlReserveUsage(6));
            assertEquals(oneWriterRecord, shard.reserveControlCapacity(6, oneWriterRecord));
            assertEquals(oneWriterRecord, shard.controlReserveUsage(6));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.reserveControlCapacity(6,
                            capacityVector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 1)));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.reserveControlCapacity(3, oneWriterByte));
            assertEquals(oneWriterRecord, CapacityVectorV1.decode(store.getValue(ColumnFamily.META,
                    KeyCodec.metaControlReserve(6, envelope.nonOutcomeControl().grantId()), 8).payload()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults(), null, envelope);
            assertEquals(oneWriterRecord, reopened.systemWriterReserveUsage());
            final CapacityVectorV1 recordAndByte = oneWriterRecord.add(oneWriterByte);
            assertEquals(recordAndByte, reopened.reserveSystemWriterCapacity(oneWriterByte));
            assertEquals(oneWriterByte, reopened.releaseSystemWriterCapacity(oneWriterRecord));
            assertEquals(CapacityVectorV1.empty(), reopened.releaseSystemWriterCapacity(oneWriterByte));
            assertNull(store.getValue(ColumnFamily.META,
                    KeyCodec.metaControlReserve(6, envelope.nonOutcomeControl().grantId()), 8));
        }
    }

    @Test
    void systemWriterReserveProjectionRejectsWrongPersistedDimensions() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-writer-reserve-fence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 22);
        final ShardCapacityEnvelopeV1 envelope = capacityEnvelopeWithSystemWriterReserve();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, 8,
                    KeyCodec.metaControlReserve(6, envelope.nonOutcomeControl().grantId()),
                    capacityVector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 1).canonicalBytes()));
            assertThrows(IllegalArgumentException.class,
                    () -> new DelayShard(store, DelayShardConfig.defaults(), null, envelope));
        }
    }

    @Test
    void sourceOrderedPublishAdmissionFailsClosedWhenGenerationBudgetIsExhausted() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-admission-budget"));
        final DelayShardConfig shardConfig = new DelayShardConfig(10_000, 1, 20_000, 10, 100, 4,
                3, 100, 10_000, 1, 0);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 25);
        final DestinationLaneId lane = new DestinationLaneId(Bytes.sha256(Bytes.utf8("lane")));
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final PreparedCommand schedule = PreparedCommand.create(shardId,
                io.nereusstream.delay.protocol.CommandId.random(shardId), messageId,
                io.nereusstream.delay.protocol.CommandType.SCHEDULE, 9_000,
                io.nereusstream.delay.protocol.CommandBodies.schedule(
                        new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                                OrderingMode.BEST_EFFORT, Bytes.utf8("budget"))));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final byte[] sourceTimelineKey = KeyCodec.timelineDue(lane, 2_000,
                schedulePosition.sourceOrderToken(), messageId, 0);
        final TimelineWorkRef sourceWork = new TimelineWorkRef(TimelineWorkKind.INITIAL_SCHEDULE,
                sourceTimelineKey, 2_000, 2_000, 1, 1, false, UncertainRetryAuthority.NONE, null, null);
        final byte[] laneIncarnation = LaneRecord.initial(lane, schedulePosition).laneIncarnation();
        final Fixture firstFixture = Fixture.createForSource(shardId, messageId, laneIncarnation,
                sourceTimelineKey, 1, 0, 0,
                GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()), sourceWork.semanticWorkDigest());
        final PublishAdmissionBody firstBody = PublishAdmissionBody.decode(firstFixture.body());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final SystemMutation firstAdmission = SystemMutation.signed(shardId,
                SystemMutationType.PUBLISH_ADMISSION, 9_000,
                Bytes.sha256(Bytes.utf8("admission-budget-first")), firstFixture.body(), firstFixture.owner(), 7,
                keyPair.getPrivate());
        final KafkaSourcePosition firstAdmissionPosition = position(shardId, 1, 1_001);
        final byte[] outcomeBody = publishNotPublishedBody(shardId, firstBody.publishAttemptId(), 1,
                StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 2_002);
        final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                Bytes.sha256(Bytes.utf8("admission-budget-outcome")), outcomeBody, firstFixture.owner(), 7,
                keyPair.getPrivate());
        final KafkaSourcePosition outcomePosition = position(shardId, 2, 2_002);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, shardConfig);
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(firstAdmission, firstAdmissionPosition, keyPair.getPublic()).stableCode());
            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE,
                    shard.applySystemMutation(outcome, outcomePosition, keyPair.getPublic()).stableCode());
            final MessageRecord retry = shard.getMessage(messageId);
            assertEquals(1, retry.runtimeIndex().admissionsUsed());
            assertEquals(TimelineWorkKind.DEFINITIVE_RETRY, retry.runtimeIndex().timeline().workKind());

            final Fixture secondFixture = Fixture.createForSource(shardId, messageId, laneIncarnation,
                    retry.runtimeIndex().timeline().encodedTimelineKey(), 2, 1, 0,
                    GenerationRuntimeIndex.obligationSetDigest(java.util.List.of()),
                    retry.runtimeIndex().timeline().semanticWorkDigest(), 2, retry.stateVersion());
            final SystemMutation secondAdmission = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_ADMISSION, 9_000,
                    Bytes.sha256(Bytes.utf8("admission-budget-second")), secondFixture.body(), secondFixture.owner(),
                    7, keyPair.getPrivate());
            final SystemMutationResult rejected = shard.applySystemMutation(secondAdmission,
                    position(shardId, 3, 2_003), keyPair.getPublic());

            assertEquals(StableCode.STALE_SYSTEM_MUTATION, rejected.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(messageId).status());
            assertEquals(1, shard.getMessage(messageId).runtimeIndex().admissionsUsed());
            assertNull(shard.findOpenPublishAttempt(firstBody.publishAttemptId()));
        }
    }

    @Test
    void resourceRetireIntentIsSourceOrderedDurableAndVersionFenced() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resource-retire-intent"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final long expectedVersion = Long.MIN_VALUE;
        final byte[] resource = localStoreResource(shardId);
        final byte[] firstProtections = resourceProtectionSet(Bytes.sha256(Bytes.utf8("first-protection")));
        final byte[] body = resourceRetireBody(shardId, resource, expectedVersion, firstProtections);
        final ResourceRetireIntentBody parsed = ResourceRetireIntentBody.decode(body);
        final byte[] service = AuthorIdentity.service(Bytes.utf8("resource-service"), Bytes.utf8("run-1"), 1)
                .canonicalBytes();
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] logicalIdentity = SystemMutation.computeResourceRetireLogicalIdentity(
                parsed.resourceKind(), parsed.resource().identityHash(), parsed.expectedResourceStateVersion());
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.RESOURCE_RETIRE_INTENT,
                9_000, logicalIdentity, body, service, 1, keyPair.getPrivate());
        final KafkaSourcePosition firstPosition = position(shardId, 0, 1_000);

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final SystemMutationResult applied = shard.applySystemMutation(mutation, firstPosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, applied.stableCode());
            final ResourceRetireIntentRecord stored = shard.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsed.resource().identityHash(), expectedVersion);
            assertNotNull(stored);
            assertArrayEquals(mutation.systemMutationId(), stored.mutationId());
            assertEquals(1, stored.appliedMutationSequence());
            assertArrayEquals(firstPosition.canonicalBytes(), stored.appliedSourcePosition());
            assertEquals(applied, shard.applySystemMutation(mutation, firstPosition, keyPair.getPublic()));

            final SystemMutation wrongIdentity = SystemMutation.signed(shardId,
                    SystemMutationType.RESOURCE_RETIRE_INTENT, 9_000, Bytes.sha256(Bytes.utf8("wrong-identity")),
                    body, service, 1, keyPair.getPrivate());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                    shard.applySystemMutation(wrongIdentity, position(shardId, 1, 1_001),
                            keyPair.getPublic()).stableCode());

            final byte[] secondProtections = resourceProtectionSet(Bytes.sha256(Bytes.utf8("second-protection")));
            final SystemMutation conflicting = SystemMutation.signed(shardId,
                    SystemMutationType.RESOURCE_RETIRE_INTENT, 9_000, logicalIdentity,
                    resourceRetireBody(shardId, resource, expectedVersion, secondProtections), service, 1,
                    keyPair.getPrivate());
            assertEquals(StableCode.VERSION_CONFLICT,
                    shard.applySystemMutation(conflicting, position(shardId, 2, 1_002),
                            keyPair.getPublic()).stableCode());
            assertArrayEquals(mutation.mutationHash(), shard.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsed.resource().identityHash(), expectedVersion).mutationHash());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            final ResourceRetireIntentRecord stored = reopened.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsed.resource().identityHash(), expectedVersion);
            assertNotNull(stored);
            assertArrayEquals(mutation.systemMutationId(), stored.mutationId());
            assertEquals(position(shardId, 2, 1_002), reopened.lastAppliedSourcePosition());
        }
    }

    @Test
    void gcRetireIntentLookupRejectsKeyValueIdentityMismatch() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("gc-key-mismatch"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 54);
        final byte[] resource = localStoreResource(shardId);
        final byte[] body = resourceRetireBody(shardId, resource, 7,
                resourceProtectionSet(Bytes.sha256(Bytes.utf8("gc-key-mismatch-protection"))));
        final ResourceRetireIntentBody parsed = ResourceRetireIntentBody.decode(body);
        final byte[] service = AuthorIdentity.service(Bytes.utf8("gc-key-mismatch-service"),
                Bytes.utf8("run-1"), 1).canonicalBytes();
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.RESOURCE_RETIRE_INTENT,
                9_000, SystemMutation.computeResourceRetireLogicalIdentity(parsed.resourceKind(),
                        parsed.resource().identityHash(), 7), body, service, 1, keyPair.getPrivate());
        final ShardId otherShardId = new ShardId(RouteIncarnation.random(), 55);
        final ResourceRetireIntentBody other = ResourceRetireIntentBody.decode(resourceRetireBody(otherShardId,
                localStoreResource(otherShardId), 7,
                resourceProtectionSet(Bytes.sha256(Bytes.utf8("gc-key-mismatch-other-protection")))));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.OK, shard.applySystemMutation(mutation, position(shardId, 0, 1_000),
                    keyPair.getPublic()).stableCode());
            final ResourceRetireIntentRecord stored = shard.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsed.resource().identityHash(), 7);
            final ResourceRetireIntentRecord misplaced = new ResourceRetireIntentRecord(stored.mutationId(),
                    stored.mutationHash(), other.resourceKind(), other.resource().canonicalBytes(),
                    other.resource().identityHash(), stored.expectedResourceStateVersion(),
                    stored.appliedMutationSequence(), stored.protections(), stored.appliedSourcePosition());
            store.write(batch -> batch.putValue(ColumnFamily.GC, ResourceRetireIntentRecord.VALUE_TYPE,
                    KeyCodec.gcRetireIntent(ResourceKind.LOCAL_STORE, parsed.resource().identityHash(), 7),
                    misplaced.encode()));

            assertThrows(IllegalStateException.class, () -> shard.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsed.resource().identityHash(), 7));
        }
    }

    @Test
    void resourceDeleteConfirmationRequiresExactIntentAndRetainsLocalTombstone() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("resource-delete-confirmed"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 27);
        final byte[] resource = localStoreResource(shardId);
        final byte[] protections = resourceProtectionSet(Bytes.sha256(Bytes.utf8("delete-protection")));
        final byte[] retireBody = resourceRetireBody(shardId, resource, 3, protections);
        final ResourceRetireIntentBody parsedRetire = ResourceRetireIntentBody.decode(retireBody);
        final byte[] service = AuthorIdentity.service(Bytes.utf8("delete-service"), Bytes.utf8("run-1"), 1)
                .canonicalBytes();
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final SystemMutation retire = SystemMutation.signed(shardId, SystemMutationType.RESOURCE_RETIRE_INTENT,
                9_000, SystemMutation.computeResourceRetireLogicalIdentity(parsedRetire.resourceKind(),
                        parsedRetire.resource().identityHash(), 3), retireBody, service, 1, keyPair.getPrivate());
        final ResourceDeleteConfirmedBody.DeleteOutcome outcome = ResourceDeleteConfirmedBody.DeleteOutcome
                .ALREADY_ABSENT;
        final byte[] confirmationBody = resourceDeleteConfirmedBody(shardId, retire, parsedRetire, outcome,
                Bytes.sha256(Bytes.utf8("provider-request-1")));
        final ResourceDeleteConfirmedBody parsedConfirmation = ResourceDeleteConfirmedBody.decode(confirmationBody);
        final SystemMutation confirmation = SystemMutation.signed(shardId,
                SystemMutationType.RESOURCE_DELETE_CONFIRMED, 9_000, parsedConfirmation.intent().mutationId(),
                confirmationBody, service, 1, keyPair.getPrivate());

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(retire, position(shardId, 0, 1_000), keyPair.getPublic()).stableCode());
            final SystemMutationResult applied = shard.applySystemMutation(confirmation,
                    position(shardId, 1, 1_001), keyPair.getPublic());
            assertEquals(StableCode.OK, applied.stableCode());
            final ResourceDeleteConfirmedRecord tombstone = shard.getResourceDeleteConfirmation(
                    ResourceKind.LOCAL_STORE, parsedRetire.resource().identityHash(), 3);
            assertNotNull(tombstone);
            assertEquals(outcome, tombstone.outcome());
            assertEquals(2, tombstone.appliedMutationSequence());
            assertArrayEquals(retire.systemMutationId(), tombstone.retireIntent().mutationId());
            assertArrayEquals(confirmation.systemMutationId(), tombstone.confirmationMutationId());
            final ResourceRetireIntentRecord stored = shard.getResourceRetireIntent(ResourceKind.LOCAL_STORE,
                    parsedRetire.resource().identityHash(), 3);
            assertNotNull(stored);
            final RecoveryFloor beforeConfirmation = RecoveryFloor.create(
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("lineage")), 16),
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("checkpoint")), 16),
                    Bytes.sha256(Bytes.utf8("manifest")), 1,
                    position(shardId, 1, 1_001), 1, Bytes.sha256(Bytes.utf8("evidence")));
            assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                    ResourceGcGuard.evaluate(stored, tombstone, beforeConfirmation));
            final RecoveryFloor covering = RecoveryFloor.create(
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("lineage")), 16),
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("checkpoint")), 16),
                    Bytes.sha256(Bytes.utf8("manifest")), 2,
                    position(shardId, 1, 1_001), 2, Bytes.sha256(Bytes.utf8("evidence-2")));
            assertEquals(ResourceGcGuard.Decision.SOURCE_AND_SEQUENCE_COVERED,
                    ResourceGcGuard.evaluate(stored, tombstone, covering));
            final RecoveryFloor conflictingPosition = RecoveryFloor.create(
                    covering.recoveryLineageId(), covering.checkpointId(), covering.manifestSha256(),
                    covering.catalogGeneration(), position(shardId, 1, 1_002), covering.includedMutationSequence(),
                    covering.evidenceCursorDigest());
            assertEquals(ResourceGcGuard.Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
                    ResourceGcGuard.evaluate(stored, tombstone, conflictingPosition));
            assertEquals(applied, shard.applySystemMutation(confirmation, position(shardId, 1, 1_001),
                    keyPair.getPublic()));

            final byte[] conflictingBody = resourceDeleteConfirmedBody(shardId, retire, parsedRetire, outcome,
                    Bytes.sha256(Bytes.utf8("provider-request-2")));
            final ResourceDeleteConfirmedBody parsedConflict = ResourceDeleteConfirmedBody.decode(conflictingBody);
            final SystemMutation conflicting = SystemMutation.signed(shardId,
                    SystemMutationType.RESOURCE_DELETE_CONFIRMED, 9_000, parsedConflict.intent().mutationId(),
                    conflictingBody, service, 1, keyPair.getPrivate());
            assertEquals(StableCode.VERSION_CONFLICT,
                    shard.applySystemMutation(conflicting, position(shardId, 2, 1_002),
                            keyPair.getPublic()).stableCode());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                    reopened.getResourceDeleteConfirmation(ResourceKind.LOCAL_STORE,
                            parsedRetire.resource().identityHash(), 3).outcome());
            assertEquals(position(shardId, 2, 1_002), reopened.lastAppliedSourcePosition());
        }
    }

    @Test
    void localClaimIsDurableAndRevokeRestoresTimelineAtomically() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-lifecycle"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 20);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-lifecycle-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                schedule.delayMessageId(), 0);
        final ClaimRecord claim;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);

            claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());

            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(CurrentSendWorkKind.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).runtimeIndex().currentWorkKind());
            assertArrayEquals(claim.claimId(), shard.getMessage(schedule.delayMessageId()).runtimeIndex().claimId());
            assertEquals(1, shard.claimSequence());
            assertEquals(claim, shard.getClaim(claim.claimId(), owner.generation()));
            assertNull(store.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
            assertNotNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverReady(10_000, 10).size());
            assertEquals(1, shard.discoverExpiry(10_000, 10).size());
        }
        try (SharedRocksDbResources reopenedResources = new SharedRocksDbResources(config);
             ShardStore reopenedStore = ShardStore.open(config, shardId, reopenedResources)) {
            final DelayShard reopened = new DelayShard(reopenedStore, DelayShardConfig.defaults());
            assertEquals(1, reopened.claimSequence());
            assertEquals(MessageStatus.CLAIMED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(claim, reopened.getClaim(claim.claimId(), owner.generation()));
            assertEquals(1, reopened.revokeClaimsForOwner(owner.generation()));
            final MessageRecord restored = reopened.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, restored.status());
            assertEquals(CurrentSendWorkKind.TIMELINE, restored.runtimeIndex().currentWorkKind());
            assertNull(reopened.getClaim(claim.claimId(), owner.generation()));
            assertEquals(0, reopened.revokeClaimsForOwner(owner.generation()));
            assertNotNull(reopenedStore.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
            assertEquals(1, reopened.discoverReady(10_000, 10).size());
        }
    }

    @Test
    void shardActivationFailsClosedForOrphanedPublishLedger() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("orphaned-ledger"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("orphaned-ledger-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("orphaned")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("orphaned-ledger-attempt"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("claim")), 42, 1, lane, new byte[16],
                    Bytes.sha256(Bytes.utf8("owner")), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(admission, admissionPosition);
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.delete(ColumnFamily.ID, KeyCodec.idMessage(schedule.delayMessageId())));
            assertThrows(IllegalStateException.class, () -> new DelayShard(store, DelayShardConfig.defaults()));
        }
    }

    @Test
    void terminalSummaryRetainsASecondOpenObligationAndReopensSafely() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("terminal-obligation-summary"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 26);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("terminal-summary-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("terminal-summary")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition firstAdmissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition terminalPosition = position(shardId, 2, 1_002);
        final byte[] firstAttemptId = Bytes.sha256(Bytes.utf8("terminal-summary-first"));
        final byte[] secondAttemptId = Bytes.sha256(Bytes.utf8("terminal-summary-second"));

        TerminalGenerationRecord summary;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger first = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    firstAttemptId, Bytes.sha256(Bytes.utf8("terminal-summary-first-claim")), 42, 1, lane,
                    new byte[16], Bytes.sha256(Bytes.utf8("terminal-summary-owner")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-prepared")),
                    Bytes.utf8("terminal-summary-first-admission"), firstAdmissionPosition.canonicalBytes());
            shard.admitPublishAttempt(first, firstAdmissionPosition);

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final PublishAttemptLedger second = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    secondAttemptId, Bytes.sha256(Bytes.utf8("terminal-summary-second-claim")), 43, 2, lane,
                    shard.getLane(lane).laneIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-owner-2")),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("terminal-summary-prepared-2")),
                    Bytes.utf8("terminal-summary-second-admission"), firstAdmissionPosition.canonicalBytes());
            final List<AttemptObligationRef> obligations = new ArrayList<>(List.of(
                    first.obligationRef(), second.obligationRef()));
            obligations.sort(DelayShardTest::compareObligations);
            final MessageRecord withSecond = new MessageRecord(MessageStatus.PUBLISHING, current.generation(),
                    current.stateVersion(), current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(),
                    current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(
                            GenerationRuntimeIndex.publishing(firstAttemptId, obligations, 2, 0, false,
                                    current.stateVersion()));
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()), withSecond.encode());
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, second.encodedKey(),
                        second.encode());
            });

            final MessageRecord published = shard.applyPublishedPublishOutcome(firstAttemptId, 42,
                    terminalPosition);
            assertEquals(MessageStatus.PUBLISHED, published.status());
            summary = shard.getTerminalGeneration(schedule.delayMessageId(), 0);
            assertNotNull(summary);
            assertEquals(List.of(second.obligationRef()), summary.openObligations());
            assertEquals(summary.openObligations(), published.runtimeIndex().attemptObligations());
            final TerminalGenerationRecord retained = summary;

            final MessageRecord latePublished = shard.applyPublishedPublishOutcome(secondAttemptId, 43,
                    position(shardId, 3, 1_003));
            assertEquals(MessageStatus.PUBLISHED, latePublished.status());
            assertTrue(latePublished.runtimeIndex().possibleDestinationDuplicate());
            assertEquals(List.of(), latePublished.runtimeIndex().attemptObligations());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(secondAttemptId, 43));
            assertEquals(List.of(second.obligationRef()), retained.openObligations());
            summary = shard.getTerminalGeneration(schedule.delayMessageId(), 0);
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(summary.openObligations(),
                    reopened.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertTrue(reopened.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertNull(reopened.getPublishAttempt(secondAttemptId, 43));
        }
    }

    @Test
    void lateOutcomeSettlesOlderGenerationThroughTerminalSummary() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("historical-terminal-outcome"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 30);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("historical-terminal-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("historical-terminal")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition generationOnePosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition outcomePosition = position(shardId, 3, 1_003);
        final KafkaSourcePosition secondOutcomePosition = position(shardId, 4, 1_004);
        final KafkaSourcePosition thirdOutcomePosition = position(shardId, 5, 1_005);
        final KafkaSourcePosition resolutionPosition = position(shardId, 6, 1_006);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("historical-terminal-attempt"));
        final byte[] secondAttemptId = Bytes.sha256(Bytes.utf8("historical-terminal-second-attempt"));
        final byte[] thirdAttemptId = Bytes.sha256(Bytes.utf8("historical-terminal-third-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        final AuthorIdentity secondOwner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 43,
                Bytes.sha256(Bytes.utf8("lease-2")));
        final AuthorIdentity thirdOwner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 44,
                Bytes.sha256(Bytes.utf8("lease-3")));
        TerminalGenerationRecord oldSummary;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger oldAttempt = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("historical-terminal-claim")), owner.generation(), 1, lane,
                    shard.getLane(lane).laneIncarnation(), owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-prepared")), Bytes.utf8("historical-admission"),
                    admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(oldAttempt, admissionPosition);

            final MessageRecord prior = shard.getMessage(schedule.delayMessageId());
            final byte[] nextTimelineKey = KeyCodec.timelineDue(lane, 3_000,
                    generationOnePosition.sourceOrderToken(), schedule.delayMessageId(), 1);
            final MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, 1,
                    prior.stateVersion() + 1, 3_000, 6_000, lane, OrderingMode.BEST_EFFORT, prior.payload(),
                    generationOnePosition.canonicalBytes(), prior.payloadReference(), 3_000).withRuntimeIndex(
                    GenerationRuntimeIndex.timeline(GenerationAggregateState.SCHEDULED,
                            TimelineWorkRef.initial(nextTimelineKey, 3_000, prior.stateVersion() + 1),
                            List.of(), 0, 0, false, prior.stateVersion() + 1));
            oldSummary = new TerminalGenerationRecord(schedule.delayMessageId(), 0, MessageStatus.SUPERSEDED,
                    StableCode.SUPERSEDED, prior.stateVersion(), generationOnePosition.canonicalBytes(), false,
                    List.of(oldAttempt.obligationRef()));
            final TerminalGenerationRecord initialSummary = oldSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()), next.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, nextTimelineKey,
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1),
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), initialSummary.encode());
            });

            final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_003, 1_003,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 7, 7,
                    Bytes.sha256(Bytes.utf8("historical-outcome-proof")), 0, null);
            final byte[] outcomeBody = publishNotPublishedBody(shardId, attemptId, 1,
                    StableCode.DESTINATION_DEFINITIVE_RETRIABLE, 3_000);
            final SystemMutation outcome = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-outcome-operation")), outcomeBody,
                    owner.canonicalBytes(), 1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(outcome, outcomePosition,
                    keyPair.getPublic());

            assertEquals(StableCode.DESTINATION_DEFINITIVE_RETRIABLE, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertFalse(shard.getMessage(schedule.delayMessageId()).runtimeIndex().possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(attemptId, owner.generation()));
            assertEquals(result, shard.getSystemMutationResult(outcome.systemMutationId()));

            final PublishAttemptLedger second = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    secondAttemptId, Bytes.sha256(Bytes.utf8("historical-terminal-second-claim")),
                    secondOwner.generation(), 2, lane, shard.getLane(lane).laneIncarnation(),
                    secondOwner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-second-prepared")),
                    Bytes.utf8("historical-second-admission"), secondOutcomePosition.canonicalBytes());
            final TerminalGenerationRecord secondSummary = new TerminalGenerationRecord(schedule.delayMessageId(), 0,
                    MessageStatus.SUPERSEDED,
                    StableCode.SUPERSEDED, prior.stateVersion(), generationOnePosition.canonicalBytes(), false,
                    List.of(second.obligationRef()));
            oldSummary = secondSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, second.encodedKey(),
                        second.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), secondSummary.encode());
            });
            final byte[] secondOutcomeBody = publishOutcomeBody(shardId, secondAttemptId, 1, 0, StableCode.OK,
                    nestedPlaceholder(), observedAt.canonicalBytes());
            final SystemMutation secondOutcome = SystemMutation.signed(shardId,
                    SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-second-outcome-operation")), secondOutcomeBody,
                    secondOwner.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult secondResult = shard.applySystemMutation(secondOutcome,
                    secondOutcomePosition, keyPair.getPublic());
            assertEquals(StableCode.OK, secondResult.stableCode());
            assertTrue(shard.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(secondAttemptId, secondOwner.generation()));

            final PublishAttemptLedger third = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    thirdAttemptId, Bytes.sha256(Bytes.utf8("historical-terminal-third-claim")),
                    thirdOwner.generation(), 3, lane, shard.getLane(lane).laneIncarnation(),
                    thirdOwner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("historical-terminal-third-prepared")),
                    Bytes.utf8("historical-third-admission"), thirdOutcomePosition.canonicalBytes());
            final TerminalGenerationRecord thirdSummary = new TerminalGenerationRecord(
                    schedule.delayMessageId(), 0, MessageStatus.SUPERSEDED, StableCode.SUPERSEDED,
                    prior.stateVersion(), generationOnePosition.canonicalBytes(), true,
                    List.of(third.obligationRef()));
            oldSummary = thirdSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, third.encodedKey(),
                        third.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), thirdSummary.encode());
            });
            final byte[] unknownBody = publishOutcomeBody(shardId, thirdAttemptId, 3, 4,
                    StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
            final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-third-unknown-operation")), unknownBody,
                    thirdOwner.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult unknownResult = shard.applySystemMutation(unknown, thirdOutcomePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, unknownResult.stableCode());
            assertEquals(AttemptLedgerState.UNCERTAIN, shard.getPublishAttempt(thirdAttemptId,
                    thirdOwner.generation()).state());
            assertEquals(AttemptLedgerState.UNCERTAIN,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations().get(0).ledgerState());

            final byte[] resolutionBody = evidenceResolutionBody(shardId, thirdAttemptId, StableCode.OK, 1, 0,
                    new TrustedUtcIntervalEvidence(1_006, 1_006,
                            TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 8, 8,
                            Bytes.sha256(Bytes.utf8("historical-resolution-proof")), 0, null).canonicalBytes());
            final AuthorIdentity service = AuthorIdentity.service(Bytes.utf8("evidence-service"),
                    Bytes.utf8("historical-run"), 2);
            final SystemMutation resolution = SystemMutation.signed(shardId,
                    SystemMutationType.EVIDENCE_RESOLUTION, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-resolution-operation")), resolutionBody,
                    service.canonicalBytes(), 1, keyPair.getPrivate());
            final SystemMutationResult resolutionResult = shard.applySystemMutation(resolution, resolutionPosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, resolutionResult.stableCode());
            assertTrue(shard.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(thirdAttemptId, thirdOwner.generation()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.SCHEDULED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(oldSummary.generation(), reopened.getTerminalGeneration(schedule.delayMessageId(), 0)
                    .generation());
            assertTrue(reopened.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertNull(reopened.getPublishAttempt(attemptId, owner.generation()));
            assertNull(reopened.getPublishAttempt(secondAttemptId, secondOwner.generation()));
            assertNull(reopened.getPublishAttempt(thirdAttemptId, thirdOwner.generation()));
        }
    }

    @Test
    void resolveUncertainPublishedEvidenceSettlesOlderGenerationThroughTerminalSummary() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                tempDir.resolve("historical-resolve-published-evidence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("historical-resolve-published-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("historical-resolve-published")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 1_001);
        final KafkaSourcePosition unknownPosition = position(shardId, 2, 1_002);
        final KafkaSourcePosition generationOnePosition = position(shardId, 3, 1_003);
        final KafkaSourcePosition resolvePosition = position(shardId, 4, 1_004);
        final byte[] attemptId = Bytes.sha256(Bytes.utf8("historical-resolve-published-attempt"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        TerminalGenerationRecord oldSummary;
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final PublishAttemptLedger oldAttempt = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, Bytes.sha256(Bytes.utf8("historical-resolve-published-claim")), owner.generation(), 1,
                    lane, shard.getLane(lane).laneIncarnation(), owner.canonicalBytes(),
                    store.metadata().storeIncarnation(), Bytes.sha256(Bytes.utf8("historical-resolve-published")),
                    Bytes.utf8("historical-admission"), admissionPosition.canonicalBytes());
            shard.admitPublishAttempt(oldAttempt, admissionPosition);

            final TrustedUtcIntervalEvidence observedAt = new TrustedUtcIntervalEvidence(1_002, 1_002,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 7, 7,
                    Bytes.sha256(Bytes.utf8("historical-resolve-published-proof")), 0, null);
            final byte[] unknownBody = publishOutcomeBody(shardId, attemptId, 3, 4,
                    StableCode.RECOVERY_FIRST_SEND_UNCERTAIN, new byte[0], observedAt.canonicalBytes());
            final SystemMutation unknown = SystemMutation.signed(shardId, SystemMutationType.PUBLISH_OUTCOME, 9_000,
                    Bytes.sha256(Bytes.utf8("historical-resolve-published-unknown")), unknownBody,
                    owner.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.RECOVERY_FIRST_SEND_UNCERTAIN,
                    shard.applySystemMutation(unknown, unknownPosition, keyPair.getPublic()).stableCode());
            final PublishAttemptLedger uncertainAttempt = shard.getPublishAttempt(attemptId, owner.generation());
            assertEquals(AttemptLedgerState.UNCERTAIN, uncertainAttempt.state());

            final MessageRecord prior = shard.getMessage(schedule.delayMessageId());
            final byte[] nextTimelineKey = KeyCodec.timelineDue(lane, 3_000,
                    generationOnePosition.sourceOrderToken(), schedule.delayMessageId(), 1);
            final MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, 1,
                    prior.stateVersion() + 1, 3_000, 6_000, lane, OrderingMode.BEST_EFFORT, prior.payload(),
                    generationOnePosition.canonicalBytes(), prior.payloadReference(), 3_000).withRuntimeIndex(
                    GenerationRuntimeIndex.timeline(GenerationAggregateState.SCHEDULED,
                            TimelineWorkRef.initial(nextTimelineKey, 3_000, prior.stateVersion() + 1),
                            List.of(), 0, 0, false, prior.stateVersion() + 1));
            oldSummary = new TerminalGenerationRecord(schedule.delayMessageId(), 0, MessageStatus.SUPERSEDED,
                    StableCode.SUPERSEDED, prior.stateVersion(), generationOnePosition.canonicalBytes(), false,
                    List.of(uncertainAttempt.obligationRef()));
            final TerminalGenerationRecord initialSummary = oldSummary;
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(schedule.delayMessageId()), next.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, nextTimelineKey,
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        KeyCodec.timelineExpiry(6_000, lane, schedule.delayMessageId(), 1),
                        new TimelineEntry(schedule.delayMessageId(), 1).encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(schedule.delayMessageId(), 0), initialSummary.encode());
            });

            final ControlRef controlRef = new ControlRef(
                    Bytes.sha256(Bytes.utf8("historical-resolve-published-operation")),
                    Bytes.sha256(Bytes.utf8("historical-resolve-published-request")), 6);
            final byte[] evidence = publishEvidence(attemptId, true, StableCode.OK);
            final byte[] resolveBody = resolveUncertainEvidenceBody(shardId, controlRef, lane,
                    shard.getLane(lane).laneIncarnation(), schedule.delayMessageId(), 0, attemptId, 1, evidence);
            final SystemMutation resolve = SystemMutation.signed(shardId,
                    SystemMutationType.RESOLVE_UNCERTAIN, 9_000,
                    controlRef.logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN), resolveBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(resolve, resolvePosition,
                    keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, shard.getMessage(schedule.delayMessageId()).generation());
            assertTrue(shard.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), shard.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(shard.getPublishAttempt(attemptId, owner.generation()));
            assertEquals(result, shard.applySystemMutation(resolve, resolvePosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.SCHEDULED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, reopened.getMessage(schedule.delayMessageId()).generation());
            assertEquals(oldSummary.generation(), reopened.getTerminalGeneration(schedule.delayMessageId(), 0)
                    .generation());
            assertTrue(reopened.getTerminalGeneration(schedule.delayMessageId(), 0).possibleDestinationDuplicate());
            assertEquals(List.of(), reopened.getTerminalGeneration(schedule.delayMessageId(), 0).openObligations());
            assertNull(reopened.getPublishAttempt(attemptId, owner.generation()));
        }
    }

    @Test
    void claimResultConsumesExactClaimAndTerminalizesCurrentGeneration() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-result-claimed"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 23);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-claimed-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition resultPosition = position(shardId, 1, 2_100);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            final byte[] body = claimResultBody(shardId, claim.claimId(), schedule.delayMessageId(), 0, lane,
                    claim.laneIncarnation(), claim.laneControlVersion(), claim.runtimeLaneVersion(),
                    claim.timelineKey(), owner.canonicalBytes(), store.metadata().storeIncarnation(), 3_000, 1,
                    2_000, 2_000);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    Bytes.sha256(Bytes.utf8("claimed-result-operation")), body, owner.canonicalBytes(), 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, resultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            final DlqExportRecord dlqExport = shard.getDlqExportRecord(schedule.delayMessageId(), 0);
            assertNotNull(dlqExport);
            assertEquals(DlqExportStateV1.NOT_CONFIGURED, dlqExport.state());
            assertEquals(DlqExportStateV1.NOT_CONFIGURED,
                    shard.queryMessageSnapshot(schedule.delayMessageId()).dlqExportState());
        }
    }

    @Test
    void replayDeadLetterCreatesNextGenerationAndRetainsOldTerminalSummary() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("replay-dead-letter"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 32);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("replay-dead-letter-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("replay-dead-letter")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition resultPosition = position(shardId, 1, 2_100);
        final KafkaSourcePosition replayPosition = position(shardId, 2, 2_200);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            final byte[] resultBody = claimResultBody(shardId, claim.claimId(), schedule.delayMessageId(), 0, lane,
                    claim.laneIncarnation(), claim.laneControlVersion(), claim.runtimeLaneVersion(),
                    claim.timelineKey(), owner.canonicalBytes(), store.metadata().storeIncarnation(), 3_000, 1,
                    2_000, 2_000);
            final SystemMutation resultMutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT,
                    9_000, Bytes.sha256(Bytes.utf8("replay-dead-letter-claim-result")), resultBody,
                    owner.canonicalBytes(), 1, keyPair.getPrivate());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE,
                    shard.applySystemMutation(resultMutation, resultPosition, keyPair.getPublic()).stableCode());
            final MessageRecord dead = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.DEAD_LETTER, dead.status());
            assertEquals(0, shard.quota().pendingMessages());

            final ControlRef controlRef = new ControlRef(Bytes.sha256(Bytes.utf8("replay-operation")),
                    Bytes.sha256(Bytes.utf8("replay-request")), 9);
            final byte[] replayBody = replayDeadLetterBody(shardId, controlRef, schedule.delayMessageId(),
                    dead.generation(), dead.stateVersion(), 3_000, 8_000);
            final SystemMutation replay = SystemMutation.signed(shardId, SystemMutationType.REPLAY_DEAD_LETTER,
                    9_000, controlRef.logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER), replayBody,
                    AuthorIdentity.control(Bytes.sha256(Bytes.utf8("actor")),
                            Bytes.sha256(Bytes.utf8("role")), Bytes.sha256(Bytes.utf8("scope"))).canonicalBytes(),
                    1, keyPair.getPrivate());
            final SystemMutationResult replayResult = shard.applySystemMutation(replay, replayPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.OK, replayResult.stableCode());
            final MessageRecord next = shard.getMessage(schedule.delayMessageId());
            assertEquals(MessageStatus.SCHEDULED, next.status());
            assertEquals(1, next.generation());
            assertEquals(3_000, next.deliverAtEpochMs());
            assertEquals(8_000, next.expireAtEpochMs());
            assertEquals(TimelineWorkKind.INITIAL_SCHEDULE, next.runtimeIndex().timeline().workKind());
            assertEquals(0, next.runtimeIndex().admissionsUsed());
            assertEquals(1, shard.quota().pendingMessages());
            assertEquals(1, shard.discoverDue(3_000, 10).size());
            assertEquals(MessageStatus.DEAD_LETTER,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertEquals(replayResult, shard.applySystemMutation(replay, replayPosition, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(MessageStatus.SCHEDULED, reopened.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, reopened.getMessage(schedule.delayMessageId()).generation());
            assertEquals(MessageStatus.DEAD_LETTER,
                    reopened.getTerminalGeneration(schedule.delayMessageId(), 0).status());
            assertEquals(1, reopened.quota().pendingMessages());
        }
    }

    @Test
    void timeFenceMonotonicallyClosesIngressWithoutOverwritingCommandIdentity() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("time-fence-ingress"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 34);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("time-fence-lane"));
        final KafkaSourcePosition fencePosition = position(shardId, 0, 2_000);
        final TrustedUtcIntervalEvidence proof = new TrustedUtcIntervalEvidence(3_000, 3_000,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("fence-clock"), 1, 9, 9,
                Bytes.sha256(Bytes.utf8("fence-proof")), 0, null);
        final int keyVersion = 7;
        final long closeThrough = 3_000;
        final byte[] proofId = Bytes.sha256(Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                shardId.routeIncarnation().bytes(), Bytes.u32be(shardId.partition()), Bytes.i64be(closeThrough),
                Bytes.u32be(keyVersion), Bytes.lp32(proof.canonicalBytes()));
        final byte[] fenceBody = timeFenceBody(shardId, closeThrough, keyVersion, proofId,
                proof.canonicalBytes());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity fence = AuthorIdentity.fence(Bytes.utf8("fence-writer"), keyVersion);
        final SystemMutation fenceMutation = SystemMutation.signed(shardId, SystemMutationType.TIME_FENCE, 9_000,
                proofId, fenceBody, fence.canonicalBytes(), keyVersion, keyPair.getPrivate());
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(fenceMutation, fencePosition, keyPair.getPublic()).stableCode());
            assertEquals(closeThrough, shard.closedIngressDeadlineThrough());
            assertArrayEquals(proofId, store.runtimeMetadata().lastIngressFenceProofId());

            final PreparedCommand closed = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 4_000, 7_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("closed")), 3_000);
            final CommandResult closedResult = shard.apply(closed, position(shardId, 1, 2_500));
            assertEquals(StableCode.COMMAND_RETRY_WINDOW_EXPIRED, closedResult.stableCode());
            assertNull(shard.getMessage(closed.delayMessageId()));
            assertNull(shard.getCommandResult(closed.commandId()));
            assertEquals(closedResult, shard.apply(closed, position(shardId, 1, 2_500)));

            final PreparedCommand open = PreparedCommand.schedule(shardId,
                    new io.nereusstream.delay.protocol.ScheduleIntent(lane, 4_000, 7_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("open")), 4_000);
            assertEquals(StableCode.SCHEDULED, shard.apply(open, position(shardId, 2, 2_501)).stableCode());
            assertEquals(closeThrough, shard.closedIngressDeadlineThrough());
            assertEquals(StableCode.OK,
                    shard.applySystemMutation(fenceMutation, position(shardId, 3, 2_502),
                            keyPair.getPublic()).stableCode());
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(closeThrough, reopened.closedIngressDeadlineThrough());
            assertArrayEquals(proofId, store.runtimeMetadata().lastIngressFenceProofId());
        }
    }

    @Test
    void publishAdmissionConsumesExactLocalClaimBeforeCreatingAttemptLedger() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-admission"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 24);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-admission-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-admission")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition admissionPosition = position(shardId, 1, 2_001);
        final AuthorIdentity owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 42,
                Bytes.sha256(Bytes.utf8("lease")));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final ClaimRecord claim = shard.claimForPublish(schedule.delayMessageId(), owner, 3_000,
                    new byte[0], chargeVector());
            final byte[] attemptId = Bytes.sha256(Bytes.utf8("claim-admission-attempt"));
            final PublishAttemptLedger admission = PublishAttemptLedger.publishing(schedule.delayMessageId(), 0,
                    attemptId, claim.claimId(), owner.generation(), 1, lane, claim.laneIncarnation(),
                    owner.canonicalBytes(), store.metadata().storeIncarnation(),
                    Bytes.sha256(Bytes.utf8("prepared")), Bytes.utf8("admission"), admissionPosition.canonicalBytes());

            shard.admitPublishAttempt(admission, admissionPosition);

            assertEquals(MessageStatus.PUBLISHING, shard.getMessage(schedule.delayMessageId()).status());
            assertNull(shard.getClaim(claim.claimId(), owner.generation()));
            assertEquals(admission, shard.getPublishAttempt(attemptId, owner.generation()));
            assertEquals(List.of(admission), shard.listOpenPublishAttempts());
            assertNull(store.getValue(ColumnFamily.TIMELINE, claim.timelineKey(), 1));
            assertNull(store.getValue(ColumnFamily.TIMELINE,
                    KeyCodec.timelineExpiry(5_000, lane, schedule.delayMessageId(), 0), 1));
        }
    }

    @Test
    void sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-claim-result"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 21);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition claimResultPosition = position(shardId, 1, 1_100);
        final KafkaSourcePosition duplicatePosition = position(shardId, 2, 1_101);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-result-claim"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            final LaneRecord laneRecord = shard.getLane(lane);
            final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            final byte[] body = claimResultBody(shardId, claimId, schedule.delayMessageId(), 0, lane,
                    laneRecord.laneIncarnation(), 1, laneRecord.laneVersion(), timelineKey, owner,
                    store.metadata().storeIncarnation(), 3_000, 1, 1_500, 1_500);
            final byte[] mismatchedTransfer = claimResultBody(shardId, claimId, schedule.delayMessageId(), 0,
                    lane, laneRecord.laneIncarnation(), 1, laneRecord.laneVersion(), timelineKey, owner,
                    store.metadata().storeIncarnation(), 3_000, 1, 1_500, 1_500,
                    chargeVectorWithActiveMessages(1));
            assertThrows(IllegalArgumentException.class, () -> ClaimResultBody.decode(mismatchedTransfer));
            final ClaimResultBody parsed = ClaimResultBody.decode(body);
            assertEquals(1, parsed.resultKind());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, parsed.stableCode());
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    claimId, body, owner, 1, keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, claimResultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE, result.stableCode());
            assertEquals(MessageStatus.DEAD_LETTER, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(StableCode.CLAIM_PERMANENT_FAILURE,
                    shard.getTerminalGeneration(schedule.delayMessageId(), 0).terminalCode());
            assertEquals(0, shard.quota().pendingMessages());
            assertEquals(0, shard.discoverDue(10_000, 10).size());
            assertEquals(0, shard.discoverExpiry(10_000, 10).size());
            assertEquals(result, shard.applySystemMutation(mutation, duplicatePosition, keyPair.getPublic()));
            assertEquals(duplicatePosition, shard.lastAppliedSourcePosition());
            assertEquals(result, shard.getSystemMutationResult(mutation.systemMutationId()));
        }
    }

    @Test
    void staleClaimResultDoesNotTerminalizeChangedTimeline() throws Exception {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("system-claim-result-stale"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 22);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("claim-result-stale-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(lane, 2_000, 5_000,
                        OrderingMode.BEST_EFFORT, Bytes.utf8("claim-result-stale")), 9_000);
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        final KafkaSourcePosition claimResultPosition = position(shardId, 1, 1_100);
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final byte[] owner = AuthorIdentity.owner(Bytes.utf8("deployment"), Bytes.utf8("worker"), 1,
                Bytes.sha256(Bytes.utf8("lease"))).canonicalBytes();
        final byte[] claimId = Bytes.sha256(Bytes.utf8("claim-result-stale-claim"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(StableCode.SCHEDULED, shard.apply(schedule, schedulePosition).stableCode());
            final LaneRecord laneRecord = shard.getLane(lane);
            final byte[] timelineKey = KeyCodec.timelineDue(lane, 2_000, schedulePosition.sourceOrderToken(),
                    schedule.delayMessageId(), 0);
            final byte[] body = claimResultBody(shardId, claimId, schedule.delayMessageId(), 0, lane,
                    laneRecord.laneIncarnation(), 1, laneRecord.laneVersion(), Bytes.sha256(timelineKey), owner,
                    store.metadata().storeIncarnation(), 3_000, 1, 1_500, 1_500);
            final SystemMutation mutation = SystemMutation.signed(shardId, SystemMutationType.CLAIM_RESULT, 9_000,
                    Bytes.sha256(Bytes.utf8("claim-result-stale-operation")), body, owner, 1,
                    keyPair.getPrivate());

            final SystemMutationResult result = shard.applySystemMutation(mutation, claimResultPosition,
                    keyPair.getPublic());

            assertEquals(StableCode.STALE_SYSTEM_MUTATION, result.stableCode());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(1, shard.quota().pendingMessages());
            assertNotNull(store.getValue(ColumnFamily.TIMELINE, timelineKey, 1));
        }
    }

    private static byte[] expiryBody(final ShardId shard, final io.nereusstream.delay.protocol.DelayMessageId messageId,
                                     final int generation, final long expireAt, final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.EXPIRE_GENERATION.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, messageId.bytes());
            CanonicalProtobuf.uint32(output, 11, generation);
            CanonicalProtobuf.int64(output, 12, expireAt);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static byte[] timeFenceBody(final ShardId shard, final long closeThrough, final int keyVersion,
                                        final byte[] proofId, final byte[] proof) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.int64(output, 10, closeThrough);
            CanonicalProtobuf.uint32(output, 11, keyVersion);
            CanonicalProtobuf.bytes(output, 12, proofId);
            CanonicalProtobuf.bytes(output, 13, proof);
        });
    }

    private static byte[] applyShardControlBody(final ShardId shard, final ControlRef controlRef,
                                                final int controlKind, final DestinationLaneId lane,
                                                final byte[] laneIncarnation, final long expectedControlVersion) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] laneTarget = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, lane.bytes());
            CanonicalProtobuf.bytes(output, 2, laneIncarnation);
            CanonicalProtobuf.int64(output, 3, expectedControlVersion);
        });
        final byte[] branch = switch (controlKind) {
            case 8, 9 -> CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneTarget);
                CanonicalProtobuf.bytes(output, 2, controlReason());
            });
            case 10 -> CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneTarget);
                CanonicalProtobuf.bytes(output, 2, acknowledgementSet());
            });
            case 11 -> CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, laneTarget);
                CanonicalProtobuf.bytes(output, 2, controlReason());
                CanonicalProtobuf.uint32(output, 3, 1);
                CanonicalProtobuf.uint32(output, 4, 1);
                CanonicalProtobuf.bytes(output, 5, acknowledgementSet());
            });
            default -> throw new IllegalArgumentException("unsupported test lane control kind");
        };
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, controlKind,
                branch));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, controlKind);
            CanonicalProtobuf.uint32(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("lane-control-semantic")));
            CanonicalProtobuf.int64(output, 14, expectedControlVersion);
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    private static byte[] trustSetControlBody(final ShardId shard, final ControlRef controlRef,
                                              final int controlKind,
                                              final PayloadProofTrustSetRefV1 trustSet,
                                              final byte[] branch) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, controlKind,
                branch));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, controlKind);
            CanonicalProtobuf.uint32(output, 12, trustSet.version());
            CanonicalProtobuf.bytes(output, 13, trustSet.semanticHash());
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    private static byte[] profileControlBody(final ShardId shard, final ControlRef controlRef,
                                             final int controlKind, final ProfileRefV1 profile,
                                             final byte[] branch) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] payload = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, controlKind,
                branch));
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, controlKind);
            CanonicalProtobuf.uint32(output, 12, profile.version());
            CanonicalProtobuf.bytes(output, 13, profile.semanticHash());
            CanonicalProtobuf.bytes(output, 15, payload);
        });
    }

    private static byte[] controlReason() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
    }

    private static byte[] acknowledgementSet() {
        final byte[] scope = Bytes.sha256(Bytes.utf8("lane-control-ack-scope"));
        final byte[] possibleDuplicate = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("possible-duplicate")));
            CanonicalProtobuf.bytes(output, 3, scope);
        });
        final byte[] orderLoss = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(Bytes.utf8("order-loss")));
            CanonicalProtobuf.bytes(output, 3, scope);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, possibleDuplicate);
            CanonicalProtobuf.bytes(output, 1, orderLoss);
        });
    }

    private static byte[] publishOutcomeBody(final ShardId shard, final byte[] attemptId, final int sideEffect,
                                              final int disposition, final StableCode stableCode,
                                              final byte[] evidence, final byte[] observedAt) {
        final byte[] retry = sideEffect == 3 ? nestedPlaceholder() : verifiedRetryDecision(stableCode);
        return publishOutcomeBody(shard, attemptId, sideEffect, disposition, stableCode, evidence, observedAt,
                retry);
    }

    private static byte[] publishOutcomeBody(final ShardId shard, final byte[] attemptId, final int sideEffect,
                                              final int disposition, final StableCode stableCode,
                                              final byte[] evidence, final byte[] observedAt, final byte[] retry) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.uint32(output, 11, sideEffect);
            CanonicalProtobuf.uint32(output, 12, disposition);
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            if (sideEffect != 3) {
                CanonicalProtobuf.bytes(output, 14, publishEvidence(attemptId, sideEffect == 1, stableCode));
            }
            CanonicalProtobuf.bytes(output, 15, sideEffect == 3 ? nestedPlaceholder() : chargeVector());
            CanonicalProtobuf.bytes(output, 16, observedAt);
            CanonicalProtobuf.bytes(output, 17, retry);
        });
    }

    private static byte[] unknownRetryDecision(final StableCode cause, final long nextRetryAt) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("unknown-retry-policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("unknown-retry-policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            CanonicalProtobuf.int64(output, 6, nextRetryAt);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, cause.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] verifiedRetryDecision(final StableCode stableCode) {
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
    }

    private static byte[] publishNotPublishedBody(final ShardId shard, final byte[] attemptId,
                                                  final int disposition, final StableCode stableCode,
                                                  final long nextRetryAt) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, switch (disposition) {
                case 1 -> 2;
                case 2 -> 3;
                case 3 -> 4;
                default -> throw new IllegalArgumentException("invalid test disposition");
            });
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            if (disposition != 2) {
                CanonicalProtobuf.int64(output, 6, nextRetryAt);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.PUBLISH_OUTCOME.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.uint32(output, 11, 2);
            CanonicalProtobuf.uint32(output, 12, disposition);
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 14,
                    publishEvidence(attemptId, false, stableCode));
            CanonicalProtobuf.bytes(output, 15, chargeVector());
            CanonicalProtobuf.bytes(output, 16, new TrustedUtcIntervalEvidence(2_002, 2_002,
                    TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 4, 4,
                    Bytes.sha256(Bytes.utf8("retry-proof")), 0, null).canonicalBytes());
            CanonicalProtobuf.bytes(output, 17, retry);
        });
    }

    private static byte[] evidenceResolutionBody(final ShardId shard, final byte[] attemptId,
                                                 final StableCode stableCode, final int sideEffect,
                                                 final int disposition, final byte[] observedAt) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] policy = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint32(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, sideEffect == 1 ? 1 : disposition == 3 ? 4 : 2);
            CanonicalProtobuf.bytes(output, 2, policy);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 5_000);
            if (sideEffect == 2 && disposition != 2) {
                CanonicalProtobuf.int64(output, 6, 2_002);
            }
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.EVIDENCE_RESOLUTION.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, attemptId);
            CanonicalProtobuf.bytes(output, 11, evidenceCursor());
            CanonicalProtobuf.bytes(output, 12,
                    publishEvidence(attemptId, sideEffect == 1, stableCode));
            CanonicalProtobuf.uint32(output, 13, stableCode.wireValue());
            CanonicalProtobuf.uint32(output, 14, sideEffect);
            CanonicalProtobuf.uint32(output, 15, disposition);
            CanonicalProtobuf.bytes(output, 16, chargeVector());
            CanonicalProtobuf.bytes(output, 17, observedAt);
            CanonicalProtobuf.bytes(output, 18, retry);
        });
    }

    private static byte[] resolveUncertainRetryBody(final ShardId shard, final ControlRef controlRef,
                                                    final DestinationLaneId lane, final byte[] laneIncarnation,
                                                    final DelayMessageId messageId, final int generation,
                                                    final byte[] attemptId, final byte[] acknowledgementHash) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOLVE_UNCERTAIN.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, lane.bytes());
            CanonicalProtobuf.bytes(output, 12, laneIncarnation);
            CanonicalProtobuf.bytes(output, 13, messageId.bytes());
            CanonicalProtobuf.uint32(output, 14, generation);
            CanonicalProtobuf.bytes(output, 15, attemptId);
            CanonicalProtobuf.uint32(output, 16, 3);
            CanonicalProtobuf.uint32(output, 18, 1);
            CanonicalProtobuf.uint32(output, 19, 0);
            CanonicalProtobuf.bytes(output, 20, acknowledgementHash);
        });
    }

    private static byte[] resolveUncertainEvidenceBody(final ShardId shard, final ControlRef controlRef,
                                                       final DestinationLaneId lane, final byte[] laneIncarnation,
                                                       final DelayMessageId messageId, final int generation,
                                                       final byte[] attemptId, final int resolutionKind,
                                                       final byte[] evidence) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOLVE_UNCERTAIN.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, lane.bytes());
            CanonicalProtobuf.bytes(output, 12, laneIncarnation);
            CanonicalProtobuf.bytes(output, 13, messageId.bytes());
            CanonicalProtobuf.uint32(output, 14, generation);
            CanonicalProtobuf.bytes(output, 15, attemptId);
            CanonicalProtobuf.uint32(output, 16, resolutionKind);
            CanonicalProtobuf.bytes(output, 17, evidence);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.uint32(output, 19, 0);
        });
    }

    private static byte[] resolvePossibleDeliveryTerminalBody(final ShardId shard, final ControlRef controlRef,
                                                               final DestinationLaneId lane,
                                                               final byte[] laneIncarnation,
                                                               final DelayMessageId messageId, final int generation,
                                                               final byte[] attemptId,
                                                               final byte[] acknowledgementHash) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOLVE_UNCERTAIN.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, lane.bytes());
            CanonicalProtobuf.bytes(output, 12, laneIncarnation);
            CanonicalProtobuf.bytes(output, 13, messageId.bytes());
            CanonicalProtobuf.uint32(output, 14, generation);
            CanonicalProtobuf.bytes(output, 15, attemptId);
            CanonicalProtobuf.uint32(output, 16, 4);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.uint32(output, 19, 1);
            CanonicalProtobuf.bytes(output, 20, acknowledgementHash);
        });
    }

    private static byte[] replayDeadLetterBody(final ShardId shard, final ControlRef controlRef,
                                               final DelayMessageId messageId, final int generation,
                                               final long stateVersion, final long deliverAt, final long expireAt) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.REPLAY_DEAD_LETTER.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, generation);
            CanonicalProtobuf.uint32(output, 13, stateVersion);
            CanonicalProtobuf.int64(output, 14, deliverAt);
            CanonicalProtobuf.int64(output, 15, expireAt);
            CanonicalProtobuf.bytes(output, 16, new io.nereusstream.delay.protocol.RetryPolicyRefV1(
                    Bytes.utf8("replay-policy"), 1, Bytes.sha256(Bytes.utf8("replay-policy-semantic")))
                    .canonicalBytes());
            CanonicalProtobuf.uint32(output, 17, 0);
        });
    }

    private static byte[] claimResultBody(final ShardId shard, final byte[] claimId,
                                          final DelayMessageId messageId, final int generation,
                                          final DestinationLaneId lane, final byte[] laneIncarnation,
                                          final long laneControlVersion, final long runtimeLaneVersion,
                                          final byte[] timelineKey, final byte[] owner,
                                          final byte[] storeIncarnation, final long claimDeadline,
                                          final int sourceWorkKind, final long observedAtEarliest,
                                          final long observedAtLatest) {
        return claimResultBody(shard, claimId, messageId, generation, lane, laneIncarnation,
                laneControlVersion, runtimeLaneVersion, timelineKey, owner, storeIncarnation, claimDeadline,
                sourceWorkKind, observedAtEarliest, observedAtLatest, chargeVector());
    }

    private static byte[] claimResultBody(final ShardId shard, final byte[] claimId,
                                          final DelayMessageId messageId, final int generation,
                                          final DestinationLaneId lane, final byte[] laneIncarnation,
                                          final long laneControlVersion, final long runtimeLaneVersion,
                                          final byte[] timelineKey, final byte[] owner,
                                          final byte[] storeIncarnation, final long claimDeadline,
                                          final int sourceWorkKind, final long observedAtEarliest,
                                          final long observedAtLatest, final byte[] transfer) {
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shard.partition());
        });
        final byte[] precondition = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, claimId);
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32(output, 3, generation);
            CanonicalProtobuf.int64(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, lane.bytes());
            CanonicalProtobuf.bytes(output, 6, laneIncarnation);
            CanonicalProtobuf.int64(output, 7, laneControlVersion);
            CanonicalProtobuf.int64(output, 8, runtimeLaneVersion);
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(timelineKey));
            CanonicalProtobuf.bytes(output, 12, chargeVector());
            CanonicalProtobuf.int64(output, 13, claimDeadline);
            CanonicalProtobuf.bytes(output, 14, owner);
            CanonicalProtobuf.bytes(output, 15, storeIncarnation);
            CanonicalProtobuf.uint32(output, 16, sourceWorkKind);
            CanonicalProtobuf.uint32(output, 17, 0);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.bytes(output, 19,
                    Bytes.sha256(Bytes.utf8("nereus-delay-attempt-obligation-set-v1\0")));
            CanonicalProtobuf.bytes(output, 20, claimTimelineSemanticDigest(messageId, lane, timelineKey,
                    sourceWorkKind));
        });
        final byte[] observedAt = new TrustedUtcIntervalEvidence(observedAtEarliest, observedAtLatest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("claim-clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("claim-result-proof")), 0, null).canonicalBytes();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.CLAIM_RESULT.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, claimId);
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, generation);
            CanonicalProtobuf.bytes(output, 13, lane.bytes());
            CanonicalProtobuf.bytes(output, 14, laneIncarnation);
            CanonicalProtobuf.bytes(output, 15, precondition);
            CanonicalProtobuf.uint32(output, 16, 1);
            CanonicalProtobuf.uint32(output, 17, StableCode.CLAIM_PERMANENT_FAILURE.wireValue());
            CanonicalProtobuf.bytes(output, 18, observedAt);
            CanonicalProtobuf.bytes(output, 20, transfer);
        });
    }

    private static byte[] claimTimelineSemanticDigest(final DelayMessageId messageId,
                                                       final DestinationLaneId lane, final byte[] timelineKey,
                                                       final int sourceWorkKind) {
        final byte[] semanticFields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, sourceWorkKind);
            CanonicalProtobuf.bytes(output, 2, timelineKey);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(timelineKey));
            CanonicalProtobuf.int64(output, 4, 2_000);
            CanonicalProtobuf.int64(output, 5, 2_000);
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.uint32(output, 9, 1);
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-timeline-work-semantic-v1\0"), semanticFields);
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static byte[] chargeVectorWithActiveMessages(final long activeMessages) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, activeMessages);
            for (int number = 2; number <= 17; number++) {
                CanonicalProtobuf.uint64(output, number, 0);
            }
        });
    }

    private static CapacityVectorV1 capacityVector(final CapacityDimensionV1 dimension, final long amount) {
        return new CapacityVectorV1(capacityAmounts(dimension, amount));
    }

    private static long[] capacityAmounts(final CapacityDimensionV1 dimension, final long amount) {
        final long[] result = new long[CapacityDimensionV1.COUNT];
        result[dimension.wireValue() - 1] = amount;
        return result;
    }

    private static ShardCapacityEnvelopeV1 capacityEnvelopeWithNonOutcomeReserve() {
        final PublishAdmissionBody.ChargeVector logicalLimit = new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        final QuotaGrantRefV1 logicalGrant = new QuotaGrantRefV1(
                Bytes.sha256(Bytes.utf8("control-reserve-logical-grant")), 1, logicalLimit);
        final CapacityVectorV1 nonOutcomeVector = capacityVector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 3);
        final CapacityGrantV1 outcome = new CapacityGrantV1(CapacityGrantKindV1.OUTCOME_RESERVE,
                Bytes.sha256(Bytes.utf8("control-reserve-outcome-grant")), 1, CapacityVectorV1.empty());
        final CapacityGrantV1 nonOutcome = new CapacityGrantV1(CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                Bytes.sha256(Bytes.utf8("control-reserve-non-outcome-grant")), 1, nonOutcomeVector);
        final CapacityGrantV1 recovery = new CapacityGrantV1(CapacityGrantKindV1.RECOVERY_WORKING,
                Bytes.sha256(Bytes.utf8("control-reserve-recovery-grant")), 1, CapacityVectorV1.empty());
        final CapacityGrantV1 emergency = new CapacityGrantV1(CapacityGrantKindV1.EMERGENCY_HEADROOM,
                Bytes.sha256(Bytes.utf8("control-reserve-emergency-grant")), 1, CapacityVectorV1.empty());
        return new ShardCapacityEnvelopeV1(Bytes.sha256(Bytes.utf8("control-reserve-envelope")), 1,
                logicalGrant, nonOutcomeVector, outcome, nonOutcome, recovery, emergency,
                Bytes.sha256(Bytes.utf8("control-reserve-artifact")));
    }

    private static ShardCapacityEnvelopeV1 capacityEnvelopeWithSystemWriterReserve() {
        final PublishAdmissionBody.ChargeVector logicalLimit = new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        final QuotaGrantRefV1 logicalGrant = new QuotaGrantRefV1(
                Bytes.sha256(Bytes.utf8("system-writer-logical-grant")), 1, logicalLimit);
        final long[] writerAmounts = new long[CapacityDimensionV1.COUNT];
        writerAmounts[CapacityDimensionV1.SYSTEM_WRITER_RESERVED_RECORDS.wireValue() - 1] = 2;
        writerAmounts[CapacityDimensionV1.SYSTEM_WRITER_RESERVED_BYTES.wireValue() - 1] = 4;
        writerAmounts[CapacityDimensionV1.SYSTEM_WRITER_RESERVED_BYTES_PER_SECOND.wireValue() - 1] = 8;
        writerAmounts[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = 3;
        final CapacityVectorV1 writerVector = new CapacityVectorV1(writerAmounts);
        final CapacityGrantV1 outcome = new CapacityGrantV1(CapacityGrantKindV1.OUTCOME_RESERVE,
                Bytes.sha256(Bytes.utf8("system-writer-outcome-grant")), 1, CapacityVectorV1.empty());
        final CapacityGrantV1 nonOutcome = new CapacityGrantV1(CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                Bytes.sha256(Bytes.utf8("system-writer-non-outcome-grant")), 1, writerVector);
        final CapacityGrantV1 recovery = new CapacityGrantV1(CapacityGrantKindV1.RECOVERY_WORKING,
                Bytes.sha256(Bytes.utf8("system-writer-recovery-grant")), 1, CapacityVectorV1.empty());
        final CapacityGrantV1 emergency = new CapacityGrantV1(CapacityGrantKindV1.EMERGENCY_HEADROOM,
                Bytes.sha256(Bytes.utf8("system-writer-emergency-grant")), 1, CapacityVectorV1.empty());
        return new ShardCapacityEnvelopeV1(Bytes.sha256(Bytes.utf8("system-writer-envelope")), 1,
                logicalGrant, writerVector, outcome, nonOutcome, recovery, emergency,
                Bytes.sha256(Bytes.utf8("system-writer-artifact")));
    }

    private static ShardCapacityEnvelopeV1 capacityEnvelope(final long envelopeVersion) {
        final PublishAdmissionBody.ChargeVector logicalLimit = new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0);
        final QuotaGrantRefV1 logicalGrant = new QuotaGrantRefV1(
                Bytes.sha256(Bytes.utf8("bound-capacity-logical-grant")), 1, logicalLimit);
        final long[] outcomeAmounts = new long[CapacityDimensionV1.COUNT];
        outcomeAmounts[CapacityDimensionV1.RESULT_RECORDS.wireValue() - 1] = 1;
        outcomeAmounts[CapacityDimensionV1.RESULT_BYTES.wireValue() - 1] = 1;
        final CapacityVectorV1 outcomeVector = new CapacityVectorV1(outcomeAmounts);
        final CapacityGrantV1 outcomeGrant = new CapacityGrantV1(CapacityGrantKindV1.OUTCOME_RESERVE,
                Bytes.sha256(Bytes.utf8("bound-capacity-outcome-grant")), 1, outcomeVector);
        final CapacityVectorV1 empty = CapacityVectorV1.empty();
        final CapacityGrantV1 nonOutcome = new CapacityGrantV1(CapacityGrantKindV1.NON_OUTCOME_CONTROL,
                Bytes.sha256(Bytes.utf8("bound-capacity-non-outcome-grant")), 1, empty);
        final CapacityGrantV1 recovery = new CapacityGrantV1(CapacityGrantKindV1.RECOVERY_WORKING,
                Bytes.sha256(Bytes.utf8("bound-capacity-recovery-grant")), 1, empty);
        final CapacityGrantV1 emergency = new CapacityGrantV1(CapacityGrantKindV1.EMERGENCY_HEADROOM,
                Bytes.sha256(Bytes.utf8("bound-capacity-emergency-grant")), 1, empty);
        return new ShardCapacityEnvelopeV1(Bytes.sha256(Bytes.utf8("bound-capacity-envelope-" + envelopeVersion)),
                envelopeVersion, logicalGrant, outcomeVector, outcomeGrant, nonOutcome, recovery, emergency,
                Bytes.sha256(Bytes.utf8("bound-capacity-artifact")));
    }

    private static byte[] replaceAdmissionCharge(final byte[] body, final long resultRecords,
                                                 final long resultBytes) {
        final byte[] charge = CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                final long value = number == 9 ? resultRecords : number == 10 ? resultBytes : 0;
                CanonicalProtobuf.uint64(output, number, value);
            }
        });
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(body);
        return CanonicalProtobuf.message(output -> {
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 19) {
                    CanonicalProtobuf.bytes(output, 19, charge);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static byte[] replaceAdmissionClaim(final byte[] body, final ClaimRecord claim,
                                                final byte[] storeIncarnation) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(body);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        final byte[] certificate = fields.stream()
                .filter(field -> field.number() == 23)
                .findFirst()
                .map(field -> replaceReadyCertificate(field.rawValue(), storeIncarnation))
                .orElseThrow();
        final byte[] certificateDigest = readyCertificateDigest(certificate);
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                switch (field.number()) {
                    case 11 -> CanonicalProtobuf.bytes(output, 11, storeIncarnation);
                    case 12 -> CanonicalProtobuf.bytes(output, 12, claim.claimId());
                    case 20 -> CanonicalProtobuf.bytes(output, 20, certificateDigest);
                    case 23 -> CanonicalProtobuf.bytes(output, 23, certificate);
                    case 25 -> CanonicalProtobuf.bytes(output, 25, claim.preconditionBytes());
                    default -> {
                        if (field.wireType() == 0) {
                            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                        } else {
                            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                        }
                    }
                }
            }
        });
    }

    private static byte[] replaceReadyCertificate(final byte[] encoded, final byte[] storeIncarnation) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 16) {
                    break;
                }
                if (field.number() == 3) {
                    CanonicalProtobuf.bytes(output, 3, storeIncarnation);
                } else if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader prefixReader = new CanonicalProtobuf.Reader(prefix);
            while (prefixReader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = prefixReader.next();
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
            CanonicalProtobuf.bytes(output, 16, Bytes.sha256(
                    Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix));
        });
    }

    private static byte[] readyCertificateDigest(final byte[] certificate) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(certificate);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 16) {
                    break;
                }
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix);
    }

    private static byte[] nestedPlaceholder() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, new byte[]{1}));
    }

    private static byte[] evidenceCursor() {
        return EvidenceCursorV1.kafka(Bytes.sha256(Bytes.utf8("evidence-resolution-lane")), new byte[16],
                java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("evidence-resolution-topic")), 16), 0, 1,
                2_002, 1, 1).canonicalBytes();
    }

    private static byte[] publishEvidence(final byte[] attemptId, final boolean published,
                                          final StableCode stableCode) {
        final byte[] owner = io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1.publishAttempt(attemptId)
                .canonicalBytes();
        final byte[] branch = published ? CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, io.nereusstream.delay.protocol.BrokerResourceIdentityV1.kafka(
                    new io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1("cluster-a",
                            java.util.UUID.nameUUIDFromBytes(Bytes.utf8("publish-evidence-topic"))))
                    .canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 2_002);
            CanonicalProtobuf.bytes(output, 6, owner);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("prepared-evidence")));
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(Bytes.utf8("response-evidence")));
        }) : CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1,
                    io.nereusstream.delay.protocol.ProtocolTestFixtures.baselineKafkaChannel());
            CanonicalProtobuf.bytes(output, 2, owner);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("prepared-evidence")));
            CanonicalProtobuf.uint32(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, Bytes.sha256(Bytes.utf8("request-evidence")));
            CanonicalProtobuf.uint32(output, 6, 1);
            CanonicalProtobuf.uint32(output, 7, stableCode.wireValue());
        });
        return io.nereusstream.delay.protocol.PublishEvidenceV1.create(
                published ? io.nereusstream.delay.protocol.PublishEvidenceKindV1.KAFKA_PRODUCE_ACK
                        : io.nereusstream.delay.protocol.PublishEvidenceKindV1.ADAPTER_NON_SUBMISSION,
                published ? io.nereusstream.delay.protocol.EvidenceVerificationStatusV1.VERIFIED_PUBLISHED
                        : io.nereusstream.delay.protocol.EvidenceVerificationStatusV1.VERIFIED_NOT_PUBLISHED,
                branch).canonicalBytes();
    }

    private static int compareObligations(final AttemptObligationRef left, final AttemptObligationRef right) {
        final int id = compareUnsigned(left.publishAttemptId(), right.publishAttemptId());
        return id != 0 ? id : compareUnsigned(left.encodedInflightKey(), right.encodedInflightKey());
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static byte[] resourceRetireBody(final ShardId shard, final byte[] resource,
                                              final long expectedVersion, final byte[] protections) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(subject -> {
                CanonicalProtobuf.bytes(subject, 1, shard.routeIncarnation().bytes());
                CanonicalProtobuf.uint32(subject, 2, shard.partition());
            }));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_RETIRE_INTENT.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.uint32(output, 10, ResourceKind.LOCAL_STORE.wireValue());
            CanonicalProtobuf.bytes(output, 11, resource);
            CanonicalProtobuf.uint64Bits(output, 12, expectedVersion);
            CanonicalProtobuf.bytes(output, 13, protections);
        });
    }

    private static byte[] localStoreResource(final ShardId shard) {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 7,
                CanonicalProtobuf.message(local -> {
                    CanonicalProtobuf.bytes(local, 1, CanonicalProtobuf.message(subject -> {
                        CanonicalProtobuf.bytes(subject, 1, shard.routeIncarnation().bytes());
                        CanonicalProtobuf.uint32(subject, 2, shard.partition());
                    }));
                    CanonicalProtobuf.bytes(local, 2, new byte[16]);
                    CanonicalProtobuf.bytes(local, 3, Bytes.sha256(Bytes.utf8("db-identity")));
                    CanonicalProtobuf.bytes(local, 4, Bytes.sha256(Bytes.utf8("root-policy")));
                })));
    }

    private static byte[] resourceProtectionSet(final byte[] protectedResourceId) {
        final byte[] reference = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, protectedResourceId);
            CanonicalProtobuf.uint32(output, 3, 1);
        });
        final byte[] repeated = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, reference));
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), repeated);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reference);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private static byte[] resourceDeleteConfirmedBody(final ShardId shard, final SystemMutation retire,
                                                       final ResourceRetireIntentBody parsedRetire,
                                                       final ResourceDeleteConfirmedBody.DeleteOutcome outcome,
                                                       final byte[] providerRequestHash) {
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(2_000, 2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("delete-clock"), 1, 8, 8,
                Bytes.sha256(Bytes.utf8("delete-time")), 0, null);
        final byte[] intent = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, retire.systemMutationId());
            CanonicalProtobuf.bytes(output, 2, retire.mutationHash());
            CanonicalProtobuf.bytes(output, 3, parsedRetire.resource().identityHash());
            CanonicalProtobuf.uint64Bits(output, 4, parsedRetire.expectedResourceStateVersion());
        });
        final byte[] evidence = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, parsedRetire.resource().identityHash());
            CanonicalProtobuf.bytes(output, 2, providerRequestHash);
            CanonicalProtobuf.uint32(output, 3, outcome.wireValue());
            CanonicalProtobuf.bytes(output, 6, Bytes.sha256(Bytes.utf8("delete-response")));
            CanonicalProtobuf.bytes(output, 7, time.canonicalBytes());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, CanonicalProtobuf.message(subject -> {
                CanonicalProtobuf.bytes(subject, 1, shard.routeIncarnation().bytes());
                CanonicalProtobuf.uint32(subject, 2, shard.partition());
            }));
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.RESOURCE_DELETE_CONFIRMED.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, intent);
            CanonicalProtobuf.uint32(output, 11, outcome.wireValue());
            CanonicalProtobuf.bytes(output, 12, evidence);
            CanonicalProtobuf.bytes(output, 13, time.canonicalBytes());
        });
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster-a", UUID.nameUUIDFromBytes(Bytes.utf8("topic")), offset,
                1, timestamp);
    }
}
