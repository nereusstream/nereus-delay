package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DlqExportMode;
import com.nereusstream.delay.protocol.DlqExportState;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.protocol.UncertainPolicy;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DlqExportApplyTest {
    @TempDir
    Path tempDir;

    @Test
    void appliesConfiguredExportOutcomeAndAdvancesTheOutboxAtomically() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("dlq-apply-lane"));
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shardId, "dlq-test", java.util.UUID.randomUUID(), 2, null, 2_000);
        final byte[] envelopeHash = Bytes.sha256(Bytes.utf8("dlq-envelope"));
        final byte[] retainedCharge = chargeVectorWithActiveMessages(2);
        final MessageRecord dead = new MessageRecord(
                        MessageStatus.DEAD_LETTER,
                        0,
                        7,
                        1_000,
                        5_000,
                        lane,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload"),
                        source.canonicalBytes())
                .withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.DEAD_LETTER, java.util.List.of(), 0, 0, false, 7));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                messageId,
                0,
                MessageStatus.DEAD_LETTER,
                StableCode.CLAIM_PERMANENT_FAILURE,
                7,
                source.canonicalBytes(),
                false);
        final DlqExportRecord pending =
                DlqExportRecord.pending(messageId, 0, 7, envelopeHash, retainedCharge, source.canonicalBytes());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity service =
                AuthorIdentity.service(Bytes.sha256(Bytes.utf8("dlq-service")), Bytes.sha256(Bytes.utf8("dlq-run")), 1);
        final byte[] mismatchedBody =
                resultBody(shardId, messageId, pending, envelopeHash, chargeVectorWithActiveMessages(1));
        final SystemMutation mismatchedMutation = SystemMutation.signed(
                shardId,
                SystemMutationType.DLQ_EXPORT_RESULT,
                10_000,
                com.nereusstream.delay.protocol.DlqExportResultBody.decode(mismatchedBody)
                        .logicalOperationIdentity(),
                mismatchedBody,
                service.canonicalBytes(),
                1,
                keyPair.getPrivate());
        final byte[] body = resultBody(shardId, messageId, pending, envelopeHash, retainedCharge);
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                SystemMutationType.DLQ_EXPORT_RESULT,
                10_000,
                com.nereusstream.delay.protocol.DlqExportResultBody.decode(body).logicalOperationIdentity(),
                body,
                service.canonicalBytes(),
                1,
                keyPair.getPrivate());
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("dlq-apply"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), dead.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, 0), terminal.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(pending.dlqExportId()),
                        pending.encode());
            });
            final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults());
            final SystemMutationResult mismatched =
                    delayShard.applySystemMutation(mismatchedMutation, sourceAfter(source), keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, mismatched.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, mismatched.stableCode());
            assertEquals(
                    DlqExportState.PENDING,
                    delayShard.getDlqExportRecord(messageId, 0).state());
            assertEquals(1, delayShard.getDlqExportRecord(messageId, 0).physicalAttemptNo());

            final SystemMutationResult result =
                    delayShard.applySystemMutation(mutation, sourceAfterTwice(source), keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(
                    DlqExportState.PUBLISHED,
                    delayShard.getDlqExportRecord(messageId, 0).state());
            assertEquals(1, delayShard.getDlqExportRecord(messageId, 0).physicalAttemptNo());
            assertArrayEquals(
                    retainedCharge, delayShard.getDlqExportRecord(messageId, 0).retainedCharge());
            assertEquals(
                    result, delayShard.applySystemMutation(mutation, sourceAfterTwice(source), keyPair.getPublic()));
        }
    }

    @Test
    void catalogBackedDlqOutcomeRecomputesPinnedPolicyBeforePersisting() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 11);
        final byte[] laneTuple = Bytes.utf8("catalog-backed-dlq-lane");
        final DestinationLaneId lane = DestinationLaneId.derive(laneTuple);
        final KafkaSourcePosition terminalSource =
                new KafkaSourcePosition(shardId, "dlq-test", java.util.UUID.randomUUID(), 20, null, 2_000);
        final RetryPolicySemantic policy = new RetryPolicySemantic(
                Bytes.utf8("catalog-dlq-policy"),
                1,
                100,
                1_000,
                3,
                4_000,
                UncertainPolicy.HOLD_FOR_EVIDENCE,
                0,
                DlqExportMode.BASELINE_AT_LEAST_ONCE,
                100,
                500,
                3,
                1_000,
                true,
                nonZero(32, 41));
        final ProfileRef profile =
                new ProfileRef(Bytes.utf8("catalog-dlq-profile"), 1, nonZero(32, 42), ProfileKind.DESTINATION);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                profile,
                policy.ref(),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                laneTuple,
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId, intent, 9_000);
        final DelayMessageId messageId = schedule.delayMessageId();
        final ScheduleBinding binding = ScheduleBinding.fromCommand(schedule, lane, laneTuple);
        final byte[] envelopeHash = Bytes.sha256(Bytes.utf8("catalog-dlq-envelope"));
        final MessageRecord dead = new MessageRecord(
                        MessageStatus.DEAD_LETTER,
                        0,
                        7,
                        1_000,
                        5_000,
                        lane,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload"),
                        terminalSource.canonicalBytes())
                .withRuntimeIndex(
                        GenerationRuntimeIndex.none(GenerationAggregateState.DEAD_LETTER, List.of(), 0, 0, false, 7));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                messageId,
                0,
                MessageStatus.DEAD_LETTER,
                StableCode.CLAIM_PERMANENT_FAILURE,
                7,
                terminalSource.canonicalBytes(),
                false);
        final DlqExportRecord pending =
                DlqExportRecord.pending(messageId, 0, 7, envelopeHash, terminalSource.canonicalBytes());
        final byte[] body = resultBody(
                shardId,
                messageId,
                pending,
                envelopeHash,
                chargeVector(),
                policy.ref().canonicalBytes());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity service = AuthorIdentity.service(
                Bytes.sha256(Bytes.utf8("catalog-dlq-service")), Bytes.sha256(Bytes.utf8("catalog-dlq-run")), 1);
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                SystemMutationType.DLQ_EXPORT_RESULT,
                10_000,
                com.nereusstream.delay.protocol.DlqExportResultBody.decode(body).logicalOperationIdentity(),
                body,
                service.canonicalBytes(),
                1,
                keyPair.getPrivate());
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("catalog-dlq-apply"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), dead.encode());
                batch.putValue(ColumnFamily.ID, 4, KeyCodec.idScheduleBinding(messageId), binding.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, 0), terminal.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(pending.dlqExportId()),
                        pending.encode());
            });
            final InMemoryRetryPolicyCatalog catalog = new InMemoryRetryPolicyCatalog();
            catalog.publish(policy, terminalSource);
            final DelayShard delayShard =
                    new DelayShard(store, DelayShardConfig.defaults(), null, null, null, null, catalog);
            final byte[] wrongBody = resultBody(shardId, messageId, pending, envelopeHash, chargeVector());
            final SystemMutation wrongMutation = SystemMutation.signed(
                    shardId,
                    SystemMutationType.DLQ_EXPORT_RESULT,
                    10_000,
                    com.nereusstream.delay.protocol.DlqExportResultBody.decode(wrongBody)
                            .logicalOperationIdentity(),
                    wrongBody,
                    service.canonicalBytes(),
                    1,
                    keyPair.getPrivate());
            final SystemMutationResult rejected =
                    delayShard.applySystemMutation(wrongMutation, sourceAfter(terminalSource), keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, rejected.applyStatus());
            assertEquals(StableCode.INTEGRITY_ERROR, rejected.stableCode());
            final SystemMutationResult result =
                    delayShard.applySystemMutation(mutation, sourceAfterTwice(terminalSource), keyPair.getPublic());
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(
                    DlqExportState.PUBLISHED,
                    delayShard.getDlqExportRecord(messageId, 0).state());
        }
    }

    @Test
    void preservesUnknownStableCodeWhenApplyingExportOutcome() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 10);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("dlq-unknown-lane"));
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shardId, "dlq-test", java.util.UUID.randomUUID(), 2, null, 2_000);
        final byte[] envelopeHash = Bytes.sha256(Bytes.utf8("dlq-unknown-envelope"));
        final MessageRecord dead = new MessageRecord(
                        MessageStatus.DEAD_LETTER,
                        0,
                        7,
                        1_000,
                        5_000,
                        lane,
                        OrderingMode.BEST_EFFORT,
                        Bytes.utf8("payload"),
                        source.canonicalBytes())
                .withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.DEAD_LETTER, java.util.List.of(), 0, 0, false, 7));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(
                messageId,
                0,
                MessageStatus.DEAD_LETTER,
                StableCode.CLAIM_PERMANENT_FAILURE,
                7,
                source.canonicalBytes(),
                false);
        final DlqExportRecord pending = DlqExportRecord.pending(messageId, 0, 7, envelopeHash, source.canonicalBytes());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity service = AuthorIdentity.service(
                Bytes.sha256(Bytes.utf8("dlq-unknown-service")), Bytes.sha256(Bytes.utf8("dlq-unknown-run")), 1);
        final byte[] body = unknownResultBody(shardId, messageId, pending, envelopeHash);
        final SystemMutation mutation = SystemMutation.signed(
                shardId,
                SystemMutationType.DLQ_EXPORT_RESULT,
                10_000,
                com.nereusstream.delay.protocol.DlqExportResultBody.decode(body).logicalOperationIdentity(),
                body,
                service.canonicalBytes(),
                1,
                keyPair.getPrivate());
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("dlq-unknown"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), dead.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, 0), terminal.encode());
                batch.putValue(
                        ColumnFamily.TERMINAL,
                        DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(pending.dlqExportId()),
                        pending.encode());
            });
            final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults());
            final SourcePosition appliedPosition = sourceAfter(source);
            final SystemMutationResult result =
                    delayShard.applySystemMutation(mutation, appliedPosition, keyPair.getPublic());
            assertEquals(StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN, result.stableCode());
            assertEquals(
                    DlqExportState.UNCERTAIN,
                    delayShard.getDlqExportRecord(messageId, 0).state());
            assertEquals(result, delayShard.applySystemMutation(mutation, appliedPosition, keyPair.getPublic()));
        }
    }

    private static SourcePosition sourceAfter(final KafkaSourcePosition prior) {
        return new KafkaSourcePosition(
                prior.shardId(),
                prior.authenticatedClusterId(),
                prior.nativeTopicUuid(),
                prior.offset() + 1,
                prior.leaderEpoch(),
                prior.brokerPersistenceTimeEpochMs() + 1);
    }

    private static SourcePosition sourceAfterTwice(final KafkaSourcePosition prior) {
        return new KafkaSourcePosition(
                prior.shardId(),
                prior.authenticatedClusterId(),
                prior.nativeTopicUuid(),
                prior.offset() + 2,
                prior.leaderEpoch(),
                prior.brokerPersistenceTimeEpochMs() + 2);
    }

    private static byte[] resultBody(
            final ShardId shardId,
            final DelayMessageId messageId,
            final DlqExportRecord pending,
            final byte[] envelopeHash) {
        return resultBody(shardId, messageId, pending, envelopeHash, chargeVector());
    }

    private static byte[] resultBody(
            final ShardId shardId,
            final DelayMessageId messageId,
            final DlqExportRecord pending,
            final byte[] envelopeHash,
            final byte[] transfer) {
        return resultBody(shardId, messageId, pending, envelopeHash, transfer, retryPolicyRef());
    }

    private static byte[] resultBody(
            final ShardId shardId,
            final DelayMessageId messageId,
            final DlqExportRecord pending,
            final byte[] envelopeHash,
            final byte[] transfer,
            final byte[] retryPolicyRef) {
        final TrustedUtcIntervalEvidence observed = new TrustedUtcIntervalEvidence(
                2_001,
                2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("clock-proof")),
                0,
                null);
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shardId.partition());
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.DLQ_EXPORT_RESULT.wireValue());
            CanonicalProtobuf.int64(output, 3, 10_000);
            CanonicalProtobuf.bytes(output, 10, pending.dlqExportId());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 7);
            CanonicalProtobuf.bytes(output, 14, envelopeHash);
            CanonicalProtobuf.uint32(output, 15, 1);
            CanonicalProtobuf.uint32(output, 16, 1);
            CanonicalProtobuf.uint32(output, 17, 0);
            CanonicalProtobuf.uint32(output, 18, StableCode.OK.wireValue());
            CanonicalProtobuf.bytes(output, 19, evidence(pending.dlqExportId()));
            CanonicalProtobuf.bytes(output, 20, transfer);
            CanonicalProtobuf.bytes(output, 21, observed.canonicalBytes());
            CanonicalProtobuf.bytes(output, 22, retryDecision(retryPolicyRef));
            CanonicalProtobuf.uint32(output, 23, DlqExportState.PUBLISHED.wireValue());
            CanonicalProtobuf.uint32(output, 24, 1);
        });
    }

    private static byte[] unknownResultBody(
            final ShardId shardId,
            final DelayMessageId messageId,
            final DlqExportRecord pending,
            final byte[] envelopeHash) {
        final TrustedUtcIntervalEvidence observed = new TrustedUtcIntervalEvidence(
                2_001,
                2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("unknown-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("unknown-clock-proof")),
                0,
                null);
        final byte[] subject = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32(output, 2, shardId.partition());
        });
        final byte[] retry = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 5);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef());
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 2_000);
            CanonicalProtobuf.uint64(output, 5, 3_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue());
            CanonicalProtobuf.uint32(output, 9, 2);
        });
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, subject);
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.DLQ_EXPORT_RESULT.wireValue());
            CanonicalProtobuf.int64(output, 3, 10_000);
            CanonicalProtobuf.bytes(output, 10, pending.dlqExportId());
            CanonicalProtobuf.bytes(output, 11, messageId.bytes());
            CanonicalProtobuf.uint32(output, 12, 0);
            CanonicalProtobuf.uint64(output, 13, 7);
            CanonicalProtobuf.bytes(output, 14, envelopeHash);
            CanonicalProtobuf.uint32(output, 15, 1);
            CanonicalProtobuf.uint32(output, 16, 3);
            CanonicalProtobuf.uint32(output, 17, 4);
            CanonicalProtobuf.uint32(output, 18, StableCode.DLQ_EXPORT_OUTCOME_UNKNOWN.wireValue());
            CanonicalProtobuf.bytes(output, 20, chargeVector());
            CanonicalProtobuf.bytes(output, 21, observed.canonicalBytes());
            CanonicalProtobuf.bytes(output, 22, retry);
            CanonicalProtobuf.uint32(output, 23, DlqExportState.UNCERTAIN.wireValue());
            CanonicalProtobuf.uint32(output, 24, 1);
        });
    }

    private static byte[] evidence(final byte[] exportId) {
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(
                    output,
                    1,
                    com.nereusstream.delay.protocol.BrokerResourceIdentity.kafka(
                                    new com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity(
                                            "cluster-a", java.util.UUID.nameUUIDFromBytes(Bytes.utf8("dlq-topic"))))
                            .canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 2_001);
            CanonicalProtobuf.bytes(
                    output,
                    6,
                    com.nereusstream.delay.protocol.ExternalDeliveryIdentity.dlqExport(exportId)
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("dlq-prepared")));
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(Bytes.utf8("dlq-response")));
        });
        return com.nereusstream.delay.protocol.PublishEvidence.create(
                        com.nereusstream.delay.protocol.PublishEvidenceKind.KAFKA_PRODUCE_ACK,
                        com.nereusstream.delay.protocol.EvidenceVerificationStatus.VERIFIED_PUBLISHED,
                        branch)
                .canonicalBytes();
    }

    private static byte[] retryDecision() {
        return retryDecision(retryPolicyRef());
    }

    private static byte[] retryDecision(final byte[] retryPolicyRef) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, retryPolicyRef);
            CanonicalProtobuf.uint32(output, 3, 1);
            CanonicalProtobuf.uint64(output, 4, 2_000);
            CanonicalProtobuf.uint64(output, 5, 3_000);
            CanonicalProtobuf.uint32(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, StableCode.OK.wireValue());
            CanonicalProtobuf.uint32(output, 9, 2);
        });
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int field = 1; field <= 17; field++) {
                CanonicalProtobuf.uint64(output, field, 0);
            }
        });
    }

    private static byte[] chargeVectorWithActiveMessages(final long activeMessages) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, activeMessages);
            for (int field = 2; field <= 17; field++) {
                CanonicalProtobuf.uint64(output, field, 0);
            }
        });
    }

    private static byte[] retryPolicyRef() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, Bytes.utf8("policy"));
            CanonicalProtobuf.uint64Bits(output, 2, 1);
            CanonicalProtobuf.bytes(output, 3, Bytes.sha256(Bytes.utf8("policy-hash")));
        });
    }

    private static byte[] nonZero(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
