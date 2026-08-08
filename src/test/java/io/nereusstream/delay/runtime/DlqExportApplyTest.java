package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DlqExportApplyTest {
    @TempDir
    Path tempDir;

    @Test
    void appliesConfiguredExportOutcomeAndAdvancesTheOutboxAtomically() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 9);
        final DelayMessageId messageId = DelayMessageId.random(shardId);
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("dlq-apply-lane"));
        final KafkaSourcePosition source = new KafkaSourcePosition(shardId, "dlq-test", java.util.UUID.randomUUID(),
                2, null, 2_000);
        final byte[] envelopeHash = Bytes.sha256(Bytes.utf8("dlq-envelope"));
        final MessageRecord dead = new MessageRecord(MessageStatus.DEAD_LETTER, 0, 7, 1_000, 5_000, lane,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"), source.canonicalBytes())
                .withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.DEAD_LETTER, java.util.List.of(),
                        0, 0, false, 7));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(messageId, 0,
                MessageStatus.DEAD_LETTER, StableCode.CLAIM_PERMANENT_FAILURE, 7, source.canonicalBytes(), false);
        final DlqExportRecord pending = DlqExportRecord.pending(messageId, 0, 7, envelopeHash,
                source.canonicalBytes());
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final AuthorIdentity service = AuthorIdentity.service(Bytes.sha256(Bytes.utf8("dlq-service")),
                Bytes.sha256(Bytes.utf8("dlq-run")), 1);
        final byte[] mismatchedBody = resultBody(shardId, messageId, pending, envelopeHash,
                chargeVectorWithActiveMessages(1));
        final SystemMutation mismatchedMutation = SystemMutation.signed(shardId,
                SystemMutationType.DLQ_EXPORT_RESULT, 10_000,
                io.nereusstream.delay.protocol.DlqExportResultBody.decode(mismatchedBody)
                        .logicalOperationIdentity(), mismatchedBody, service.canonicalBytes(), 1,
                keyPair.getPrivate());
        final byte[] body = resultBody(shardId, messageId, pending, envelopeHash);
        final SystemMutation mutation = SystemMutation.signed(shardId,
                SystemMutationType.DLQ_EXPORT_RESULT, 10_000,
                io.nereusstream.delay.protocol.DlqExportResultBody.decode(body).logicalOperationIdentity(), body,
                service.canonicalBytes(), 1, keyPair.getPrivate());
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("dlq-apply"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> {
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), dead.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, 0), terminal.encode());
                batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(pending.dlqExportId()), pending.encode());
            });
            final DelayShard delayShard = new DelayShard(store, DelayShardConfig.defaults());
            final SystemMutationResult mismatched = delayShard.applySystemMutation(mismatchedMutation,
                    sourceAfter(source), keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, mismatched.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, mismatched.stableCode());
            assertEquals(DlqExportStateV1.PENDING, delayShard.getDlqExportRecord(messageId, 0).state());
            assertEquals(1, delayShard.getDlqExportRecord(messageId, 0).physicalAttemptNo());

            final SystemMutationResult result = delayShard.applySystemMutation(mutation,
                    sourceAfterTwice(source), keyPair.getPublic());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(DlqExportStateV1.PUBLISHED, delayShard.getDlqExportRecord(messageId, 0).state());
            assertEquals(1, delayShard.getDlqExportRecord(messageId, 0).physicalAttemptNo());
            assertEquals(result, delayShard.applySystemMutation(mutation, sourceAfterTwice(source),
                    keyPair.getPublic()));
        }
    }

    private static SourcePosition sourceAfter(final KafkaSourcePosition prior) {
        return new KafkaSourcePosition(prior.shardId(), prior.authenticatedClusterId(), prior.nativeTopicUuid(),
                prior.offset() + 1, prior.leaderEpoch(), prior.brokerPersistenceTimeEpochMs() + 1);
    }

    private static SourcePosition sourceAfterTwice(final KafkaSourcePosition prior) {
        return new KafkaSourcePosition(prior.shardId(), prior.authenticatedClusterId(), prior.nativeTopicUuid(),
                prior.offset() + 2, prior.leaderEpoch(), prior.brokerPersistenceTimeEpochMs() + 2);
    }

    private static byte[] resultBody(final ShardId shardId, final DelayMessageId messageId,
                                     final DlqExportRecord pending, final byte[] envelopeHash) {
        return resultBody(shardId, messageId, pending, envelopeHash, chargeVector());
    }

    private static byte[] resultBody(final ShardId shardId, final DelayMessageId messageId,
                                     final DlqExportRecord pending, final byte[] envelopeHash,
                                     final byte[] transfer) {
        final TrustedUtcIntervalEvidence observed = new TrustedUtcIntervalEvidence(2_001, 2_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 1, 1,
                Bytes.sha256(Bytes.utf8("clock-proof")), 0, null);
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
            CanonicalProtobuf.bytes(output, 22, retryDecision());
            CanonicalProtobuf.uint32(output, 23, DlqExportStateV1.PUBLISHED.wireValue());
            CanonicalProtobuf.uint32(output, 24, 1);
        });
    }

    private static byte[] evidence(final byte[] exportId) {
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, io.nereusstream.delay.protocol.BrokerResourceIdentityV1.kafka(
                    new io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1("cluster-a",
                            java.util.UUID.nameUUIDFromBytes(Bytes.utf8("dlq-topic"))))
                    .canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, 0);
            CanonicalProtobuf.uint64(output, 3, 1);
            CanonicalProtobuf.uint64(output, 5, 2_001);
            CanonicalProtobuf.bytes(output, 6,
                    io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1.dlqExport(exportId)
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(Bytes.utf8("dlq-prepared")));
            CanonicalProtobuf.bytes(output, 8, Bytes.sha256(Bytes.utf8("dlq-response")));
        });
        return io.nereusstream.delay.protocol.PublishEvidenceV1.create(
                io.nereusstream.delay.protocol.PublishEvidenceKindV1.KAFKA_PRODUCE_ACK,
                io.nereusstream.delay.protocol.EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch)
                .canonicalBytes();
    }

    private static byte[] retryDecision() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, nestedMarker());
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

    private static byte[] nestedMarker() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1));
    }
}
