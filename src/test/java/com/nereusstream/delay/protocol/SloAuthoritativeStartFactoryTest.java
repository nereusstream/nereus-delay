package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SloAuthoritativeStartFactoryTest {
    @Test
    void commandAppliedUsesSourcePositionIdentityAndBrokerPersistenceEvidence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-a", UUID.randomUUID(), 17, 4, 1_234);
        final SloObjective objective = objective(SloObjectiveName.COMMAND_APPLIED_LATENCY, 100);

        final SloSampleStart start = SloAuthoritativeStartFactory.commandApplied(objective, source);

        assertEquals(objective.objectiveDigest().length, SloSampleStart.HASH_LENGTH);
        assertEquals(SloPath.NOT_APPLICABLE, start.path());
        assertEquals(SloTimeEndpointKind.BROKER_PERSISTENCE, start.start().kind());
        assertEquals(source.brokerPersistenceTimeEpochMs(), start.start().earliestEpochMs());
        assertArrayEquals(
                Bytes.sha256(QueryCodecSupport.encodeSourcePosition(source)),
                start.start().evidenceSha256());
        assertEquals(1_334L, start.timeoutAtEpochMs());
        assertEquals(start, SloSampleStart.decode(start.canonicalBytes()));

        final CanonicalProtobuf.Reader identityReader =
                new CanonicalProtobuf.Reader(start.eventIdentity().branchPayload());
        final CanonicalProtobuf.Reader.Field sourceField = identityReader.next();
        assertEquals(1, sourceField.number());
        assertArrayEquals(QueryCodecSupport.encodeSourcePosition(source), sourceField.rawValue());
    }

    @Test
    void dueAdmissionPreservesUnsignedGenerationAndExactManagedPathStart() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final SloObjective objective = objective(SloObjectiveName.DUE_ADMISSION_LAG, 250);
        final byte[] evidence = bytes(32, 9);

        final SloSampleStart start = SloAuthoritativeStartFactory.dueAdmission(
                objective, messageId, 0x8000_0000L, SloPath.MANAGED_PULSAR_HANDOFF, 2_000, evidence);

        assertEquals(SloPath.MANAGED_PULSAR_HANDOFF, start.path());
        assertEquals(SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH, start.start().kind());
        assertEquals(2_000, start.start().earliestEpochMs());
        assertArrayEquals(evidence, start.start().evidenceSha256());
        assertEquals(2_250L, start.timeoutAtEpochMs());
        assertEquals(2_000L, start.eventIdentity().dueAdmissionPathStartEpochMs());
        assertEquals(SloPath.MANAGED_PULSAR_HANDOFF, start.eventIdentity().dueAdmissionPath());

        final CanonicalProtobuf.Reader reader =
                new CanonicalProtobuf.Reader(start.eventIdentity().branchPayload());
        assertArrayEquals(messageId.bytes(), reader.next().rawValue());
        assertEquals(0x8000_0000L, reader.next().unsignedValue());
        assertEquals(2_000L, reader.next().unsignedValue());
        assertEquals(SloPath.MANAGED_PULSAR_HANDOFF.wireValue(), reader.next().unsignedValue());
        assertEquals(start, SloSampleStart.decode(start.canonicalBytes()));
    }

    @Test
    void factoryRejectsWrongObjectiveAndUntrustedShapeInputs() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-b", UUID.randomUUID(), 1, null, 10);
        assertThrows(
                IllegalArgumentException.class,
                () -> SloAuthoritativeStartFactory.commandApplied(
                        objective(SloObjectiveName.QUERY_LATENCY, 10), source));

        final SloObjective due = objective(SloObjectiveName.DUE_ADMISSION_LAG, 10);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        assertThrows(
                IllegalArgumentException.class,
                () -> SloAuthoritativeStartFactory.dueAdmission(
                        due, messageId, 0x1_0000_0000L, SloPath.ORDINARY_MANAGED, 0, bytes(32, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> SloAuthoritativeStartFactory.dueAdmission(
                        due, messageId, 0, SloPath.AUTO_FAST_NATIVE, 0, bytes(32, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> SloAuthoritativeStartFactory.dueAdmission(
                        due, messageId, 0, SloPath.ORDINARY_MANAGED, -1, bytes(32, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> SloAuthoritativeStartFactory.dueAdmission(
                        due, messageId, 0, SloPath.ORDINARY_MANAGED, 0, bytes(31, 1)));
    }

    private static SloObjective objective(final SloObjectiveName name, final long threshold) {
        return new SloObjective(
                name,
                SloPopulation.ALL_ACCEPTED,
                SloThresholdDirection.AT_MOST,
                SloThresholdUnit.MILLISECONDS,
                threshold,
                99,
                100,
                60_000,
                10,
                List.of(),
                7,
                bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
