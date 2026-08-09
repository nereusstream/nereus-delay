package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SloAuthoritativeStartFactoryTest {
    @Test
    void commandAppliedUsesSourcePositionIdentityAndBrokerPersistenceEvidence() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-a", UUID.randomUUID(), 17,
                4, 1_234);
        final SloObjectiveV1 objective = objective(SloObjectiveNameV1.COMMAND_APPLIED_LATENCY, 100);

        final SloSampleStartV1 start = SloAuthoritativeStartFactory.commandApplied(objective, source);

        assertEquals(objective.objectiveDigest().length, SloSampleStartV1.HASH_LENGTH);
        assertEquals(SloPathV1.NOT_APPLICABLE, start.path());
        assertEquals(SloTimeEndpointKindV1.BROKER_PERSISTENCE, start.start().kind());
        assertEquals(source.brokerPersistenceTimeEpochMs(), start.start().earliestEpochMs());
        assertArrayEquals(Bytes.sha256(QueryCodecSupport.encodeSourcePosition(source)), start.start().evidenceSha256());
        assertEquals(1_334L, start.timeoutAtEpochMs());
        assertEquals(start, SloSampleStartV1.decode(start.canonicalBytes()));

        final CanonicalProtobuf.Reader identityReader = new CanonicalProtobuf.Reader(
                start.eventIdentity().branchPayload());
        final CanonicalProtobuf.Reader.Field sourceField = identityReader.next();
        assertEquals(1, sourceField.number());
        assertArrayEquals(QueryCodecSupport.encodeSourcePosition(source), sourceField.rawValue());
    }

    @Test
    void dueAdmissionPreservesUnsignedGenerationAndExactManagedPathStart() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final SloObjectiveV1 objective = objective(SloObjectiveNameV1.DUE_ADMISSION_LAG, 250);
        final byte[] evidence = bytes(32, 9);

        final SloSampleStartV1 start = SloAuthoritativeStartFactory.dueAdmission(objective, messageId,
                0x8000_0000L, SloPathV1.MANAGED_PULSAR_HANDOFF, 2_000, evidence);

        assertEquals(SloPathV1.MANAGED_PULSAR_HANDOFF, start.path());
        assertEquals(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, start.start().kind());
        assertEquals(2_000, start.start().earliestEpochMs());
        assertArrayEquals(evidence, start.start().evidenceSha256());
        assertEquals(2_250L, start.timeoutAtEpochMs());
        assertEquals(2_000L, start.eventIdentity().dueAdmissionPathStartEpochMs());
        assertEquals(SloPathV1.MANAGED_PULSAR_HANDOFF, start.eventIdentity().dueAdmissionPath());

        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(start.eventIdentity().branchPayload());
        assertArrayEquals(messageId.bytes(), reader.next().rawValue());
        assertEquals(0x8000_0000L, reader.next().unsignedValue());
        assertEquals(2_000L, reader.next().unsignedValue());
        assertEquals(SloPathV1.MANAGED_PULSAR_HANDOFF.wireValue(), reader.next().unsignedValue());
        assertEquals(start, SloSampleStartV1.decode(start.canonicalBytes()));
    }

    @Test
    void factoryRejectsWrongObjectiveAndUntrustedShapeInputs() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "cluster-b", UUID.randomUUID(), 1, null,
                10);
        assertThrows(IllegalArgumentException.class, () -> SloAuthoritativeStartFactory.commandApplied(
                objective(SloObjectiveNameV1.QUERY_LATENCY, 10), source));

        final SloObjectiveV1 due = objective(SloObjectiveNameV1.DUE_ADMISSION_LAG, 10);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        assertThrows(IllegalArgumentException.class, () -> SloAuthoritativeStartFactory.dueAdmission(
                due, messageId, 0x1_0000_0000L, SloPathV1.ORDINARY_MANAGED, 0, bytes(32, 1)));
        assertThrows(IllegalArgumentException.class, () -> SloAuthoritativeStartFactory.dueAdmission(
                due, messageId, 0, SloPathV1.AUTO_FAST_NATIVE, 0, bytes(32, 1)));
        assertThrows(IllegalArgumentException.class, () -> SloAuthoritativeStartFactory.dueAdmission(
                due, messageId, 0, SloPathV1.ORDINARY_MANAGED, -1, bytes(32, 1)));
        assertThrows(IllegalArgumentException.class, () -> SloAuthoritativeStartFactory.dueAdmission(
                due, messageId, 0, SloPathV1.ORDINARY_MANAGED, 0, bytes(31, 1)));
    }

    private static SloObjectiveV1 objective(final SloObjectiveNameV1 name, final long threshold) {
        return new SloObjectiveV1(name, SloPopulationV1.ALL_ACCEPTED,
                SloThresholdDirectionV1.AT_MOST, SloThresholdUnitV1.MILLISECONDS, threshold,
                99, 100, 60_000, 10, List.of(), 7, bytes(32, 3));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
