package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SloObjectiveV1Test {
    @Test
    void objectiveAndBoundStartRoundTrip() {
        final SloObjectiveV1 objective = new SloObjectiveV1(SloObjectiveNameV1.QUERY_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 99, 100, 60_000, 10, List.of(), 7,
                bytes(32, 1));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.QUERY_LATENCY, identityPayload());
        final SloSampleStartV1 start = new SloSampleStartV1(objective, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(1_000), 1_100L);

        assertArrayEquals(objective.canonicalBytes(), SloObjectiveV1.decode(objective.canonicalBytes()).canonicalBytes());
        assertArrayEquals(start.canonicalBytes(), SloSampleStartV1.decode(start.canonicalBytes()).canonicalBytes());
    }

    @Test
    void objectiveRejectsWrongPopulationExclusionsAndTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new SloObjectiveV1(
                SloObjectiveNameV1.QUERY_LATENCY, SloPopulationV1.HEALTHY,
                SloThresholdDirectionV1.AT_MOST, SloThresholdUnitV1.MILLISECONDS, 1, 1, 1, 1, 1,
                List.of(), 1, bytes(32, 1)));
        final SloObjectiveV1 objective = new SloObjectiveV1(SloObjectiveNameV1.QUERY_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloThresholdDirectionV1.AT_MOST,
                SloThresholdUnitV1.MILLISECONDS, 100, 1, 1, 1, 1, List.of(), 1, bytes(32, 1));
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.QUERY_LATENCY, identityPayload());
        final SloSampleStartV1 wrongTimeout = new SloSampleStartV1(objective.objectiveDigest(),
                objective.name(), objective.population(), SloPathV1.NOT_APPLICABLE, identity, endpoint(1_000),
                1_101L);
        assertThrows(IllegalArgumentException.class, () -> objective.validateStart(wrongTimeout));
    }

    private static byte[] identityPayload() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, Bytes.utf8("query")));
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) value);
        return result;
    }
}
