package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LaneCloseMaterializerTest {
    @Test
    void materializationResultRejectsCounterAdditionOverflow() {
        final DestinationLaneId lane = lane("single");

        assertThrows(IllegalArgumentException.class, () -> new DelayShard.LaneCloseMaterializationResult(
                lane, 1, Integer.MAX_VALUE, Integer.MAX_VALUE, 1, false));
    }

    @Test
    void turnAggregatesFailClosedInsteadOfWrappingCounts() {
        final DestinationLaneId lane = lane("turn");
        final DelayShard.LaneCloseMaterializationResult first =
                new DelayShard.LaneCloseMaterializationResult(
                        lane, 1, Integer.MAX_VALUE, Integer.MAX_VALUE, 0, false);
        final DelayShard.LaneCloseMaterializationResult second =
                new DelayShard.LaneCloseMaterializationResult(
                        lane, 2, Integer.MAX_VALUE, Integer.MAX_VALUE, 0, true);
        final LaneCloseMaterializer.TurnResult turn = new LaneCloseMaterializer.TurnResult(List.of(first, second));

        assertThrows(IllegalStateException.class, turn::scannedRecords);
        assertThrows(IllegalStateException.class, turn::materializedMessages);
    }

    private static DestinationLaneId lane(final String value) {
        return DestinationLaneId.derive(Bytes.utf8("lane-close-materializer-" + value));
    }
}
