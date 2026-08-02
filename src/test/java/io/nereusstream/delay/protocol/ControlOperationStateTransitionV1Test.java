package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlOperationStateTransitionV1Test {
    @Test
    void operationGraphAllowsForwardProgressAndRejectsRollback() {
        assertDoesNotThrow(() -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.PENDING, ControlOperationStateV1.DISPATCHING));
        assertDoesNotThrow(() -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.DISPATCHING, ControlOperationStateV1.PARTIALLY_EFFECTIVE));
        assertDoesNotThrow(() -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.PARTIALLY_EFFECTIVE, ControlOperationStateV1.IN_PROGRESS));
        assertDoesNotThrow(() -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.IN_PROGRESS, ControlOperationStateV1.SUCCEEDED));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.IN_PROGRESS, ControlOperationStateV1.FAILED_BEFORE_EFFECT));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationStateTransitionV1.validate(
                ControlOperationStateV1.SUCCEEDED, ControlOperationStateV1.IN_PROGRESS));
    }

    @Test
    void targetIndexesAreImmutableAndMarkerRevisionMustAdvance() {
        final ControlTargetStateViewV1 pending = new ControlTargetStateViewV1(0,
                TargetMarkerStateV1.PENDING, StableCode.OK, 0, null);
        final ControlTargetStateViewV1 queued = new ControlTargetStateViewV1(0,
                TargetMarkerStateV1.QUEUED, StableCode.OK, 1, null);
        assertDoesNotThrow(() -> ControlOperationStateTransitionV1.validateTargets(
                List.of(pending), List.of(queued)));
        final ControlTargetStateViewV1 stale = new ControlTargetStateViewV1(0,
                TargetMarkerStateV1.QUEUED, StableCode.OK, 0, null);
        assertThrows(IllegalArgumentException.class, () -> ControlOperationStateTransitionV1.validateTargets(
                List.of(pending), List.of(stale)));
        final ControlTargetStateViewV1 another = new ControlTargetStateViewV1(1,
                TargetMarkerStateV1.QUEUED, StableCode.OK, 1, null);
        assertThrows(IllegalArgumentException.class, () -> ControlOperationStateTransitionV1.validateTargets(
                List.of(pending), List.of(another)));
    }

    @Test
    void targetIndexUsesTheFullUnsigned32BitRange() {
        final long highIndex = 0xffff_ffffL;
        final ControlTargetStateViewV1 state = new ControlTargetStateViewV1(highIndex,
                TargetMarkerStateV1.PENDING, StableCode.OK, 0, null);
        assertDoesNotThrow(() -> ControlTargetStateViewV1.decode(state.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> new ControlTargetStateViewV1(
                highIndex + 1, TargetMarkerStateV1.PENDING, StableCode.OK, 0, null));
    }
}
