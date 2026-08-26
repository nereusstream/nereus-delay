package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlOperationStateTransitionTest {
    @Test
    void operationGraphAllowsForwardProgressAndRejectsRollback() {
        assertDoesNotThrow(() -> ControlOperationStateTransition.validate(
                ControlOperationState.PENDING, ControlOperationState.DISPATCHING));
        assertDoesNotThrow(() -> ControlOperationStateTransition.validate(
                ControlOperationState.DISPATCHING, ControlOperationState.PARTIALLY_EFFECTIVE));
        assertDoesNotThrow(() -> ControlOperationStateTransition.validate(
                ControlOperationState.PARTIALLY_EFFECTIVE, ControlOperationState.IN_PROGRESS));
        assertDoesNotThrow(() -> ControlOperationStateTransition.validate(
                ControlOperationState.IN_PROGRESS, ControlOperationState.SUCCEEDED));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationStateTransition.validate(
                        ControlOperationState.IN_PROGRESS, ControlOperationState.FAILED_BEFORE_EFFECT));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationStateTransition.validate(
                        ControlOperationState.SUCCEEDED, ControlOperationState.IN_PROGRESS));
    }

    @Test
    void targetIndexesAreImmutableAndMarkerRevisionMustAdvance() {
        final ControlTargetStateView pending =
                new ControlTargetStateView(0, TargetMarkerState.PENDING, StableCode.OK, 0, null);
        final ControlTargetStateView queued =
                new ControlTargetStateView(0, TargetMarkerState.QUEUED, StableCode.OK, 1, null);
        assertDoesNotThrow(() -> ControlOperationStateTransition.validateTargets(List.of(pending), List.of(queued)));
        final ControlTargetStateView stale =
                new ControlTargetStateView(0, TargetMarkerState.QUEUED, StableCode.OK, 0, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationStateTransition.validateTargets(List.of(pending), List.of(stale)));
        final ControlTargetStateView another =
                new ControlTargetStateView(1, TargetMarkerState.QUEUED, StableCode.OK, 1, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationStateTransition.validateTargets(List.of(pending), List.of(another)));
    }

    @Test
    void targetIndexUsesTheFullUnsigned32BitRange() {
        final long highIndex = 0xffff_ffffL;
        final ControlTargetStateView state =
                new ControlTargetStateView(highIndex, TargetMarkerState.PENDING, StableCode.OK, 0, null);
        assertDoesNotThrow(() -> ControlTargetStateView.decode(state.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlTargetStateView(highIndex + 1, TargetMarkerState.PENDING, StableCode.OK, 0, null));
    }
}
