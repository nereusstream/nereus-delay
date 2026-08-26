package com.nereusstream.delay.protocol;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Closed monotonic state graph for the public Control Operation projection.
 *
 * <p>This is a projection guard, not a replacement for the source-ordered
 * target mutation or the Oxia CAS. It prevents a local authority from
 * reporting an impossible rollback after a target has taken effect.</p>
 */
public final class ControlOperationStateTransition {
    private ControlOperationStateTransition() {}

    public static void validate(final ControlOperationState current, final ControlOperationState next) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(next, "next");
        if (current == next) {
            return;
        }
        final boolean allowed =
                switch (current) {
                    case PENDING ->
                        next == ControlOperationState.DISPATCHING
                                || next == ControlOperationState.REJECTED
                                || next == ControlOperationState.FAILED_BEFORE_EFFECT;
                    case DISPATCHING ->
                        next == ControlOperationState.PARTIALLY_EFFECTIVE
                                || next == ControlOperationState.IN_PROGRESS
                                || next == ControlOperationState.REJECTED
                                || next == ControlOperationState.FAILED_BEFORE_EFFECT;
                    case PARTIALLY_EFFECTIVE ->
                        next == ControlOperationState.IN_PROGRESS
                                || next == ControlOperationState.SUCCEEDED
                                || next == ControlOperationState.SUCCEEDED_WITH_OUTSTANDING;
                    case IN_PROGRESS ->
                        next == ControlOperationState.SUCCEEDED
                                || next == ControlOperationState.SUCCEEDED_WITH_OUTSTANDING;
                    case SUCCEEDED, SUCCEEDED_WITH_OUTSTANDING, REJECTED, FAILED_BEFORE_EFFECT -> false;
                };
        if (!allowed) {
            throw new IllegalArgumentException(
                    "invalid Control Operation state transition: " + current + " -> " + next);
        }
    }

    /**
     * Validates the target-level projection when both revisions contain the
     * immutable target set. The empty-list compatibility projection is
     * accepted only for the first local observation.
     */
    public static void validateTargets(
            final List<ControlTargetStateView> current, final List<ControlTargetStateView> next) {
        Objects.requireNonNull(current, "current target states");
        Objects.requireNonNull(next, "next target states");
        if (current.isEmpty()) {
            return;
        }
        if (next.isEmpty()) {
            throw new IllegalArgumentException("Control Operation target set cannot disappear");
        }
        final Set<Long> currentIndexes = indexes(current);
        final Set<Long> nextIndexes = indexes(next);
        if (!currentIndexes.equals(nextIndexes)) {
            throw new IllegalArgumentException("Control Operation target set cannot change");
        }
        for (int index = 0; index < current.size(); index++) {
            final ControlTargetStateView before = current.get(index);
            final ControlTargetStateView after = next.get(index);
            if (before.targetIndex() != after.targetIndex()) {
                throw new IllegalArgumentException("Control Operation target states are not index aligned");
            }
            validateTarget(before.markerState(), after.markerState());
            if (before.markerState() != after.markerState() && after.targetRevision() <= before.targetRevision()) {
                throw new IllegalArgumentException("target revision must advance with marker state");
            }
        }
    }

    public static void validateTarget(final TargetMarkerState current, final TargetMarkerState next) {
        Objects.requireNonNull(current, "current target marker state");
        Objects.requireNonNull(next, "next target marker state");
        if (current == next) {
            return;
        }
        final boolean allowed =
                switch (current) {
                    case PENDING ->
                        next == TargetMarkerState.ENQUEUE_UNCERTAIN
                                || next == TargetMarkerState.QUEUED
                                || next == TargetMarkerState.REJECTED
                                || next == TargetMarkerState.FAILED_BEFORE_EFFECT;
                    case ENQUEUE_UNCERTAIN ->
                        next == TargetMarkerState.QUEUED
                                || next == TargetMarkerState.EFFECTIVE
                                || next == TargetMarkerState.REJECTED
                                || next == TargetMarkerState.FAILED_BEFORE_EFFECT;
                    case QUEUED ->
                        next == TargetMarkerState.EFFECTIVE
                                || next == TargetMarkerState.REJECTED
                                || next == TargetMarkerState.FAILED_BEFORE_EFFECT;
                    case EFFECTIVE -> next == TargetMarkerState.MATERIALIZING || next == TargetMarkerState.COMPLETED;
                    case MATERIALIZING -> next == TargetMarkerState.COMPLETED;
                    case COMPLETED, REJECTED, FAILED_BEFORE_EFFECT -> false;
                };
        if (!allowed) {
            throw new IllegalArgumentException("invalid Control target marker transition: " + current + " -> " + next);
        }
    }

    private static Set<Long> indexes(final List<ControlTargetStateView> states) {
        final Set<Long> indexes = new HashSet<>();
        for (ControlTargetStateView state : states) {
            Objects.requireNonNull(state, "target state");
            if (!indexes.add(state.targetIndex())) {
                throw new IllegalArgumentException("duplicate Control Operation target state index");
            }
        }
        return indexes;
    }
}
