package com.nereusstream.delay.ownership;

import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** Complete original Control/Outcome collection on one frozen Worker cut, without waiting for provider futures. */
public final class TargetCheckpointSemanticInputs implements Supplier<Optional<TargetCheckpointSemanticSnapshot>> {
    @FunctionalInterface
    public interface SnapshotFactory {
        /** Supplies and authenticates all other semantic values under the caller's finite budgets and held rights. */
        TargetCheckpointSemanticSnapshot create(TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes);
    }

    private final TargetCheckpointControlInputs controls;
    private final TargetCheckpointOutcomeInputs outcomes;
    private final SnapshotFactory factory;

    public TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, SnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    @Override
    public Optional<TargetCheckpointSemanticSnapshot> get() {
        final var controlTurn = controls.runTurn();
        if (controlTurn.failure() != null) {
            throw new IllegalStateException("Target checkpoint Control collection failed", controlTurn.failure());
        }
        if (controlTurn.status() != TargetCheckpointControlInputs.Status.COMPLETE) { return Optional.empty(); }
        final var outcomeTurn = outcomes.runTurn();
        if (outcomeTurn.failure() != null) {
            throw new IllegalStateException("Target checkpoint Outcome collection failed", outcomeTurn.failure());
        }
        if (outcomeTurn.status() != TargetCheckpointOutcomeInputs.Status.COMPLETE) { return Optional.empty(); }
        final var control = controls.result().orElseThrow();
        final var outcome = outcomes.result().orElseThrow();
        if (control.cut().owner != outcome.cut().owner || !control.cut().view.equals(outcome.cut().view)) {
            throw new IllegalStateException("Target checkpoint history collectors captured different Store cuts");
        }
        final var snapshot = Objects.requireNonNull(factory.create(control, outcome), "semantic snapshot");
        controls.result().orElseThrow();
        outcomes.result().orElseThrow();
        if (snapshot.schemaVersion() != 3) {
            throw new IllegalArgumentException("complete Target history collection requires original Outcome schema");
        }
        return Optional.of(snapshot);
    }
}
