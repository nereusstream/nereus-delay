package com.nereusstream.delay.ownership;

import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import java.util.List;
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

    @FunctionalInterface
    public interface CompleteSnapshotFactory {
        TargetCheckpointSemanticSnapshot create(TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes, List<TargetCheckpointAdmissionInputs.Input> admissions);
    }

    private final TargetCheckpointControlInputs controls;
    private final TargetCheckpointOutcomeInputs outcomes;
    private final TargetCheckpointAdmissionInputs admissions;
    private final CompleteSnapshotFactory factory;

    public TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, SnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        admissions = null;
        final var supplied = Objects.requireNonNull(factory, "factory");
        this.factory = (control, outcome, inputs) -> supplied.create(control, outcome);
    }

    public TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, TargetCheckpointAdmissionInputs admissions,
            CompleteSnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        this.admissions = Objects.requireNonNull(admissions, "admissions");
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
        List<TargetCheckpointAdmissionInputs.Input> retained = List.of();
        if (admissions != null) {
            final var admissionTurn = admissions.runTurn();
            if (admissionTurn.failure() != null) {
                throw new IllegalStateException("Target checkpoint Admission collection failed",
                        admissionTurn.failure());
            }
            if (admissionTurn.status() != TargetCheckpointAdmissionInputs.Status.COMPLETE) { return Optional.empty(); }
            retained = admissions.result(control.cut()).orElseThrow();
        }
        final var snapshot = Objects.requireNonNull(factory.create(control, outcome, retained), "semantic snapshot");
        controls.result().orElseThrow();
        outcomes.result().orElseThrow();
        if (admissions != null) { admissions.result(control.cut()).orElseThrow(); }
        if (snapshot.schemaVersion() != 3) {
            throw new IllegalArgumentException("complete Target history collection requires original Outcome schema");
        }
        return Optional.of(snapshot);
    }
}
