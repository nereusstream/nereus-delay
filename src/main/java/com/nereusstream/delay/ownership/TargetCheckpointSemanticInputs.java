package com.nereusstream.delay.ownership;

import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Complete original Control/Outcome collection on one frozen Worker cut, without waiting for provider futures. */
public final class TargetCheckpointSemanticInputs implements Supplier<Optional<TargetCheckpointSemanticSnapshot>>,
        com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor.PreparationIo {
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

    @FunctionalInterface
    public interface AsyncSnapshotFactory {
        /** Advances one bounded step; empty means its caller-owned I/O is still pending. */
        Optional<TargetCheckpointSemanticSnapshot> create(TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes, List<TargetCheckpointAdmissionInputs.Input> admissions);

        default boolean ioExited() { return true; }

        default Throwable pendingIoFailure() { return null; }

        default void requestStop() {}

        default void retryCleanup() {}
    }

    /** Original history and issuer authorities, supplied under held role/key/Owner/credential/pin protection. */
    public record OriginalAuthorities(TargetCheckpointDependencies.AdmissionHistoryAuthority admissionHistory,
            com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier.Authority admissions,
            TargetCheckpointDependencies.ControlImageAuthority controls,
            TargetCheckpointDependencies.OutcomeImageAuthority outcomes,
            TargetCheckpointDependencies.ExternalEvidenceAuthority externalEvidence,
            TargetCheckpointDependencies.RecoveryEvidenceAuthority recoveryEvidence) {
        public OriginalAuthorities(TargetCheckpointDependencies.AdmissionHistoryAuthority history,
                com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier.Authority admissions,
                TargetCheckpointDependencies.ControlImageAuthority controls,
                TargetCheckpointDependencies.OutcomeImageAuthority outcomes,
                TargetCheckpointDependencies.ExternalEvidenceAuthority externalEvidence){
            this(history,admissions,controls,outcomes,externalEvidence,null);
        }
        public OriginalAuthorities(TargetCheckpointDependencies.AdmissionHistoryAuthority history,
                com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier.Authority admissions,
                TargetCheckpointDependencies.ControlImageAuthority controls,
                TargetCheckpointDependencies.OutcomeImageAuthority outcomes) {
            this(history,admissions,controls,outcomes,null);
        }
        public OriginalAuthorities {
            Objects.requireNonNull(admissionHistory, "admissionHistory");
            Objects.requireNonNull(admissions, "admissions");
            Objects.requireNonNull(controls, "controls");
            Objects.requireNonNull(outcomes, "outcomes");
        }
    }

    @FunctionalInterface
    public interface OriginalAuthorityFactory {
        /** Binds trusted authorities to the exact original collected inputs; cannot infer rights from their bytes. */
        OriginalAuthorities create(TargetCheckpointControlInputs.Result controls,
                TargetCheckpointOutcomeInputs.Result outcomes,
                List<TargetCheckpointDependencies.AdmissionImage> admissions,
                TargetCheckpointDependencies dependencies);
    }

    /** All I/O, audit and authority dependencies for one complete preparation on the existing pending task. */
    public record Preparation(TargetWorkerShardRuntime worker, Supplier<BoundedReadBudget> reads,
            LongSupplier ownerClock, Runnable protection, TargetCheckpointRegistryInputs.Providers providers,
            TargetCheckpointRegistryInputs.Limits registryLimits,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            OriginalAuthorityFactory authorities) {
        public Preparation {
            Objects.requireNonNull(worker, "worker");
            Objects.requireNonNull(reads, "reads");
            Objects.requireNonNull(ownerClock, "ownerClock");
            Objects.requireNonNull(protection, "protection");
            Objects.requireNonNull(providers, "providers");
            Objects.requireNonNull(registryLimits, "registryLimits");
            Objects.requireNonNull(semanticLimits, "semanticLimits");
            Objects.requireNonNull(authorities, "authorities");
        }
    }

    /** Complete original-history authentication and asynchronous public Registry collection, without new I/O queues. */
    public static TargetCheckpointSemanticInputs authenticated(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, TargetCheckpointAdmissionInputs admissions,
            Preparation preparation) {
        return asynchronous(controls, outcomes, admissions,
                new RegistrySnapshotFactory(Objects.requireNonNull(preparation, "preparation")));
    }

    private static final class RegistrySnapshotFactory implements AsyncSnapshotFactory {
        private final Preparation preparation;
        private TargetCheckpointControlInputs.Cut cut;
        private TargetCheckpointDependencies dependencies;
        private List<TargetCheckpointDependencies.AdmissionImage> originals;
        private OriginalAuthorities authorities;
        private TargetCheckpointRegistryInputs registry;

        RegistrySnapshotFactory(Preparation preparation) { this.preparation = preparation; }

        @Override
        public boolean ioExited() { return registry == null || registry.ioExited(); }

        @Override
        public Throwable pendingIoFailure() { return registry == null ? null : registry.pendingIoFailure(); }

        @Override
        public void requestStop() { if (registry != null) { registry.requestStop(); } }

        @Override
        public void retryCleanup() { if (registry != null) { registry.retryCleanup(); } }

        @Override
        public Optional<TargetCheckpointSemanticSnapshot> create(TargetCheckpointControlInputs.Result control,
                TargetCheckpointOutcomeInputs.Result outcome, List<TargetCheckpointAdmissionInputs.Input> retained) {
            final var p = preparation;
            p.protection().run();
            if (registry == null) {
                p.worker().requireCheckpointControlInputsCurrent(
                        Objects.requireNonNull(p.reads().get(), "preparation read budget"), control.cut(),
                        p.ownerClock());
                final var images = retained.stream().map(input -> new TargetCheckpointDependencies.AdmissionImage(
                        input.reference().source(), input.image())).toList();
                final var complete = control.cut().dependencies().withAdmissionInputs(retained,
                        p.semanticLimits().maximumAdmissionFrameBytes());
                final var supplied = Objects.requireNonNull(p.authorities().create(control, outcome, images, complete),
                        "original authorities");
                final var authenticated = authenticate(complete, images, supplied);
                registry = new TargetCheckpointRegistryInputs(p.worker(), control.cut(), authenticated, p.reads(),
                        p.ownerClock(), p.protection(), p.providers(), p.registryLimits());
                cut = control.cut();
                dependencies = authenticated;
                originals = images;
                authorities = supplied;
            }
            if (cut != control.cut()) {
                throw new IllegalStateException("Target Registry preparation changed its original Control cut");
            }
            final var turn = registry.runTurn();
            if (turn.failure() != null) {
                throw new IllegalStateException("Target checkpoint Registry collection failed", turn.failure());
            }
            final var result = registry.result();
            if (result.isEmpty()) { return Optional.empty(); }
            final var values = result.orElseThrow();
            // Rights may have changed while a Registry future was pending; reauthenticate every original Admission.
            final var complete = authenticate(dependencies, originals, authorities);
            final var budget = Objects.requireNonNull(p.reads().get(), "snapshot read budget");
            final var snapshot = TargetCheckpointSemanticSnapshot.fromCollectedValues(complete, originals,
                    values.profiles(), values.retries(), values.trusts(), p.semanticLimits(), p.protection())
                    .withControlInputs(complete, control.inputs(), budget, p.semanticLimits(), authorities.controls(),
                            p.protection())
                    .withOutcomeInputs(complete, outcome.inputs(), budget, p.semanticLimits(), authorities.outcomes(),
                            p.protection())
                    .withExternalEvidence(complete, authorities.externalEvidence(), budget, p.semanticLimits(), p.protection())
                    .withRecoveryEvidence(complete,authorities.recoveryEvidence(),budget,p.semanticLimits(),p.protection());
            if (registry.result().isEmpty()) { return Optional.empty(); }
            p.protection().run();
            return Optional.of(snapshot);
        }

        private TargetCheckpointDependencies authenticate(TargetCheckpointDependencies complete,
                List<TargetCheckpointDependencies.AdmissionImage> images, OriginalAuthorities supplied) {
            final var p = preparation;
            return complete.authenticateAdmissionImages(images, p.semanticLimits().maximumAdmissionFrameBytes(),
                    Objects.requireNonNull(p.reads().get(), "Admission read budget"), supplied.admissionHistory(),
                    supplied.admissions(), p.protection());
        }
    }

    private final TargetCheckpointControlInputs controls;
    private final TargetCheckpointOutcomeInputs outcomes;
    private final TargetCheckpointAdmissionInputs admissions;
    private final AsyncSnapshotFactory factory;

    public TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, SnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        admissions = null;
        final var supplied = Objects.requireNonNull(factory, "factory");
        this.factory = (control, outcome, inputs) -> Optional.of(supplied.create(control, outcome));
    }

    public TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, TargetCheckpointAdmissionInputs admissions,
            CompleteSnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        this.admissions = Objects.requireNonNull(admissions, "admissions");
        final var supplied = Objects.requireNonNull(factory, "factory");
        this.factory = (control, outcome, inputs) -> Optional.of(supplied.create(control, outcome, inputs));
    }

    public static TargetCheckpointSemanticInputs asynchronous(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, TargetCheckpointAdmissionInputs admissions,
            AsyncSnapshotFactory factory) {
        return new TargetCheckpointSemanticInputs(controls, outcomes, admissions,
                Objects.requireNonNull(factory, "factory"));
    }

    private TargetCheckpointSemanticInputs(TargetCheckpointControlInputs controls,
            TargetCheckpointOutcomeInputs outcomes, TargetCheckpointAdmissionInputs admissions,
            AsyncSnapshotFactory factory) {
        this.controls = Objects.requireNonNull(controls, "controls");
        this.outcomes = Objects.requireNonNull(outcomes, "outcomes");
        this.admissions = Objects.requireNonNull(admissions, "admissions");
        this.factory = factory;
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
        if (snapshot.isEmpty()) { return Optional.empty(); }
        if (snapshot.orElseThrow().schemaVersion() < 3) {
            throw new IllegalArgumentException("complete Target history collection requires original Outcome schema");
        }
        return snapshot;
    }

    @Override
    public boolean ioExited() {
        return controls.ioExited() && outcomes.ioExited() && (admissions == null || admissions.ioExited())
                && factory.ioExited();
    }

    @Override
    public void requestStop() {
        controls.requestStop();
        outcomes.requestStop();
        if (admissions != null) { admissions.requestStop(); }
        factory.requestStop();
    }

    @Override
    public void retryCleanup() {
        controls.retryCleanup();
        outcomes.retryCleanup();
        if (admissions != null) { admissions.retryCleanup(); }
        factory.retryCleanup();
    }

    @Override
    public Throwable pendingIoFailure() {
        if (controls.pendingIoFailure() != null) { return controls.pendingIoFailure(); }
        if (outcomes.pendingIoFailure() != null) { return outcomes.pendingIoFailure(); }
        if (admissions != null && admissions.pendingIoFailure() != null) { return admissions.pendingIoFailure(); }
        return factory.pendingIoFailure();
    }
}
