package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Stops an exact dedicated original native component and waits for its real callback/native exits. */
public final class TargetMigrationNativeRetirementAction implements TargetMigrationMaintenanceInputs.ResourceAction {
    public interface OriginalOwner extends CheckpointInputFuture.Cleanup, AutoCloseable {
        /**
         * Local actual exclusive component ownership before stop/freeze; shared/current Source clients are excluded.
         */
        void requireComponent(
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationRetirementInventory.Resource resource,
                TargetOwnedBackendGroup actual);
        /**
         * Already acquired original ownership; binds the exact component registrations, fees, history and Source cut.
         */
        void requireBound(
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationMaintenanceInputs.Barrier barrier,
                TargetMigrationRetirementInventory.Resource resource,
                TargetOwnedBackendGroup actual,
                List<Object> registrations);

        void requireCurrent();
        /**
         * Persists/reloads this exact native exit and original owner fee settlement, never a guessed empty component.
         */
        TargetMigrationRetirementResourceReceipt recordExited(
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationRetirementInventory.Resource resource,
                TargetOwnedBackendGroup actual,
                List<Object> registrations);

        long retainedBytes();

        boolean ioExited();

        @Override
        void close();
    }

    private final TargetMigrationRetirementStore.AppliedIntent intent;
    private final TargetMigrationMaintenanceInputs.Barrier barrier;
    private final TargetMigrationRetirementInventory.Resource resource;
    private final TargetOwnedBackendGroup component;
    private final OriginalOwner owner;
    private List<Object> registrations;
    private CompletableFuture<Void> nativeClose;
    private TargetMigrationRetirementResourceReceipt receipt;
    private boolean closing;
    private boolean qualified;

    /** The provider retains this action before acquiring/freeze/close can enter a native owner. */
    public TargetMigrationNativeRetirementAction(
            TargetMigrationMaintenance.Opening opening,
            TargetMigrationRetirementStore.AppliedIntent intent,
            TargetMigrationMaintenanceInputs.Barrier barrier,
            TargetMigrationRetirementInventory.Resource resource,
            TargetOwnedBackendGroup component,
            OriginalOwner owner) {
        this.intent = Objects.requireNonNull(intent);
        this.barrier = Objects.requireNonNull(barrier);
        this.resource = Objects.requireNonNull(resource);
        this.component = Objects.requireNonNull(component);
        this.owner = Objects.requireNonNull(owner);
        if (resource.disposition() != TargetMigrationRetirementInventory.Disposition.RETIRE
                || resource.kind() != TargetMigrationRetirementInventory.Kind.WRITER
                        && resource.kind() != TargetMigrationRetirementInventory.Kind.READER
                        && resource.kind() != TargetMigrationRetirementInventory.Kind.CHANNEL) {
            throw new IllegalArgumentException(
                    "native close requires an exact dedicated old writer/reader/channel retirement");
        }
        component.requireWorkerGraph(opening.graph().classes(), opening.graph().resources());
        opening.retainResourceAction(this);
        opening.requireDedicatedRetirementComponent(component);
        owner.requireComponent(intent, resource, component);
        qualified = true;
    }

    @Override
    public void requireBound(
            TargetMigrationRetirementStore.AppliedIntent actual,
            TargetMigrationMaintenanceInputs.Barrier actualBarrier,
            TargetMigrationRetirementInventory.Resource exact) {
        if (!actual.intention().equals(intent.intention())
                || !actual.intentAt().equals(intent.intentAt())
                || actualBarrier != barrier
                || !resource.equals(exact)) {
            throw new IllegalArgumentException("native exit changed its exact original owner/resource");
        }
        barrier.requireIntent(intent);
        barrier.requireCurrent();
        owner.requireCurrent();
        owner.requireComponent(intent, resource, component);
        if (registrations != null) {
            owner.requireBound(intent, barrier, resource, component, registrations);
        }
    }

    @Override
    public void advance(Runnable current) {
        requireBound(intent, barrier, resource);
        current.run();
        if (registrations == null) {
            registrations = List.copyOf(component.freezeRegistrations());
        }
        owner.requireBound(intent, barrier, resource, component, registrations);
        nativeClose = component.closeBeforeHostBinding();
        if (!nativeClose.isDone() || !component.resourcesTerminated() || !component.nativeResourcesTerminated()) {
            return;
        }
        nativeClose.join();
        current.run();
        owner.requireCurrent();
        if (receipt == null) {
            receipt = Objects.requireNonNull(owner.recordExited(intent, resource, component, registrations));
        }
        receipt.requireResource(resource);
        current.run();
    }

    @Override
    public Optional<TargetMigrationRetirementResourceReceipt> terminalReceipt() {
        return receipt != null && ioExited() ? Optional.of(receipt) : Optional.empty();
    }

    @Override
    public long retainedBytes() {
        return Math.addExact(owner.retainedBytes(), receipt == null ? 0 : receipt.canonicalBytes().length);
    }

    @Override
    public boolean ioExited() {
        return (!qualified
                        || nativeClose != null
                                && nativeClose.isDone()
                                && component.resourcesTerminated()
                                && component.nativeResourcesTerminated())
                && owner.ioExited();
    }

    @Override
    public void requestStop() {
        closing = true;
        if (qualified) {
            component.freezeRegistrations();
            nativeClose = component.closeBeforeHostBinding();
        }
    }

    @Override
    public void close() {
        closing = true;
        owner.close();
    }

    @Override
    public boolean cleanupComplete() {
        return closing && (!qualified || ioExited()) && owner.cleanupComplete();
    }

    @Override
    public CompletableFuture<Void> retryCleanup() {
        if (closing) {
            requestStop();
            owner.close();
        }
        return owner.retryCleanup();
    }
}
