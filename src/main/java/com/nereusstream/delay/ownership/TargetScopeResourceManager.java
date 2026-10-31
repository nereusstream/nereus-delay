package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetParentControlCreditVerifier;
import com.nereusstream.delay.runtime.TargetScopeControlObligationStore;
import com.nereusstream.delay.runtime.TargetScopeControlObligationVerifier;
import com.nereusstream.delay.runtime.TargetScopeFeeAuthority;
import com.nereusstream.delay.runtime.TrustedUtcClock;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * The finite shared manager over the two original, externally owned Source/Oxia allocations.
 * There is deliberately no physical allocator in this repository: the trusted deployment must
 * supply both opaque original owners. Policy files, signatures and registry observations cannot
 * create those owners. All acquiring, refreshing and closing I/O belongs to the existing bounded
 * process I/O path; methods used by Store and SDK guards inspect already-held local material only.
 */
public final class TargetScopeResourceManager implements AutoCloseable, CheckpointInputFuture.Cleanup {
    private static final byte[] CLOSE_DOMAIN = Bytes.utf8("nereus-delay-actual-scope-manager-close\0");

    public record Inputs(TargetScopeResourceConfiguration configuration,
            TargetScopeDeploymentAllocation.Owners opaqueOwners, byte[] processIncarnationDigest) {
        public Inputs {
            Objects.requireNonNull(configuration);
            if (opaqueOwners == null)
                throw new TargetScopeDeploymentAllocation.ExternalAllocationUnavailable(
                        "external original Source/Oxia allocation owners are required");
            processIncarnationDigest = TargetScopeResourceConfiguration.assigned(
                    processIncarnationDigest, 32, "actual process incarnation digest");
        }
        @Override public byte[] processIncarnationDigest() { return processIncarnationDigest.clone(); }
    }

    /** Aggregate process heap reservation, made before opening any of these original resources. */
    public static long maximumResidentBytes(Inputs inputs, long maximumNativeBytes, long maximumProtectionBytes) {
        Objects.requireNonNull(inputs);
        TargetScopeResourceConfiguration.positive(maximumNativeBytes, "maximum Native read bytes");
        TargetScopeResourceConfiguration.positive(maximumProtectionBytes, "maximum complete Scope protection bytes");
        final var configuration = inputs.configuration();
        final long each = Math.addExact(2048, Math.addExact(Math.multiplyExact(2, maximumNativeBytes), maximumProtectionBytes));
        final long result = Math.addExact(Math.addExact(configuration.limits().maximumRetainedBytes(),
                configuration.policy().management().maximumRootBytes()),
                Math.multiplyExact((long) configuration.limits().maximumLocalBorrows(), each));
        TargetScopeResourceConfiguration.positive(result, "aggregate Scope resident bytes");
        return result;
    }

    private final Inputs inputs;
    private final TargetScopeDeploymentAllocation.Owned actualOwned;
    private final Storage storage;
    private final Runnable guard;
    private volatile RootRecord observedRoot;
    private volatile boolean closing;
    private CaptureContext captureContext;
    public TargetWorkerSourceRuntimeFactory.SourceFrameContract captureSourceFrames(TargetWorkerSourceStartup.FrameOpening opening){
        requireCurrent();final var result=actualOwned.captureSourceFrames(opening);requireCurrent();return result;
    }

    private TargetScopeResourceManager(Inputs inputs, TargetScopeDeploymentAllocation.Owned actualOwned,
            Storage storage, Runnable guard) {
        this.inputs = inputs;
        this.actualOwned = actualOwned;
        this.storage = storage;
        this.guard = guard;
    }

    /** Ordinary startup attaches only. A missing Root, including an old lost Root, cannot rebirth. */
    public static TargetScopeResourceManager attach(OxiaControlTargetRegistrationAuthority registrations,
            TargetScopeDeploymentAllocation.ActualBinding binding, Inputs inputs, Runnable guard) {
        return acquire(registrations, binding, inputs, guard, null);
    }

    /** Alias with the same attach-only startup semantics. */
    public static TargetScopeResourceManager open(OxiaControlTargetRegistrationAuthority registrations,
            TargetScopeDeploymentAllocation.ActualBinding binding, Inputs inputs, Runnable guard) {
        return attach(registrations, binding, inputs, guard);
    }

    /** Explicit independently authorized management entry; never called by ordinary startup. */
    public static TargetScopeResourceManager initialize(OxiaControlTargetRegistrationAuthority registrations,
            TargetScopeDeploymentAllocation.ActualBinding binding, Inputs inputs, Runnable guard, AuthorIdentity actor) {
        return acquire(registrations, binding, inputs, guard, Objects.requireNonNull(actor));
    }

    private static TargetScopeResourceManager acquire(OxiaControlTargetRegistrationAuthority registrations,
            TargetScopeDeploymentAllocation.ActualBinding binding, Inputs inputs, Runnable guard, AuthorIdentity initializer) {
        Objects.requireNonNull(inputs); Objects.requireNonNull(registrations); Objects.requireNonNull(guard);
        final var owned = TargetScopeDeploymentAllocation.acquire(inputs.configuration(), binding, inputs.opaqueOwners(), guard);
        try {
            final var storage = Objects.requireNonNull(registrations.scopeResourceStorage(inputs.configuration().policy()));
            final var manager = new TargetScopeResourceManager(inputs, owned, storage, guard);
            manager.requireStorageCurrent();
            if (initializer != null)
                owned.requireManagementPurpose(TargetScopeDeploymentAllocation.ManagerPurpose.INITIALIZE_ROOT, initializer);
            final var root = initializer == null ? storage.readRoot() : storage.initialize(inputs.configuration().policy());
            if (root == null) throw unavailable("original shared Scope manager Root has not been initialized");
            manager.observe(root);
            manager.requireCurrent(); manager.retainedBytes();
            return manager;
        } catch (RuntimeException | Error failure) {
            // The caller is on the original I/O path. A failed close retains that exact owner,
            // including native futures; it is never replaced with an empty cleanup receipt.
            try { owned.close(); }
            catch (RuntimeException | Error close) { if (failure != close) failure.addSuppressed(close); }
            throw new ManagerFailure("original Scope manager acquisition retained its original cleanup", failure, owned);
        }
    }

    public TargetScopeResourceConfiguration configuration() { return inputs.configuration(); }
    public long originalOwnerRetainedBytes() { return actualOwned.retainedBytes(); }
    public long retainedBytes() {
        final var root = observedRoot;
        return Math.addExact(256, Math.addExact(actualOwned.retainedBytes(),
                root == null ? 0 : root.value().canonicalBytes().length));
    }

    /** Local only: no current balance is reconstructed from a caller's snapshot. */
    public void requireCurrent() {
        if (closing) throw unavailable("original shared Scope manager is closing");
        actualOwned.requireCurrent();
    }

    private void requireStorageCurrent() { guard.run(); requireCurrent(); storage.requireCurrent(); requireCurrent(); guard.run(); }

    public TargetScopeResourceRoot snapshot() { requireCurrent(); return originalRoot().value(); }

    /** Actual remote refresh, on the existing bounded process I/O path, outside Store/SDK monitors. */
    public void refresh() {
        requireCurrent(); actualOwned.refresh(); refreshRoot(); requireCurrent();
    }

    public TargetScopeResourceRoot refreshRoot() {
        requireStorageCurrent();
        final var read = storage.readRoot();
        if (read == null) throw unavailable("original shared Scope manager Root disappeared; retain UNKNOWN");
        observe(read); requireCurrent(); return originalRoot().value();
    }

    /** The trusted process composition root may replace its recovery reader by its active successor. */
    public synchronized void bindSource(TargetSourceApplyRuntime source, LongSupplier clock) {
        requireCurrent();
        captureContext = new CaptureContext(Objects.requireNonNull(source), null, Objects.requireNonNull(clock));
    }

    public synchronized void bindWorker(TargetWorkerShardRuntime worker, LongSupplier clock) {
        requireCurrent();
        if (captureContext == null) throw unavailable("actual Source must be bound before its Worker");
        captureContext = new CaptureContext(captureContext.source(), Objects.requireNonNull(worker), Objects.requireNonNull(clock));
    }

    private record CaptureContext(TargetSourceApplyRuntime source, TargetWorkerShardRuntime worker, LongSupplier clock) {}

    private synchronized CaptureContext captureContext(LongSupplier clock) {
        requireCurrent();
        final var selected = captureContext;
        if (selected == null || selected.clock() != Objects.requireNonNull(clock))
            throw unavailable("original bounded Source capture/clock is not attached");
        return selected;
    }

    private synchronized CaptureContext captureContext() {
        requireCurrent();
        if (captureContext == null) throw unavailable("original bounded Source capture is not attached");
        return captureContext;
    }

    /** Caller already retains a finite local owner loan until the actual Native cleanup exits. */
    public TargetSourceApplyRuntime.NativeExposureHold capturePaidScope(TargetSloControlScopeHeader.Reference reference,
            long maximumBytes, LongSupplier clock) {
        return capturePaidScope(Objects.requireNonNull(reference), maximumBytes, captureContext(clock));
    }

    private TargetSourceApplyRuntime.NativeExposureHold capturePaidScope(TargetSloControlScopeHeader.Reference reference,
            long maximumBytes, CaptureContext selected) {
        requireCurrent();
        // Only selection used the manager monitor. The real Native guard/read remains outside it.
        return selected.worker() == null
                ? selected.source().capturePaidScope(reference, maximumBytes, selected.clock(), this::requireCurrent)
                : selected.worker().capturePaidScope(reference, maximumBytes, selected.clock());
    }

    public TargetSourceApplyRuntime.NativeExposureHold captureScopeFeeOrigin(SystemMutation exact,
            long maximumBytes, LongSupplier clock) {
        final var selected = captureContext(clock);
        requireCurrent();
        return selected.worker() == null
                ? selected.source().captureScopeFeeOrigin(Objects.requireNonNull(exact), maximumBytes, selected.clock(), this::requireCurrent)
                : selected.worker().captureScopeFeeOrigin(Objects.requireNonNull(exact), maximumBytes, selected.clock());
    }

    /** Reserve before Native64 funding. No failure/timeout ever returns this permanent fixed share. */
    public TargetScopeResourceRoot.Child reserveChild(TargetScopeControlObligation.Definition definition,
            long retainedUntil, AuthorIdentity actor) {
        requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.RESERVE_CHILD, actor);
        requireDefinition(definition);
        if (retainedUntil < 1 || retainedUntil > configuration().limits().minimumRetainedUntilEpochMs())
            throw new IllegalArgumentException("child horizon exceeds the independently acquired original retention bound");
        final var shares = fixedShares(definition);
        final var result = transition(root -> root.reserveChild(definition.header(), definition.digest(), shares, retainedUntil),
                root -> matchingChild(root, definition.reference(), definition.digest(), shares, retainedUntil) != null,
                "original child reservation requires retry without refund");
        final var child = child(result, definition.reference());
        openDurableLoan(child, TargetScopeResourceRoot.Purpose.CAPACITY);
        requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.RESERVE_CHILD, actor);
        return child;
    }

    /** The immutable Definition and its schema-3 coefficients freeze the complete physical share. */
    public TargetScopeResourcePolicy.Shares fixedShares(TargetScopeControlObligation.Definition definition) {
        requireDefinition(definition);
        final var header = definition.header(); final var plan = definition.plan();
        final long prefix = Bytes.utf8(header.registryKeyPrefix()).length;
        final long base = Math.addExact(prefix, 74); // /e4-scope/ plus the original 32-byte hex ID
        final long wrapper = plan.ceilings().registryWrapperBytes();
        final long root = Math.addExact(plan.exposureFormat().maximumRootReservation(header, plan.exposurePolicy()),
                Math.addExact(Math.addExact(base, 5), wrapper));
        final long birth = Math.addExact(header.canonicalBytes().length, Math.addExact(Math.addExact(base, 6), wrapper));
        final long inputs = Math.addExact(header.limits().maximumEntriesBytes(),
                Math.multiplyExact((long) header.limits().maximumSlots(), Math.addExact(Math.addExact(prefix, 75), wrapper)));
        final var lifetime = plan.exposurePolicy().lifetime();
        return new TargetScopeResourcePolicy.Shares(lifetime.records(), lifetime.bytes(),
                Math.addExact(2, header.limits().maximumSlots()), Math.addExact(Math.addExact(root, birth), inputs));
    }

    /** Confirmation consumes a private actual Native64 hold, never a caller-supplied confirmation DTO. */
    public TargetScopeResourceRoot.Child confirmChild(TargetSloControlScopeHeader.Reference reference, AuthorIdentity actor) {
        requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.CONFIRM_NATIVE, actor);
        final var loan = borrow(reference, TargetScopeResourceRoot.Purpose.CAPACITY);
        TargetSourceApplyRuntime.NativeExposureHold nativeHold = null;
        try {
            nativeHold = capturePaidScope(reference, configuration().limits().maximumLocalReadBytes(), captureContext());
            final var paid = requirePaid(nativeHold, loan.child(), false);
            final var confirmation = confirmation(paid);
            final var selected = loan.child();
            final var result = transition(root -> root.confirmChild(selected.childIndex(), confirmation),
                    root -> root.child(selected.childIndex()).state() == TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED
                            && confirmation.equals(root.child(selected.childIndex()).confirmation()),
                    "actual Native64 confirmation requires retry without refund");
            requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.CONFIRM_NATIVE, actor);
            closePair(nativeHold, loan);
            return result.child(selected.childIndex());
        } catch (RuntimeException | Error failure) {
            throw releaseFailure(nativeHold, loan, failure);
        }
    }

    public TargetScopeResourceRoot closeNewAllocations(AuthorIdentity actor) {
        requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.CLOSE_CHILD_ALLOCATIONS, actor);
        final byte[] proof = Bytes.sha256(CLOSE_DOMAIN, configuration().policy().reference().canonicalBytes(),
                configuration().policy().managerAuthorizationDigest(), Objects.requireNonNull(actor).canonicalBytes());
        final var result = transition(root -> root.allocationState() == TargetScopeResourceRoot.AllocationState.CLOSED
                        ? root : root.closeNewAllocations(proof),
                root -> root.allocationState() == TargetScopeResourceRoot.AllocationState.CLOSED,
                "original child allocation closure requires retry");
        requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.CLOSE_CHILD_ALLOCATIONS, actor);
        return result;
    }

    public TargetScopeResourceRoot.Child requireConfirmedChild(TargetSloControlScopeHeader.Reference reference) {
        requireCurrent();
        final var child = child(originalRoot().value(), Objects.requireNonNull(reference));
        if (child.state() != TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED)
            throw unavailable("original child still has UNKNOWN/PENDING Native64 funding");
        return child;
    }

    /** Local owner reference only, including the independently qualified invalid/shared fee payer. */
    public TargetScopeDeploymentAllocation.Loan borrowOriginalOwner() { requireCurrent(); return actualOwned.borrow(); }

    /** One durable process/child/purpose slot; later frames only borrow bounded local references. */
    public Loan borrow(TargetSloControlScopeHeader.Reference reference, TargetScopeResourceRoot.Purpose purpose) {
        requireCurrent(); Objects.requireNonNull(purpose);
        final var local = actualOwned.borrow();
        try {
            var root = originalRoot().value();
            var selected = findChild(root, Objects.requireNonNull(reference));
            if (selected == null) { refreshRoot(); root = originalRoot().value(); selected = child(root, reference); }
            if (purpose == TargetScopeResourceRoot.Purpose.SDK_ISSUER
                    && selected.state() != TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED)
                throw unavailable("SDK loan requires original Native64 confirmation");
            final var durable = openDurableLoan(selected, purpose);
            return new Loan(this, local, selected, durable);
        } catch (RuntimeException | Error failure) { local.close(); throw failure; }
    }

    public void requireOwnLoan(Loan loan) {
        if (Objects.requireNonNull(loan).manager != this) throw new IllegalArgumentException("foreign original manager loan");
        loan.requireCurrent();
    }

    private TargetScopeResourceRoot.ProcessLoan openDurableLoan(TargetScopeResourceRoot.Child child,
            TargetScopeResourceRoot.Purpose purpose) {
        var root = originalRoot().value();
        var loan = findLoan(root, child.childIndex(), purpose);
        if (loan == null) {
            root = transition(value -> value.openProcessLoan(child.childIndex(), inputs.processIncarnationDigest(), purpose),
                    value -> findLoan(value, child.childIndex(), purpose) != null,
                    "original process/purpose loan requires retry without another issuance");
            loan = findLoan(root, child.childIndex(), purpose);
        }
        if (loan == null || loan.state() != TargetScopeResourceRoot.LoanState.OPEN)
            throw unavailable("original process/purpose loan is permanently closed");
        return loan;
    }

    public static final class Loan implements AutoCloseable {
        private final TargetScopeResourceManager manager;
        private final TargetScopeDeploymentAllocation.Loan original;
        private final TargetScopeResourceRoot.Child child;
        private final TargetScopeResourceRoot.ProcessLoan durable;
        private boolean closed;
        private Loan(TargetScopeResourceManager manager, TargetScopeDeploymentAllocation.Loan original,
                TargetScopeResourceRoot.Child child, TargetScopeResourceRoot.ProcessLoan durable) {
            this.manager = manager; this.original = original; this.child = child; this.durable = durable;
        }
        public TargetScopeResourceRoot.Child child() { requireCurrent(); return child; }
        public TargetScopeDeploymentAllocation.Loan originalOwnerLoan() { requireCurrent(); return original; }
        public long retainedBytes() { return Math.addExact(192, Math.addExact(child.canonicalBytes().length, durable.canonicalBytes().length)); }
        public synchronized void requireCurrent() {
            if (closed) throw unavailable("original local Scope resource loan is closed");
            manager.requireCurrent(); original.requireCurrent();
            final var root = manager.originalRoot().value();
            final var observed = root.processLoan(durable.loanIndex());
            if (!observed.equals(durable) || observed.state() != TargetScopeResourceRoot.LoanState.OPEN
                    || !root.child(child.childIndex()).headerReference().equals(child.headerReference()))
                throw unavailable("original durable process/purpose loan is unavailable");
            original.requireCurrent();
        }
        public void requireRegisteredInput(TargetSloControlRegisteredInput input) { requireCurrent(); original.requireRegisteredInput(input); requireCurrent(); }
        public OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput input) {
            requireCurrent(); final var until = original.requiredServiceRetryUntil(input); requireCurrent(); return until;
        }
        public TargetScopeFeeAuthority.Payment requireScopeFeeOccurrence(TargetScopeFeeAuthority.Occurrence occurrence) {
            requireCurrent(); final var payment = original.requireScopeFeeOccurrence(occurrence); requireCurrent(); return payment;
        }
        /** Pure local callback. Durable loan, child charge, UNKNOWN and native retries remain retained. */
        @Override public synchronized void close() { if (!closed) { original.close(); closed = true; } }
    }

    /** Ordinary Scope acquisition grants no independent management purpose. */
    public TargetSloControlScopeAuthority.Protections protections() { return protections(null); }
    public TargetSloControlScopeAuthority.Protections managementProtections(AuthorIdentity actor) {
        return protections(Objects.requireNonNull(actor));
    }
    private TargetSloControlScopeAuthority.Protections protections(AuthorIdentity actor) {
        return (reference, maximumRetainedBytes, localGuard) -> acquireProtection(reference, maximumRetainedBytes, localGuard, actor);
    }

    private ScopeProtection acquireProtection(TargetSloControlScopeHeader.Reference reference, long maximumBytes,
            Runnable localGuard, AuthorIdentity actor) {
        Objects.requireNonNull(localGuard).run(); requireCurrent();
        TargetScopeResourceConfiguration.positive(maximumBytes, "Scope protection retained bytes");
        final var loan = borrow(reference, TargetScopeResourceRoot.Purpose.SCOPE_REGISTRY);
        TargetSourceApplyRuntime.NativeExposureHold nativeHold = null;
        try {
            final long available = Math.subtractExact(maximumBytes, loan.retainedBytes());
            if (available < 1) throw unavailable("original Scope loan does not fit its bounded acquisition");
            nativeHold = capturePaidScope(reference, Math.min(available, configuration().limits().maximumLocalReadBytes()), captureContext());
            final var paid = requirePaid(nativeHold, loan.child(), true);
            final var protection = new ScopeProtection(loan, nativeHold, paid, actor);
            if (protection.retainedBytes() > maximumBytes) throw unavailable("actual Scope protection exceeds its finite acquisition bound");
            protection.requireCurrent(); localGuard.run(); return protection;
        } catch (RuntimeException | Error failure) { throw releaseFailure(nativeHold, loan, failure); }
    }

    private final class ScopeProtection implements TargetSloControlScopeAuthority.Protection, CheckpointInputFuture.Cleanup {
        private final Loan loan;
        private final TargetSourceApplyRuntime.NativeExposureHold nativeHold;
        private final TargetScopeControlObligation paid;
        private final AuthorIdentity managementActor;
        private final long bytes;
        private TargetSourceApplyRuntime.NativeExposureHold exposureNative;
        private boolean closeRequested;
        private boolean loanClosed;
        private ScopeProtection(Loan loan, TargetSourceApplyRuntime.NativeExposureHold nativeHold,
                TargetScopeControlObligation paid, AuthorIdentity managementActor) {
            this.loan = loan; this.nativeHold = nativeHold; this.paid = paid;
            this.managementActor = managementActor;
            bytes = Math.addExact(128, Math.addExact(loan.retainedBytes(), nativeHold.retainedBytes()));
        }
        @Override public TargetSloControlScopeHeader header() { requireCurrent(); return paid.definition().header(); }
        @Override public long retainedBytes() { return bytes; }
        @Override public synchronized void requireCurrent() {
            if (closeRequested) throw unavailable("actual Scope protection has requested local release");
            loan.requireCurrent(); nativeHold.requireLocalCurrent();
            requirePaid(nativeHold, loan.child(), true);
        }
        @Override public void requireBirth() { requireCurrent(); requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.BIRTH_SCOPE, managementActor); }
        @Override public void requireSeal(TargetSloControlScopeRoot root) {
            requireRoot(root); requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.SEAL_SCOPE, managementActor);
        }
        @Override public void requireCloseIssuance(TargetSloControlScopeRoot root) {
            requireRoot(root); requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose.CLOSE_SCOPE_ISSUANCE, managementActor);
        }
        @Override public void requireInput(TargetSloControlRegisteredInput original) {
            requireCurrent(); original.requireHeader(header());
            if (Long.compareUnsigned(original.sourceRetryUntil(), loan.child().retainedUntil()) > 0)
                throw unavailable("original Control input exceeds its retained child horizon");
            loan.requireRegisteredInput(original); requireCurrent();
        }
        @Override public OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput original) {
            requireInput(original);
            final var until = loan.requiredServiceRetryUntil(original);
            if (until.isPresent() && Long.compareUnsigned(until.getAsLong(), loan.child().retainedUntil()) > 0)
                throw unavailable("referenced original SERVICE retries exceed their retained child horizon");
            return until;
        }
        private void requireRoot(TargetSloControlScopeRoot root) {
            requireCurrent();
            if (!paid.definition().header().equals(Objects.requireNonNull(root).header()))
                throw new IllegalArgumentException("actual Scope protection changed its immutable Root/header");
            paid.definition().plan().requireHeader(root.header()); requireCurrent();
        }
        /** Only the typed actual Scope authority calls this hook; a frame/Origin DTO cannot do so. */
        public synchronized void requireNativeExposure(TargetSourceApplyRuntime.NativeExposureHold actual) {
            requireCurrent(); Objects.requireNonNull(actual).requireExposureFrame();
            requirePaid(actual, loan.child(), true);
            if (exposureNative != null && exposureNative != actual)
                throw unavailable("original Scope protection cannot switch its actual SDK Native hold");
            exposureNative = actual;
        }
        @Override public TargetSloControlScopeAuthority.ExposureQualification requireExposure(TargetSloControlScopeRoot root,
                SystemMutation exact, TargetScopeExposureDebit.Origin origin, TargetScopeExposureSource.CopyContract sdk) {
            requireRoot(root); Objects.requireNonNull(exact); Objects.requireNonNull(origin); Objects.requireNonNull(sdk);
            final var actual = exposureNative;
            if (actual == null) throw unavailable("actual SDK exposure requires its original typed Native frame hold");
            actual.requireExposureFrame(); actual.requireLocalCurrent();
            if (!Arrays.equals(actual.exactFrame(), exact.encodeFrame()))
                throw unavailable("actual SDK exposure changed its privately captured complete Source frame");
            if (Long.compareUnsigned(exact.retryUntilEpochMs(), loan.child().retainedUntil()) > 0
                    || exact.encodeFrame().length > paid.definition().plan().maximumSourceFrameBytes()
                    || exact.encodeFrame().length > configuration().limits().maximumSourceFrameBytes())
                throw unavailable("actual SDK frame exceeds its original paid Source/retention bound");
            actualOwned.requireSdkContract(sdk);
            final var issuer = openDurableLoan(loan.child(), TargetScopeResourceRoot.Purpose.SDK_ISSUER);
            final var kind = exposureKind(actual, exact, origin);
            return new TargetSloControlScopeAuthority.ExposureQualification() {
                private TargetScopeDeploymentAllocation.ClosureCoverage closureCoverage;
                private TargetSloControlScopeAuthority.RootHold debitExpected;
                private TargetScopeExposureDebit issuedDebit;
                private boolean transitionAttempted, committed;
                public TargetScopeExposurePolicy.Kind kind() { return kind; }
                public TrustedUtcInterval currentUtcInterval() { requireCurrent(); return captureHandoffTime().interval(); }
                public synchronized void requireCurrent() {
                    ScopeProtection.this.requireCurrent(); actualOwned.requireSdkContract(sdk);
                    actual.requireLocalCurrent();
                    if (!originalRoot().value().processLoan(issuer.loanIndex()).equals(issuer))
                        throw unavailable("original SDK issuer process/purpose loan changed");
                    if (closureCoverage != null) closureCoverage.requireCurrent();
                }
                public synchronized void requireContract(TargetScopeExposureSource.CopyContract actual) {
                    if (!committed || closureCoverage == null)
                        throw unavailable("actual SDK exposure has no original committed closure-coverage proof");
                    requireCurrent();
                    if (actual == null || actual.maximumCopies() != sdk.maximumCopies() || !Arrays.equals(actual.digest(), sdk.digest()))
                        throw unavailable("actual original SDK copy contract changed");
                    actualOwned.requireSdkContract(actual); requireCurrent();
                }
                public synchronized void requireDebitTransition(TargetSloControlScopeAuthority.RootHold expected,
                        TargetSloControlScopeRoot next, TargetScopeExposureDebit debit) {
                    if (transitionAttempted) throw unavailable("original closure coverage cannot be reused for another debit transition");
                    transitionAttempted = true; requireCurrent(); requireRoot(Objects.requireNonNull(expected).root());
                    Objects.requireNonNull(debit).requireFrame(actual.exactFrame());
                    if (expected.birthVersion().isEmpty() || debit.kind() != kind || !debit.origin().equals(origin)
                            || debit.maximumCopies() != sdk.maximumCopies()
                            || !Arrays.equals(debit.expansionContractDigest(), sdk.digest()))
                        throw unavailable("original closure coverage changed its actual Root2 birth/frame/SDK/debit binding");
                    final var request = new TargetScopeDeploymentAllocation.ClosureRequest(actual, configuration().policy(),
                            loan.child(), expected, origin, sdk, kind);
                    closureCoverage = Objects.requireNonNull(loan.originalOwnerLoan().requireClosureCoverage(request),
                            "original Source closure coverage");
                    closureCoverage.requireCurrent(); closureCoverage.requireDebitTransition(expected, Objects.requireNonNull(next), debit);
                    debitExpected = expected; issuedDebit = debit; requireCurrent();
                }
                public synchronized void requireCommittedDebit(TargetSloControlScopeAuthority.RootHold expected,
                        TargetSloControlScopeAuthority.RootHold observed, TargetScopeExposureDebit debit) {
                    requireCurrent();
                    if (closureCoverage == null || expected != debitExpected || !Objects.equals(issuedDebit, debit) || committed)
                        throw unavailable("original closure coverage has no matching unconsumed debit transition");
                    requireRoot(Objects.requireNonNull(observed).root());
                    closureCoverage.requireCommittedDebit(expected, observed, debit);
                    closureCoverage.requireCurrent(); committed = true; requireCurrent();
                }
                public TrustedUtcClock.CustodyTime captureHandoffTime() {
                    requireCurrent(); final var time = actualOwned.captureHandoffTime();
                    if (time.interval().latestEpochMs() > loan.child().retainedUntil())
                        throw unavailable("actual SDK custody exceeds its original retained child horizon");
                    requireCurrent(); return time;
                }
            };
        }
        @Override public synchronized void close() {
            closeRequested = true;
            Throwable retained = null;
            try { nativeHold.close(); }
            catch (RuntimeException | Error failure) {
                if (retained == null) retained = failure;
                else if (retained != failure) retained.addSuppressed(failure);
            }
            if (retained != null || !nativeHold.cleanupComplete()
                    || exposureNative != null && !exposureNative.cleanupComplete())
                throw new ScopeCleanup(this, retained);
            if (!loanClosed) { loan.close(); loanClosed = true; }
        }
        @Override public synchronized boolean cleanupComplete() {
            return closeRequested && nativeHold.cleanupComplete()
                    && (exposureNative == null || exposureNative.cleanupComplete()) && loanClosed;
        }
        @Override public CompletableFuture<Void> retryCleanup() {
            final TargetSourceApplyRuntime.NativeExposureHold actual;
            synchronized (this) {
                if (!closeRequested) return CompletableFuture.failedFuture(unavailable("actual Scope consumer has not requested release"));
                actual = exposureNative;
            }
            final var paidCleanup = nativeHold.retryCleanup();
            final var actualCleanup = actual == null ? CompletableFuture.<Void>completedFuture(null) : actual.retryCleanup();
            return CompletableFuture.allOf(paidCleanup, actualCleanup).thenRun(this::close);
        }
    }

    private static final class ScopeCleanup extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient ScopeProtection original;
        private ScopeCleanup(ScopeProtection original, Throwable cause) {
            super("actual paid/SDK Native holds retain their original finite Scope loan", cause); this.original = original;
        }
        public boolean cleanupComplete() { return original.cleanupComplete(); }
        public CompletableFuture<Void> retryCleanup() { return original.retryCleanup(); }
    }

    public TargetScopeControlObligationVerifier.CapacityAuthority capacityAuthority() {
        return new TargetScopeControlObligationVerifier.CapacityAuthority() {
            public void requireCurrent() { TargetScopeResourceManager.this.requireCurrent(); }
            public void requireHistoricalAuthorized(TargetScopeControlObligation obligation, TargetParentControlCredit parent) {
                requireCurrent();
                final var selected = requireReservedDefinition(obligation.definition());
                TargetScopeControlObligationStore.requireParentAllocation(parent, obligation);
                requireNativeBinding(obligation);
                if (selected.state() == TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED
                        && !selected.confirmation().equals(confirmation(obligation)))
                    throw unavailable("actual historical Native64 funding changed its original manager confirmation");
                requireCurrent();
            }
            public void requireAuthorized(TargetScopeControlObligation.Definition definition,
                    TargetParentControlCreditVerifier.View view, TargetParentControlCredit parent,
                    SourcePosition source, TargetStoreBackend.Mutation complete) {
                requireCurrent();
                final var selected = requireReservedDefinition(definition);
                final var loan = findLoan(originalRoot().value(), selected.childIndex(), TargetScopeResourceRoot.Purpose.CAPACITY);
                if (loan == null || loan.state() != TargetScopeResourceRoot.LoanState.OPEN)
                    throw unavailable("actual Scope funding lacks its original durable process CAPACITY loan");
                if (selected.state() != TargetScopeResourceRoot.ChildState.PENDING)
                    throw unavailable("confirmed original child cannot acquire a second Native64 funding");
                definition.requireRoot(view.actualRoot()); parent.requireReference(definition.parentReference());
                parent.requireRoot(view.actualRoot()); definition.requireSource(source);
                if (!source.sameSourceIdentity(configuration().policy().binding().sourceAnchor())
                        || !Arrays.equals(complete.quota().counters().mutation().source().canonicalBytes(), source.canonicalBytes()))
                    throw unavailable("actual Native64 funding changed its original Source occurrence");
                requireCurrent();
            }
        };
    }

    private TargetScopeResourceRoot.Child requireReservedDefinition(TargetScopeControlObligation.Definition definition) {
        requireDefinition(definition);
        final var selected = child(originalRoot().value(), definition.reference());
        if (!Arrays.equals(selected.definitionDigest(), definition.digest()) || !selected.fixedShares().equals(fixedShares(definition)))
            throw unavailable("actual Scope definition has no matching permanent original fixed share");
        return selected;
    }

    private void requireDefinition(TargetScopeControlObligation.Definition definition) {
        Objects.requireNonNull(definition);
        final var policy = configuration().policy(); final var binding = policy.binding();
        policy.requireHeader(definition.header());
        if (definition.plan().schemaVersion() != TargetSloScopeNativePlan.EXPOSURE_VERSION
                || definition.header().schemaVersion() != TargetSloControlScopeHeader.VERSION
                || !Arrays.equals(definition.originalRoot().identity().accountingIncarnation(), binding.accountingIncarnation())
                || !definition.sourceAnchor().sameSourceIdentity(binding.sourceAnchor())
                || definition.plan().maximumSourceFrameBytes() > configuration().limits().maximumSourceFrameBytes())
            throw unavailable("actual Scope definition differs from original Header2/Plan3/accounting/Source backing");
    }

    private void requireNativeBinding(TargetScopeControlObligation paid) {
        requireDefinition(paid.definition());
        if (!paid.fundedAt().source().sameSourceIdentity(configuration().policy().binding().sourceAnchor()))
            throw unavailable("actual Native64 payment changed its original physical Source");
    }

    private TargetScopeControlObligation requirePaid(TargetSourceApplyRuntime.NativeExposureHold held,
            TargetScopeResourceRoot.Child selected, boolean confirmed) {
        held.requireLocalCurrent();
        final var paid = Objects.requireNonNull(held.paidScope(), "actual original Native64 Scope payment");
        requireNativeBinding(paid);
        if (!selected.headerReference().equals(held.reference()) || !selected.headerReference().equals(paid.reference())
                || !held.scope().equals(configuration().policy().binding().scope())
                || !Arrays.equals(held.lineage(), configuration().policy().binding().lineage())
                || !Arrays.equals(selected.definitionDigest(), paid.definition().digest())
                || !selected.fixedShares().equals(fixedShares(paid.definition())))
            throw unavailable("actual Native64 hold differs from its permanent original child allocation");
        paid.fundedAt().requireAtOrBefore(held.cut());
        if (confirmed && (selected.state() != TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED
                || !selected.confirmation().equals(confirmation(paid))))
            throw unavailable("actual Native64 payment has no matching original confirmed manager child");
        held.requireLocalCurrent(); return paid;
    }

    private static TargetScopeResourceRoot.NativeConfirmation confirmation(TargetScopeControlObligation paid) {
        return TargetScopeResourceRoot.NativeConfirmation.from(paid.fundedAt(), Bytes.sha256(paid.funding().input().encodeFrame()));
    }

    private static TargetScopeExposurePolicy.Kind exposureKind(TargetSourceApplyRuntime.NativeExposureHold actual,
            SystemMutation exact, TargetScopeExposureDebit.Origin origin) {
        if (origin instanceof TargetScopeExposureDebit.ServiceWriter writer) {
            if (exact.type() != SystemMutationType.TARGET_SLO_OBSERVATION
                    || !writer.exactWriter().equals(TargetSloObservationEventBody.decode(exact.canonicalBody()).writerEpoch()))
                throw new IllegalArgumentException("actual SERVICE exposure changed its original writer");
            return actual.closureDischarge() ? TargetScopeExposurePolicy.Kind.CLOSURE : TargetScopeExposurePolicy.Kind.NEW_PROMISE;
        }
        if (TargetSloWriterControlBody.isControl(exact))
            return TargetSloWriterControlBody.decode(exact.canonicalBody()).request().contract() == null
                    ? TargetScopeExposurePolicy.Kind.CLOSURE : TargetScopeExposurePolicy.Kind.NEW_PROMISE;
        if (TargetSloObservationControlBody.isControl(exact))
            return TargetSloObservationControlBody.decode(exact.canonicalBody()).request().policy() == null
                    ? TargetScopeExposurePolicy.Kind.CLOSURE : TargetScopeExposurePolicy.Kind.NEW_PROMISE;
        throw new IllegalArgumentException("actual Source exposure is outside the original finite SLO frame family");
    }

    private void requireManagement(TargetScopeDeploymentAllocation.ManagerPurpose purpose, AuthorIdentity actor) {
        requireCurrent();
        if (actor == null) throw unavailable("independent original management purpose is unavailable");
        actualOwned.requireManagementPurpose(purpose, actor); requireCurrent();
    }

    private RootRecord originalRoot() {
        final var root = observedRoot;
        if (root == null) throw unavailable("actual original manager Root has not been attached");
        return root;
    }

    private synchronized void observe(RootRecord root) {
        Objects.requireNonNull(root);
        if (!configuration().policy().equals(root.value().policy())
                || !Arrays.equals(storage.namespaceBinding(), configuration().policy().binding().registryNamespaceBinding())
                || !storage.keyPrefix().equals(configuration().policy().binding().registryKeyPrefix()))
            throw unavailable("observed manager Root changed its exact original policy/namespace binding");
        final var prior = observedRoot;
        if (prior != null) {
            if (prior.birthVersionId() != root.birthVersionId())
                throw unavailable("observed manager Root changed its immutable first-birth version");
            if (root.value().revision() < prior.value().revision()) return;
            if (root.value().revision() == prior.value().revision() && (!root.value().equals(prior.value()) || root.versionId() != prior.versionId()))
                throw unavailable("observed manager Root changed bytes/version at the same original revision");
        }
        observedRoot = root;
    }

    private TargetScopeResourceRoot transition(UnaryOperator<TargetScopeResourceRoot> change,
            Predicate<TargetScopeResourceRoot> completed, String unknown) {
        requireStorageCurrent();
        final var actual = storage.readRoot();
        if (actual == null) throw unavailable("original manager Root disappeared; retain UNKNOWN");
        observe(actual);
        final var next = Objects.requireNonNull(change.apply(actual.value()));
        final var observed = Objects.requireNonNull(storage.compareRoot(actual, next));
        observe(observed); requireCurrent();
        final var result = originalRoot().value();
        if (!completed.test(result)) throw new ConcurrentModificationException(unknown);
        return result;
    }

    private TargetScopeResourceRoot.ProcessLoan findLoan(TargetScopeResourceRoot root, int childIndex,
            TargetScopeResourceRoot.Purpose purpose) {
        for (var loan : root.processLoans())
            if (loan.childIndex() == childIndex && loan.purpose() == purpose
                    && Arrays.equals(loan.processIncarnationDigest(), inputs.processIncarnationDigest())) return loan;
        return null;
    }

    private static TargetScopeResourceRoot.Child findChild(TargetScopeResourceRoot root, TargetSloControlScopeHeader.Reference reference) {
        for (var child : root.children()) if (child.headerReference().equals(reference)) return child;
        return null;
    }
    private static TargetScopeResourceRoot.Child child(TargetScopeResourceRoot root, TargetSloControlScopeHeader.Reference reference) {
        final var found = findChild(root, reference);
        if (found == null) throw unavailable("original Scope has no permanently charged manager child slot");
        return found;
    }
    private static TargetScopeResourceRoot.Child matchingChild(TargetScopeResourceRoot root,
            TargetSloControlScopeHeader.Reference reference, byte[] digest, TargetScopeResourcePolicy.Shares shares, long retainedUntil) {
        final var child = findChild(root, reference);
        return child != null && Arrays.equals(child.definitionDigest(), digest) && child.fixedShares().equals(shares)
                && child.retainedUntil() == retainedUntil ? child : null;
    }

    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message) {
        return new TargetSourceAuthorityPreparation.InputUnavailable(message);
    }

    private static void closePair(TargetSourceApplyRuntime.NativeExposureHold held, Loan loan) {
        if (held != null) {
            held.close();
            if (!held.cleanupComplete()) throw new PairCleanup(held, loan, null);
        }
        loan.close();
    }

    private static RuntimeException releaseFailure(TargetSourceApplyRuntime.NativeExposureHold held, Loan loan, Throwable failure) {
        // A capture may throw before returning its Native handle. Its exact original Cleanup
        // still owns that in-flight capture, so its finite local slot cannot be returned yet.
        if (failure instanceof CheckpointInputFuture.Cleanup cleanup && !cleanup.cleanupComplete())
            return new PairCleanup(held, loan, failure);
        try { closePair(held, loan); }
        catch (RuntimeException | Error close) {
            if (failure != close) failure.addSuppressed(close);
            return new PairCleanup(held, loan, failure);
        }
        if (failure instanceof Error fatal) throw fatal;
        return (RuntimeException) failure;
    }

    private static final class PairCleanup extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetSourceApplyRuntime.NativeExposureHold held;
        private final transient Loan loan;
        private final transient CheckpointInputFuture.Cleanup acquisitionCleanup;
        private transient CompletableFuture<Void> acquisitionRetry;
        private boolean complete;
        private PairCleanup(TargetSourceApplyRuntime.NativeExposureHold held, Loan loan, Throwable cause) {
            super("actual Native/Scope local loan cleanup remains retained", cause); this.held = held; this.loan = loan;
            acquisitionCleanup = cause instanceof CheckpointInputFuture.Cleanup cleanup ? cleanup : null;
        }
        public synchronized boolean cleanupComplete() { return complete; }
        public CompletableFuture<Void> retryCleanup() {
            final CompletableFuture<Void> pending;
            synchronized (this) {
                if (complete) return CompletableFuture.completedFuture(null);
                if (acquisitionCleanup != null && !acquisitionCleanup.cleanupComplete()) {
                    if (acquisitionRetry == null || acquisitionRetry.isDone()) {
                        try { acquisitionRetry = Objects.requireNonNull(acquisitionCleanup.retryCleanup(), "original Native acquisition cleanup retry"); }
                        catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
                    }
                    pending = acquisitionRetry;
                } else pending = null;
            }
            if (pending != null) return pending.thenCompose(ignored -> {
                if (!acquisitionCleanup.cleanupComplete())
                    return CompletableFuture.failedFuture(unavailable("original Native acquisition cleanup remains retained"));
                return releaseNative();
            });
            return releaseNative();
        }
        private CompletableFuture<Void> releaseNative() {
            if (held == null) return finish();
            try { held.close(); }
            catch (RuntimeException | Error failure) {
                if (!held.cleanupComplete()) return held.retryCleanup().thenCompose(ignored -> finish());
            }
            return finish();
        }
        private synchronized CompletableFuture<Void> finish() {
            try {
                closePair(held, loan); complete = true; return CompletableFuture.completedFuture(null);
            } catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
    }

    /** Process teardown only, invoked on its original bounded I/O path; never from an SDK callback. */
    @Override public void close() {
        closing = true;
        try { actualOwned.close(); }
        catch (RuntimeException | Error failure) { throw new ManagerFailure("original manager close retains its physical owner cleanup", failure, actualOwned); }
    }
    @Override public boolean cleanupComplete() { return closing && actualOwned.cleanupComplete(); }
    @Override public CompletableFuture<Void> retryCleanup() {
        closing = true;
        return actualOwned.retryCleanup();
    }

    private static final class ManagerFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetScopeDeploymentAllocation.Owned retained;
        private ManagerFailure(String message, Throwable cause, TargetScopeDeploymentAllocation.Owned retained) {
            super(message, cause); this.retained = retained;
        }
        public boolean cleanupComplete() { return retained.cleanupComplete(); }
        public CompletableFuture<Void> retryCleanup() { return retained.retryCleanup(); }
    }

    record RootRecord(TargetScopeResourceRoot value,long versionId,long birthVersionId) {
        RootRecord {
            Objects.requireNonNull(value);
            if(versionId<0||versionId==Long.MAX_VALUE||birthVersionId<0||birthVersionId==Long.MAX_VALUE)
                throw new IllegalArgumentException("actual manager root and first-birth versions required");
        }
    }

    interface Storage {
        byte[] namespaceBinding();
        String keyPrefix();
        RootRecord readRoot();
        RootRecord initialize(TargetScopeResourcePolicy policy);
        RootRecord compareRoot(RootRecord expected,TargetScopeResourceRoot next);
        void requireCurrent();
    }
}
