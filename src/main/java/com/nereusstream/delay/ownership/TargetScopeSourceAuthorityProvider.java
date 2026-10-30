package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/** Adds original allocation and Scope payment holds to the existing Source content provider. */
public final class TargetScopeSourceAuthorityProvider implements TargetSourceAuthorityLoader.Provider {
    private final TargetSourceAuthorityLoader.Provider original;
    private final OxiaControlTargetRegistrationAuthority registrations;
    private final TargetScopeResourceManager manager;
    private final long maximumNativeBytes, maximumRetainedBytes;
    private final LongSupplier clock;

    public TargetScopeSourceAuthorityProvider(TargetSourceAuthorityLoader.Provider original,
            OxiaControlTargetRegistrationAuthority registrations, TargetScopeResourceManager manager,
            long maximumNativeBytes, long maximumRetainedBytes, LongSupplier clock) {
        this.original = Objects.requireNonNull(original);
        this.manager = Objects.requireNonNull(manager);
        this.clock = Objects.requireNonNull(clock);
        if (maximumNativeBytes < 1 || maximumNativeBytes == Long.MAX_VALUE
                || maximumRetainedBytes < 1 || maximumRetainedBytes == Long.MAX_VALUE)
            throw new IllegalArgumentException("finite original Source Scope acquisition bounds required");
        this.maximumNativeBytes = maximumNativeBytes;
        this.maximumRetainedBytes = maximumRetainedBytes;
        this.registrations = Objects.requireNonNull(registrations)
                .withSloScopes(manager.protections(), maximumRetainedBytes);
    }

    @Override public TargetSourceAuthorityLoader.HeldInputs load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard) {
        return acquire(entry, family, guard, () -> original.load(entry, family, guard));
    }

    @Override public TargetSourceAuthorityLoader.HeldInputs load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard,
            TargetSourceApplyRuntime.ConfirmationRead source) {
        return acquire(entry, family, guard, () -> original.load(entry, family, guard, source));
    }

    @Override public TargetSourceAuthorityLoader.HeldInputs load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard,
            TargetSourceApplyRuntime.ConfirmationRead source, TargetSourceApplyRuntime.ReclaimRead reclaim) {
        return acquire(entry, family, guard, () -> original.load(entry, family, guard, source, reclaim));
    }

    private TargetSourceAuthorityLoader.HeldInputs acquire(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard, OriginalLoad load) {
        Objects.requireNonNull(guard);
        final var held = new Held(Objects.requireNonNull(entry));
        try {
            guard.run();
            // Fee-only replay must not acquire a business role which the rejected input never had.
            if (family != TargetSourceAuthorityPreparation.Family.SCOPE_FEES) {
                held.original = Objects.requireNonNull(load.load(), "original Source content hold");
                held.retain(held.original, held.original::retainedBytes);
                held.original.requireCurrent();
                if (!SourceApplyCoordinator.sameEntry(entry, held.original.entry()))
                    throw new IllegalStateException("Scope decorator changed the original Source entry");
                held.legacyFees = Objects.requireNonNull(held.original.scopeFees());
            }
            held.owner = manager.borrowOriginalOwner();
            held.retain(held.owner, manager::originalOwnerRetainedBytes);
            if (entry instanceof SourceReplayMutation mutation) {
                held.exact = mutation.mutation();
                held.frame = held.exact.encodeFrame();
                held.charge(held.frame.length);
                if (hasNativeOrigin(held.exact)) {
                    held.origin = manager.captureScopeFeeOrigin(held.exact, held.nativeBound(), clock);
                    held.retain(held.origin, held.origin::retainedBytes);
                    if (!Arrays.equals(held.frame, held.origin.exactFrame()))
                        throw unavailable("actual Native fee capture changed this Source occurrence's complete frame");
                }
            }
            if (requiresScopeSet(family)) {
                // This is the finite child set of this same allocation Root, not another Source inventory.
                final var allocation = manager.snapshot();
                for (var child : allocation.children()) {
                    if (child.state() != TargetScopeResourceRoot.ChildState.NATIVE_CONFIRMED) continue;
                    final var reference = child.headerReference();
                    final var loan = manager.borrow(reference, TargetScopeResourceRoot.Purpose.SOURCE_FEES);
                    held.retainLoan(loan);
                    manager.requireOwnLoan(loan);
                    final TargetSourceApplyRuntime.NativeExposureHold nativeScope;
                    if (held.origin != null && reference.equals(held.origin.reference())) nativeScope = held.origin;
                    else {
                        nativeScope = manager.capturePaidScope(reference, held.nativeBound(), clock);
                        held.retain(nativeScope, nativeScope::retainedBytes);
                    }
                    final var root = registrations.sloScopes().captureRoot(reference, held.remaining(), guard);
                    held.retain(root, root::retainedBytes);
                    final var capture = new ScopeCapture(loan, nativeScope, root);
                    capture.requireCurrent();
                    held.scopes.put(key(reference), capture);
                }
                held.captureMember(guard);
                held.requireOriginalOccurrenceFacts();
            }
            if (held.original == null && held.origin != null && held.origin.paidScope() != null
                    && !held.scopes.containsKey(key(held.origin.reference())) && legacy(held.origin.paidScope())) {
                held.original = Objects.requireNonNull(load.load(), "original legacy Source fee hold");
                held.releases.add(0, new Release(held.original));
                held.charge(held.original.retainedBytes());
                held.original.requireCurrent();
                if (!SourceApplyCoordinator.sameEntry(entry, held.original.entry())
                        || !(held.original.inputs() instanceof TargetSourceAuthorityLoader.ScopeFees fees))
                    throw unavailable("original legacy Source fee provider changed its fee-only input");
                held.legacyFees = fees.fees();
            }
            held.inputs = family == TargetSourceAuthorityPreparation.Family.SCOPE_FEES
                    ? new TargetSourceAuthorityLoader.ScopeFees(held)
                    : decorate(Objects.requireNonNull(held.original.inputs()), held);
            held.requireCurrent();
            guard.run();
            return held;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof CheckpointInputFuture.Cleanup cleanup) held.retainFailure(cleanup);
            try { held.close(); }
            catch (RuntimeException | Error cleanup) {
                final var retained = new CleanupFailure(held, failure);
                if (cleanup != failure) retained.addSuppressed(cleanup);
                throw retained;
            }
            throw failure;
        }
    }

    private static boolean hasNativeOrigin(SystemMutation exact) {
        if (exact.type() == SystemMutationType.TARGET_SLO_OBSERVATION)
            return TargetSloObservationEventBody.decode(exact.canonicalBody()).writerEpoch() != null;
        return TargetSloControlScopeAuthority.reference(exact).isPresent();
    }

    private static boolean requiresScopeSet(TargetSourceAuthorityPreparation.Family family) {
        return switch (family) {
            case COMMAND, ADMISSION, SLO_MODE, SLO_EVENT, SLO_WRITER, SCOPE_FEES -> true;
            default -> false;
        };
    }

    private ControlTargetRegistrationAuthority scoped(ControlTargetRegistrationAuthority originalAuthority, Held held) {
        if (held.origin != null && held.origin.paidScope() != null
                && !held.scopes.containsKey(key(held.origin.reference())) && legacy(held.origin.paidScope()))
            return originalAuthority;
        if (!(originalAuthority instanceof OxiaControlTargetRegistrationAuthority managed))
            throw unavailable("original SLO Control registration backend is not the actual managed backend");
        return managed.withSloScopes(manager.protections(), maximumRetainedBytes);
    }

    private TargetSourceAuthorityLoader.Inputs decorate(TargetSourceAuthorityLoader.Inputs inputs, Held held) {
        return switch (inputs) {
            case TargetSourceAuthorityLoader.SloWriter value -> {
                final var a = value.authority();
                yield new TargetSourceAuthorityLoader.SloWriter(value.prepared(),
                        new TargetSloWriterEpochVerifier.Authority(scoped(a.registrations(), held), a.keys(),
                                a.actor(), a.scopeProof(), a.materialization()));
            }
            case TargetSourceAuthorityLoader.SloMode value -> {
                final var a = value.authority();
                yield new TargetSourceAuthorityLoader.SloMode(value.prepared(),
                        new TargetSloObservationVerifier.Authority(scoped(a.registrations(), held), a.keys(),
                                a.actor(), a.scopeProof(), a.materialization()));
            }
            case TargetSourceAuthorityLoader.ScopeObligation value -> {
                final var a = value.authority();
                yield new TargetSourceAuthorityLoader.ScopeObligation(value.prepared(),
                        new TargetScopeControlObligationVerifier.Authority(a.registrations(), a.keys(),
                                a.actor(), a.scopeProof(), manager.capacityAuthority()), value.commit());
            }
            case TargetSourceAuthorityLoader.Command value -> new TargetSourceAuthorityLoader.Command(
                    value.policy(), value.cancellations(), value.reservations(), value.schedules(),
                    value.payloadProofs(), samples(value.samples(), held));
            case TargetSourceAuthorityLoader.Admission value -> new TargetSourceAuthorityLoader.Admission(
                    value.authority(), samples(value.samples(), held));
            default -> inputs;
        };
    }

    private static TargetSloObservationSamples.Authority samples(TargetSloObservationSamples.Authority original,
            TargetScopeFeeAuthority fees) {
        return new TargetSloObservationSamples.Authority() {
            @Override public TargetScopeFeeAuthority scopeFees() { return fees; }
            @Override public void requireSample(TargetSloObservationGeneration generation,
                    TargetSloObservationSample sample, long bytes) {
                original.requireSample(generation, sample, bytes);
            }
            @Override public void requireCurrent() { original.requireCurrent(); }
        };
    }

    @FunctionalInterface private interface OriginalLoad { TargetSourceAuthorityLoader.HeldInputs load(); }

    private final class ScopeCapture {
        private final TargetScopeResourceManager.Loan loan;
        private final TargetSourceApplyRuntime.NativeExposureHold nativeScope;
        private final TargetSloControlScopeAuthority.RootHold root;
        private final TargetScopeControlObligation paid;
        private ScopeCapture(TargetScopeResourceManager.Loan loan,
                TargetSourceApplyRuntime.NativeExposureHold nativeScope,
                TargetSloControlScopeAuthority.RootHold root) {
            this.loan = loan; this.nativeScope = nativeScope; this.root = root;
            paid = Objects.requireNonNull(nativeScope.paidScope(), "actual Native Scope payment");
        }
        private void requireCurrent() {
            manager.requireOwnLoan(loan); loan.requireCurrent(); nativeScope.requireLocalCurrent(); root.requireCurrent();
            final var child = loan.child();
            final var confirmation = TargetScopeResourceRoot.NativeConfirmation.from(paid.fundedAt(),
                    Bytes.sha256(paid.funding().input().encodeFrame()));
            if (!child.headerReference().equals(paid.reference())
                    || !Arrays.equals(child.definitionDigest(), paid.definition().digest())
                    || !confirmation.equals(child.confirmation())
                    || !Arrays.equals(root.root().header().canonicalBytes(), paid.definition().header().canonicalBytes())
                    || !nativeScope.scope().equals(paid.definition().scope())
                    || !Arrays.equals(nativeScope.lineage(), paid.definition().lineage())
                    || root.root().schemaVersion() == TargetSloControlScopeRoot.VERSION && root.birthVersion().isEmpty())
                throw unavailable("Scope fee hold changed its original confirmed child, funding or actual registry birth");
        }
        private void requireScope(TargetScopeControlObligation selected) {
            requireCurrent();
            if (!paid.reference().equals(selected.reference())
                    || !Arrays.equals(paid.definition().canonicalBytes(), selected.definition().canonicalBytes())
                    || !paid.fundedAt().equals(selected.fundedAt())
                    || !paid.parentCommitment().equals(selected.parentCommitment()))
                throw unavailable("actual reader selected a different original Scope definition, funding or C0");
            // Usage and the latest settlement stamp may advance while the immutable funding remains held.
            requireCurrent();
        }
    }

    private final class Held implements TargetSourceAuthorityLoader.HeldInputs, TargetScopeFeeAuthority,
            CheckpointInputFuture.Cleanup {
        private final SourceReplayEntry entry;
        private final List<Release> releases = new ArrayList<>();
        private final Map<String, ScopeCapture> scopes = new LinkedHashMap<>();
        private TargetSourceAuthorityLoader.HeldInputs original;
        private TargetSourceAuthorityLoader.Inputs inputs;
        private TargetScopeFeeAuthority legacyFees = TargetScopeFeeAuthority.UNAVAILABLE;
        private TargetScopeDeploymentAllocation.Loan owner;
        private TargetSourceApplyRuntime.NativeExposureHold origin;
        private TargetSloControlScopeAuthority.MemberHold member;
        private PreparedControlOperation prepared;
        private SystemMutation exact;
        private byte[] frame;
        private long bytes;
        private boolean closing, closed;
        private Held(SourceReplayEntry entry) { this.entry = entry; }
        private long remaining() {
            final long remaining = Math.subtractExact(maximumRetainedBytes, bytes);
            if (remaining < 1) throw unavailable("complete Source Scope fee hold exceeds its single retained-byte bound");
            return remaining;
        }
        private long nativeBound() { return Math.min(maximumNativeBytes, remaining()); }
        private void charge(long added) {
            if (added < 0) throw new IllegalStateException("negative original Source Scope retained bytes");
            bytes = Math.addExact(bytes, added);
            if (bytes > maximumRetainedBytes)
                throw unavailable("complete Source Scope fee hold exceeds its single retained-byte bound");
        }
        private void retain(AutoCloseable resource, LongSupplier retained) {
            // Install release duty before a byte check can fail.
            releases.add(new Release(resource)); charge(retained.getAsLong());
        }
        private void retainLoan(TargetScopeResourceManager.Loan loan) {
            // The origin capture preceded selection of its actual child. Its real cleanup must
            // nevertheless exit before this child's durable-purpose loan can be returned locally.
            int beforeOrigin = releases.size();
            for (int index = 0; index < releases.size(); index++) {
                if (releases.get(index).resource == origin) { beforeOrigin = index; break; }
            }
            releases.add(beforeOrigin, new Release(loan)); charge(loan.retainedBytes());
        }
        private void retainFailure(CheckpointInputFuture.Cleanup cleanup) {
            for (var release : releases) if (release.pending == cleanup) return;
            releases.add(new Release(cleanup));
        }
        private void captureMember(Runnable guard) {
            if (origin == null || exact == null || origin.reference() == null
                    || !scopes.containsKey(key(origin.reference()))
                    || exact.type() == SystemMutationType.TARGET_SLO_OBSERVATION) return;
            final byte[] operationId;
            if (TargetSloWriterControlBody.isControl(exact))
                operationId = TargetSloWriterControlBody.decode(exact.canonicalBody()).controlRef().operationId();
            else operationId = TargetSloObservationControlBody.decode(exact.canonicalBody()).controlRef().operationId();
            prepared = registrations.find(operationId).orElseThrow(
                    () -> unavailable("original Scope fee Control registration is unavailable"));
            if (prepared.targets().size() != 1)
                throw unavailable("original Scope fee Control does not retain its exact single target");
            member = registrations.captureScopedMutation(prepared, prepared.targets().getFirst(), exact, remaining(), guard);
            retain(member, member::retainedBytes);
            requireMember();
        }
        private void requireMember() {
            try { member.validate(prepared, prepared.targets().getFirst(), exact); }
            catch (RuntimeException missing) {
                throw unavailable("funded Scope fee Control lacks its actual original MEMBER", missing);
            }
        }
        private void requireOriginalOccurrenceFacts() {
            if (origin == null || origin.paidScope() == null) return;
            final var capture = scopes.get(key(origin.reference()));
            if (capture == null) {
                if (!legacy(origin.paidScope()))
                    throw unavailable("actual Native fee origin is outside the held confirmed child set");
                return;
            }
            capture.requireScope(origin.paidScope());
            final TargetScopeExposureDebit.Origin expectedOrigin;
            if (exact.type() == SystemMutationType.TARGET_SLO_OBSERVATION) {
                final var writer = origin.writerEpoch();
                final var body = TargetSloObservationEventBody.decode(exact.canonicalBody());
                if (writer == null || !writer.reference().equals(body.writerEpoch())
                        || !origin.reference().equals(writer.register().body().admissionScope()))
                    throw unavailable("Scope fee SERVICE occurrence lost its actual original REGISTER");
                expectedOrigin = TargetScopeExposureDebit.Origin.serviceWriter(writer.reference());
            } else {
                if (member == null || prepared == null)
                    throw unavailable("funded Scope fee Control lacks its actual original MEMBER");
                requireMember();
                final var registered = member.input();
                expectedOrigin = TargetScopeExposureDebit.Origin.controlMember(registered.slot(), registered.digest());
            }
            if (capture.root.root().schemaVersion() == TargetSloControlScopeRoot.VERSION) {
                final byte[] digest = Bytes.sha256(frame);
                for (var debit : capture.root.root().exposureDebits()) {
                    if (expectedOrigin.equals(debit.origin()) && Arrays.equals(debit.frameDigest(), digest)) {
                        debit.requireFrame(frame); return;
                    }
                }
                throw unavailable("funded Source occurrence lacks its complete original Root2 frame/origin debit");
            }
        }
        @Override public SourceReplayEntry entry() { return entry; }
        @Override public TargetSourceAuthorityLoader.Inputs inputs() { requireCurrent(); return inputs; }
        @Override public long retainedBytes() { return bytes; }
        @Override public TargetScopeFeeAuthority scopeFees() { requireCurrent(); return this; }
        @Override public synchronized void requireCurrent() {
            if (closing || closed) throw unavailable("original Source Scope fee protection is closing");
            try {
                if (original != null) original.requireCurrent();
                if (owner != null) owner.requireCurrent();
                if (origin != null) origin.requireLocalCurrent();
                for (var capture : scopes.values()) capture.requireCurrent();
                if (member != null) member.requireCurrent();
            } catch (TargetSourceAuthorityPreparation.InputUnavailable missing) { throw missing; }
            catch (RuntimeException missing) {
                throw unavailable("original Source Scope fee protection is no longer locally held", missing);
            }
        }
        @Override public SystemMutation occurrenceInput(SystemMutation firstInput,
                TargetResultRecord first, TargetResultRecord position) {
            requireCurrent(); requirePosition(position);
            if (exact == null || !Arrays.equals(Bytes.sha256(exact.canonicalEnvelope()), position.mutation().mutationDigest()))
                throw unavailable("this physical Source occurrence's exact original frame is unavailable");
            return exact;
        }
        @Override public void requireScope(TargetScopeControlObligation selected) {
            requireCurrent();
            final var capture = scopes.get(key(Objects.requireNonNull(selected).reference()));
            if (capture != null) capture.requireScope(selected);
            else if (legacy(selected)) legacyFees.requireScope(selected);
            else throw unavailable("actual Native Scope is outside this held original allocation child set");
            requireCurrent();
        }
        @Override public Payment requireOccurrence(Occurrence occurrence) {
            requireCurrent(); requirePosition(occurrence.position());
            if (exact == null || occurrence.actualInput() == null
                    || !Arrays.equals(frame, occurrence.actualInput().encodeFrame())
                    || !Arrays.equals(Bytes.sha256(exact.canonicalEnvelope()), occurrence.position().mutation().mutationDigest()))
                throw unavailable("Scope fee occurrence changed this Source entry's exact complete frame");
            requireOriginalOccurrenceFacts();
            final Payment payment;
            try { payment = owner.requireScopeFeeOccurrence(occurrence); }
            catch (TargetSourceAuthorityPreparation.InputUnavailable missing) { throw missing; }
            catch (RuntimeException missing) {
                throw unavailable("original Source occurrence fee qualification is unavailable", missing);
            }
            if (payment == Payment.INVALID_SHARED) { requireCurrent(); return payment; }
            final var selected = occurrence.expectedScope();
            if (selected == null || origin == null || origin.paidScope() == null
                    || !selected.reference().equals(origin.reference()))
                throw unavailable("funded Source occurrence lacks its actual original Native payer");
            final var capture = scopes.get(key(selected.reference()));
            if (capture == null) {
                if (!legacy(selected)) throw unavailable("funded Source occurrence has no held confirmed allocation child");
                final var originalPayment = legacyFees.requireOccurrence(occurrence);
                if (originalPayment != Payment.FUNDED_SCOPE)
                    throw unavailable("original legacy payer differs from the actual funded Source occurrence");
                requireCurrent(); return originalPayment;
            }
            capture.requireScope(selected);
            requireCurrent(); return Payment.FUNDED_SCOPE;
        }
        private void requirePosition(TargetResultRecord position) {
            if (!Arrays.equals(entry.position().canonicalBytes(), position.mutation().source().canonicalBytes()))
                throw unavailable("Scope fee authority was invoked for another physical Source occurrence");
        }
        @Override public synchronized void close() {
            closing = true;
            // The existing Source preparation may retry close through its own Cleanup wrapper.
            // Keep driving the same retained child receipts rather than requiring that wrapper
            // to know this decorator's private release stack.
            advance(true);
            if (!closed) throw new CleanupFailure(this, null);
        }
        @Override public synchronized boolean cleanupComplete() { return closed; }
        @Override public synchronized CompletableFuture<Void> retryCleanup() {
            closing = true;
            try {
                final var pending = advance(true);
                if (closed) return CompletableFuture.completedFuture(null);
                return Objects.requireNonNull(pending).handle((ignored, failure) -> {
                    synchronized (Held.this) {
                        advance(false);
                        if (!closed) throw new CleanupFailure(Held.this, failure);
                    }
                    return null;
                });
            } catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
        private CompletableFuture<Void> advance(boolean retry) {
            for (int index = releases.size() - 1; index >= 0; index--) {
                final var release = releases.get(index);
                if (!release.release(retry)) return release.retry;
            }
            closed = true; return CompletableFuture.completedFuture(null);
        }
    }

    private static boolean legacy(TargetScopeControlObligation paid) {
        return paid.definition().header().schemaVersion() == TargetSloControlScopeHeader.LEGACY_VERSION
                || paid.definition().plan().schemaVersion() < TargetSloScopeNativePlan.EXPOSURE_VERSION;
    }
    private static String key(TargetSloControlScopeHeader.Reference reference) { return Bytes.hex(reference.canonicalBytes()); }
    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message) {
        return new TargetSourceAuthorityPreparation.InputUnavailable(message);
    }
    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message, Throwable failure) {
        final var unavailable = unavailable(message); unavailable.initCause(failure); return unavailable;
    }

    /** Reverse ownership order keeps allocation loans until every later actual Native cleanup has exited. */
    private static final class Release {
        private final AutoCloseable resource;
        private CheckpointInputFuture.Cleanup pending;
        private CompletableFuture<Void> retry;
        private boolean done;
        private Release(AutoCloseable resource) { this.resource = Objects.requireNonNull(resource); }
        private Release(CheckpointInputFuture.Cleanup cleanup) { resource = null; pending = cleanup; }
        private boolean release(boolean driveRetry) {
            if (done) return true;
            if (retry != null && !retry.isDone()) return false;
            if (pending != null) {
                if (!pending.cleanupComplete()) {
                    if (driveRetry) retry = Objects.requireNonNull(pending.retryCleanup());
                    if (retry == null) retry = CompletableFuture.failedFuture(
                            new IllegalStateException("original Source Scope release has not exited"));
                    if (!retry.isDone() || !pending.cleanupComplete()) return false;
                }
                pending = null; retry = null;
            }
            if (resource == null) { done = true; return true; }
            try { resource.close(); done = true; return true; }
            catch (RuntimeException | Error failure) {
                if (failure instanceof CheckpointInputFuture.Cleanup cleanup) pending = cleanup;
                retry = CompletableFuture.failedFuture(failure); return false;
            } catch (Exception failure) {
                retry = CompletableFuture.failedFuture(failure); return false;
            }
        }
    }

    private static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held held;
        private CleanupFailure(Held held, Throwable failure) {
            super("original Source Scope fee acquisition/release retains its exact cleanup duty", failure);
            this.held = held;
        }
        @Override public boolean cleanupComplete() { return held.cleanupComplete(); }
        @Override public CompletableFuture<Void> retryCleanup() { return held.retryCleanup(); }
    }
}
