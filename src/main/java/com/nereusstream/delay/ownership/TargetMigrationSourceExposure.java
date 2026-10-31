package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMigrationActivateRequest;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Borrows the original Source owner's paid management obligation; it creates no physical allocation. */
public final class TargetMigrationSourceExposure implements CheckpointInputFuture.Cleanup, AutoCloseable {
    /**
     * An additional capability of the already acquired original physical Source handle. Implementations
     * authenticate independent migration CONTROL management rights, the entire producer/admin domain and
     * registration/history/retention backing. Neither a Source SDK reservation nor SLO Scope membership
     * supplies this capability. Missing capability fails closed at acquire().
     */
    public interface ManagementHandle extends TargetScopeDeploymentAllocation.SourceHandle {
        /** Uses the same already held management grant, exact actor/frame and actual physical Source. */
        void requireMigrationManagement(Request request);

        /**
         * Durably reserves this exact original operation before SDK exposure. The original owner enforces
         * the finite frame/byte/copy/retry and concurrent-process limits, including registration, failed
         * attempts, UNKNOWN, late writes and retention until explicit retirement. Reopening the same
         * operation cannot obtain a fresh share or silently change its Prepared bytes/copy contract.
         * Partial failures retain their actual native work through Cleanup and inFlight().
         */
        Debit reserveMigrationControl(Request request, Runnable current);

        /** Actual original controlled namespace/cost/retention loan; raw custody algorithm remains in this repository. */
        default TargetMigrationPreparedCustody.Backing maintenancePreparedBacking(PreparedRequest request) {
            throw new IllegalStateException("original Source owner lacks prepaid exact Prepared history backing");
        }
    }

    public record PreparedRequest(TargetMigrationSourceOpening opening,
            com.nereusstream.delay.protocol.TargetMigrationRequest request) {
        public PreparedRequest {
            Objects.requireNonNull(opening); Objects.requireNonNull(request);
            final byte[] migrationId = switch (request) {
                case com.nereusstream.delay.protocol.TargetMigrationSettlementRequest value -> value.migrationId();
                case com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest value -> value.migrationId();
                case com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest value -> value.migrationId();
                case com.nereusstream.delay.protocol.TargetMigrationArchiveRequest value -> value.migrationId();
                default -> throw new IllegalArgumentException("live custody does not authorize ACTIVATE");
            };
            if (!request.scope().equals(opening.configuration().scope())
                    || !java.util.Arrays.equals(migrationId, opening.configuration().migrationId()))
                throw new IllegalArgumentException("live migration Prepared custody changed its original scope/operation");
        }
        /** Same candidate forever; expiry/UNKNOWN/response loss cannot create another operation ID. */
        public byte[] candidateId() { return candidateId(request); }
        /** Pure identity for startup recovery's independent original Source/registration loader; grants no capability. */
        public static byte[] candidateId(com.nereusstream.delay.protocol.TargetMigrationRequest request) {
            final byte[] identity = switch (request) {
                case com.nereusstream.delay.protocol.TargetMigrationSettlementRequest value -> Bytes.concat(value.migrationId(),
                        value.planDigest(), value.messageId().bytes(), Bytes.u32be(Integer.toUnsignedLong(value.originalGeneration())),
                        Bytes.u32be(value.guardKind().wireValue()), value.expectedGuardDigest());
                case com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest value -> Bytes.concat(value.migrationId(),
                        value.planDigest(), value.activation().canonicalBytes());
                case com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest value -> Bytes.concat(value.migrationId(),
                        value.planDigest(), value.intentDigest(), value.intentAt().canonicalBytes());
                case com.nereusstream.delay.protocol.TargetMigrationArchiveRequest value -> value.canonicalBytes();
                default -> throw new IllegalArgumentException("live custody cannot activate a migration");
            };
            return Bytes.sha256(Bytes.utf8("nereus-delay-live-migration-prepared-v1\0"), Bytes.u32be(request.controlKind()), identity);
        }
    }

    /** Actual durable original operation handle. close releases a local loan, never persistent UNKNOWN. */
    public interface PreparedCustody extends CheckpointInputFuture.Cleanup, AutoCloseable {
        TargetMigrationControlMutationFactory.Prepared exact();
        void requireBound(PreparedRequest request);
        void requireCurrent();
        long retainedBytes();
        boolean ioExited();
        void requestStop();
        /** Actual first/POS and Native67/71 have been checked; this does not authorize physical deletion. */
        void observedApplied(com.nereusstream.delay.runtime.SystemMutationResult first);
        @Override void close();
    }

    /** Data borrowed from the same original opening and SDK; construction is not a grant. */
    public record Request(TargetMigrationSourceOpening opening,
            TargetMigrationControlMutationFactory.Prepared exact,
            TargetScopeExposureSource.CopyContract copies, SourcePosition source,
            TargetScopeDeploymentAllocation.RegistryHandle registry) {
        public Request {
            Objects.requireNonNull(opening); Objects.requireNonNull(exact);
            Objects.requireNonNull(copies); Objects.requireNonNull(source); Objects.requireNonNull(registry);
            final var body = TargetMigrationControlBody.decode(exact.mutation().canonicalBody());
            body.requireInput(exact.mutation());
            if (!body.request().scope().equals(opening.configuration().scope())
                    || !source.sameSourceIdentity(opening.baseline())
                    || AuthorIdentity.decode(exact.mutation().authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL) {
                throw new IllegalArgumentException("migration management changed its original CONTROL/Source binding");
            }
        }
    }

    /**
     * Opaque original-owner loan, not a balance DTO. requireBound/current and claimNative are pure local
     * checks of the same actual debit/revocation/ownership token, with no I/O in an SDK lock. The original
     * token protects the whole native ownership/retry interval, not an instantaneous clock sample.
     * Closing returns only this local loan: ACK/native exit never refund its persistent physical share,
     * settle another UNKNOWN operation, retire the old writer domain or release the old custody root.
     */
    public interface Debit extends CheckpointInputFuture.Cleanup, AutoCloseable {
        void requireBound(Request exact);
        void requireCurrent();
        void claimNative(Request exact);
        /** Another real native attempt against the same durable operation; no fresh physical share. */
        default void claimRetry(Request exact) {
            throw new IllegalStateException("original Source owner has no remaining bounded exact-frame retry capability");
        }
        long retainedBytes();
        boolean ioExited();
        void requestStop();
        @Override CompletableFuture<Void> retryCleanup();
        @Override void close();
    }

    private final Request request;
    private final ManagementHandle source;
    private final TargetScopeDeploymentAllocation.RegistryHandle registry;
    private final TargetMigrationControlWriter writer;
    private final long maximumRetainedBytes;
    private final CompletableFuture<Void> cleaned = new CompletableFuture<>();
    private volatile Debit debit;
    private volatile CheckpointInputFuture.Cleanup failedAcquisition;
    private volatile boolean acquired;
    private volatile boolean closing;
    private boolean claimed;
    private volatile boolean applied;

    private TargetMigrationSourceExposure(Request request, ManagementHandle source,
            TargetMigrationControlWriter writer, long maximumRetainedBytes) {
        this.request = request; this.source = source; this.writer = writer;
        this.registry = request.registry();
        if (maximumRetainedBytes <= 0 || maximumRetainedBytes == Long.MAX_VALUE)
            throw new IllegalArgumentException("migration Source loan requires a finite retained-input bound");
        this.maximumRetainedBytes = maximumRetainedBytes;
    }

    /** Run on the original owned I/O path, before taking any producer/native handoff lock. */
    public static TargetMigrationSourceExposure acquire(TargetMigrationSourceOpening opening,
            TargetScopeDeploymentAllocation.SourceHandle original,
            TargetScopeDeploymentAllocation.RegistryHandle registry,
            TargetMigrationControlMutationFactory.Prepared exact, TargetMigrationControlWriter writer,
            long maximumRetainedBytes) {
        Objects.requireNonNull(opening).requireCurrent();
        Objects.requireNonNull(writer).requireBinding(opening);
        if (!(original instanceof ManagementHandle management)) {
            throw new IllegalStateException("original Source owner has no prepaid migration management capability");
        }
        final var request = new Request(opening, exact, writer.copyContract(), opening.baseline(), registry);
        final var held = new TargetMigrationSourceExposure(request, management, writer, maximumRetainedBytes);
        // Retain before the original provider may allocate, time out or return a late handle.
        opening.retainCleanup(held);
        try {
            management.requireLocalCurrent();
            registry.requireLocalCurrent();
            final var purpose = switch (exact.control().kind()) {
                case ACTIVATE_TARGET_MIGRATION -> TargetScopeDeploymentAllocation.ManagerPurpose.MIGRATION_ACTIVATE;
                case SETTLE_TARGET_MIGRATION_CUSTODY -> TargetScopeDeploymentAllocation.ManagerPurpose.MIGRATION_SETTLE;
                case RETIRE_TARGET_MIGRATION -> TargetScopeDeploymentAllocation.ManagerPurpose.MIGRATION_RETIRE_INTENT;
                case CONFIRM_TARGET_MIGRATION_RETIRED -> TargetScopeDeploymentAllocation.ManagerPurpose.MIGRATION_RETIRE_CONFIRM;
                case MIGRATE_TARGET_LOGICAL_FIRST_ARCHIVE -> TargetScopeDeploymentAllocation.ManagerPurpose.MIGRATION_ARCHIVE;
                default -> throw new IllegalArgumentException("unassigned migration management purpose");
            };
            final var actor = AuthorIdentity.decode(exact.mutation().authorIdentity());
            management.requireManagementPurpose(purpose, actor);
            registry.requireManagementPurpose(purpose, actor);
            Objects.requireNonNull(registry.readAuthenticated(), "original managed registration allocation");
            final var snapshot = Objects.requireNonNull(management.readAuthenticated(), "original Source allocation");
            if (!snapshot.source().sameSourceIdentity(request.source())
                    || exact.mutation().encodeFrame().length > snapshot.maximumSourceFrameBytes()) {
                throw new IllegalArgumentException("migration CONTROL exceeds its actual original Source binding/frame cap");
            }
            management.requireSdkContract(request.copies());
            final var body = TargetMigrationControlBody.decode(exact.mutation().canonicalBody());
            if (body.request() instanceof TargetMigrationActivateRequest activate
                    && !Bytes.constantTimeEquals(management.managementAuthorizationDigest(),
                            activate.plan().managementAuthorityDigest())) {
                throw new IllegalArgumentException("migration changed the actual independently held management grant");
            }
            management.requireMigrationManagement(request);
            held.debit = Objects.requireNonNull(management.reserveMigrationControl(request, opening::requireCurrent),
                    "actual original paid migration Source debit");
            if (held.debit.retainedBytes() < 0 || held.debit.retainedBytes() > maximumRetainedBytes) {
                throw new IllegalStateException("original migration Source loan exceeds its retained-input envelope");
            }
            held.debit.requireBound(request);
            held.acquired = true;
            held.requireCurrent();
            return held;
        } catch (RuntimeException | Error failure) {
            Throwable owned = failure;
            for (int depth = 0; depth < 16 && owned instanceof CompletionException
                    && owned.getCause() != null && owned.getCause() != owned; depth++) owned = owned.getCause();
            if (owned instanceof CheckpointInputFuture.Cleanup cleanup) held.failedAcquisition = cleanup;
            held.closing = true;
            throw failure;
        }
    }

    /** No remote reads or allocation; the original token remains held through actual SDK cleanup. */
    public void requireCurrent() {
        if (!acquired || closing) throw new IllegalStateException("original migration Source debit is unavailable");
        request.opening().requireLocalCurrent();
        source.requireLocalCurrent(); source.requireMigrationManagement(request);
        registry.requireLocalCurrent();
        source.requireSdkContract(request.copies());
        if (debit.retainedBytes() < 0 || debit.retainedBytes() > maximumRetainedBytes)
            throw new IllegalStateException("original migration Source loan outgrew its retained-input envelope");
        debit.requireBound(request); debit.requireCurrent();
    }

    /** Called only by the same private driver's permit at the actual SDK handoff. */
    public synchronized void claimNative(TargetMigrationControlWriter actual,
            TargetMigrationControlMutationFactory.Prepared exact, TargetScopeExposureSource.CopyContract copies) {
        requireCurrent();
        if (claimed || actual != writer || exact != request.exact() || !request.copies().equals(copies)) {
            throw new IllegalStateException("migration Source debit cannot change frame/producer/copies or be reused");
        }
        debit.claimNative(request);
        claimed = true;
    }

    /** A new same-byte SDK invocation consumes another actual original-owner attempt, including after UNKNOWN. */
    synchronized void claimRetry(TargetMigrationControlWriter actual,
            TargetMigrationControlMutationFactory.Prepared exact, TargetScopeExposureSource.CopyContract copies) {
        requireCurrent();
        if (!claimed || actual != writer || exact != request.exact() || !request.copies().equals(copies)
                || !writer.operationsExited())
            throw new IllegalStateException("migration retry changed original frame/copies or prior native I/O remains live");
        debit.claimRetry(request);
    }

    public long retainedBytes() { return debit == null ? 0 : debit.retainedBytes(); }

    /** The live caller has independently checked exact first/POS/Native before disabling this permit. */
    void observedApplied(TargetMigrationControlMutationFactory.Prepared exact,
            com.nereusstream.delay.runtime.SystemMutationResult first) {
        if (exact != request.exact()
                || !java.util.Arrays.equals(first.mutationId(), exact.mutation().systemMutationId())
                || !java.util.Arrays.equals(first.mutationHash(), exact.mutation().mutationHash())
                || first.applyStatus() != com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                || first.stableCode() != com.nereusstream.delay.protocol.StableCode.OK)
            throw new IllegalArgumentException("migration Source loan release changed its actual applied frame");
        applied = true; closing = true;
    }

    @Override public void close() { closing = true; }

    @Override public boolean cleanupComplete() { return cleaned.isDone(); }

    @Override public CompletableFuture<Void> retryCleanup() {
        if (cleaned.isDone()) return cleaned;
        // This is only the same actual SDK owner's terminal fact, never a noFuture/business conclusion.
        if (!writer.operationsExited() || !applied && (!writer.backend().resourcesTerminated()
                || !writer.backend().nativeResourcesTerminated())) return cleaned;
        closing = true;
        final var failed = failedAcquisition;
        if (failed != null) {
            failed.retryCleanup();
            if (!failed.cleanupComplete()) return cleaned;
        }
        final var originalIo = source.inFlight();
        if (originalIo != null && !originalIo.isDone()) return cleaned;
        final var registryIo = registry.inFlight();
        if (registryIo != null && !registryIo.isDone()) return cleaned;
        final var original = debit;
        if (original != null) {
            original.requestStop(); original.close(); original.retryCleanup();
            if (!original.ioExited() || !original.cleanupComplete()) return cleaned;
        }
        cleaned.complete(null);
        return cleaned;
    }
}
