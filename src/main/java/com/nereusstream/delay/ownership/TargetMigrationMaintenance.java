package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRequest;
import com.nereusstream.delay.protocol.TargetMigrationArchiveState;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest;
import com.nereusstream.delay.protocol.TargetMigrationRetirementInventory;
import com.nereusstream.delay.protocol.TargetMigrationRetirementResourceReceipt;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetMigrationSettlementRequest;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetMigrationArchivePreparation;
import com.nereusstream.delay.runtime.TargetMigrationArchiveStore;
import com.nereusstream.delay.runtime.TargetMigrationRetirementStore;
import com.nereusstream.delay.runtime.TargetMigrationStore;
import com.nereusstream.delay.runtime.TargetSystemReplayStore;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetMigrationArchiveCustody;
import com.nereusstream.delay.store.TargetMigrationArchiveHeld;
import com.nereusstream.delay.store.TargetMigrationArchiveReceived;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Live F2 caller. Every durable transition remains an original Source input applied by the ordinary Source loop. */
public final class TargetMigrationMaintenance {
    public enum State {
        OPENING,
        SETTLEMENTS,
        PREPARED,
        REGISTER,
        APPEND,
        APPLY,
        FRAME_CLOSE,
        INVENTORY,
        CATALOG,
        ARCHIVE_OPEN,
        ARCHIVE_SCAN,
        ARCHIVE_COPY,
        ARCHIVE_AUDIT,
        ARCHIVE_TAKEOVER,
        ARCHIVE_EXIT,
        RESOURCES,
        CONFIRM,
        ADOPT,
        CLOSING,
        COMPLETE,
        STOPPED
    }

    public record Turn(State state, boolean blocked, Throwable failure) {}

    /**
     * Minted only by the actual Process/Startup root after registering this maintenance and reserving
     * its finite inputs/SDKs. The live checks bind the same actual Worker, Owner, Source, Native Store,
     * WorkClass registry and local token. They do not relax the pre-A or Source assembly gates.
     */
    interface Admission {
        TargetWorkerProcessRuntime.Assembly graph();

        TargetWorkerSourceStartup startup();

        TargetWorkerShardRuntime worker();

        TargetStoreBackend backend();

        TargetStoreBackend.ReadAuthority reads();

        TargetDestinationSdkBudget sdkBudget();

        SystemWriterAppendBudget systemWriters();

        OwnerIdentity ownerIdentity();

        LongSupplier clock();

        void requireCurrent(TargetMigrationMaintenance actual);

        void requireLocalCurrent(TargetMigrationMaintenance actual);

        Construction beginConstruction(TargetMigrationMaintenance actual);

        TargetMigrationArchivePreparation.Held openArchive(
                TargetMigrationMaintenance actual,
                Opening opening,
                TargetMigrationRetirementStore.AppliedIntent intent,
                TargetMigrationArchivePreparation.Limits limits,
                BoundedReadBudget cumulative);

        boolean exitArchive(TargetMigrationMaintenance actual, TargetMigrationArchiveStore.AppliedTakeover takeover);

        boolean archiveActive(TargetMigrationMaintenance actual, TargetMigrationArchiveStore.AppliedTakeover takeover);

        long reservedBytes();

        void release(TargetMigrationMaintenance actual);
    }

    interface Construction extends AutoCloseable {
        @Override
        void close();
    }

    private final Admission admission;
    private final TargetMigrationMaintenanceConfiguration configuration;
    private final TargetMigrationMaintenanceInputs.Provider provider;
    private final Function<TargetMigrationSourceOpening, TargetMigrationTransport> transportFactory;
    private final LongSupplier nanos;
    private final TargetMigrationMaintenanceWorkClassExecutor executor;
    private final Opening opening = new Opening(this);
    private final List<TargetOwnedBackendGroup> backends = new ArrayList<>();
    private final List<TargetWorkerRecoverySource> readers = new ArrayList<>();
    private final List<CheckpointInputFuture.Cleanup> partial = new ArrayList<>();
    private final List<TargetMigrationRetirementResourceReceipt> receipts = new ArrayList<>();
    private List<TargetMigrationRetirementInventory.Resource> resourceOrder = List.of();
    private volatile State state = State.OPENING;
    private volatile boolean stopping, exited, blocked;
    private volatile Throwable failure;
    private volatile TargetMigrationMaintenanceInputs inputs;
    private TargetMigrationRoot root;
    private TargetMigrationPlan plan;
    private SourcePosition baseline;
    private volatile TargetMigrationTransport transport;
    private volatile TargetMigrationControlWriter writer;
    private TargetMigrationMaintenanceInputs.Page page;
    private int settlementIndex, resourceIndex;
    private byte[] settlementCursor;
    private TargetMigrationRequest request;
    private State afterFrame;
    private volatile TargetMigrationSourceExposure.PreparedCustody custody;
    private TargetMigrationControlMutationFactory.Prepared exact;
    private volatile TargetMigrationSourceExposure exposure;
    private volatile AppendPermit permit;
    private SystemMutationResult first;
    private TargetMigrationRetirementStore.AppliedIntent intent;
    private volatile TargetMigrationRetirementStore.AppliedConfirmation confirmed;
    private TargetMigrationMaintenanceInputs.Barrier barrier;
    private volatile TargetMigrationMaintenanceInputs.ResourceAction resource;
    private volatile TargetMigrationArchiveCustody archiveCustody;
    private TargetMigrationMaintenanceInputs.Archive archiveInputs;
    private TargetMigrationArchivePreparation.Held archiveHold;
    private TargetMigrationArchivePreparation.Cursor archiveScan;
    private TargetMigrationArchiveHeld.Cursor archiveAudit;
    private TargetMigrationArchiveState archiveHead;
    private TargetMigrationArchivePreparation.PreparedBatch archiveCopy;
    private TargetMigrationArchivePreparation.PreparedTakeover archiveTakeover;
    private TargetMigrationArchiveReceived archiveReceived;
    private TargetMigrationArchiveStore.AppliedTakeover confirmedArchive;

    private record ArchiveSourceProof(
            TargetMigrationControlMutationFactory.Prepared exact,
            TargetMigrationArchivePreparation.PreparedBatch copy,
            TargetMigrationArchivePreparation.PreparedTakeover takeover,
            TargetMigrationArchiveReceived received) {}

    private volatile ArchiveSourceProof archiveSourceProof;
    private TargetMigrationRetirementResourceReceipt resourceReceipt;
    private OwnedCheckpointIo<Boolean> cleanup;
    private boolean legacyReleased, reservationReleased;
    private boolean retirementCompleted;
    private long operationStarted;
    private int attempts;

    TargetMigrationMaintenance(
            Admission admission,
            TargetMigrationMaintenanceConfiguration configuration,
            TargetMigrationMaintenanceInputs.Provider provider,
            Function<TargetMigrationSourceOpening, TargetMigrationTransport> transportFactory,
            LongSupplier nanos) {
        this.admission = Objects.requireNonNull(admission);
        this.configuration = Objects.requireNonNull(configuration);
        this.provider = Objects.requireNonNull(provider);
        this.transportFactory = Objects.requireNonNull(transportFactory);
        this.nanos = Objects.requireNonNull(nanos);
        if (admission.reservedBytes() != configuration.maximumRetainedBytes()) {
            throw new IllegalArgumentException("live migration changed its actual Process reservation");
        }
        if (configuration.migration().taskBytes() < admission.graph().config().maximumSourceApplyTaskBytes()) {
            throw new IllegalArgumentException("live migration omitted its original decoded Source I/O window");
        }
        admission.backend().requireStore(admission.startup().store());
        admission.startup().requireAdmittedWorker(admission.worker());
        admission
                .sdkBudget()
                .requireWorker(admission.graph().classes(), admission.graph().resources());
        executor = new TargetMigrationMaintenanceWorkClassExecutor(
                admission.graph(),
                "target-migration-maintenance-"
                        + Bytes.hex(configuration.migration().migrationId()),
                configuration.migration().taskBytes());
    }

    public TargetMigrationMaintenanceConfiguration configuration() {
        return configuration;
    }

    public State state() {
        return state;
    }

    public boolean ioExited() {
        return exited;
    }

    public Optional<TargetMigrationRetirementStore.AppliedConfirmation> confirmation() {
        return Optional.ofNullable(confirmed);
    }

    public Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    /** Called by the ordinary Host maintenance tick; the original registry runs at most one bounded I/O step. */
    public synchronized Turn runTurn() {
        if (exited) {
            return new Turn(state, blocked, failure);
        }
        if (stopping || state == State.CLOSING) {
            retryCleanup();
            return new Turn(state, blocked, failure);
        }
        try {
            if (!executor.idle()) {
                return new Turn(state, blocked, failure);
            }
            requireLocalCurrent();
            executor.submit(
                    () -> {
                        try {
                            blocked = false;
                            failure = null;
                            step();
                        } catch (TargetMigrationMaintenanceInputs.StaleCursor stale) {
                            if (state != State.SETTLEMENTS || custody != null) {
                                throw stale;
                            }
                            page = null;
                            settlementCursor = null;
                            settlementIndex = 0;
                            blocked = true;
                            failure = stale;
                        } catch (ReadIncompleteException unavailable) {
                            blocked = true;
                            failure = unavailable;
                        } catch (RuntimeException | Error rejected) {
                            retain(rejected);
                            stopping = true;
                            throw rejected;
                        }
                    },
                    this::requireCurrent);
        } catch (RuntimeException | Error rejected) {
            retain(rejected);
            requestStop();
        }
        return new Turn(state, blocked, failure);
    }

    private void step() {
        requireCurrent();
        switch (state) {
            case OPENING -> openInputs();
            case SETTLEMENTS -> nextSettlement();
            case INVENTORY -> prepareInventory();
            case PREPARED -> prepareExact();
            case REGISTER -> register();
            case APPEND, APPLY -> appendOrObserve();
            case FRAME_CLOSE -> closeFrame();
            case CATALOG -> {
                intent.requireCurrent();
                barrier = Objects.requireNonNull(inputs.retirement().closeAdmissions(intent, this::requireCurrent));
                barrier.requireIntent(intent);
                barrier.requireCurrent();
                resourceOrder = new ArrayList<>(intent.intention().inventory().resources());
                resourceOrder.sort(java.util.Comparator.comparingInt(TargetMigrationMaintenance::resourcePhase)
                        .thenComparing(value -> Bytes.hex(value.id())));
                state = State.ARCHIVE_OPEN;
            }
            case ARCHIVE_OPEN -> openArchive();
            case ARCHIVE_SCAN -> {
                if (!archiveScan.complete()) {
                    archiveScan.advance();
                }
                if (archiveScan.complete()) {
                    state = State.ARCHIVE_COPY;
                }
            }
            case ARCHIVE_COPY -> prepareArchiveCopy();
            case ARCHIVE_AUDIT -> auditArchive();
            case ARCHIVE_TAKEOVER -> prepareArchiveTakeover();
            case ARCHIVE_EXIT -> exitArchive();
            case RESOURCES -> advanceResources();
            case CONFIRM -> prepareConfirmation();
            case ADOPT -> {
                confirmed.requireCurrent();
                barrier.requireIntent(confirmed.intent());
                barrier.requireCurrent();
                barrier.requireDrained();
                inputs.retirement().adoptConfirmed(confirmed, barrier, this::requireCurrent);
                requireCurrent();
                inputs.retirement().releaseLegacySubLoan(confirmed, this::requireCurrent);
                legacyReleased = true;
                retirementCompleted = true;
                state = State.CLOSING;
            }
            default -> throw new IllegalStateException("live migration has no admitted step in " + state);
        }
        requireRetainedBound();
    }

    private void openInputs() {
        final var acquired =
                Objects.requireNonNull(provider.open(opening), "actual original legacy maintenance sub-loan");
        if (inputs == null || inputs != acquired) {
            throw new IllegalStateException(
                    "maintenance provider did not retain its actual sub-loan before acquisition");
        }
        inputs.requireCurrent();
        final var c = configuration.migration();
        final var nativeRoot = Objects.requireNonNull(
                TargetMigrationStore.readNative(admission.startup().store(), c.newReadBudget(nanos)),
                "actual original migration root");
        root = admission
                .backend()
                .guardedRead(
                        c.newReadBudget(nanos),
                        reader -> TargetMigrationStore.requireActivated(
                                reader,
                                List.of(),
                                c.scope(),
                                nativeRoot.plan().newLineage(),
                                reader.aggregate().mutation()),
                        admission.reads());
        plan = root.plan();
        if (!plan.scope().equals(c.scope()) || !Arrays.equals(plan.migrationId(), c.migrationId())) {
            throw new IllegalArgumentException("live maintenance changed its actual activated migration/Store");
        }
        baseline = Objects.requireNonNull(admission.startup().store().appliedShardLogPosition());
        intent = readIntent();
        confirmed = readConfirmation();
        state = confirmed != null ? State.CATALOG : intent != null ? State.CATALOG : State.SETTLEMENTS;
    }

    private void nextSettlement() {
        if (page == null) {
            page = Objects.requireNonNull(inputs.settlements(
                    settlementCursor, configuration.migration().newReadBudget(nanos), this::requireCurrent));
            if (page.settlements().size() > configuration.migration().maximumRecordsPerTurn()
                    || page.retainedBytes() > configuration.migration().maximumBytesPerTurn()) {
                throw new IllegalStateException("original settlement census exceeded its finite read page");
            }
            if (!page.complete() && Arrays.equals(settlementCursor, page.cursor())) {
                throw new IllegalStateException("incomplete original settlement census made no bounded progress");
            }
            settlementIndex = 0;
        }
        if (settlementIndex == page.settlements().size()) {
            settlementCursor = page.cursor();
            final boolean complete = page.complete();
            page = null;
            if (complete) {
                state = State.INVENTORY;
            }
            return;
        }
        final var selected = page.settlements().get(settlementIndex);
        requirePlan(selected.request());
        inputs.authenticateSettlement(selected, root, this::requireCurrent);
        final var guard = admission
                .backend()
                .guardedRead(
                        configuration.migration().newReadBudget(nanos),
                        reader -> {
                            final var actual = TargetMigrationStore.requireActivated(
                                    reader,
                                    List.of(),
                                    plan.scope(),
                                    plan.newLineage(),
                                    reader.aggregate().mutation());
                            final byte[] raw = Objects.requireNonNull(
                                    reader.get(
                                            ColumnFamily.META,
                                            TargetMigrationCustodyGuard.keyFor(
                                                    selected.request().messageId())),
                                    "original Native67 custody guard");
                            final var value = TargetMigrationCustodyGuard.decode(
                                    TargetValueEnvelope.decode(raw, TargetMigrationCustodyGuard.VALUE_TYPE)
                                            .payload());
                            TargetMigrationStore.requireGuard(actual.plan(), value);
                            if (value.settled()) {
                                final var body = TargetMigrationControlBody.decode(
                                        value.settlement().canonicalBody());
                                if (!body.request().equals(selected.request())) {
                                    throw new IllegalStateException(
                                            "settlement census changed the original already settled ID");
                                }
                                TargetMigrationStore.requireApplied(
                                        reader,
                                        List.of(),
                                        actual.owner(),
                                        value.settlement(),
                                        value.settledAt(),
                                        reader.aggregate().mutation());
                            } else if (!Arrays.equals(
                                    value.digest(), selected.request().expectedGuardDigest())) {
                                throw new IllegalStateException("settlement changed original held Native67");
                            }
                            return value;
                        },
                        admission.reads());
        if (guard.settled()) {
            settlementIndex++;
            return;
        }
        select(selected.request(), State.SETTLEMENTS);
    }

    private void prepareInventory() {
        intent = readIntent();
        if (intent != null) {
            state = State.CATALOG;
            return;
        }
        final var inventory =
                inputs.retirementInventory(root, configuration.migration().newReadBudget(nanos), this::requireCurrent);
        if (inventory.isEmpty()) {
            // New original terminals can become eligible while whole-domain custody remains held.
            settlementCursor = null;
            page = null;
            state = State.SETTLEMENTS;
            blocked = true;
            return;
        }
        final var readers = Objects.requireNonNull(
                inputs.eligibleReaders(root, configuration.migration().newReadBudget(nanos), this::requireCurrent));
        select(
                new TargetMigrationRetirementIntentRequest(
                        plan.scope(),
                        plan.migrationId(),
                        plan.digest(),
                        root.activatedAt(),
                        inventory.orElseThrow(),
                        readers),
                State.CATALOG);
    }

    private void select(TargetMigrationRequest original, State next) {
        requirePlan(original);
        request = original;
        afterFrame = next;
        operationStarted = nanos.getAsLong();
        state = State.PREPARED;
    }

    private void requirePlan(TargetMigrationRequest original) {
        final byte[] migrationId;
        final byte[] digest;
        switch (original) {
            case TargetMigrationSettlementRequest value -> {
                migrationId = value.migrationId();
                digest = value.planDigest();
            }
            case TargetMigrationRetirementIntentRequest value -> {
                migrationId = value.migrationId();
                digest = value.planDigest();
            }
            case TargetMigrationRetirementConfirmRequest value -> {
                migrationId = value.migrationId();
                digest = value.planDigest();
            }
            case TargetMigrationArchiveRequest value -> {
                migrationId = value.migrationId();
                digest = value.planDigest();
            }
            default -> throw new IllegalArgumentException("live maintenance cannot activate a migration");
        }
        if (!original.scope().equals(plan.scope())
                || !Arrays.equals(migrationId, plan.migrationId())
                || !Arrays.equals(digest, plan.digest())) {
            throw new IllegalArgumentException("live operation changed its actual original migration plan");
        }
    }

    @SuppressWarnings("try") // Construction owns this scope and releases the original admission on every exit.
    private void prepareExact() {
        final var preparedRequest = new TargetMigrationSourceExposure.PreparedRequest(opening, request);
        final var control = Objects.requireNonNull(inputs.controlPreparation(request));
        // The original owner serializes this stable candidate with crash recovery BEFORE invoking any signer.
        if (custody == null) {
            custody = Objects.requireNonNull(TargetMigrationPreparedCustody.retainOrRecover(
                    inputs.originalManagementSource(),
                    preparedRequest,
                    () -> {
                        return control.factory()
                                .prepare(
                                        control.operationId(),
                                        request,
                                        control.authenticated(),
                                        control.exactScope(),
                                        control.registrationRetryUntil(),
                                        control.sourceRetryUntil());
                    },
                    this::requireCurrent));
            opening.retainCleanup(custody);
        }
        custody.requireBound(preparedRequest);
        custody.requireCurrent();
        exact = Objects.requireNonNull(custody.exact());
        final var body = TargetMigrationControlBody.decode(exact.mutation().canonicalBody());
        body.requireInput(exact.mutation());
        if (!body.request().equals(request)) {
            throw new IllegalStateException("durable Prepared recovery changed its exact candidate");
        }
        if (!Arrays.equals(exact.control().operationId(), control.operationId())) {
            throw new IllegalStateException("durable Prepared winner changed its original Control operation ID");
        }
        ControlOperationAuthorization.authorize(exact.control(), control.authenticated(), control.exactScope());
        TargetMigrationControlMutationFactory.Prepared.restore(exact.control(), exact.mutation());
        if (request instanceof TargetMigrationArchiveRequest) {
            archiveSourceProof = new ArchiveSourceProof(
                    exact, archiveCopy, archiveTakeover, Objects.requireNonNull(archiveReceived));
        }
        if (observeFirst()) {
            state = State.FRAME_CLOSE;
            return;
        }
        if (transport == null) {
            try (var ignored = admission.beginConstruction(this)) {
                transport = Objects.requireNonNull(transportFactory.apply(opening));
                transport.requireBinding(opening);
            }
        }
        if (!transport.advance()) {
            return;
        }
        final var actual = Objects.requireNonNull(transport.writer());
        actual.requireBinding(opening);
        if (writer != actual || !backends.contains(actual.backend())) {
            throw new IllegalStateException("live CONTROL writer was not retained before SDK exposure");
        }
        exposure = TargetMigrationSourceExposure.acquire(
                opening,
                inputs.originalManagementSource(),
                inputs.originalManagementRegistry(),
                exact,
                writer,
                Math.subtractExact(configuration.maximumRetainedBytes(), retainedBytes()));
        permit = new AppendPermit(this, exact, writer, exposure, false);
        attempts = 0;
        state = State.REGISTER;
    }

    private void register() {
        custody.requireCurrent();
        exposure.requireCurrent();
        inputs.registration().register(exact.control());
        inputs.registration()
                .validateMutation(exact.control(), exact.control().targets().getFirst(), exact.mutation());
        requireCurrent();
        state = State.APPEND;
    }

    private void appendOrObserve() {
        if (observeFirst()) {
            if (permit != null) {
                permit.disable();
            }
            state = State.FRAME_CLOSE;
            return;
        }
        // An incomplete/failed Native lookup above throws. Only an actual completed absence can reach the SDK.
        custody.requireCurrent();
        exposure.requireCurrent();
        final long elapsed = nanos.getAsLong() - operationStarted;
        if (elapsed < 0 || elapsed >= configuration.preparationTimeoutNanos()) {
            blocked = true;
            state = State.APPLY;
            return; // retain exact UNKNOWN; never resign with a fresh deadline
        }
        if (!writer.operationsExited()) {
            blocked = true;
            return;
        }
        if (attempts > 0) {
            permit.disable();
            permit = new AppendPermit(this, exact, writer, exposure, true);
        }
        try {
            writer.append(exact, permit);
        } catch (RuntimeException | Error uncertain) {
            if (!permit.exposed()) {
                throw uncertain;
            }
            failure = uncertain; // Native callback/response loss changes no Source fact.
        }
        if (permit.exposed()) {
            attempts = Math.incrementExact(attempts);
        }
        state = State.APPLY;
    }

    private boolean observeFirst() {
        first = new TargetSystemReplayStore(admission.backend(), plan.scope(), plan.newLineage(), 2, 1)
                .appliedResult(configuration.migration().newReadBudget(nanos), exact.mutation(), admission.reads())
                .orElse(null);
        if (first == null) {
            return false;
        }
        if (first.applyStatus() != ApplyStatus.APPLIED || first.stableCode() != StableCode.OK) {
            throw new IllegalStateException(
                    ("original migration Source input has a terminal non-success first; retain its "
                            + "original obligations"));
        }
        if (request instanceof TargetMigrationSettlementRequest settlement) {
            admission
                    .backend()
                    .guardedRead(
                            configuration.migration().newReadBudget(nanos),
                            reader -> {
                                final var actual = TargetMigrationStore.requireActivated(
                                        reader,
                                        List.of(),
                                        plan.scope(),
                                        plan.newLineage(),
                                        reader.aggregate().mutation());
                                final byte[] raw = Objects.requireNonNull(
                                        reader.get(
                                                ColumnFamily.META,
                                                TargetMigrationCustodyGuard.keyFor(settlement.messageId())),
                                        "actual Native67");
                                final var guard = TargetMigrationCustodyGuard.decode(
                                        TargetValueEnvelope.decode(raw, TargetMigrationCustodyGuard.VALUE_TYPE)
                                                .payload());
                                TargetMigrationStore.requireGuard(actual.plan(), guard);
                                if (!guard.settled()
                                        || !Arrays.equals(
                                                guard.settlement().canonicalEnvelope(),
                                                exact.mutation().canonicalEnvelope())
                                        || !Arrays.equals(
                                                guard.settledAt().source().canonicalBytes(),
                                                first.appliedSourcePosition())) {
                                    throw new IllegalStateException("settlement first lacks its exact actual Native67");
                                }
                                TargetMigrationStore.requireApplied(
                                        reader,
                                        List.of(),
                                        actual.owner(),
                                        exact.mutation(),
                                        guard.settledAt(),
                                        reader.aggregate().mutation());
                                return true;
                            },
                            admission.reads());
        } else if (request instanceof TargetMigrationRetirementIntentRequest) {
            intent = Objects.requireNonNull(readIntent(), "actual Native71 intent");
            if (!Arrays.equals(
                    intent.record().intentInput().canonicalEnvelope(),
                    exact.mutation().canonicalEnvelope())) {
                throw new IllegalStateException("retirement first changed actual original intent");
            }
        } else if (request instanceof TargetMigrationArchiveRequest archive) {
            admission
                    .backend()
                    .guardedRead(
                            configuration.migration().newReadBudget(nanos),
                            reader -> {
                                final byte[] raw = Objects.requireNonNull(
                                        reader.get(
                                                ColumnFamily.META,
                                                TargetMigrationArchiveState.historyKey(
                                                        plan.migrationId(),
                                                        TargetQuotaMutation.increment(archive.coveredThrough()
                                                                .sequence()))),
                                        "actual Native72 management history");
                                final var actual = TargetMigrationArchiveState.decode(
                                        TargetValueEnvelope.decode(raw, TargetMigrationArchiveState.VALUE_TYPE)
                                                .payload());
                                if (!actual.request().equals(archive)
                                        || !Arrays.equals(
                                                actual.input().canonicalEnvelope(),
                                                exact.mutation().canonicalEnvelope())
                                        || !Arrays.equals(
                                                actual.managementAt().source().canonicalBytes(),
                                                first.appliedSourcePosition())) {
                                    throw new IllegalStateException(
                                            "archive first lacks its exact actual Native72 management history");
                                }
                                TargetMigrationStore.requireApplied(
                                        reader,
                                        List.of(),
                                        root.owner(),
                                        exact.mutation(),
                                        actual.managementAt(),
                                        reader.aggregate().mutation());
                                return true;
                            },
                            admission.reads());
            archiveHead = readArchiveHead();
            if (archive.takeover()) {
                confirmedArchive = Objects.requireNonNull(readTakeover(), "actual Source archive takeover");
            }
        } else {
            confirmed = Objects.requireNonNull(readConfirmation(), "actual Native71 confirmation");
            if (!Arrays.equals(
                    confirmed.record().confirmationInput().canonicalEnvelope(),
                    exact.mutation().canonicalEnvelope())) {
                throw new IllegalStateException("confirmation first changed its actual original Native71");
            }
        }
        return true;
    }

    private void closeFrame() {
        if (permit != null) {
            permit.disable();
        }
        if (writer != null && !writer.operationsExited()) {
            return;
        }
        if (exposure != null) {
            exposure.observedApplied(exact, first);
            exposure.retryCleanup();
            if (!exposure.cleanupComplete()) {
                return;
            }
        }
        custody.observedApplied(first);
        custody.requestStop();
        custody.close();
        custody.retryCleanup();
        if (!custody.ioExited() || !custody.cleanupComplete()) {
            return;
        }
        final boolean settled = request instanceof TargetMigrationSettlementRequest;
        final boolean archived = request instanceof TargetMigrationArchiveRequest;
        custody = null;
        exact = null;
        exposure = null;
        permit = null;
        first = null;
        request = null;
        synchronized (this) {
            partial.removeIf(CheckpointInputFuture.Cleanup::cleanupComplete);
        }
        if (settled) {
            settlementIndex++;
        }
        if (archived) {
            archiveSourceProof = null;
            archiveCopy = null;
            archiveTakeover = null;
        }
        state = afterFrame;
    }

    private void openArchive() {
        intent.requireCurrent();
        barrier.requireCurrent();
        confirmedArchive = readTakeover();
        if (confirmedArchive != null) {
            state = State.ARCHIVE_EXIT;
            return;
        }
        archiveInputs = Objects.requireNonNull(inputs.archive(), "actual original archive/receiver acquisition");
        archiveInputs.requireCurrent();
        if (archiveHold == null) {
            archiveHold = Objects.requireNonNull(admission.openArchive(
                    this, opening, intent, archiveInputs.preparationLimits(), archiveInputs.preparationReads()));
        }
        if (archiveScan == null) {
            archiveScan = TargetMigrationArchivePreparation.begin(
                    archiveHold, archiveInputs.preparationLimits(), archiveInputs.preparationReads());
        }
        if (archiveCustody == null) {
            TargetMigrationArchiveCustody.open(
                    opening, intent, archiveInputs.objects(), archiveInputs.receiver(), archiveInputs.receiverLimits());
        }
        state = State.ARCHIVE_SCAN;
    }

    private TargetMigrationArchiveState readArchiveHead() {
        return TargetMigrationArchiveStore.readProgress(
                admission.backend(),
                plan.scope(),
                plan.newLineage(),
                plan,
                configuration.migration().newReadBudget(nanos),
                admission.reads());
    }

    private TargetMigrationArchiveStore.AppliedTakeover readTakeover() {
        return TargetMigrationArchiveStore.readAppliedTakeover(
                admission.backend(),
                plan.scope(),
                plan.newLineage(),
                plan,
                configuration.migration().newReadBudget(nanos),
                admission.reads());
    }

    private void prepareArchiveCopy() {
        archiveInputs.requireCurrent();
        archiveHold.requireCurrent();
        archiveHead = readArchiveHead();
        if (archiveHead != null && archiveHead.taken()) {
            confirmedArchive = Objects.requireNonNull(readTakeover());
            state = State.ARCHIVE_EXIT;
            return;
        }
        if (archiveHead != null && archiveHead.coverage().complete()) {
            archiveReceived = archiveCustody.receive(
                    TargetMigrationArchivePreparation.finalReceiveMaterial(archiveHold, archiveHead));
            archiveAudit = archiveCustody.audit(archiveReceived);
            opening.retainCleanup(archiveAudit);
            state = State.ARCHIVE_AUDIT;
            return;
        }
        final var material = archiveScan.prepareBatch(archiveHead);
        archiveReceived = archiveCustody.receive(material);
        archiveCopy = material.received(archiveReceived);
        select(archiveCopy.request(), State.ARCHIVE_COPY);
    }

    private void auditArchive() {
        archiveInputs.requireCurrent();
        final var complete = archiveAudit.advance(archiveInputs.receiverAuditReads());
        if (complete.isEmpty()) {
            return;
        }
        archiveReceived.requireCompleteAudit(complete.orElseThrow());
        state = State.ARCHIVE_TAKEOVER;
    }

    private void prepareArchiveTakeover() {
        archiveInputs.requireCurrent();
        final var actual = readArchiveHead();
        if (!Objects.equals(actual, archiveHead)) {
            throw new IllegalStateException(
                    "actual Source archive frontier changed during the protected complete receiving audit");
        }
        archiveTakeover = TargetMigrationArchivePreparation.takeover(archiveHold, actual, archiveReceived);
        select(archiveTakeover.request(), State.ARCHIVE_EXIT);
    }

    private void exitArchive() {
        if (confirmedArchive == null) {
            confirmedArchive = Objects.requireNonNull(readTakeover(), "actual Source T before original legacy exit");
        }
        confirmedArchive.requireCurrent();
        if (!admission.archiveActive(this, confirmedArchive)) {
            return;
        }
        if (archiveHold != null) {
            archiveHold.close();
            archiveHold = null;
            archiveScan = null;
        }
        if (!admission.exitArchive(this, confirmedArchive)) {
            return;
        }
        legacyReleased = true;
        state = confirmed == null ? State.RESOURCES : State.ADOPT;
    }

    private void advanceResources() {
        intent.requireCurrent();
        barrier.requireIntent(intent);
        barrier.requireCurrent();
        if (confirmed != null) {
            state = State.ADOPT;
            return;
        }
        final var all = resourceOrder;
        for (int count = 0; count < configuration.maximumResourcesPerTurn() && resourceIndex < all.size(); count++) {
            final var exactResource = all.get(resourceIndex);
            if (resource == null) {
                final var acquired = Objects.requireNonNull(inputs.retirement()
                        .openResource(opening, intent, barrier, exactResource, this::requireCurrent));
                if (resource != acquired) {
                    throw new IllegalStateException(
                            "resource owner did not retain its actual action before acquisition");
                }
            }
            resource.requireBound(intent, barrier, exactResource);
            resource.advance(this::requireCurrent);
            resourceReceipt = resource.terminalReceipt().orElse(null);
            if (resourceReceipt == null || !resource.ioExited()) {
                return;
            }
            resourceReceipt.requireResource(exactResource);
            resource.close();
            resource.retryCleanup();
            if (!resource.ioExited() || !resource.cleanupComplete()) {
                return;
            }
            receipts.add(resourceReceipt);
            resource = null;
            resourceReceipt = null;
            resourceIndex++;
            requireRetainedBound();
        }
        if (resourceIndex == all.size()) {
            state = State.CONFIRM;
        }
    }

    private void prepareConfirmation() {
        intent.requireCurrent();
        barrier.requireCurrent();
        barrier.requireDrained();
        receipts.sort(java.util.Comparator.comparing(value -> Bytes.hex(value.resourceId())));
        final var closure = inputs.retirement()
                .closure(
                        intent,
                        barrier,
                        List.copyOf(receipts),
                        configuration.migration().newReadBudget(nanos),
                        this::requireCurrent);
        final var confirmation = new TargetMigrationRetirementConfirmRequest(
                plan.scope(),
                plan.migrationId(),
                plan.digest(),
                intent.intention().digest(),
                intent.intentAt(),
                barrier.receipt(),
                List.copyOf(receipts),
                closure,
                Objects.requireNonNull(confirmedArchive, "actual Source T and old-only exit before R")
                        .takeoverAt());
        confirmation.requireIntent(intent.intention(), intent.intentAt());
        select(confirmation, State.ADOPT);
    }

    private static int resourcePhase(TargetMigrationRetirementInventory.Resource resource) {
        if (resource.kind() == TargetMigrationRetirementInventory.Kind.WRITER
                || resource.kind() == TargetMigrationRetirementInventory.Kind.READER
                || resource.kind() == TargetMigrationRetirementInventory.Kind.CHANNEL) {
            return 0;
        }
        if (resource.disposition() == TargetMigrationRetirementInventory.Disposition.TRANSFER
                || resource.disposition() == TargetMigrationRetirementInventory.Disposition.SHARED_RETAINED
                || resource.disposition() == TargetMigrationRetirementInventory.Disposition.PERMANENT) {
            return 1;
        }
        if (resource.kind() == TargetMigrationRetirementInventory.Kind.COUNTER) {
            return 3;
        }
        return 2;
    }

    private TargetMigrationRetirementStore.AppliedIntent readIntent() {
        return TargetMigrationRetirementStore.readAppliedIntent(
                admission.backend(),
                plan.scope(),
                plan.newLineage(),
                plan,
                configuration.migration().newReadBudget(nanos),
                admission.reads());
    }

    private TargetMigrationRetirementStore.AppliedConfirmation readConfirmation() {
        return TargetMigrationRetirementStore.readAppliedConfirmation(
                admission.backend(),
                plan.scope(),
                plan.newLineage(),
                plan,
                configuration.migration().newReadBudget(nanos),
                admission.reads());
    }

    /** Source-apply joins consume the same retained original owner authorities; no success callbacks are accepted. */
    TargetMigrationStore.Authority sourceAuthority() {
        requireLocalCurrent();
        return Objects.requireNonNull(inputs.sourceAuthority());
    }

    TargetMigrationCustodyGuard.SettlementAuthority settlementAuthority() {
        requireLocalCurrent();
        return Objects.requireNonNull(inputs.settlementAuthority());
    }

    TargetMigrationRetirementStore.OriginalInputs retirementAuthority() {
        requireLocalCurrent();
        return Objects.requireNonNull(inputs.retirementAuthority());
    }

    TargetSourceAuthorityLoader.HeldInputs loadSource(
            SourceReplayMutation original,
            TargetMigrationMaintenanceInputs.Provider expectedProvider,
            Runnable current) {
        if (provider != expectedProvider) {
            throw new IllegalArgumentException("archive Source load changed its original material provider");
        }
        final var held = Objects.requireNonNull(provider.loadSource(original, current));
        final var proof = archiveSourceProof;
        if (proof == null
                || !Arrays.equals(
                        proof.exact().mutation().canonicalEnvelope(),
                        original.mutation().canonicalEnvelope())) {
            return held;
        }
        try {
            requireLocalCurrent();
            if (!(held.inputs() instanceof TargetSourceAuthorityLoader.Migration material)
                    || !material.prepared().equals(proof.exact().control())
                    || material.archive() == null) {
                throw new IllegalStateException(
                        "original archive Source material changed its exact Prepared/payment acquisition");
            }
            return new TargetMigrationArchiveSourceInputs(
                    held, material, proof.copy(), proof.takeover(), proof.received());
        } catch (RuntimeException | Error rejected) {
            held.close();
            throw rejected;
        }
    }

    private void requireCurrent() {
        admission.requireCurrent(this);
        if (stopping || exited) {
            throw new IllegalStateException("live original migration maintenance is stopped");
        }
        if (inputs != null) {
            inputs.requireCurrent();
        }
    }

    private void requireLocalCurrent() {
        admission.requireLocalCurrent(this);
        if (stopping || exited) {
            throw new IllegalStateException("live original migration maintenance is stopped");
        }
        if (inputs != null) {
            inputs.requireLocalCurrent();
        }
    }

    private synchronized void retain(Throwable rejected) {
        if (rejected instanceof CheckpointInputFuture.Cleanup original && !partial.contains(original)) {
            opening.retainCleanup(original);
        }
        if (failure == null) {
            failure = rejected;
        } else if (failure != rejected && failure.getSuppressed().length < 16) {
            failure.addSuppressed(rejected);
        }
        blocked = true;
    }

    private void requireRetainedBound() {
        final long bytes = retainedBytes();
        if (bytes < 0 || bytes > configuration.maximumRetainedBytes()) {
            throw new IllegalStateException("live migration outgrew its original retained-input reservation");
        }
    }

    private long retainedBytes() {
        long bytes = inputs == null ? 0 : inputs.retainedBytes();
        if (root != null) {
            bytes = Math.addExact(bytes, root.canonicalBytes().length);
        }
        if (intent != null) {
            bytes = Math.addExact(bytes, intent.record().canonicalBytes().length);
        }
        if (confirmed != null && (intent == null || confirmed.record() != intent.record())) {
            bytes = Math.addExact(bytes, confirmed.record().canonicalBytes().length);
        }
        if (page != null) {
            bytes = Math.addExact(bytes, page.retainedBytes());
        }
        if (custody != null) {
            bytes = Math.addExact(bytes, custody.retainedBytes());
        }
        if (exposure != null) {
            bytes = Math.addExact(bytes, exposure.retainedBytes());
        }
        if (request != null) {
            bytes = Math.addExact(bytes, request.canonicalBytes().length);
        }
        if (resource != null) {
            bytes = Math.addExact(bytes, resource.retainedBytes());
        }
        if (archiveCustody != null) {
            bytes = Math.addExact(bytes, archiveCustody.retainedBytes());
        }
        if (archiveScan != null) {
            bytes = Math.addExact(bytes, archiveScan.retainedBytes());
        }
        if (archiveAudit != null) {
            bytes = Math.addExact(bytes, archiveAudit.retainedBytes());
        }
        if (archiveCopy != null) {
            bytes = Math.addExact(bytes, archiveCopy.retainedBytes());
        }
        if (archiveTakeover != null) {
            bytes = Math.addExact(bytes, archiveTakeover.retainedBytes());
        }
        if (archiveHead != null) {
            bytes = Math.addExact(bytes, archiveHead.canonicalBytes().length);
        }
        for (var receipt : receipts) {
            bytes = Math.addExact(bytes, receipt.canonicalBytes().length);
        }
        return bytes;
    }

    public void requestStop() {
        stopping = true;
        executor.requestStop();
        final var actualPermit = permit;
        if (actualPermit != null) {
            actualPermit.disable();
        }
        final var actualResource = resource;
        if (actualResource != null) {
            actualResource.requestStop();
        }
        final var actualWriter = writer;
        if (actualWriter != null) {
            actualWriter.requestStop();
        }
        final var actualTransport = transport;
        if (actualTransport != null) {
            actualTransport.requestStop();
        }
        final var actualCustody = custody;
        if (actualCustody != null) {
            actualCustody.requestStop();
        }
        final var actualInputs = inputs;
        if (actualInputs != null) {
            actualInputs.requestStop();
        }
    }
    /** Normal daily maintenance completion closes its own SDK/loans here, before Host final shutdown. */
    public synchronized boolean retryCleanup() {
        if (exited) {
            return true;
        }
        if (!stopping && state != State.CLOSING) {
            return false;
        }
        executor.requestStop();
        if (!executor.ioExited()) {
            return false;
        }
        executor.clearExited();
        if (cleanup != null) {
            cleanup.retryCleanup();
            if (!cleanup.operationExited() || !cleanup.ioExited()) {
                return false;
            }
            if (!cleanup.completedValue().orElse(false)) {
                final var rejected = CheckpointInputFuture.ownedFailure(cleanup.receipt());
                if (rejected != null) {
                    retain(rejected);
                }
            } else {
                exited = true;
                state = retirementCompleted && !stopping ? State.COMPLETE : State.STOPPED;
                return true;
            }
            cleanup = null;
        }
        try {
            cleanup = OwnedCheckpointIo.submit(
                    ignored -> closeOriginals(), () -> {}, admission.graph().io());
        } catch (java.util.concurrent.RejectedExecutionException full) {
            if (admission.graph().io().isShutdown()) {
                retain(full);
            }
        }
        return false;
    }

    private boolean closeOriginals() {
        if (permit != null) {
            permit.disable();
        }
        if (archiveHold != null) {
            archiveHold.close();
            archiveHold = null;
            archiveScan = null;
        }
        if (resource != null) {
            resource.requestStop();
            resource.close();
            resource.retryCleanup();
            if (!resource.ioExited() || !resource.cleanupComplete()) {
                return false;
            }
        }
        if (transport != null) {
            transport.requestStop();
        }
        if (writer != null) {
            writer.requestStop();
        }
        for (var reader : readers) {
            reader.close();
        }
        for (var backend : backends) {
            backend.freezeRegistrations();
            backend.closeBeforeHostBinding();
        }
        for (var backend : backends) {
            if (!backend.resourcesTerminated() || !backend.nativeResourcesTerminated()) {
                return false;
            }
        }
        if (transport != null && (!transport.ioExited() || !transport.cleanupComplete())) {
            return false;
        }
        if (writer != null && (!writer.operationsExited() || !writer.cleanupComplete())) {
            return false;
        }
        if (custody != null) {
            custody.requestStop();
            custody.close();
            custody.retryCleanup();
            if (!custody.ioExited() || !custody.cleanupComplete()) {
                return false;
            }
        }
        if (archiveAudit != null) {
            archiveAudit.close();
            archiveAudit.retryCleanup();
            if (!archiveAudit.ioExited() || !archiveAudit.cleanupComplete()) {
                return false;
            }
        }
        if (archiveCustody != null) {
            archiveCustody.requestStop();
            archiveCustody.close();
            archiveCustody.retryCleanup();
            if (!archiveCustody.ioExited() || !archiveCustody.cleanupComplete()) {
                return false;
            }
        }
        if (inputs != null) {
            inputs.requestStop();
            inputs.close();
            inputs.retryCleanup();
            if (!inputs.ioExited() || !inputs.cleanupComplete()) {
                return false;
            }
        }
        for (var original : partial) {
            original.retryCleanup();
            if (!original.cleanupComplete()) {
                return false;
            }
        }
        if (!reservationReleased) {
            // Durable exact frames, Native facts and original owner histories survive independently. Drop this
            // caller's completed decoded pages/SDK handles before returning its original retained-input share.
            page = null;
            request = null;
            custody = null;
            exposure = null;
            permit = null;
            first = null;
            root = null;
            intent = null;
            confirmed = null;
            barrier = null;
            resource = null;
            resourceReceipt = null;
            archiveCustody = null;
            archiveInputs = null;
            archiveAudit = null;
            archiveHead = null;
            archiveCopy = null;
            archiveTakeover = null;
            archiveReceived = null;
            confirmedArchive = null;
            archiveSourceProof = null;
            resourceOrder = List.of();
            receipts.clear();
            readers.clear();
            backends.clear();
            partial.clear();
            transport = null;
            writer = null;
            inputs = null;
            settlementCursor = null;
            admission.release(this);
            reservationReleased = true;
        }
        return true;
    }

    /** Only the registered live caller can mint this SDK acquisition capability. */
    public static final class Opening implements TargetMigrationSourceOpening {
        private final TargetMigrationMaintenance owner;

        private Opening(TargetMigrationMaintenance owner) {
            this.owner = owner;
        }

        @Override
        public TargetWorkerProcessRuntime.Assembly graph() {
            return owner.admission.graph();
        }

        @Override
        public TargetMigrationConfiguration configuration() {
            return owner.configuration.migration();
        }

        @Override
        public void requireCurrent() {
            owner.requireCurrent();
        }

        @Override
        public void requireLocalCurrent() {
            owner.requireLocalCurrent();
        }

        @Override
        public SourceAssignment assignment() {
            return owner.admission.startup().acceptedAssignment();
        }

        @Override
        public OwnerLease ownerLease() {
            return owner.admission.startup().ownerLease();
        }

        @Override
        public OwnerIdentity ownerIdentity() {
            return owner.admission.ownerIdentity();
        }

        @Override
        public SourcePosition baseline() {
            return Objects.requireNonNull(owner.baseline);
        }

        @Override
        public TargetWorkerSourceStartup.FrameDecodeLoan acquireFrameDecode(
                SourcePosition actual, long bytes, Long generation, byte[] attestation) {
            owner.requireLocalCurrent();
            return owner.admission.startup().acquireFrameDecode(actual, bytes, generation, attestation);
        }

        @Override
        public TargetWorkerSourceRuntimeFactory.SourceFrameContract sourceFrames() {
            owner.requireLocalCurrent();
            return owner.admission.startup().sourceFrameContract();
        }

        @Override
        public TargetDestinationSdkBudget sdkBudget() {
            return owner.admission.sdkBudget();
        }

        @Override
        public SystemWriterAppendBudget systemWriterBudget() {
            return owner.admission.systemWriters();
        }

        public void retainInputs(TargetMigrationMaintenanceInputs actual) {
            synchronized (owner) {
                requireCurrent();
                if (owner.inputs != null && owner.inputs != actual) {
                    throw new IllegalStateException("live migration changed original input sub-loan");
                }
                owner.inputs = Objects.requireNonNull(actual);
            }
        }

        public long maximumRetainedInputBytes() {
            return owner.configuration.maximumRetainedBytes();
        }

        long remainingPreparedBytes() {
            return Math.subtractExact(maximumRetainedInputBytes(), owner.retainedBytes());
        }

        void requireDedicatedRetirementComponent(TargetOwnedBackendGroup actual) {
            synchronized (owner) {
                if (actual == owner.admission.worker().sourceBackendGroup() || owner.backends.contains(actual)) {
                    throw new IllegalArgumentException(
                            "old-domain retirement cannot close the actual current Source or maintenance SDK group");
                }
            }
        }

        public void retainResourceAction(TargetMigrationMaintenanceInputs.ResourceAction actual) {
            synchronized (owner) {
                requireCurrent();
                if (owner.resource != null && owner.resource != actual) {
                    throw new IllegalStateException("original resource action remains owned");
                }
                owner.resource = Objects.requireNonNull(actual);
            }
        }

        public void retainArchiveCustody(TargetMigrationArchiveCustody actual) {
            synchronized (owner) {
                requireCurrent();
                if (owner.archiveCustody != null && owner.archiveCustody != actual) {
                    throw new IllegalStateException("original archive receiving loan is already retained");
                }
                retainCleanup(actual);
                owner.archiveCustody = Objects.requireNonNull(actual);
            }
        }

        @Override
        public void retainBackend(TargetOwnedBackendGroup actual) {
            actual.requireWorkerGraph(graph().classes(), graph().resources());
            synchronized (owner) {
                requireCurrent();
                if (!owner.backends.contains(actual)) {
                    if (owner.backends.size() >= configuration().maximumBackends()) {
                        throw new IllegalStateException("live migration native owner bound exhausted");
                    }
                    owner.backends.add(actual);
                }
            }
        }

        @Override
        public void retainRecoverySource(TargetWorkerRecoverySource actual) {
            synchronized (owner) {
                requireCurrent();
                if (!owner.backends.contains(actual.backend())
                        || !owner.readers.contains(actual) && owner.readers.size() >= 2) {
                    throw new IllegalStateException(
                            "live migration reader was not retained within its original native envelope");
                }
                if (!owner.readers.contains(actual)) {
                    owner.readers.add(actual);
                }
            }
        }

        @Override
        public void retainControlWriter(TargetMigrationControlWriter actual) {
            synchronized (owner) {
                requireCurrent();
                actual.requireBinding(this);
                if (!owner.backends.contains(actual.backend()) || owner.writer != null && owner.writer != actual) {
                    throw new IllegalStateException(
                            "live migration changed its original independently authorized producer");
                }
                owner.writer = actual;
            }
        }

        @Override
        public void retainCleanup(CheckpointInputFuture.Cleanup actual) {
            synchronized (owner) {
                owner.partial.removeIf(CheckpointInputFuture.Cleanup::cleanupComplete);
                if (!owner.partial.contains(actual)) {
                    if (owner.partial.size() >= owner.configuration.maximumCleanupOwners()) {
                        throw new IllegalStateException("live migration partial cleanup bound exhausted");
                    }
                    owner.partial.add(Objects.requireNonNull(actual));
                }
            }
        }

        @Override
        public void bindSourceIdentity(
                LocalBrokerMtlsResolver.SourceReceipt receipt, OwnerIdentity actual, TrustedUtcIntervalEvidence now) {
            requireCurrent();
            receipt.authorization().requireAssignment(assignment());
            receipt.requireActive(now);
            if (!Arrays.equals(actual.canonicalBytes(), ownerIdentity().canonicalBytes())
                    || !Arrays.equals(
                            actual.deploymentId(), receipt.authorization().deploymentId())
                    || actual.ownerEpoch() != ownerLease().ownerEpoch()
                    || !Arrays.equals(actual.leaseFencingDigest(), ownerLease().leaseToken())) {
                throw new IllegalArgumentException("live migration SDK changed actual Worker/Owner/Source identity");
            }
        }
    }

    /** A stopped/unknown/recovered call cannot mint a new frame, token or producer identity. */
    public static final class AppendPermit implements TargetMigrationAppendPermit {
        private final TargetMigrationMaintenance owner;
        private final TargetMigrationControlMutationFactory.Prepared exact;
        private final TargetMigrationControlWriter writer;
        private final TargetMigrationSourceExposure exposure;
        private final boolean retry;
        private boolean disabled, claimed, exposed;

        private AppendPermit(
                TargetMigrationMaintenance owner,
                TargetMigrationControlMutationFactory.Prepared exact,
                TargetMigrationControlWriter writer,
                TargetMigrationSourceExposure exposure,
                boolean retry) {
            this.owner = owner;
            this.exact = exact;
            this.writer = writer;
            this.exposure = exposure;
            this.retry = retry;
        }

        @Override
        public synchronized void beforeSdk(
                TargetMigrationControlWriter actual,
                TargetMigrationControlMutationFactory.Prepared frame,
                TargetScopeExposureSource.CopyContract copies) {
            owner.requireLocalCurrent();
            owner.custody.requireCurrent();
            if (disabled
                    || owner.permit != this
                    || actual != writer
                    || frame != exact
                    || owner.writer != writer
                    || owner.state != State.APPEND && owner.state != State.APPLY
                    || !writer.copyContract().equals(copies)) {
                throw new IllegalStateException(
                        "live SDK handoff changed durable Prepared/original producer/Owner/token");
            }
            if (!claimed) {
                if (retry) {
                    exposure.claimRetry(actual, frame, copies);
                } else {
                    exposure.claimNative(actual, frame, copies);
                }
                claimed = true;
            } else {
                exposure.requireCurrent();
            }
            exposed = true;
        }

        private synchronized void disable() {
            disabled = true;
        }

        private synchronized boolean exposed() {
            return exposed;
        }
    }
}
