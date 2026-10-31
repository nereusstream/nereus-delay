package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetSloWriterContract;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Protected, process-local REGISTER-prefix qualification. Cold verification never consults this object. */
public final class TargetSloWriterOrigin {
    /** The original owned preparation must reserve these limits; values alone never qualify a prefix. */
    public record Limits(int maximumPrefixes, long maximumRetainedBytes, long totalReadRecords,
            long totalReadBytes, long totalElapsedNanos, int turnReadRecords, long turnReadBytes,
            long turnElapsedNanos) {
        public Limits {
            if (maximumPrefixes < 1 || maximumPrefixes == Integer.MAX_VALUE || maximumRetainedBytes < 1
                    || maximumRetainedBytes == Long.MAX_VALUE || totalReadRecords < 1
                    || totalReadRecords == Long.MAX_VALUE || totalReadBytes < 1 || totalReadBytes == Long.MAX_VALUE
                    || totalElapsedNanos < 1 || totalElapsedNanos == Long.MAX_VALUE || turnReadRecords < 1
                    || turnReadRecords == Integer.MAX_VALUE || turnReadBytes < 1 || turnReadBytes == Long.MAX_VALUE
                    || turnElapsedNanos < 1 || turnElapsedNanos == Long.MAX_VALUE) {
                throw new IllegalArgumentException("writer preparation requires its original finite reservation");
            }
        }
    }

    /**
     * Actual original Root/Owner/session/history/payment retention loan, including every dependency in the verified
     * prefix. Relevant removal/rewrite admission must revoke it before becoming visible. Methods are local checks only.
     * requestRelease is idempotent and must not close native resources or perform RPC while a Store monitor is held.
     */
    public interface Protection {
        OwnerIdentity owner();
        long reservedBytes();
        void requireCurrent();
        void requireBinding(StoreMetadata metadata, TargetQuotaIncarnation root, TargetQuotaMutation actualCut);
        void requireOriginalControl(TargetSloWriterEpoch writer, TargetSloWriterEpoch.Control control);
        void requireSigning(TargetSloWriterContract.Reference writer, long maximumRetryUntilEpochMs);
        void requestRelease();
        boolean released();
    }

    /** Only a real hot Reader can request the corresponding original protected preparation. */
    public static final class PreparationRequired extends RuntimeException {
        private static final long serialVersionUID = 1L;
        // Actual process-local preparation material is never transferred by exception serialization.
        private final transient TargetSloWriterOrigin origin;
        private final transient TargetSloWriterEpoch selected;
        private final transient TargetSloWriterEpoch head;
        private PreparationRequired(TargetSloWriterOrigin origin, TargetSloWriterEpoch selected,
                TargetSloWriterEpoch head) {
            super("original SLO writer REGISTER prefix requires protected preparation");
            this.origin = origin; this.selected = selected; this.head = head;
        }
        public TargetSloWriterContract.Reference reference() { return selected.reference(); }
        public TargetSloWriterEpoch selected() { return selected; }
        public TargetSloWriterOrigin origin() { return origin; }
    }

    private record Binding(StoreMetadata metadata, TargetQuotaIdentity root, byte[] accounting,
            byte[] source, long sequence) {
        private Binding { accounting = Bytes.copy(accounting); source = Bytes.copy(source); }
    }

    private static final class Prefix {
        private final byte[] writer;
        private TargetSloWriterContract.Reference through;
        private byte[] registerDigest;
        private final Protection protection;
        private final Binding binding;
        private long retained;
        private final LinkedHashMap<Long, LoanState> loans = new LinkedHashMap<>();
        private volatile boolean valid = true;
        private volatile boolean installationExited = true;
        private Prefix(byte[] writer, TargetSloWriterContract.Reference through, byte[] registerDigest,
                Protection protection, Binding binding) {
            this.writer = Bytes.copy(writer); this.through = through;
            this.registerDigest = registerDigest == null ? null : Bytes.copy(registerDigest);
            this.protection = protection; this.binding = binding;
            long retainedFields = Math.addExact(writer.length, binding.metadata().encode().length);
            retainedFields = Math.addExact(retainedFields, binding.root().canonicalBytes().length);
            retainedFields = Math.addExact(retainedFields, binding.accounting().length);
            retainedFields = Math.addExact(retainedFields, binding.source().length);
            if (through != null) retainedFields = Math.addExact(retainedFields, through.canonicalBytes().length);
            if (registerDigest != null) retainedFields = Math.addExact(retainedFields, registerDigest.length);
            retained = retainedFields;
        }
    }

    private static final class LoanState {
        private final TargetSloWriterContract.Reference reference;
        private final long maximumRetryUntil;
        private final long retained;
        private volatile boolean valid = true;
        private LoanState(TargetSloWriterEpoch epoch) {
            reference = epoch.reference(); maximumRetryUntil = epoch.contract().maximumRetryUntil();
            retained = Math.addExact(reference.canonicalBytes().length, Long.BYTES);
        }
    }

    private record Candidate(Prefix prior, TargetSloWriterEpoch after, byte[] canonical) {
        private Candidate { canonical = Bytes.copy(canonical); }
    }

    /** Plan-local state; it cannot be installed or transferred by a caller-provided DTO. */
    public static final class Context {
        private final TargetSloWriterOrigin origin;
        private final long generation;
        private Candidate candidate;
        private TargetSloWriterContract.Reference signingReference;
        private byte[] signingRow;
        private Context(TargetSloWriterOrigin origin, long generation) {
            this.origin = origin; this.generation = generation;
        }
        public void requireOrigin(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
                TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity root, TargetSloWriterEpoch epoch,
                TargetQuotaMutation frontier) {
            origin.requireHot(this, view, overlay, scope, lineage, root, epoch, frontier);
        }
        public Loan loan(TargetSloWriterEpoch epoch) { return origin.loan(this, epoch); }
    }

    /** SDK guards inspect only this local original loan; no Store read, readOpen or RPC is performed. */
    public static final class Loan {
        private final TargetSloWriterOrigin origin;
        private final Prefix prefix;
        private final LoanState state;
        private final long generation;
        private Loan(TargetSloWriterOrigin origin, Prefix prefix, LoanState state) {
            this.origin = origin; this.prefix = prefix; this.state = state; generation = origin.generation;
        }
        public void requireCurrent() {
            // Held checks are local and run outside the kernel monitor as well as outside Store/RPC.
            origin.requireLocal(); prefix.protection.requireCurrent();
            prefix.protection.requireSigning(state.reference, state.maximumRetryUntil);
            synchronized (origin) {
                if (origin.stopped || generation != origin.generation || !prefix.valid || !state.valid) {
                    throw new IllegalStateException("original SLO writer loan is no longer current");
                }
            }
            origin.requireLocal(); prefix.protection.requireCurrent();
            prefix.protection.requireSigning(state.reference, state.maximumRetryUntil);
        }
    }

    private final ShardStore store;
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final OwnerIdentity owner;
    private final Runnable localOwner;
    private final Limits limits;
    private final LongSupplier nanos;
    private final LinkedHashMap<String, Prefix> prefixes = new LinkedHashMap<>();
    private final List<Prefix> retired = new ArrayList<>();
    private final List<Preparation> preparing = new ArrayList<>();
    private long nativeSequence, generation, retainedBytes, preparationBytes;
    private volatile boolean stopped;

    public TargetSloWriterOrigin(ShardStore store, TargetStoreBackend backend, TargetQuotaScope scope,
            byte[] lineage, OwnerIdentity owner, Runnable localOwner, Limits limits, LongSupplier nanos) {
        this.store = Objects.requireNonNull(store); this.backend = Objects.requireNonNull(backend);
        backend.requireStore(store); this.scope = Objects.requireNonNull(scope);
        Bytes.requireLength(lineage, 16, "writer origin lineage"); this.lineage = Bytes.copy(lineage);
        if (scope.target() != null || !scope.shard().equals(store.shardId())) {
            throw new IllegalArgumentException("writer origin requires its actual SHARD Store");
        }
        this.owner = Objects.requireNonNull(owner); this.localOwner = Objects.requireNonNull(localOwner);
        this.limits = Objects.requireNonNull(limits); this.nanos = Objects.requireNonNull(nanos);
        nativeSequence = store.latestSequenceNumber();
    }

    public void requireStore(ShardStore actual) {
        if (store != actual) throw new IllegalArgumentException("writer qualification belongs to another Store");
    }

    public synchronized Context openView() {
        requireLocal();
        if (!Thread.holdsLock(store)) throw new IllegalStateException("writer context requires its actual Store read view");
        requireStoreOwner();
        final long actualSequence = store.latestSequenceNumber();
        if (nativeSequence != actualSequence) { invalidate(); nativeSequence = actualSequence; }
        return new Context(this, generation);
    }

    /** Begin once for the exact requested prefix. The original loan owns all pages and their cumulative limits. */
    public Preparation prepare(PreparationRequired request, Protection protection,
            TargetStoreBackend.ReadAuthority reads) {
        requireLocal(); Objects.requireNonNull(request); Objects.requireNonNull(protection); Objects.requireNonNull(reads);
        if (request.origin != this || !owner.equals(protection.owner())) {
            throw new IllegalArgumentException("writer preparation changed its actual Owner/session/origin");
        }
        protection.requireCurrent();
        final long reserved = protection.reservedBytes(), started = nanos.getAsLong();
        synchronized (this) {
            if (stopped) throw new IllegalStateException("writer origin already stopped");
            reap();
            if (qualificationCount() >= limits.maximumPrefixes() || reserved < limits.maximumRetainedBytes()) {
                throw new IllegalStateException("original writer preparation reservation is unavailable");
            }
            if (prefixes.values().stream().anyMatch(prefix -> prefix.protection == protection)
                    || retired.stream().anyMatch(prefix -> prefix.protection == protection)
                    || preparing.stream().anyMatch(cursor -> cursor.protection == protection)) {
                throw new IllegalArgumentException("one original writer reservation cannot fund two live preparations");
            }
            final var cursor = new Preparation(request, protection, reads, generation, started);
            cursor.chargedRetained = cursor.measureRetained();
            if (cursor.chargedRetained > limits.maximumRetainedBytes() - retainedBytes - preparationBytes) {
                throw new IllegalStateException("writer preparation copies exceed the original retained reservation");
            }
            preparing.add(cursor); preparationBytes = Math.addExact(preparationBytes, cursor.chargedRetained);
            return cursor;
        }
    }

    /** One bounded epoch per original owned-I/O turn. No iterator or Store monitor escapes a turn. */
    public final class Preparation {
        private final PreparationRequired request;
        private final Protection protection;
        private final TargetStoreBackend.ReadAuthority reads;
        private final long originalGeneration, started;
        private long records, bytes;
        private Binding binding;
        private TargetSloWriterEpoch next;
        private byte[] headRegister;
        private volatile long chargedRetained;
        private volatile boolean complete, transferred, stopped;
        private volatile boolean advancing;
        private Prefix installedPrefix;
        private Preparation(PreparationRequired request, Protection protection, TargetStoreBackend.ReadAuthority reads,
                long originalGeneration, long started) {
            this.request = request; this.protection = protection; this.reads = reads;
            this.originalGeneration = originalGeneration; this.started = started; next = request.head;
        }
        public boolean advance() {
            synchronized (this) {
                if (stopped || transferred || advancing) throw new IllegalStateException("writer preparation already exited or advancing");
                if (complete) return true;
                advancing = true;
            }
            try {
            requirePreparation();
            final long elapsed = nanos.getAsLong() - started;
            final long remainingRecords = limits.totalReadRecords() - records;
            final long remainingBytes = limits.totalReadBytes() - bytes;
            if (elapsed < 0 || elapsed >= limits.totalElapsedNanos() || remainingRecords <= 0 || remainingBytes <= 0) {
                throw new IllegalStateException("original writer preparation cumulative envelope exhausted");
            }
            final var budget = new BoundedReadBudget((int) Math.min(limits.turnReadRecords(), remainingRecords),
                    Math.min(limits.turnReadBytes(), remainingBytes),
                    Math.min(limits.turnElapsedNanos(), limits.totalElapsedNanos() - elapsed), nanos);
            try {
                final var step = backend.guardedRead(budget, reader -> {
                    requirePreparation();
                    final var currentBinding = capture(reader, protection);
                    if (binding == null) binding = currentBinding; else requireSameCut(binding, currentBinding);
                    final var selected = request.selected;
                    if (next == null) {
                        if (selected.contract().epoch() != 1 || reader.get(ColumnFamily.META, selected.key()) != null) {
                            throw new IllegalStateException("fresh writer preparation changed its actual empty prefix");
                        }
                        protection.requireOriginalControl(selected, selected.register());
                        reader.requireWithinElapsedBudget();
                        return new Step(null, digestRegister(selected), true);
                    }
                    final var actual = readEpoch(reader, next.key());
                    if (!sameRegister(actual, next)) {
                        throw new IllegalStateException("writer preparation changed its original REGISTER prefix");
                    }
                    final var root = TargetSloWriterEpochStore.root(reader, List.of(), scope, lineage,
                            currentBinding.root(), reader.aggregate().mutation());
                    TargetSloWriterEpochStore.requireFrontier(reader, List.of(), root, reader.aggregate().mutation());
                    TargetSloWriterEpochStore.requireNodeOrigin(reader, List.of(), scope, lineage,
                            currentBinding.root(), root, actual, reader.aggregate().mutation());
                    protection.requireOriginalControl(actual, actual.register());
                    requirePayment(reader, actual.register());
                    if (actual.closed()) {
                        protection.requireOriginalControl(actual, actual.close());
                        requirePayment(reader, actual.close());
                    }
                    final var contract = actual.contract();
                    final byte[] predecessor = actual.register().body().request().expectedPriorDigest();
                    if (contract.epoch() == 1) {
                        if (predecessor != null) throw new IllegalStateException("first writer REGISTER has a predecessor");
                        reader.requireWithinElapsedBudget();
                        return new Step(null, digestRegister(actual), true);
                    }
                    if (predecessor == null) throw new IllegalStateException("writer REGISTER lost its prior identity");
                    final var prior = readEpoch(reader, TargetSloWriterEpoch.key(scope, contract.writerIdentityDigest(), contract.epoch() - 1));
                    requirePredecessor(actual, prior);
                    reader.requireWithinElapsedBudget();
                    return new Step(prior, digestRegister(actual), false);
                }, reads);
                synchronized (this) {
                    if (stopped) throw new IllegalStateException("writer preparation stopped before its read exited");
                    if (headRegister == null && step.registerDigest() != null) headRegister = step.registerDigest();
                    next = step.prior(); complete = step.done();
                }
                requireRetainedWindow();
                return complete;
            } finally {
                synchronized (this) {
                    records = Math.addExact(records, budget.actualRecords());
                    bytes = Math.addExact(bytes, budget.actualBytes());
                }
            }
            } finally { finishOperation(); }
        }
        public void install() {
            synchronized (this) {
                if (!complete || transferred || stopped || advancing) throw new IllegalStateException("writer prefix is not completely verified or is advancing");
                advancing = true;
            }
            try {
            requirePreparation();
            final long elapsed = nanos.getAsLong() - started;
            final long remainingRecords = limits.totalReadRecords() - records;
            final long remainingBytes = limits.totalReadBytes() - bytes;
            if (remainingRecords <= 0 || remainingBytes <= 0) {
                throw new IllegalStateException("writer installation exhausted its original cumulative read reservation");
            }
            final var budget = new BoundedReadBudget((int) Math.min(limits.turnReadRecords(), remainingRecords),
                    Math.min(limits.turnReadBytes(), remainingBytes),
                    Math.min(limits.turnElapsedNanos(), limits.totalElapsedNanos() - elapsed), nanos);
            try {
                backend.guardedRead(budget, reader -> {
                    requirePreparation(); requireSameCut(binding, capture(reader, protection));
                    if (request.head == null) {
                        if (reader.get(ColumnFamily.META, request.selected.key()) != null) {
                            throw new IllegalStateException("fresh REGISTER became visible before its actual Source commit");
                        }
                    } else if (!Arrays.equals(digestRegister(readEpoch(reader, request.head.key())), headRegister)) {
                        throw new IllegalStateException("writer installation changed its fully verified original REGISTER");
                    }
                    reader.requireWithinElapsedBudget();
                    requireLocal(); protection.requireCurrent();
                    final var prefix = new Prefix(request.selected.contract().service().canonicalBytes(),
                            request.head == null ? null : request.head.reference(), headRegister, protection, binding);
                    prefix.installationExited = false;
                    final var key = writerKey(request.selected);
                    synchronized (TargetSloWriterOrigin.this) {
                        if (TargetSloWriterOrigin.this.stopped || generation != originalGeneration) {
                            throw new IllegalStateException("writer installation lost its original protected generation");
                        }
                        if (prefix.retained > limits.maximumRetainedBytes() - retainedBytes - preparationBytes) {
                            throw new IllegalStateException("writer prefixes exceed their original retained reservation");
                        }
                        final Prefix old;
                        synchronized (this) {
                            if (stopped || !advancing) throw new IllegalStateException("writer installation stopped before transfer");
                            old = prefixes.put(key, prefix);
                            retainedBytes = Math.addExact(retainedBytes, prefix.retained);
                            if (!preparing.remove(this)) throw new IllegalStateException("writer preparation lost its original reservation");
                            preparationBytes = Math.subtractExact(preparationBytes, chargedRetained);
                            installedPrefix = prefix; transferred = true;
                        }
                        if (old != null) retire(old);
                    }
                    return Boolean.TRUE;
                }, reads);
            } catch (RuntimeException | Error failure) {
                if (transferred) invalidate();
                throw failure;
            } finally {
                synchronized (this) {
                    records = Math.addExact(records, budget.actualRecords());
                    bytes = Math.addExact(bytes, budget.actualBytes());
                }
            }
            } finally { finishOperation(); }
        }
        public void requestStop() {
            final boolean release;
            synchronized (this) {
                if (transferred) return;
                stopped = true; release = !advancing;
            }
            if (release) protection.requestRelease();
        }
        public boolean exited() {
            final boolean installed, stoppedAndIdle;
            synchronized (this) {
                if (advancing) return false;
                installed = transferred; stoppedAndIdle = stopped;
            }
            return installed || stoppedAndIdle && protection.released();
        }
        private void finishOperation() {
            final boolean releaseCursor;
            final Prefix installed;
            synchronized (this) {
                installed = installedPrefix;
                if (installed != null) installed.installationExited = true;
                advancing = false; releaseCursor = stopped && !transferred;
            }
            // The guarded read and its original authority have exited before a release is requested.
            if (releaseCursor) protection.requestRelease();
            if (installed != null && !installed.valid) installed.protection.requestRelease();
        }
        public synchronized long actualReadRecords() { return records; }
        public synchronized long actualReadBytes() { return bytes; }
        /** Canonical retained inputs only; the original reservation must independently qualify JVM/copy overhead. */
        public long retainedBytes() { return chargedRetained; }
        private long measureRetained() {
            long retained = request.selected.canonicalBytes().length;
            if (next != null) retained = Math.addExact(retained, next.canonicalBytes().length);
            if (headRegister != null) retained = Math.addExact(retained, headRegister.length);
            if (binding != null) retained = Math.addExact(retained, Math.addExact(binding.metadata().encode().length,
                    Math.addExact(binding.root().canonicalBytes().length,
                            Math.addExact(binding.accounting().length, binding.source().length))));
            return retained;
        }
        private void requireRetainedWindow() {
            final long actual = measureRetained();
            synchronized (TargetSloWriterOrigin.this) {
                final long additional = actual - chargedRetained;
                if (additional > limits.maximumRetainedBytes() - retainedBytes - preparationBytes) {
                    throw new IllegalStateException("writer preparation exceeds its original retained copy reservation");
                }
                if (!transferred) preparationBytes = Math.addExact(preparationBytes, additional);
                chargedRetained = actual;
            }
        }
        private void requirePreparation() {
            if (stopped) throw new IllegalStateException("writer preparation already stopped");
            requireLocal();
            final long actualSequence = store.latestSequenceNumber();
            synchronized (TargetSloWriterOrigin.this) {
                if (TargetSloWriterOrigin.this.stopped || generation != originalGeneration || nativeSequence != actualSequence) {
                    throw new IllegalStateException("writer preparation lost its actual protected Store generation");
                }
            }
            final long elapsed = nanos.getAsLong() - started;
            if (elapsed < 0 || elapsed >= limits.totalElapsedNanos()) {
                throw new IllegalStateException("writer preparation exceeded its original cumulative hold deadline");
            }
            if (store.closeFenceRaised() || store.runtimeMetadata().lastOpenedOwnerEpoch() != owner.ownerEpoch()) {
                throw new IllegalStateException("writer preparation lost its original Store/Owner epoch");
            }
            requireRetainedWindow();
            protection.requireCurrent();
        }
    }

    private record Step(TargetSloWriterEpoch prior, byte[] registerDigest, boolean done) {}

    private void requireHot(Context context, TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope actualScope, byte[] actualLineage, TargetQuotaIdentity root, TargetSloWriterEpoch epoch,
            TargetQuotaMutation frontier) {
        requireStoreOwner();
        final Prefix prefix;
        synchronized (this) {
            requireContext(context);
            if (!scope.equals(actualScope) || !Arrays.equals(lineage, actualLineage) || !scope.shard().equals(view.shardId())) {
                throw new IllegalArgumentException("writer hot proof changed its Root scope/lineage");
            }
            prefix = prefixes.get(writerKey(epoch));
        }
        final var persisted = view.projected(ColumnFamily.META, epoch.key(), List.of());
        final var before = persisted == null ? null : decodeEpoch(epoch.key(), persisted);
        final boolean created = before == null && overlay.stream().anyMatch(e -> e.family() == ColumnFamily.META
                && Arrays.equals(e.key(), epoch.key()) && e.after() != null);
        final TargetSloWriterEpoch prior;
        if (epoch.contract().epoch() == 1) prior = null;
        else {
            final var key = TargetSloWriterEpoch.key(scope, epoch.contract().writerIdentityDigest(), epoch.contract().epoch() - 1);
            final var raw = Objects.requireNonNull(view.projected(ColumnFamily.META, key, overlay), "actual writer predecessor");
            prior = decodeEpoch(key, raw); requirePredecessor(epoch, prior);
        }
        if (prefix == null || !covers(prefix, epoch, created)) {
            throw new PreparationRequired(this, epoch, created ? prior : epoch);
        }
        requirePrefix(prefix);
        final var currentRoot = TargetSloWriterEpochStore.root(view, overlay, scope, lineage, root, frontier);
        requireBinding(prefix.binding, currentRoot, view);
        TargetSloWriterEpochStore.requireFrontier(view, overlay, currentRoot, frontier);
        TargetSloWriterEpochStore.requireNodeOrigin(view, overlay, scope, lineage, root, currentRoot, epoch, frontier);
        if (prior != null) {
            TargetSloWriterEpochStore.requireNodeOrigin(view, overlay, scope, lineage, root, currentRoot, prior, frontier);
        }
        if (before != null && !sameRegister(before, epoch)) {
            throw new IllegalStateException("writer hot mutation rewrites its stable REGISTER");
        }
        if (overlay.isEmpty() && !epoch.closed()) {
            context.signingReference = epoch.reference();
            context.signingRow = Bytes.sha256(epoch.canonicalBytes());
        }
        final var edit = overlay.stream().filter(e -> e.family() == ColumnFamily.META && Arrays.equals(e.key(), epoch.key()))
                .findFirst().orElse(null);
        if (edit != null && edit.after() != null) {
            final var candidate = new Candidate(prefix, epoch, epoch.canonicalBytes());
            if (context.candidate != null && !Arrays.equals(context.candidate.canonical(), candidate.canonical())) {
                throw new IllegalStateException("one Source plan proposed conflicting writer successors");
            }
            context.candidate = candidate;
        }
    }

    private synchronized Loan loan(Context context, TargetSloWriterEpoch epoch) {
        requireContext(context);
        if (context.signingReference == null || !context.signingReference.equals(epoch.reference())
                || !Arrays.equals(context.signingRow, Bytes.sha256(epoch.canonicalBytes()))) {
            throw new IllegalStateException("writer signing loan lacks this exact guarded Native read");
        }
        final var prefix = prefixes.get(writerKey(epoch));
        if (prefix == null || !covers(prefix, epoch, false) || epoch.closed()) {
            throw new PreparationRequired(this, epoch, epoch);
        }
        requirePrefix(prefix);
        var state = prefix.loans.get(epoch.contract().epoch());
        if (state == null) {
            state = new LoanState(epoch);
            if (qualificationCount() >= limits.maximumPrefixes()
                    || state.retained > limits.maximumRetainedBytes() - retainedBytes - preparationBytes) {
                throw new IllegalStateException("original writer signing-loan reservation is full");
            }
            prefix.loans.put(epoch.contract().epoch(), state);
            retainedBytes = Math.addExact(retainedBytes, state.retained);
        } else if (!state.reference.equals(epoch.reference())) {
            throw new IllegalStateException("writer signing loan changed its exact REGISTER reference");
        }
        return new Loan(this, prefix, state);
    }

    /** Called only after this backend's actual native write succeeds, while its original Store monitor is held. */
    public synchronized void committed(List<TargetStoreBackend.Edit> edits, Context context) {
        if (stopped) return;
        if (context == null || context.origin != this || context.generation != generation) { invalidate(); return; }
        for (var edit : edits) {
            if (edit.before() == null) continue;
            if (edit.family() == ColumnFamily.META && edit.key()[0] == TargetKeyCodec.SLO_WRITER_EPOCH_TAG) {
                if (edit.after() == null || !sameRegister(decodeEpoch(edit.key(), edit.before()), decodeEpoch(edit.key(), edit.after()))) {
                    invalidate(); return;
                }
                final var actual = decodeEpoch(edit.key(), edit.after());
                final var prefix = prefixes.get(writerKey(actual));
                if (prefix != null && actual.closed()) {
                    final var loan = prefix.loans.remove(actual.contract().epoch());
                    if (loan != null) { loan.valid = false; retainedBytes = Math.subtractExact(retainedBytes, loan.retained); }
                }
            } else if (edit.after() == null) {
                // The original history protection classifies removals locally; no key/epoch high-water proof is substituted.
                for (var prefix : prefixes.values()) prefix.protection.requireCurrent();
            } else if (!Arrays.equals(edit.before(), edit.after()) && stableDependencyChanged(edit)) {
                invalidate(); return;
            }
        }
        final var candidate = context.candidate;
        if (candidate != null) {
            final var exact = edits.stream().filter(e -> e.family() == ColumnFamily.META
                    && Arrays.equals(e.key(), candidate.after().key())).findFirst().orElse(null);
            if (exact == null || exact.after() == null || !Arrays.equals(
                    TargetValueEnvelope.decode(exact.after(), TargetSloWriterEpoch.VALUE_TYPE).payload(), candidate.canonical())) {
                invalidate(); return;
            }
            final var old = candidate.prior();
            requirePrefix(old);
            final boolean extendsPrefix = old.through == null
                    || Long.compareUnsigned(candidate.after().contract().epoch(), old.through.epoch()) > 0;
            if (extendsPrefix) {
                final var next = new Prefix(old.writer, candidate.after().reference(), digestRegister(candidate.after()),
                        old.protection, old.binding);
                final long additional = next.retained - old.retained;
                if (additional > limits.maximumRetainedBytes() - retainedBytes - preparationBytes) {
                    invalidate(); return;
                }
                old.through = next.through; old.registerDigest = next.registerDigest;
                old.retained = next.retained; retainedBytes = Math.addExact(retainedBytes, additional);
            }
        }
        nativeSequence = store.latestSequenceNumber();
    }

    public synchronized void invalidate() {
        generation = Math.incrementExact(generation);
        for (var prefix : prefixes.values()) retire(prefix);
        prefixes.clear(); nativeSequence = -1;
        for (var cursor : preparing) cursor.requestStop();
        reap();
    }
    public synchronized void requestStop() { stopped = true; invalidate(); }
    public synchronized boolean exited() { reap(); return prefixes.isEmpty() && retired.isEmpty() && preparing.isEmpty(); }

    private Binding capture(TargetStoreBackend.Reader reader, Protection protection) {
        final var aggregate = reader.aggregate();
        final var cut = Objects.requireNonNull(aggregate.mutation(), "actual writer preparation Source cut");
        final var root = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        final var owner = TargetSloWriterEpochStore.root(reader, List.of(), scope, lineage, root, cut);
        TargetSloWriterEpochStore.requireFrontier(reader, List.of(), owner, cut);
        protection.requireBinding(reader.metadata(), owner, cut);
        return new Binding(reader.metadata(), root, owner.accounting().canonicalBytes(),
                Objects.requireNonNull(reader.source()).canonicalBytes(), reader.sourceSequence());
    }
    private void requirePayment(TargetStoreBackend.Reader reader, TargetSloWriterEpoch.Control control) {
        final var reference = control.body().admissionScope();
        if (reference != null) TargetScopeControlObligationStore.requirePaidScope(reader, scope, lineage, reference);
    }
    private void requireBinding(Binding binding, TargetQuotaIncarnation root, TargetRecordAccounting.View view) {
        if (!binding.root().equals(root.identity()) || !Arrays.equals(binding.accounting(), root.accounting().canonicalBytes())
                || !Arrays.equals(root.recoveryLineage(), lineage) || !scope.shard().equals(view.shardId())) {
            throw new IllegalStateException("writer prefix changed its original Root/accounting/lineage");
        }
    }
    private static void requireSameCut(Binding expected, Binding actual) {
        if (!Arrays.equals(expected.metadata().encode(), actual.metadata().encode()) || !expected.root().equals(actual.root())
                || !Arrays.equals(expected.accounting(), actual.accounting()) || expected.sequence() != actual.sequence()
                || !Arrays.equals(expected.source(), actual.source())) {
            throw new IllegalStateException("writer preparation pages changed their original exact Store/Source cut");
        }
    }
    private synchronized void requireContext(Context context) {
        requireLocal();
        if (!Thread.holdsLock(store) || context.origin != this || context.generation != generation
                || nativeSequence != store.latestSequenceNumber()) {
            throw new IllegalStateException("writer hot context changed its actual native generation");
        }
    }
    private void requireLocal() {
        if (stopped) throw new IllegalStateException("writer prefix lost its actual Store/Owner admission");
        localOwner.run();
    }
    private void requireStoreOwner() {
        if (store.isCloseStarted() || store.runtimeMetadata().lastOpenedOwnerEpoch() != owner.ownerEpoch()) {
            throw new IllegalStateException("writer prefix changed its actual Store/Owner epoch");
        }
    }
    private boolean stableDependencyChanged(TargetStoreBackend.Edit edit) {
        if (edit.family() == ColumnFamily.DEDUPE) return true;
        if (edit.family() != ColumnFamily.META) return false;
        final var before = TargetValueEnvelope.decodeAny(edit.before());
        final var after = TargetValueEnvelope.decodeAny(edit.after());
        if (before.valueType() != after.valueType()) return true;
        if (before.valueType() == TargetQuotaIncarnation.VALUE_TYPE) {
            return TargetQuotaIncarnation.decodeForStore(edit.key(), before.payload(), scope.shard(), scope.tenantScope())
                    .identity().kind() == TargetQuotaIdentity.Kind.SHARD;
        }
        if (before.valueType() == TargetScopeControlObligation.VALUE_TYPE) {
            final var prior = TargetScopeControlObligation.decodeForStore(edit.key(), before.payload(), scope, lineage);
            final var next = TargetScopeControlObligation.decodeForStore(edit.key(), after.payload(), scope, lineage);
            return !Arrays.equals(prior.funding().canonicalBytes(), next.funding().canonicalBytes())
                    || !Arrays.equals(prior.definition().canonicalBytes(), next.definition().canonicalBytes());
        }
        if (before.valueType() == TargetParentControlCredit.VALUE_TYPE) {
            final var prior = TargetParentControlCredit.decodeForStore(edit.key(), before.payload(), scope, lineage);
            final var next = TargetParentControlCredit.decodeForStore(edit.key(), after.payload(), scope, lineage);
            if (!Arrays.equals(prior.funding().canonicalBytes(), next.funding().canonicalBytes())
                    || !Arrays.equals(prior.actualPayerGrant().canonicalBytes(), next.actualPayerGrant().canonicalBytes())
                    || prior.allocations().size() > next.allocations().size()) return true;
            for (int index = 0; index < prior.allocations().size(); index++) {
                if (!Arrays.equals(prior.allocations().get(index).canonicalBytes(), next.allocations().get(index).canonicalBytes())) return true;
            }
        }
        return before.valueType() == com.nereusstream.delay.protocol.TargetProtocolActivationRecord.VALUE_TYPE;
    }
    private void requirePrefix(Prefix prefix) {
        if (!prefix.valid || !owner.equals(prefix.protection.owner())) throw new IllegalStateException("writer prefix is invalid");
        prefix.protection.requireCurrent();
    }
    private void retire(Prefix prefix) {
        prefix.valid = false; retired.add(prefix);
        if (prefix.installationExited) prefix.protection.requestRelease();
    }
    private void reap() {
        for (var iterator = retired.iterator(); iterator.hasNext();) {
            final var prefix = iterator.next();
            if (prefix.installationExited && prefix.protection.released()) {
                long released = prefix.retained;
                for (var loan : prefix.loans.values()) released = Math.addExact(released, loan.retained);
                retainedBytes = Math.subtractExact(retainedBytes, released); iterator.remove();
            }
        }
        for (var iterator = preparing.iterator(); iterator.hasNext();) {
            final var cursor = iterator.next();
            if (cursor.stopped && !cursor.advancing && cursor.protection.released()) {
                preparationBytes = Math.subtractExact(preparationBytes, cursor.chargedRetained); iterator.remove();
            }
        }
    }
    private long qualificationCount() {
        long count = Math.addExact((long) prefixes.size(), Math.addExact((long) retired.size(), preparing.size()));
        for (var prefix : prefixes.values()) count = Math.addExact(count, prefix.loans.size());
        for (var prefix : retired) count = Math.addExact(count, prefix.loans.size());
        return count;
    }
    private boolean covers(Prefix prefix, TargetSloWriterEpoch epoch, boolean created) {
        if (!prefix.valid || !Arrays.equals(prefix.writer, epoch.contract().service().canonicalBytes())) return false;
        final long through = prefix.through == null ? 0 : prefix.through.epoch();
        final long selected = epoch.contract().epoch();
        if (Long.compareUnsigned(selected, through) <= 0) return true;
        return created && selected == through + 1 && (through == 0
                ? epoch.register().body().request().expectedPriorDigest() == null
                    && Arrays.equals(digestRegister(epoch), prefix.registerDigest)
                : Arrays.equals(epoch.register().body().request().expectedPriorDigest(), prefix.through.contractDigest()));
    }
    private TargetSloWriterEpoch readEpoch(TargetStoreBackend.Reader reader, byte[] key) {
        return decodeEpoch(key, Objects.requireNonNull(reader.get(ColumnFamily.META, key), "original writer epoch"));
    }
    private TargetSloWriterEpoch decodeEpoch(byte[] key, byte[] raw) {
        return TargetSloWriterEpoch.decodeForStore(key, TargetValueEnvelope.decode(raw, TargetSloWriterEpoch.VALUE_TYPE).payload(), scope, lineage);
    }
    private static void requirePredecessor(TargetSloWriterEpoch current, TargetSloWriterEpoch prior) {
        if (!Arrays.equals(current.contract().service().canonicalBytes(), prior.contract().service().canonicalBytes())
                || current.contract().epoch() != prior.contract().epoch() + 1
                || !Arrays.equals(current.register().body().request().expectedPriorDigest(), prior.contractDigest())) {
            throw new IllegalStateException("writer REGISTER changed its actual complete predecessor identity");
        }
        current.register().appliedAt().requireAfter(prior.register().appliedAt());
    }
    private static boolean sameRegister(TargetSloWriterEpoch left, TargetSloWriterEpoch right) {
        return Arrays.equals(left.register().canonicalBytes(), right.register().canonicalBytes());
    }
    private static byte[] digestRegister(TargetSloWriterEpoch epoch) { return Bytes.sha256(epoch.register().canonicalBytes()); }
    private static String writerKey(TargetSloWriterEpoch epoch) { return Bytes.hex(epoch.contract().writerIdentityDigest()); }
}
