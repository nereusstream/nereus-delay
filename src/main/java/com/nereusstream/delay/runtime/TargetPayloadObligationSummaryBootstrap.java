package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifest;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.LegacyCheckpointStateInventory;
import com.nereusstream.delay.store.LegacyMigrationBundle;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Narrow initialization proofs, minted from actual birth graphs or complete protected folds. */
public final class TargetPayloadObligationSummaryBootstrap {
    private TargetPayloadObligationSummaryBootstrap() {}

    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-payload-summary-origin-v2\0");

    public static final class ViewChanged extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public ViewChanged() {
            super("payload summary protected Native view changed");
        }
    }

    public static final class Qualification {
        private final TargetQuotaPayloadOwner before;
        private final TargetQuotaMutation initializedAt;
        private final TargetQuotaPayloadOwner.SummaryOrigin origin;
        private final long count;
        private final byte[] digest;
        private final Fold fold;

        private Qualification(
                TargetQuotaPayloadOwner before,
                TargetQuotaMutation initializedAt,
                TargetQuotaPayloadOwner.SummaryOrigin origin,
                long count,
                byte[] digest,
                Fold fold) {
            this.before = before;
            this.initializedAt = initializedAt;
            this.origin = origin;
            this.count = count;
            this.digest = Bytes.copy(digest);
            this.fold = fold;
            if (count < 0 || before.summaryQualified()) {
                throw new IllegalArgumentException("summary initialization requires an unqualified exact owner");
            }
        }

        public void requireOwner(TargetQuotaPayloadOwner owner) {
            if (!Arrays.equals(before.canonicalBytes(), owner.canonicalBytes())) {
                throw new IllegalStateException("summary qualification changed its complete original owner");
            }
        }

        public long unresolvedCount() {
            return count;
        }

        public TargetQuotaMutation initializedAt() {
            return initializedAt;
        }

        public byte[] originDigest() {
            return Bytes.copy(digest);
        }

        public boolean birth() {
            return origin != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP;
        }

        public TargetQuotaPayloadOwner.SummaryOrigin origin() {
            return origin;
        }
    }

    public static Qualification fresh(
            TargetStoreBackend.Reader reader,
            TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding,
            TargetQuotaMutation stamp,
            List<TargetStoreBackend.Edit> edits) {
        requireFreshGraph(reader, owner, binding, stamp, edits);
        return new Qualification(
                owner, stamp, TargetQuotaPayloadOwner.SummaryOrigin.FRESH, 0, freshDigest(owner, binding, stamp), null);
    }

    public static Qualification imported(
            LegacyCheckpointStateInventory.LiveView complete,
            Collection<LegacyMigrationBundle.TargetRow> projected,
            TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding,
            TargetMigrationPlan plan,
            TargetQuotaMutation stamp) {
        LegacyMigrationBundle.requireSummaryImport(complete, projected, owner, binding, plan);
        if (!binding.imported()
                || !Arrays.equals(binding.migrationPlanDigest(), plan.digest())
                || !owner.mutation().equals(stamp)) {
            throw new IllegalStateException("summary import changed its explicit original baseline");
        }
        for (var row : projected) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            if (value.valueType() == TargetQuotaAttemptBudget.VALUE_TYPE) {
                final var budget = TargetQuotaAttemptBudget.decodeForStore(
                        row.key(), value.payload(), plan.scope().shard());
                if (budget.locator().messageId().equals(owner.messageId())) {
                    throw new IllegalStateException(
                            "initial imported summary requires the complete absence of Target attempts");
                }
            } else if (value.valueType() == TargetMessageRecord.VALUE_TYPE) {
                final var message = TargetMessageRecord.decodeForStore(
                        row.key(), value.payload(), plan.scope().shard());
                if (message.locator().messageId().equals(owner.messageId())
                        && !message.runtime().attemptObligations().isEmpty()) {
                    throw new IllegalStateException("imported current runtime has an unaccounted obligation");
                }
            } else if (value.valueType() == TargetTerminalGenerationRecord.VALUE_TYPE) {
                final var terminal = TargetTerminalGenerationRecord.decode(value.payload());
                if (terminal.locator().messageId().equals(owner.messageId())
                        && !terminal.runtime().attemptObligations().isEmpty()) {
                    throw new IllegalStateException("imported historical runtime has an unaccounted obligation");
                }
            }
        }
        return new Qualification(
                owner,
                stamp,
                TargetQuotaPayloadOwner.SummaryOrigin.IMPORTED,
                0,
                Bytes.sha256(
                        DOMAIN,
                        plan.digest(),
                        owner.canonicalBytes(),
                        binding.canonicalBytes(),
                        complete.applied().canonicalBytes(),
                        Bytes.u64beBits(complete.sequence())),
                null);
    }

    public static void requireInitialization(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaPayloadOwner before,
            TargetQuotaPayloadOwner after,
            TargetQuotaMutation stamp,
            List<TargetStoreBackend.Edit> edits,
            Qualification qualification) {
        if (!after.summaryQualified()
                || after.summaryRevision() != 1
                || !after.initializationSource().equals(stamp)
                || !after.summaryMutation().equals(stamp)
                || !Arrays.equals(after.tenantScope(), scope.tenantScope())
                || !Arrays.equals(after.recoveryLineage(), lineage)) {
            throw new IllegalStateException("payload summary initialization changed actual Source/owner scope");
        }
        if (before == null) {
            if (after.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.FRESH || after.unresolvedCount() != 0) {
                throw new IllegalStateException(
                        "ordinary Source birth cannot substitute imported/bootstrap qualification");
            }
            final var bindingKey = TargetKeyCodec.scheduleBinding(after.initialBindingDigest());
            final var raw = reader.projected(ColumnFamily.ID, bindingKey, edits);
            final var binding = TargetScheduleBinding.decodeForStore(
                    bindingKey,
                    TargetValueEnvelope.decode(Objects.requireNonNull(raw), TargetScheduleBinding.VALUE_TYPE)
                            .payload(),
                    scope.shard());
            requireFreshGraph(reader, after, binding, stamp, edits);
            if (!Arrays.equals(after.summaryOriginDigest(), freshDigest(after, binding, stamp))) {
                throw new IllegalStateException("fresh summary changed its actual birth graph digest");
            }
        } else {
            final var proof = Objects.requireNonNull(
                    qualification, "explicit old-owner initialization needs its actual protected fold");
            proof.requireOwner(before);
            if (proof.origin != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP || proof.fold == null) {
                throw new IllegalStateException("old owner cannot use a fresh/import initialization");
            }
            proof.fold.requireReader(reader, stamp);
            final var expected = before.initializeSummary(
                    proof, stamp, (prior, next, floor) -> proof.fold.requireReader(reader, stamp));
            if (!Arrays.equals(expected.canonicalBytes(), after.canonicalBytes())) {
                throw new IllegalStateException("summary upgrade differs from the actual full-fold successor");
            }
        }
    }

    public static void requireColdInitialization(
            TargetCheckpointDependencies dependencies, TargetQuotaPayloadOwner owner, long reconstructedInitialCount) {
        final var first = owner.summaryOrigin() == TargetQuotaPayloadOwner.SummaryOrigin.IMPORTED
                ? null
                : dependencies.resultRecords().stream()
                        .filter(value ->
                                !value.kind().position() && value.mutation().equals(owner.initializationSource()))
                        .findFirst()
                        .orElseThrow(() ->
                                new IllegalStateException("summary initialization lacks its original Source first"));
        requireColdInitialization(dependencies, owner, reconstructedInitialCount, first);
    }

    /** Incremental collector supplies its actual complete-fold first index; the same immutable joins still apply. */
    public static void requireColdInitialization(
            TargetCheckpointDependencies dependencies,
            TargetQuotaPayloadOwner owner,
            long reconstructedInitialCount,
            TargetResultRecord first) {
        if (owner.summaryOrigin() == TargetQuotaPayloadOwner.SummaryOrigin.IMPORTED) {
            final var binding = Objects.requireNonNull(
                    dependencies.binding(owner.initialBindingDigest()), "imported original summary binding");
            final var migration = Objects.requireNonNull(
                    dependencies.migrationRoot(), "imported summary lacks its actual migration root");
            if (!binding.imported()
                    || !Arrays.equals(
                            binding.migrationPlanDigest(), migration.plan().digest())
                    || !owner.initializationSource()
                            .equals(
                                    binding.migrationSource() == null
                                            ? TargetMigrationRoot.baselineFor(migration.plan())
                                            : binding.migrationSource())
                    || reconstructedInitialCount != 0) {
                throw new IllegalStateException("imported summary changed its explicit complete baseline");
            }
            return;
        }
        Objects.requireNonNull(first, "actual initialization first from the complete fold");
        first.requireOwner(dependencies.root().root());
        if (first.kind().position()
                || !first.mutation().equals(owner.initializationSource())
                || !first.appliedFirst()
                || owner.summaryOrigin() == TargetQuotaPayloadOwner.SummaryOrigin.FRESH
                        && (first.kind() != TargetResultRecord.Kind.COMMAND || reconstructedInitialCount != 0)
                || owner.summaryOrigin() == TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP
                        && (first.kind() != TargetResultRecord.Kind.SYSTEM
                                || SystemMutationResult.decode(first.typedPayload())
                                                .mutationType()
                                        != SystemMutationType.APPLY_SHARD_CONTROL)) {
            throw new IllegalStateException("summary initialization changed its actual first purpose");
        }
        if (first.kind() == TargetResultRecord.Kind.SYSTEM) {
            dependencies.requireSystemPosition(first);
        } else {
            final var binding = Objects.requireNonNull(
                    dependencies.binding(owner.initialBindingDigest()), "fresh original summary binding");
            if (binding.imported()
                    || !Arrays.equals(
                            binding.bindingSource().canonicalBytes(),
                            owner.initializationSource().source().canonicalBytes())
                    || !Arrays.equals(
                            freshDigest(owner, binding, owner.initializationSource()), owner.summaryOriginDigest())) {
                throw new IllegalStateException("fresh cold summary changed its original birth binding");
            }
        }
    }

    /** Full original CONTROL frames remain mandatory cold dependencies, independently of stored summary numbers. */
    public static void requireControlInputs(
            TargetCheckpointDependencies dependencies, List<TargetCheckpointDependencies.ControlImage> inputs) {
        final var originals = new HashMap<String, TargetCheckpointDependencies.ControlImage>();
        for (var input : inputs) {
            if (TargetPayloadSummaryInitializationBody.isControl(input.image())) {
                final String at = Bytes.hex(input.source().canonicalBytes());
                if (originals.putIfAbsent(at, input) != null) {
                    throw new IllegalStateException("duplicate original summary initialization Source");
                }
                final var first = Objects.requireNonNull(
                        dependencies.resultRecord(Bytes.concat(
                                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                                input.image().systemMutationId())),
                        "summary input lacks actual first");
                if (first.appliedFirst()) {
                    final var body = TargetPayloadSummaryInitializationBody.decode(
                            input.image().canonicalBody());
                    final var owner = Objects.requireNonNull(
                            dependencies.payloadOwner(
                                    body.request().expectedOwner().messageId()),
                            "APPLIED summary upgrade lost its owner");
                    if (!owner.summaryQualified()
                            || owner.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP
                            || !owner.initializationSource().equals(first.mutation())) {
                        throw new IllegalStateException("APPLIED summary upgrade lost its exact v2 initialization");
                    }
                }
            }
        }
        for (var owner : dependencies.payloadOwners()) {
            if (!owner.summaryQualified() || owner.summaryOrigin() != TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP) {
                continue;
            }
            final var original = Objects.requireNonNull(
                    originals.get(
                            Bytes.hex(owner.initializationSource().source().canonicalBytes())),
                    "old-owner summary lacks its complete original CONTROL input");
            final var body = TargetPayloadSummaryInitializationBody.decode(
                    original.image().canonicalBody());
            body.requireInput(original.image());
            final var request = body.request();
            final var before = request.expectedOwner();
            long count = 0;
            for (var admission : dependencies.admissions()) {
                if (admission.attempt().locator().messageId().equals(owner.messageId())
                        && Long.compareUnsigned(
                                        admission.first().mutation().sequence(),
                                        owner.initializationSource().sequence())
                                < 0
                        && (admission.attempt().resolvedAt() == null
                                || Long.compareUnsigned(
                                                admission.attempt().resolvedAt().sequence(),
                                                owner.initializationSource().sequence())
                                        > 0)) {
                    count = Math.incrementExact(count);
                }
            }
            final var first = Objects.requireNonNull(
                    dependencies.resultRecord(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            original.image().systemMutationId())),
                    "summary initialization original first");
            if (!first.appliedFirst()
                    || !first.mutation().equals(owner.initializationSource())
                    || !Arrays.equals(
                            first.typedPayload(),
                            SystemMutationResult.from(
                                            original.image(),
                                            ApplyStatus.APPLIED,
                                            StableCode.OK,
                                            original.source().canonicalBytes())
                                    .encode())
                    || !Arrays.equals(
                            Bytes.sha256(original.image().canonicalEnvelope()),
                            owner.initializationSource().mutationDigest())
                    || request.unresolvedCount() != count
                    || !Arrays.equals(request.foldDigest(), owner.summaryOriginDigest())
                    || !before.primaryIdentity().equals(owner.primaryIdentity())
                    || !Arrays.equals(before.initialBindingDigest(), owner.initialBindingDigest())
                    || !Arrays.equals(before.payloadSha256(), owner.payloadSha256())
                    || before.length() != owner.length()
                    || !before.accounting().equals(owner.accounting())
                    || !Arrays.equals(before.tenantScope(), owner.tenantScope())
                    || !Arrays.equals(before.recoveryLineage(), owner.recoveryLineage())
                    || TargetQuotaMutation.increment(request.auditedCut().sequence())
                            != owner.initializationSource().sequence()) {
                throw new IllegalStateException(
                        "summary initialization lost its complete original cut/owner/count/Source causality");
            }
            owner.initializationSource().requireAfter(request.auditedCut());
            dependencies.requireSystemPosition(first);
        }
    }

    /** Reconstructs the historical fold from complete protected bytes, without minting a live initialization proof. */
    public static void requireColdProtectedFold(
            TargetCheckpointRootVerifier.AuthenticatedImage image, TargetPayloadSummaryInitializationRequest request) {
        Objects.requireNonNull(image).requireCurrent();
        final var dependencies = image.payloadSummaryDependencies();
        final var owner = Objects.requireNonNull(
                dependencies.payloadOwner(request.expectedOwner().messageId()), "original complete image owner");
        final byte[] manifest = image.manifest().canonicalJsonBytes();
        if (!Arrays.equals(owner.canonicalBytes(), request.expectedOwner().canonicalBytes())
                || !dependencies.root().aggregate().mutation().equals(request.auditedCut())
                || !Arrays.equals(dependencies.root().root().recoveryLineage(), request.lineage())
                || !dependencies.root().root().scope().equals(request.scope())
                || !Arrays.equals(
                        image.manifest().checkpointId(),
                        request.protectedImage().checkpointId())
                || manifest.length != request.protectedImage().manifestLength()
                || !Arrays.equals(
                        Bytes.sha256(manifest), request.protectedImage().manifestSha256())) {
            throw new IllegalStateException("summary historical fold changed its original complete v1 image/cut/Root");
        }
        final long count = Objects.requireNonNull(TargetPayloadObligationSummaryAudit.recompute(dependencies)
                .get(Bytes.hex(owner.messageId().bytes())));
        byte[] digest = Bytes.sha256(DOMAIN, manifest, owner.canonicalBytes());
        final var message = dependencies.messageState(owner.messageId());
        if (message != null) {
            digest = foldDigest(
                    digest, ColumnFamily.ID, TargetKeyCodec.message(owner.messageId()), message.primaryDigest());
        }
        final var terminals = dependencies.terminalGenerations().stream()
                .filter(value -> value.locator().messageId().equals(owner.messageId()))
                .sorted((left, right) -> Arrays.compareUnsigned(left.key(), right.key()))
                .toList();
        for (var terminal : terminals) {
            digest = foldDigest(digest, ColumnFamily.TERMINAL, terminal.key(), Bytes.sha256(terminal.canonicalBytes()));
        }
        final var budgets = dependencies.admissions().stream()
                .map(TargetCheckpointDependencies.Admission::attempt)
                .filter(value -> value.locator().messageId().equals(owner.messageId()))
                .sorted((left, right) -> Arrays.compareUnsigned(left.key(), right.key()))
                .toList();
        for (var budget : budgets) {
            digest = foldDigest(digest, ColumnFamily.META, budget.key(), Bytes.sha256(budget.canonicalBytes()));
        }
        if (count != request.unresolvedCount() || !Arrays.equals(digest, request.foldDigest())) {
            throw new IllegalStateException(
                    "summary historical complete runtime/Budget/Source fold differs from original initialization");
        }
        image.requireCurrent();
    }

    private static byte[] foldDigest(byte[] prior, ColumnFamily family, byte[] key, byte[] fingerprint) {
        return Bytes.sha256(DOMAIN, prior, Bytes.u8(family.ordinal()), key, fingerprint);
    }

    private static void requireFreshGraph(
            TargetStoreBackend.Reader reader,
            TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding,
            TargetQuotaMutation stamp,
            List<TargetStoreBackend.Edit> edits) {
        if (binding.imported()
                || owner.revision() != 1
                || !owner.mutation().equals(stamp)
                || !Arrays.equals(owner.initialBindingDigest(), binding.digest())
                || !owner.messageId().equals(binding.messageId())
                || !Arrays.equals(
                        binding.bindingSource().canonicalBytes(), stamp.source().canonicalBytes())
                || reader.get(ColumnFamily.META, owner.key()) != null
                || reader.get(ColumnFamily.ID, TargetKeyCodec.message(owner.messageId())) != null) {
            throw new IllegalStateException("summary fresh birth is not an absent exact payload identity");
        }
        TargetResultRecord first = null, position = null;
        boolean actualBinding = false;
        for (var edit : edits) {
            if (edit.after() == null) {
                continue;
            }
            final var value = TargetValueEnvelope.decodeAny(edit.after());
            if (edit.family() == ColumnFamily.ID
                    && value.valueType() == TargetScheduleBinding.VALUE_TYPE
                    && Arrays.equals(edit.key(), TargetKeyCodec.scheduleBinding(binding.digest()))) {
                actualBinding = Arrays.equals(value.payload(), binding.canonicalBytes());
            }
            if (edit.family() != ColumnFamily.DEDUPE || value.valueType() != TargetResultRecord.VALUE_TYPE) {
                continue;
            }
            final var result = TargetResultRecord.decode(value.payload());
            if (!result.mutation().equals(stamp)
                    || result.kind() != TargetResultRecord.Kind.COMMAND
                            && result.kind() != TargetResultRecord.Kind.POSITION_COMMAND) {
                continue;
            }
            if (!new CommandId(result.logicalId())
                    .routingId()
                    .shardId()
                    .equals(owner.messageId().routingId().shardId())) {
                continue;
            }
            if (result.kind() == TargetResultRecord.Kind.COMMAND) {
                if (result.appliedFirst()) {
                    first = result;
                }
            } else {
                // POSITION carries the logical ID; its outcome belongs to the exact first record.
                position = result;
            }
        }
        if (first == null || position == null || !actualBinding) {
            throw new IllegalStateException("fresh summary lacks its same-batch binding/first/POS birth");
        }
        position.requireFirst(first);
    }

    private static byte[] freshDigest(
            TargetQuotaPayloadOwner owner, TargetScheduleBinding binding, TargetQuotaMutation at) {
        return Bytes.sha256(
                DOMAIN,
                owner.key(),
                binding.canonicalBytes(),
                at.canonicalBytes(),
                owner.payloadSha256(),
                Bytes.u64beBits(owner.length()),
                owner.accounting().canonicalBytes(),
                owner.tenantScope(),
                owner.recoveryLineage());
    }

    public record Limits(int maximumRecords, long maximumRetainedBytes) {
        public Limits {
            if (maximumRecords < 1
                    || maximumRecords == Integer.MAX_VALUE
                    || maximumRetainedBytes < 1
                    || maximumRetainedBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("payload summary fold needs finite original read/retained limits");
            }
        }
    }

    /** Actual original checkpoint/Source history/Pin hold, retained by the existing maintenance Owned I/O. */
    public interface Protection {
        TargetCheckpointRootVerifier.AuthenticatedImage image();

        void requireImageResource(CheckpointResource resource);

        void requireCurrent();
        /** Cached original paid-loan/revocation checks only: no RPC, Startup monitor, or outer Store lock acquisition. */
        void requireLocalCurrent();
    }

    public static Cursor begin(
            ShardStore store,
            TargetQuotaPayloadOwner owner,
            Protection protection,
            Limits limits,
            BoundedReadBudget budget) {
        Objects.requireNonNull(store);
        Objects.requireNonNull(owner);
        Objects.requireNonNull(protection);
        Objects.requireNonNull(limits);
        if (owner.summaryQualified()) {
            throw new IllegalArgumentException("payload summary already initialized");
        }
        protection.requireCurrent();
        final var before = store.readWithBudget(budget, () -> {
            protection.requireLocalCurrent();
            final byte[] raw =
                    Objects.requireNonNull(store.get(ColumnFamily.META, owner.key()), "original payload owner");
            if (!Arrays.equals(
                    TargetValueEnvelope.decode(raw, TargetQuotaPayloadOwner.VALUE_TYPE)
                            .payload(),
                    owner.canonicalBytes())) {
                throw new IllegalStateException("payload bootstrap changed its original owner");
            }
            return store.metadata();
        });
        final var image = Objects.requireNonNull(protection.image(), "actual protected original complete image");
        final var dependencies = image.payloadSummaryDependencies();
        final var root = dependencies.root();
        if (!Arrays.equals(root.metadata().storeIncarnation(), before.value().storeIncarnation())
                || !root.metadata().shardId().equals(before.value().shardId())
                || !Arrays.equals(
                        root.source().canonicalBytes(),
                        store.appliedShardLogPosition().canonicalBytes())
                || root.mutationSequence() != store.shardMutationSequence()
                || !Arrays.equals(
                        Objects.requireNonNull(dependencies.payloadOwner(owner.messageId()))
                                .canonicalBytes(),
                        owner.canonicalBytes())) {
            throw new IllegalStateException(
                    "payload bootstrap image differs from the actual protected Native cut/owner");
        }
        protection.requireCurrent();
        return store.withReadView(before.view(), () -> {
            store.readWithBudget(budget, () -> {
                protection.requireLocalCurrent();
                final byte[] actual =
                        store.get(ColumnFamily.META, root.aggregate().key());
                if (!Arrays.equals(
                        actual,
                        TargetValueEnvelope.encode(
                                TargetQuotaAggregate.VALUE_TYPE,
                                root.aggregate().canonicalBytes()))) {
                    throw new IllegalStateException(
                            "protected bootstrap image differs from actual local Native accounting cut");
                }
                return Boolean.TRUE;
            });
            return new Cursor(store, before.view(), owner, protection, limits, dependencies, image);
        });
    }

    /** One specific owner over a fixed physical view. A changed view requires a new protected fold. */
    public static final class Cursor {
        private final ShardStore store;
        private final ShardStore.ReadView view;
        private final TargetQuotaPayloadOwner owner;
        private final Protection protection;
        private final Limits limits;
        private final TargetCheckpointDependencies dependencies;
        private final CheckpointManifest manifest;
        private final Map<String, byte[]> remaining = new HashMap<>();
        private final Map<String, TargetCheckpointDependencies.Admission> budgets = new HashMap<>();

        private record RuntimeReference(TargetMessageLocator locator, AttemptObligationRef obligation) {}

        private final Map<String, RuntimeReference> references = new HashMap<>();
        private final Map<TargetMessageLocator, byte[]> generations = new HashMap<>();
        private final Map<String, TargetResultRecord> sourceFirsts = new HashMap<>();
        private byte[] digest;
        private int phase, records;
        private byte[] lower;
        private long retainedBytes, count;
        private boolean failed;
        private Fold completed;

        private Cursor(
                ShardStore store,
                ShardStore.ReadView view,
                TargetQuotaPayloadOwner owner,
                Protection protection,
                Limits limits,
                TargetCheckpointDependencies dependencies,
                TargetCheckpointRootVerifier.AuthenticatedImage image) {
            this.store = store;
            this.view = view;
            this.owner = owner;
            this.protection = protection;
            this.limits = limits;
            this.dependencies = dependencies;
            this.manifest = image.manifest();
            digest = Bytes.sha256(DOMAIN, image.manifest().canonicalJsonBytes(), owner.canonicalBytes());
            chargeRetained(owner.canonicalBytes().length);
            final var message = dependencies.messageState(owner.messageId());
            if (message != null) {
                retain(ColumnFamily.ID, TargetKeyCodec.message(owner.messageId()), message.primaryDigest());
                chargeRetained(message.runtime().canonicalBytes().length);
            }
            for (var terminal : dependencies.terminalGenerations()) {
                if (terminal.locator().messageId().equals(owner.messageId())) {
                    retain(ColumnFamily.TERMINAL, terminal.key(), Bytes.sha256(terminal.canonicalBytes()));
                    chargeRetained(terminal.canonicalBytes().length);
                }
            }
            final var neededSources = new HashSet<String>();
            for (var admission : dependencies.admissions()) {
                if (admission.attempt().locator().messageId().equals(owner.messageId())) {
                    final var budget = admission.attempt();
                    if (budgets.putIfAbsent(Bytes.hex(budget.key()), admission) != null) {
                        throw new IllegalStateException("bootstrap image repeats a Budget");
                    }
                    retain(ColumnFamily.META, budget.key(), Bytes.sha256(budget.canonicalBytes()));
                    chargeRetained(
                            budget.canonicalBytes().length + admission.first().canonicalBytes().length);
                    if (budget.resolvedAt() != null) {
                        neededSources.add(Bytes.hex(budget.resolvedAt().canonicalBytes()));
                    } else if (budget.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                        neededSources.add(Bytes.hex(budget.mutation().canonicalBytes()));
                    }
                }
            }
            for (var first : dependencies.resultRecords()) {
                if (first.kind() == TargetResultRecord.Kind.SYSTEM
                        && neededSources.contains(Bytes.hex(first.mutation().canonicalBytes()))) {
                    sourceFirsts.put(Bytes.hex(first.mutation().canonicalBytes()), first);
                    chargeRetained(first.canonicalBytes().length);
                }
            }
        }

        private void retain(ColumnFamily family, byte[] key, byte[] fingerprint) {
            retainedBytes = Math.addExact(retainedBytes, Math.addExact(key.length, fingerprint.length));
            if (retainedBytes > limits.maximumRetainedBytes()
                    || remaining.putIfAbsent(token(family, key), fingerprint) != null) {
                throw new IllegalStateException(
                        "payload bootstrap exceeds its held input budget or repeats a dependency");
            }
        }

        private void chargeRetained(long bytes) {
            retainedBytes = Math.addExact(retainedBytes, bytes);
            if (retainedBytes > limits.maximumRetainedBytes()) {
                throw new IllegalStateException("payload bootstrap exceeds original held input bytes");
            }
        }

        public Optional<Fold> advance(BoundedReadBudget budget, int maximumRows) {
            if (failed || maximumRows < 1 || maximumRows > limits.maximumRecords()) {
                throw new IllegalStateException("invalid or failed payload summary fold");
            }
            if (completed != null) {
                completed.requireCurrent();
                return Optional.of(completed);
            }
            protection.requireCurrent();
            store.withReadView(view, () -> null);
            try {
                store.readWithBudget(budget, () -> {
                    protection.requireLocalCurrent();
                    final ColumnFamily family = family();
                    final byte tag = tag();
                    final byte[] start = lower == null ? new byte[] {tag, TargetKeyCodec.KEY_FORMAT} : lower;
                    final var result = store.visitResult(
                            family,
                            start,
                            new byte[] {tag, (byte) (TargetKeyCodec.KEY_FORMAT + 1)},
                            maximumRows,
                            budget,
                            (row, shared) -> {
                                if (++records > limits.maximumRecords()) {
                                    throw new IllegalStateException(
                                            "payload bootstrap total inventory exceeds original limit");
                                }
                                consume(family, row);
                                lower = Bytes.concat(row.key(), new byte[] {0});
                                protection.requireLocalCurrent();
                                return true;
                            });
                    if (result.stop() == ShardStore.VisitStop.RANGE_END) {
                        phase++;
                        lower = null;
                    }
                    return Boolean.TRUE;
                });
                protection.requireCurrent();
                return store.withReadView(view, () -> {
                    if (phase != 3) {
                        return Optional.empty();
                    }
                    if (!remaining.isEmpty() || !references.isEmpty()) {
                        throw new IllegalStateException("payload fold omitted a protected historical dependency");
                    }
                    completed = new Fold(
                            store,
                            view,
                            owner,
                            dependencies.root().aggregate().mutation(),
                            count,
                            digest,
                            protection,
                            manifest);
                    return Optional.of(completed);
                });
            } catch (RuntimeException | Error failure) {
                failed = true;
                throw failure;
            }
        }

        private ColumnFamily family() {
            return phase == 0 ? ColumnFamily.ID : phase == 1 ? ColumnFamily.TERMINAL : ColumnFamily.META;
        }

        private byte tag() {
            return (byte)
                    (phase == 0
                            ? TargetKeyCodec.MESSAGE_TAG
                            : phase == 1
                                    ? TargetKeyCodec.TERMINAL_GENERATION_TAG
                                    : TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG);
        }

        private void consume(ColumnFamily family, ShardStore.KeyValue row) {
            final var value = TargetValueEnvelope.decodeAny(row.value());
            TargetMessageLocator locator;
            TargetGenerationRuntimeIndex runtime;
            if (phase == 0) {
                final var message = TargetMessageRecord.decodeForStore(row.key(), value.payload(), store.shardId());
                locator = message.locator();
                runtime = message.runtime();
            } else if (phase == 1) {
                final var terminal = TargetTerminalGenerationRecord.decode(value.payload());
                if (!Arrays.equals(terminal.key(), row.key())) {
                    throw new IllegalStateException("payload bootstrap terminal key changed");
                }
                locator = terminal.locator();
                runtime = terminal.runtime();
            } else {
                final var actual = TargetQuotaAttemptBudget.decodeForStore(row.key(), value.payload(), store.shardId());
                if (!actual.locator().messageId().equals(owner.messageId())) {
                    return;
                }
                final var admission = Objects.requireNonNull(
                        budgets.get(Bytes.hex(row.key())), "live Budget absent from original protected image");
                requireActualFirst(admission.first());
                if (actual.resolvedAt() != null) {
                    requireActualFirst(Objects.requireNonNull(
                            sourceFirsts.get(Bytes.hex(actual.resolvedAt().canonicalBytes()))));
                } else if (actual.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                    requireActualFirst(Objects.requireNonNull(
                            sourceFirsts.get(Bytes.hex(actual.mutation().canonicalBytes()))));
                }
                requireSelected(family, row, value.payload());
                final var reference = references.remove(Bytes.hex(actual.publishAttemptId()));
                final boolean open = actual.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                        || actual.phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
                if (open != (reference != null)
                        || reference != null
                                && (!reference.locator().equals(actual.locator())
                                        || reference.obligation().ledgerState()
                                                != (actual.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                                                        ? AttemptLedgerState.PUBLISHING
                                                        : AttemptLedgerState.UNCERTAIN))) {
                    throw new IllegalStateException("bootstrap unresolved Budget/runtime mismatch");
                }
                if (open) {
                    count = Math.incrementExact(count);
                }
                return;
            }
            if (!locator.messageId().equals(owner.messageId())) {
                return;
            }
            requireSelected(family, row, value.payload());
            final var prior = generations.putIfAbsent(locator, runtime.runtimeDigest());
            if (prior != null) {
                if (!Arrays.equals(prior, runtime.runtimeDigest())) {
                    throw new IllegalStateException("bootstrap current/terminal mirrors disagree");
                }
                return;
            }
            for (var reference : runtime.attemptObligations()) {
                retainedBytes = Math.addExact(retainedBytes, reference.canonicalBytes().length);
                if (retainedBytes > limits.maximumRetainedBytes()
                        || references.putIfAbsent(
                                        Bytes.hex(reference.publishAttemptId()),
                                        new RuntimeReference(locator, reference))
                                != null) {
                    throw new IllegalStateException(
                            "bootstrap obligation duplicated or exceeds original held input budget");
                }
            }
        }

        private void requireSelected(ColumnFamily family, ShardStore.KeyValue row, byte[] payload) {
            final var expected = Objects.requireNonNull(
                    remaining.remove(token(family, row.key())), "live dependency absent from the protected image");
            if (!Arrays.equals(expected, Bytes.sha256(payload))) {
                throw new IllegalStateException("live payload dependency changed since its protected image");
            }
            digest = foldDigest(digest, family, row.key(), expected);
        }

        private void requireActualFirst(TargetResultRecord first) {
            if (!Arrays.equals(
                    store.get(ColumnFamily.DEDUPE, first.key()),
                    TargetValueEnvelope.encode(TargetResultRecord.VALUE_TYPE, first.canonicalBytes()))) {
                throw new IllegalStateException("bootstrap lost its original actual Source first");
            }
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    first.mutation().source().canonicalBytes());
            final var position =
                    Objects.requireNonNull(dependencies.resultRecord(key), "protected original Source POS");
            if (!Arrays.equals(
                    store.get(ColumnFamily.DEDUPE, key),
                    TargetValueEnvelope.encode(TargetResultRecord.VALUE_TYPE, position.canonicalBytes()))) {
                throw new IllegalStateException("bootstrap lost its original actual Source POS");
            }
        }

        private static String token(ColumnFamily family, byte[] key) {
            return family.ordinal() + "/" + Bytes.hex(key);
        }
    }

    /** The full protected fold and exact live view are owned until the initialization Source batch commits. */
    public static final class Fold {
        private final ShardStore store;
        private final ShardStore.ReadView view;
        private final TargetQuotaPayloadOwner owner;
        private final TargetQuotaMutation cut;
        private final long count;
        private final byte[] digest;
        private final Runnable current, localCurrent;
        private final Protection protection;
        private final CheckpointManifest manifest;

        private Fold(
                ShardStore store,
                ShardStore.ReadView view,
                TargetQuotaPayloadOwner owner,
                TargetQuotaMutation cut,
                long count,
                byte[] digest,
                Protection protection,
                CheckpointManifest manifest) {
            this.store = store;
            this.view = view;
            this.owner = owner;
            this.cut = cut;
            this.count = count;
            this.digest = Bytes.copy(digest);
            this.protection = protection;
            this.manifest = manifest;
            this.current = protection::requireCurrent;
            this.localCurrent = protection::requireLocalCurrent;
        }

        public TargetQuotaPayloadOwner owner() {
            return owner;
        }

        public TargetQuotaMutation cut() {
            return cut;
        }

        public long unresolvedCount() {
            return count;
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public void requireCurrent() {
            current.run();
            store.withReadView(view, () -> null);
            current.run();
        }

        public void requireLocalCurrent() {
            localCurrent.run();
        }
        /** Local consumption by the exact separately authorized legacy event; this never upgrades Native32. */
        public void requireEventView(TargetStoreBackend.Reader reader) {
            if (!currentReader(reader)) {
                throw new ViewChanged();
            }
        }

        public void requireRequest(TargetPayloadSummaryInitializationRequest request) {
            requireCurrent();
            requireOriginalRequest(request);
            requireCurrent();
        }
        /** Keeps the same immutable issued request after a concurrent Native change; only Source may decide STALE. */
        public void requireArchivedRequest(TargetPayloadSummaryInitializationRequest request) {
            requireOriginalRequest(request);
        }

        void requireOriginalRequest(TargetPayloadSummaryInitializationRequest request) {
            current.run();
            protection.requireImageResource(request.protectedImage());
            final byte[] encoded = manifest.canonicalJsonBytes();
            if (!Arrays.equals(owner.canonicalBytes(), request.expectedOwner().canonicalBytes())
                    || !cut.equals(request.auditedCut())
                    || count != request.unresolvedCount()
                    || !Arrays.equals(digest, request.foldDigest())
                    || !Arrays.equals(
                            Bytes.sha256(encoded), request.protectedImage().manifestSha256())
                    || encoded.length != request.protectedImage().manifestLength()
                    || !Arrays.equals(
                            manifest.checkpointId(), request.protectedImage().checkpointId())) {
                throw new IllegalStateException(
                        "registered summary initialization differs from the actual protected complete fold");
            }
            current.run();
        }

        boolean currentReader(TargetStoreBackend.Reader reader) {
            localCurrent.run();
            try {
                store.requirePayloadSummaryReadView(view);
            } catch (ViewChanged changed) {
                return false;
            }
            return Thread.holdsLock(store)
                    && reader.metadata().equals(store.metadata())
                    && reader.aggregate().mutation().equals(cut)
                    && Arrays.equals(
                            reader.get(ColumnFamily.META, owner.key()),
                            TargetValueEnvelope.encode(TargetQuotaPayloadOwner.VALUE_TYPE, owner.canonicalBytes()));
        }

        private void requireReader(TargetStoreBackend.Reader reader, TargetQuotaMutation at) {
            localCurrent.run();
            store.requirePayloadSummaryReadView(view);
            if (!Thread.holdsLock(store)
                    || !reader.metadata().equals(store.metadata())
                    || !reader.aggregate().mutation().equals(cut)
                    || !Arrays.equals(
                            reader.get(ColumnFamily.META, owner.key()),
                            TargetValueEnvelope.encode(TargetQuotaPayloadOwner.VALUE_TYPE, owner.canonicalBytes()))) {
                throw new IllegalStateException("payload initialization changed its original Store/owner/cut");
            }
            at.requireAfter(cut);
        }

        Qualification forSource(TargetStoreBackend.Reader reader, TargetQuotaMutation at) {
            requireReader(reader, at);
            return new Qualification(owner, at, TargetQuotaPayloadOwner.SummaryOrigin.BOOTSTRAP, count, digest, this);
        }
    }
}
