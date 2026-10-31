package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetMigrationActivateRequest;
import com.nereusstream.delay.protocol.TargetMigrationActivationAttempt;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import com.nereusstream.delay.protocol.TargetMigrationCustodyGuard;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRequest;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization;
import com.nereusstream.delay.protocol.TargetMigrationSettlementRequest;
import com.nereusstream.delay.protocol.TargetProtocolReaderSet;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Explicit import-baseline interpretation and the only migration Source activation/settlement writer. */
public final class TargetMigrationStore {
    private TargetMigrationStore() {}

    @FunctionalInterface
    public interface Keys {
        PublicKey resolve(long version, SourcePosition source);
    }
    /** Complete original content/reader/freeze/custody authority held by the original process recovery root.
     * Authentication runs outside Native locks. The returned check is the same acquisition's local token;
     * it must not perform RPC or wait for external I/O under a Store monitor. */
    @FunctionalInterface
    public interface OriginalInputs {
        /** Actual independent deployment/management policy and complete original issuer/succession history.
         * Must authenticate statement issuer ID/version/purpose/allowed Root tuples under this held authority;
         * an A signer, embedded key, or a hash of unavailable policy is not an issuer pin. */
        default PublicKey originalRootIssuer(
                TargetMigrationRootIssuerAuthorization statement,
                TargetMigrationPlan plan,
                PreparedControlOperation prepared,
                SystemMutation exactInput,
                SourcePosition actualSource) {
            throw new IllegalStateException("A2 lacks its original independent management Root-issuer policy");
        }
        /**
         * Reads the complete immutable plan.readerEvidence object through the original owned I/O acquisition.
         * The same authenticate hold must authenticate its exact object identity/version, plan/Source/Store/lineage,
         * complete eligible revisions/sessions and actual binary/SDK locks, and retain every pin through commit.
         * Returning a caller-created reader DTO or an unprotected current membership is insufficient.
         */
        default TargetProtocolReaderSet parentControlCreditReaders(
                TargetMigrationRootIssuerAuthorization statement,
                TargetMigrationPlan plan,
                PreparedControlOperation prepared,
                SystemMutation exactInput,
                SourcePosition actualSource) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "policy2 lacks its original authenticated pre-A reader object and held membership");
        }

        Runnable authenticate(
                PreparedControlOperation prepared,
                SystemMutation input,
                SourcePosition source,
                TargetMigrationRequest request);
    }

    @FunctionalInterface
    public interface RootIssuerPolicy {
        PublicKey authenticate(
                TargetMigrationRootIssuerAuthorization statement,
                TargetMigrationPlan plan,
                PreparedControlOperation registered,
                SystemMutation exactInput,
                SourcePosition actualSource);
    }
    /** Adds the genuine original management issuer purpose to the same original migration hold. */
    public static OriginalInputs withRootIssuerPolicy(OriginalInputs original, RootIssuerPolicy policy) {
        Objects.requireNonNull(original);
        Objects.requireNonNull(policy);
        return new OriginalInputs() {
            public Runnable authenticate(
                    PreparedControlOperation control,
                    SystemMutation input,
                    SourcePosition source,
                    TargetMigrationRequest request) {
                return original.authenticate(control, input, source, request);
            }

            public PublicKey originalRootIssuer(
                    TargetMigrationRootIssuerAuthorization statement,
                    TargetMigrationPlan plan,
                    PreparedControlOperation prepared,
                    SystemMutation input,
                    SourcePosition source) {
                return policy.authenticate(statement, plan, prepared, input, source);
            }

            public TargetProtocolReaderSet parentControlCreditReaders(
                    TargetMigrationRootIssuerAuthorization statement,
                    TargetMigrationPlan plan,
                    PreparedControlOperation prepared,
                    SystemMutation input,
                    SourcePosition source) {
                return original.parentControlCreditReaders(statement, plan, prepared, input, source);
            }
        };
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            Keys keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,
            OriginalInputs inputs) {
        public Authority {
            Objects.requireNonNull(registrations);
            Objects.requireNonNull(keys);
            Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof);
            Objects.requireNonNull(inputs);
        }
    }

    public static final class PreparedActivation {
        private final TargetStoreBackend backend;
        private final TargetStoreBackend.Prepared batch;
        private final TargetMigrationImporter.PreparedCandidate candidate;
        private final TargetMigrationRoot root;
        private final SystemMutationResult result;
        private final Runnable current;

        private PreparedActivation(
                TargetStoreBackend backend,
                TargetStoreBackend.Prepared batch,
                TargetMigrationImporter.PreparedCandidate candidate,
                TargetMigrationRoot root,
                SystemMutationResult result,
                Runnable current) {
            this.backend = backend;
            this.batch = batch;
            this.candidate = candidate;
            this.root = root;
            this.result = result;
            this.current = current;
        }
    }

    public static final class PreparedSettlement {
        private final TargetStoreBackend backend;
        private final TargetStoreBackend.Prepared batch;
        private final Runnable current;
        private final SystemMutationResult result;

        private PreparedSettlement(
                TargetStoreBackend backend,
                TargetStoreBackend.Prepared batch,
                Runnable current,
                SystemMutationResult result) {
            this.backend = backend;
            this.batch = batch;
            this.current = current;
            this.result = result;
        }
    }
    /** Actual Native A committed. This does not replace post-A history/Pin/reader/Owner qualification. */
    public static final class Activated {
        private final PreparedActivation committed;

        private Activated(PreparedActivation committed) {
            this.committed = committed;
        }

        public TargetStoreBackend backend() {
            return committed.backend;
        }

        public TargetQuotaIncarnation root() {
            return committed.root.owner();
        }

        public TargetMigrationRoot migration() {
            return committed.root;
        }

        public SystemMutationResult result() {
            return committed.result;
        }

        public TargetMigrationImporter.PreparedCandidate candidate() {
            return committed.candidate;
        }

        public void requireCurrent() {
            final var candidate = committed.candidate;
            candidate.requireExternalCurrent();
            committed.current.run();
            final long bytes = Math.addExact(
                    (long) TargetMigrationRoot.MAX_CANONICAL_BYTES,
                    Math.addExact(
                            2L * TargetResultRecord.MAX_CANONICAL_BYTES,
                            4L * TargetQuotaIncarnation.MAX_CANONICAL_BYTES));
            final var budget = new BoundedReadBudget(
                    32, bytes, java.time.Duration.ofSeconds(5).toNanos(), System::nanoTime);
            final var read = committed.backend.prepareRead(budget, reader -> {
                candidate.requireStore(candidate.store());
                final var actual = requireActivated(
                        reader,
                        List.of(),
                        candidate.plan().scope(),
                        candidate.plan().newLineage(),
                        reader.aggregate().mutation());
                if (!Arrays.equals(actual.canonicalBytes(), committed.root.canonicalBytes())) {
                    throw new IllegalStateException("activated migration changed its exact original A/root");
                }
                return Boolean.TRUE;
            });
            candidate.requireExternalCurrent();
            committed.backend.completeRead(read, (metadata, scope) -> new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    candidate.requireStore(candidate.store());
                }

                @Override
                public void close() {}
            });
        }
    }

    public static PreparedActivation prepareActivation(
            TargetMigrationImporter.PreparedCandidate candidate,
            TargetMigrationImporter.Limits limits,
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition actualA,
            Authority authority) {
        Objects.requireNonNull(candidate);
        Objects.requireNonNull(limits);
        candidate.requireExternalCurrent();
        candidate.requireStore(candidate.store());
        final var plan = candidate.plan();
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationActivateRequest request)
                || !request.plan().equals(plan)
                || !request.candidate().equals(candidate.candidate())) {
            throw new IllegalArgumentException("ACTIVATE changed exact sealed migration candidate");
        }
        final Runnable current = authenticate(control, input, actualA, body, authority);
        final var backend = TargetStoreBackend.forMigration(
                candidate.store(),
                plan.scope(),
                candidate.root().identity().accountingIncarnation(),
                plan.newLineage(),
                limits.writes(),
                candidate);
        final TargetMigrationRoot[] next = {null};
        final SystemMutationResult[] outcome = {null};
        final var batch = backend.prepare(budget, reader -> {
            current.run();
            final var prior = requirePrepared(candidate.store(), plan);
            final var attempt =
                    Objects.requireNonNull(readAttempt(reader, plan), "durable before-SDK exact activation attempt");
            if (!Arrays.equals(attempt.control().canonicalBytes(), control.canonicalBytes())
                    || !Arrays.equals(attempt.input().canonicalEnvelope(), input.canonicalEnvelope())
                    || !attempt.candidate().equals(candidate.candidate())) {
                throw new IllegalStateException("actual A changed its original durable append intent");
            }
            if (reader.source() == null
                    || actualA.compareTo(reader.source()) <= 0
                    || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()) {
                throw new IllegalStateException(
                        "migration A must follow the full actual old-domain prefix and remain unexpired");
            }
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    actualA,
                    Bytes.sha256(input.canonicalEnvelope()));
            next[0] = prior.activate(input, stamp);
            outcome[0] = SystemMutationResult.from(input, ApplyStatus.APPLIED, StableCode.OK, actualA.canonicalBytes());
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            edits.add(reader.replace(
                    ColumnFamily.META, next[0].key(), TargetMigrationRoot.VALUE_TYPE, next[0].canonicalBytes()));
            appendResults(reader, edits, candidate.root(), input, outcome[0], stamp);
            return new TargetSourceAccounting(
                            plan.scope(),
                            plan.newLineage(),
                            actualA,
                            stamp.mutationDigest(),
                            limits.maximumCounters(),
                            limits.maximumTargets(),
                            limits.maximumDomains())
                    .assemble(reader, edits);
        });
        return new PreparedActivation(backend, batch, candidate, next[0], outcome[0], current);
    }

    public static Activated commitActivation(
            PreparedActivation prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared);
        prepared.candidate.requireExternalCurrent();
        prepared.backend.commit(prepared.batch, guarded(authority, () -> {
            prepared.candidate.requireStore(prepared.candidate.store());
            prepared.current.run();
        }));
        return new Activated(prepared);
    }

    public static Activated resumeActivated(
            TargetMigrationImporter.PreparedCandidate candidate,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition actualA,
            Authority authority,
            BoundedReadBudget budget) {
        candidate.requireExternalCurrent();
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationActivateRequest request)
                || !request.plan().equals(candidate.plan())
                || !request.candidate().equals(candidate.candidate())) {
            throw new IllegalArgumentException("recovery changed original migration ACTIVATE indexes");
        }
        final var current = authenticate(control, input, actualA, body, authority);
        final var backend = TargetStoreBackend.forMigration(
                candidate.store(),
                candidate.plan().scope(),
                candidate.root().identity().accountingIncarnation(),
                candidate.plan().newLineage(),
                candidate.importerLimits().writes(),
                candidate);
        final var read = backend.prepareRead(budget, reader -> {
            current.run();
            final var migration = requireActivated(
                    reader,
                    List.of(),
                    candidate.plan().scope(),
                    candidate.plan().newLineage(),
                    reader.aggregate().mutation());
            if (!Arrays.equals(migration.activation().canonicalEnvelope(), input.canonicalEnvelope())
                    || !Arrays.equals(migration.activatedAt().source().canonicalBytes(), actualA.canonicalBytes())) {
                throw new IllegalStateException("actual Root/first/POS disagree with recovered exact A");
            }
            return migration;
        });
        final var migration = backend.completeRead(read, (metadata, scope) -> new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                current.run();
                candidate.requireStore(candidate.store());
            }

            @Override
            public void close() {}
        });
        return new Activated(new PreparedActivation(
                backend,
                null,
                candidate,
                migration,
                SystemMutationResult.from(input, ApplyStatus.APPLIED, StableCode.OK, actualA.canonicalBytes()),
                current));
    }

    /** Replays actual physical occurrences of the one exact A frame; only the original first changes the Root. */
    public static SystemMutationResult applyActivationDuplicate(
            Activated original,
            SourceReplayMutation delivered,
            BoundedReadBudget budget,
            TargetStoreBackend.CommitAuthority commits) {
        original.requireCurrent();
        if (!Arrays.equals(
                delivered.mutation().canonicalEnvelope(),
                original.migration().activation().canonicalEnvelope())) {
            throw new IllegalArgumentException("migration physical replay changed original exact A frame");
        }
        final var candidate = original.candidate();
        final var backend = original.backend();
        final var prior = backend.prepareRead(budget, reader -> {
            if (delivered.position().compareTo(reader.source()) > 0) {
                return Boolean.FALSE;
            }
            final byte[] firstKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1},
                    delivered.mutation().systemMutationId());
            final var first = TargetResultRecord.decode(
                    TargetValueEnvelope.decode(reader.get(ColumnFamily.DEDUPE, firstKey), TargetResultRecord.VALUE_TYPE)
                            .payload());
            final byte[] positionKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                    delivered.position().canonicalBytes());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            reader.get(ColumnFamily.DEDUPE, positionKey), TargetResultRecord.VALUE_TYPE)
                    .payload());
            position.requireStored(positionKey, TargetResultRecord.VALUE_TYPE, position.canonicalBytes());
            position.requireFirst(first);
            position.requireOwner(original.root());
            if (!Arrays.equals(
                    position.mutation().mutationDigest(),
                    Bytes.sha256(delivered.mutation().canonicalEnvelope()))) {
                throw new IllegalStateException("historical physical A replay lost exact Native frame/POS");
            }
            position.mutation().requireAtOrBefore(reader.aggregate().mutation());
            return Boolean.TRUE;
        });
        final TargetStoreBackend.ReadAuthority reads = (metadata, scope) -> new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                candidate.requireStore(candidate.store());
                original.committed.current.run();
            }

            @Override
            public void close() {}
        };
        candidate.requireExternalCurrent();
        if (backend.completeRead(prior, reads)) {
            return original.result();
        }
        final var limits = candidate.importerLimits();
        final var replay = new TargetSystemReplayStore(
                backend,
                candidate.plan().scope(),
                candidate.plan().newLineage(),
                limits.maximumCounters(),
                limits.maximumDomains());
        final var prepared = replay.prepareIfPresent(budget, delivered.mutation(), delivered.position())
                .orElseThrow(() -> new IllegalStateException("migration A replay has no actual original first"));
        candidate.requireExternalCurrent();
        return replay.commit(
                prepared,
                guarded(commits, () -> {
                    candidate.requireStore(candidate.store());
                    original.committed.current.run();
                }),
                reads);
    }

    /** Reads a recovered exact A/first/POS after an unknown native response; never manufactures activation. */
    public static TargetMigrationRoot requireActivated(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation frontier) {
        final byte[] key = TargetMigrationRoot.keyFor(scope.shard());
        final var root = decodeRoot(view.projected(ColumnFamily.META, key, overlay));
        if (root == null
                || !root.activated()
                || !root.plan().scope().equals(scope)
                || !Arrays.equals(root.plan().newLineage(), lineage)) {
            throw new IllegalStateException("normal Target requires the actual migration Source A");
        }
        requireApplied(view, overlay, root.owner(), root.activation(), root.activatedAt(), frontier);
        return root;
    }

    /** Local Native fact only. The existing process/read root must retain Owner/Pin/history authority around it. */
    public static TargetMigrationRoot readNative(ShardStore store, BoundedReadBudget budget) {
        final var read = store.readWithBudget(budget, () -> {
            final var root = decodeRoot(store.get(ColumnFamily.META, TargetMigrationRoot.keyFor(store.shardId())));
            if (root == null) {
                return null;
            }
            if (!root.activated()) {
                throw new IllegalStateException("MIGRATION_PREPARED is not adopted/serviceable Native state");
            }
            final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
                @Override
                public ShardId shardId() {
                    return store.shardId();
                }

                @Override
                public byte[] projected(ColumnFamily family, byte[] key, List<TargetStoreBackend.Edit> overlay) {
                    if (!overlay.isEmpty()) {
                        throw new IllegalArgumentException("Native migration inspection has no overlay");
                    }
                    return store.get(family, key);
                }
            };
            final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    TargetQuotaAggregate.genesis(
                                                    store.shardId(),
                                                    root.owner().identity().accountingIncarnation())
                                            .key()),
                            TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            if (aggregate.mutation() == null
                    || aggregate.mutation().sequence() != store.shardMutationSequence()
                    || !Arrays.equals(
                            aggregate.mutation().source().canonicalBytes(),
                            store.appliedShardLogPosition().canonicalBytes())) {
                throw new IllegalStateException(
                        "migration Native root disagrees with actual Source accounting frontier");
            }
            return requireActivated(
                    view, List.of(), root.plan().scope(), root.plan().newLineage(), aggregate.mutation());
        });
        return store.withReadView(read.view(), read::value);
    }

    public static void requireSendAllowed(TargetStoreBackend.Reader reader, DelayMessageId id, int generation) {
        final var migration = decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId())));
        if (migration != null && !migration.activated()) {
            throw new IllegalStateException("MIGRATION_PREPARED cannot send Target work");
        }
        final byte[] raw = reader.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(id));
        if (raw != null) {
            TargetMigrationCustodyGuard.decode(TargetValueEnvelope.decode(raw, TargetMigrationCustodyGuard.VALUE_TYPE)
                            .payload())
                    .requireSendAllowed(id, generation);
        }
    }

    /** No other mutation/GC is allowed to remove a guard or change its original custody fields. */
    public static void requireGuardEdits(
            TargetStoreBackend.Reader reader, List<TargetStoreBackend.Edit> edits, TargetQuotaMutation source) {
        for (var edit : edits) {
            if (edit.family() == ColumnFamily.META && edit.key().length > 2) {
                final int tag = Byte.toUnsignedInt(edit.key()[0]);
                if (tag == TargetKeyCodec.MIGRATION_ORIGIN_TAG || tag == TargetKeyCodec.MIGRATION_PREFIX_TAG) {
                    if (edit.before() != null || edit.after() == null) {
                        throw new IllegalStateException("migration origin/prefix history is immutable and retained");
                    }
                    final var root =
                            decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId())));
                    if (root == null
                            || reader.migrationCandidate() == null
                            || !reader.migrationCandidate().plan().equals(root.plan())) {
                        throw new IllegalStateException(
                                "only original held maintenance migration may add import history");
                    }
                    if (tag == TargetKeyCodec.MIGRATION_PREFIX_TAG) {
                        final var record = TargetMigrationPrefixRecord.decode(
                                TargetValueEnvelope.decode(edit.after(), TargetMigrationPrefixRecord.VALUE_TYPE)
                                        .payload());
                        record.requirePlan(root.plan());
                        if (!record.at().equals(source)) {
                            throw new IllegalStateException("prefix history changed actual Source stamp");
                        }
                    }
                } else if (tag == TargetKeyCodec.MIGRATION_ROOT_TAG) {
                    if (edit.before() == null || edit.after() == null) {
                        throw new IllegalStateException("Source cannot remove or invent migration root");
                    }
                    final var before = decodeRoot(edit.before());
                    final var after = decodeRoot(edit.after());
                    if (before.activated()
                            || !after.activated()
                            || !before.plan().equals(after.plan())
                            || !after.activatedAt().equals(source)) {
                        throw new IllegalStateException("migration root only advances once at exact actual A");
                    }
                }
            }
        }
        for (var edit : edits) {
            if (edit.family() == ColumnFamily.META
                    && edit.key().length > 2
                    && Byte.toUnsignedInt(edit.key()[0]) == TargetKeyCodec.MIGRATION_CUSTODY_GUARD_TAG) {
                if (edit.before() == null && edit.after() != null) {
                    final var migration =
                            decodeRoot(reader.get(ColumnFamily.META, TargetMigrationRoot.keyFor(reader.shardId())));
                    final var guard = TargetMigrationCustodyGuard.decode(
                            TargetValueEnvelope.decode(edit.after(), TargetMigrationCustodyGuard.VALUE_TYPE)
                                    .payload());
                    if (migration == null
                            || reader.migrationCandidate() == null
                            || !reader.migrationCandidate().plan().equals(migration.plan())
                            || guard.settled()) {
                        throw new IllegalStateException("only original held maintenance prefix can add legacy custody");
                    }
                    requireGuard(migration.plan(), guard);
                    continue;
                }
                if (edit.before() == null || edit.after() == null) {
                    throw new IllegalStateException("normal Source cannot create or delete migration custody");
                }
                final var before = TargetMigrationCustodyGuard.decode(
                        TargetValueEnvelope.decode(edit.before(), TargetMigrationCustodyGuard.VALUE_TYPE)
                                .payload());
                final var after = TargetMigrationCustodyGuard.decode(
                        TargetValueEnvelope.decode(edit.after(), TargetMigrationCustodyGuard.VALUE_TYPE)
                                .payload());
                if (before.settled()
                        || !after.settled()
                        || !after.settledAt().equals(source)
                        || !Arrays.equals(
                                before.digest(),
                                ((TargetMigrationSettlementRequest) TargetMigrationControlBody.decode(
                                                        after.settlement().canonicalBody())
                                                .request())
                                        .expectedGuardDigest())) {
                    throw new IllegalStateException("custody changes require the exact Source settlement transition");
                }
            }
        }
    }

    public static PreparedSettlement prepareSettlement(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetMigrationImporter.Limits limits,
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            Authority authority,
            TargetMigrationCustodyGuard.SettlementAuthority terminal) {
        return prepareSettlement(
                backend, scope, lineage, limits, budget, control, input, source, authority, terminal, null);
    }

    public static PreparedSettlement prepareSettlement(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetMigrationImporter.Limits limits,
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            Authority authority,
            TargetMigrationCustodyGuard.SettlementAuthority terminal,
            com.nereusstream.delay.ownership.TargetPayloadSummaryLegacyPrefix.EventPermit legacyPrefix) {
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationSettlementRequest request)
                || !request.scope().equals(scope)) {
            throw new IllegalArgumentException("expected exact migration custody settlement");
        }
        final var current = authenticate(control, input, source, body, authority);
        final var result =
                SystemMutationResult.from(input, ApplyStatus.APPLIED, StableCode.OK, source.canonicalBytes());
        final var batch = backend.prepare(budget, reader -> {
            if (legacyPrefix != null) {
                legacyPrefix.requireCurrent(reader, input, source);
            }
            current.run();
            if (reader.source() == null
                    || source.compareTo(reader.source()) <= 0
                    || reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()) {
                throw new IllegalStateException("custody settlement must be an unexpired later actual Source input");
            }
            final var migration = requireActivated(
                    reader, List.of(), scope, lineage, reader.aggregate().mutation());
            final byte[] raw = Objects.requireNonNull(
                    reader.get(ColumnFamily.META, TargetMigrationCustodyGuard.keyFor(request.messageId())),
                    "original held legacy custody");
            final var prior = TargetMigrationCustodyGuard.decode(
                    TargetValueEnvelope.decode(raw, TargetMigrationCustodyGuard.VALUE_TYPE)
                            .payload());
            requireGuard(migration.plan(), prior);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(input.canonicalEnvelope()));
            final var next = prior.settle(input, stamp, terminal);
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            edits.add(reader.replace(
                    ColumnFamily.META, next.key(), TargetMigrationCustodyGuard.VALUE_TYPE, next.canonicalBytes()));
            retainSettledPayload(reader, scope, lineage, next, stamp, edits, legacyPrefix);
            appendResults(reader, edits, migration.owner(), input, result, stamp);
            final var plainAccounting = new TargetSourceAccounting(
                    scope,
                    lineage,
                    source,
                    stamp.mutationDigest(),
                    limits.maximumCounters(),
                    limits.maximumTargets(),
                    limits.maximumDomains());
            return (legacyPrefix == null ? plainAccounting : plainAccounting.withLegacyPrefix(legacyPrefix))
                    .assemble(reader, edits);
        });
        return new PreparedSettlement(backend, batch, current, result);
    }

    /** The last old responsibility may end after the final Native attempt already resolved. */
    private static void retainSettledPayload(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetMigrationCustodyGuard settled,
            TargetQuotaMutation stamp,
            List<TargetStoreBackend.Edit> edits,
            com.nereusstream.delay.ownership.TargetPayloadSummaryLegacyPrefix.EventPermit legacyPrefix) {
        if (!settled.settled() || !settled.settledAt().equals(stamp)) {
            throw new IllegalStateException("payload retention requires this actual legacy settlement Source");
        }
        final byte[] ownerKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                settled.messageId().bytes());
        final byte[] ownerRaw = reader.get(ColumnFamily.META, ownerKey);
        if (ownerRaw == null) {
            return;
        }
        final var owner = TargetQuotaPayloadOwner.decodeForStore(
                ownerKey,
                TargetValueEnvelope.decode(ownerRaw, TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(owner.recoveryLineage(), lineage)) {
            throw new IllegalStateException("legacy settlement changed the actual payload lineage");
        }
        if ((!owner.summaryQualified() && legacyPrefix == null)
                || owner.summaryQualified() && owner.unresolvedCount() != 0
                || owner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            return;
        }
        if (legacyPrefix != null
                && (!legacyPrefix.noUnresolvedAfter(reader, owner, edits)
                        || !legacyPrefix.noLegacyCustodyAfter(reader, owner, edits))) {
            return;
        }
        final var frontier = reader.aggregate().mutation();
        owner.mutation().requireAtOrBefore(frontier);
        if (owner.summaryQualified()) {
            owner.summaryMutation().requireAtOrBefore(frontier);
        }
        final byte[] messageKey = TargetKeyCodec.message(owner.messageId());
        final var current = TargetMessageRecord.decodeForStore(
                messageKey,
                TargetValueEnvelope.decode(
                                Objects.requireNonNull(
                                        reader.get(ColumnFamily.ID, messageKey), "actual current payload Message"),
                                TargetMessageRecord.VALUE_TYPE)
                        .payload(),
                scope.shard());
        owner.requireMessagePayload(current);
        if (!current.runtime().terminal()
                || current.runtime().currentWorkKind() != CurrentSendWorkKind.NONE
                || !current.runtime().attemptObligations().isEmpty()) {
            return;
        }
        final byte[] terminalKey = TargetTerminalGenerationRecord.key(current.locator());
        final var terminal = TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(
                        Objects.requireNonNull(
                                reader.get(ColumnFamily.TERMINAL, terminalKey), "actual current terminal generation"),
                        TargetTerminalGenerationRecord.VALUE_TYPE)
                .payload());
        terminal.requireOwner(owner);
        terminal.mutation().requireAtOrBefore(frontier);
        if (!terminal.locator().equals(current.locator())
                || terminal.stateVersion() != current.stateVersion()
                || !Arrays.equals(
                        terminal.runtime().canonicalBytes(), current.runtime().canonicalBytes())) {
            throw new IllegalStateException("legacy settlement changed the exact current terminal projection");
        }
        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(owner.initialBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(
                                Objects.requireNonNull(
                                        reader.get(ColumnFamily.ID, bindingKey), "actual original payload binding"),
                                TargetScheduleBinding.VALUE_TYPE)
                        .payload(),
                scope.shard());
        owner.requireInitialBinding(binding);
        binding.requireLocator(current.locator());
        binding.requireMessageSource(current.scheduleSource());
        final var retained = owner.retain(stamp, (before, after, floor) -> {
            if (floor != null
                    || before != owner
                    || !after.mutation().equals(stamp)
                    || after.phase() != TargetQuotaPayloadOwner.Phase.RETAINED) {
                throw new IllegalStateException("legacy settlement changed its exact payload retention");
            }
            before.requireSameSummaryOrigin(after);
            if (before.summaryQualified()
                    && (before.unresolvedCount() != after.unresolvedCount()
                            || before.summaryRevision() != after.summaryRevision()
                            || !before.summaryMutation().equals(after.summaryMutation()))) {
                throw new IllegalStateException("legacy settlement cannot change the Native obligation summary");
            }
            after.requireMessagePayload(current);
            terminal.requireOwner(after);
        });
        edits.add(reader.replace(
                ColumnFamily.META, retained.key(), TargetQuotaPayloadOwner.VALUE_TYPE, retained.canonicalBytes()));
    }

    public static SystemMutationResult commitSettlement(
            PreparedSettlement prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared);
        prepared.backend.commit(prepared.batch, guarded(authority, prepared.current));
        return prepared.result;
    }

    public static void requireIdentity(StoreMetadata metadata, TargetMigrationPlan plan) {
        if (metadata.storeFormatVersion() != plan.newFormat()
                || !metadata.shardId().equals(plan.scope().shard())
                || !Arrays.equals(metadata.storeIncarnation(), plan.newStore())
                || !Arrays.equals(metadata.dbIdentity(), plan.newDbIdentity())) {
            throw new IllegalArgumentException("migration must build the exact separately identified new Store");
        }
    }

    public static TargetMigrationRoot requirePrepared(ShardStore store, TargetMigrationPlan plan) {
        requireIdentity(store.metadata(), plan);
        final var marker = decodeRoot(store.get(
                ColumnFamily.META, TargetMigrationRoot.keyFor(plan.scope().shard())));
        if (marker == null || marker.activated() || !marker.plan().equals(plan)) {
            throw new IllegalStateException("expected same MIGRATION_PREPARED root");
        }
        return marker;
    }

    public static TargetMigrationRoot requireMaintenance(
            ShardStore store, TargetMigrationPlan plan, TargetMigrationImporter.PreparedCandidate candidate) {
        candidate.requireStore(store);
        requireIdentity(store.metadata(), plan);
        final var root = Objects.requireNonNull(
                decodeRoot(store.get(
                        ColumnFamily.META,
                        TargetMigrationRoot.keyFor(plan.scope().shard()))),
                "original held migration root");
        if (!root.plan().equals(plan) || !candidate.plan().equals(plan)) {
            throw new IllegalStateException("maintenance candidate changed exact original migration");
        }
        return root;
    }

    /** The actual Source event under an optional local migration-preparation accounting ordinal. */
    public static TargetQuotaMutation actualFrontier(ShardStore store, TargetMigrationRoot root) {
        final var source = Objects.requireNonNull(store.appliedShardLogPosition(), "actual migration Source frontier");
        final long sequence = store.shardMutationSequence();
        if (sequence == root.plan().baseline().sequence()
                && Arrays.equals(
                        source.canonicalBytes(), root.plan().baseline().source().canonicalBytes())) {
            return root.plan().baseline();
        }
        if (root.activated()
                && sequence == root.activatedAt().sequence()
                && Arrays.equals(
                        source.canonicalBytes(), root.activatedAt().source().canonicalBytes())) {
            return root.activatedAt();
        }
        final var raw = store.get(
                ColumnFamily.META,
                TargetMigrationPrefixRecord.keyFor(root.plan().migrationId(), sequence));
        if (raw != null) {
            final var prefix = TargetMigrationPrefixRecord.decode(
                    TargetValueEnvelope.decode(raw, TargetMigrationPrefixRecord.VALUE_TYPE)
                            .payload());
            prefix.requirePlan(root.plan());
            if (prefix.at().sequence() != sequence
                    || !Arrays.equals(prefix.at().source().canonicalBytes(), source.canonicalBytes())) {
                throw new IllegalStateException("actual Source frontier lost its exact immutable migration prefix");
            }
            return prefix.at();
        }
        if (!root.activated()) {
            throw new IllegalStateException("prepared Source frontier has no exact legacy prefix");
        }
        final byte[] positionKey =
                Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1}, source.canonicalBytes());
        final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        Objects.requireNonNull(
                                store.get(ColumnFamily.DEDUPE, positionKey), "actual Target-only Source position"),
                        TargetResultRecord.VALUE_TYPE)
                .payload());
        position.requireStored(positionKey, TargetResultRecord.VALUE_TYPE, position.canonicalBytes());
        position.requireOwner(root.owner());
        if (!position.kind().position() || position.mutation().sequence() != sequence) {
            throw new IllegalStateException("Target-only frontier changed exact physical Source sequence");
        }
        return position.mutation();
    }

    /** Read-only Native plan fact for same-candidate crash discovery; this grants no activation or service rights. */
    public static TargetMigrationRoot readRootNative(ShardStore store, BoundedReadBudget budget) {
        final var read = store.readWithBudget(
                budget, () -> decodeRoot(store.get(ColumnFamily.META, TargetMigrationRoot.keyFor(store.shardId()))));
        return store.withReadView(read.view(), read::value);
    }

    public static TargetMigrationActivationAttempt readAttemptNative(
            ShardStore store, TargetMigrationPlan plan, BoundedReadBudget budget) {
        final var read = store.readWithBudget(budget, () -> {
            final byte[] raw =
                    store.get(ColumnFamily.META, TargetMigrationActivationAttempt.keyFor(plan.migrationId()));
            if (raw == null) {
                return null;
            }
            final var attempt = TargetMigrationActivationAttempt.decode(
                    TargetValueEnvelope.decode(raw, TargetMigrationActivationAttempt.VALUE_TYPE)
                            .payload());
            attempt.requirePlan(plan);
            final var root = Objects.requireNonNull(
                    decodeRoot(store.get(ColumnFamily.META, TargetMigrationRoot.keyFor(store.shardId()))),
                    "durable migration root");
            if (!root.plan().equals(plan)) {
                throw new IllegalStateException("durable activation intent lost exact original root");
            }
            final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.META,
                                    TargetQuotaAggregate.genesis(
                                                    store.shardId(),
                                                    root.owner().identity().accountingIncarnation())
                                            .key()),
                            TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            attempt.preparedAt().requireAtOrBefore(aggregate.mutation());
            return attempt;
        });
        return store.withReadView(read.view(), read::value);
    }

    private static TargetMigrationActivationAttempt readAttempt(
            TargetStoreBackend.Reader reader, TargetMigrationPlan plan) {
        final byte[] raw = reader.get(ColumnFamily.META, TargetMigrationActivationAttempt.keyFor(plan.migrationId()));
        if (raw == null) {
            return null;
        }
        final var attempt = TargetMigrationActivationAttempt.decode(
                TargetValueEnvelope.decode(raw, TargetMigrationActivationAttempt.VALUE_TYPE)
                        .payload());
        attempt.requirePlan(plan);
        attempt.preparedAt().requireAtOrBefore(reader.aggregate().mutation());
        return attempt;
    }

    public static TargetMigrationRoot decodeRoot(byte[] raw) {
        return raw == null
                ? null
                : TargetMigrationRoot.decode(TargetValueEnvelope.decode(raw, TargetMigrationRoot.VALUE_TYPE)
                        .payload());
    }

    public static void requireServiceable(ShardStore store) {
        final var marker = decodeRoot(store.get(ColumnFamily.META, TargetMigrationRoot.keyFor(store.shardId())));
        if (marker != null && !marker.activated()) {
            throw new IllegalStateException("MIGRATION_PREPARED is not a serviceable/restore-ready Target Store");
        }
    }

    public static void requireImportOrigin(TargetMigrationPlan plan, TargetMigrationImporter.ImportedRow row) {
        row.origin().requirePlan(plan);
        row.origin().requireRow(row.family(), row.key(), row.value());
        if (row.family() == ColumnFamily.DEDUPE
                || isImportInfrastructure(row.family(), row.key())
                || row.family() == ColumnFamily.META && row.key()[0] == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG
                || row.key()[0] == TargetKeyCodec.MIGRATION_ROOT_TAG
                || row.key()[0] == TargetKeyCodec.MIGRATION_ORIGIN_TAG
                || row.key()[0] == TargetKeyCodec.MIGRATION_CUSTODY_GUARD_TAG
                || row.key()[0] == TargetKeyCodec.MIGRATION_PREFIX_TAG
                || row.key()[0] == TargetKeyCodec.MIGRATION_ACTIVATION_ATTEMPT_TAG) {
            throw new IllegalArgumentException("migration cannot fabricate ordinary results or backend-owned rows");
        }
    }

    public static void requireGuard(TargetMigrationPlan plan, TargetMigrationCustodyGuard guard) {
        if (!Arrays.equals(guard.migrationId(), plan.migrationId())
                || !Arrays.equals(guard.planDigest(), plan.digest())
                || !guard.owner().scope().shardScope().equals(plan.scope())
                || !Arrays.equals(guard.owner().recoveryLineage(), plan.newLineage())) {
            throw new IllegalArgumentException("custody guard changed exact migration/owner");
        }
    }

    public static TargetMigrationOrigin origin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetMigrationPlan plan,
            ColumnFamily family,
            byte[] key) {
        final byte[] index = TargetMigrationOrigin.keyFor(family, Bytes.sha256(key));
        final byte[] raw = view.projected(ColumnFamily.META, index, overlay);
        if (raw == null) {
            throw new IllegalStateException("imported record has no explicit migration origin");
        }
        final var origin =
                TargetMigrationOrigin.decode(TargetValueEnvelope.decode(raw, TargetMigrationOrigin.VALUE_TYPE)
                        .payload());
        origin.requirePlan(plan);
        if (!Arrays.equals(index, origin.key())) {
            throw new IllegalStateException("migration origin key changed");
        }
        return origin;
    }

    public static void requireBaselineRow(
            TargetRecordAccounting.View view, TargetMigrationPlan plan, ColumnFamily family, byte[] key, byte[] raw) {
        origin(view, List.of(), plan, family, key).requireRow(family, key, raw);
    }

    public static boolean isImportInfrastructure(ColumnFamily family, byte[] key) {
        if (family != ColumnFamily.META || key.length < 2 || key[1] != 1) {
            return false;
        }
        return key[0] == 1
                || key[0] == 7
                || key[0] == TargetKeyCodec.QUOTA_COUNTER_TAG
                || key[0] == TargetKeyCodec.QUOTA_TOTAL_TAG
                || key[0] == TargetKeyCodec.QUOTA_AGGREGATE_TAG
                || key[0] == TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
    }

    public static TargetRecordAccounting.Charge nativeCharge(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation frontier,
            ColumnFamily family,
            byte[] key,
            byte[] payload,
            int type) {
        if (family != ColumnFamily.META) {
            throw new IllegalArgumentException("migration Native row belongs to META");
        }
        final var migration =
                decodeRoot(view.projected(ColumnFamily.META, TargetMigrationRoot.keyFor(scope.shard()), overlay));
        if (migration == null
                || !migration.plan().scope().equals(scope)
                || !Arrays.equals(lineage, migration.plan().newLineage())) {
            throw new IllegalStateException("migration Native row has no exact original root");
        }
        final TargetQuotaIncarnation frozen;
        if (type == TargetMigrationRoot.VALUE_TYPE) {
            if (!Arrays.equals(key, migration.key()) || !Arrays.equals(payload, migration.canonicalBytes())) {
                throw new IllegalStateException("migration root changed selected bytes");
            }
            frozen = migration.owner();
            migration.mutation().requireAtOrBefore(frontier);
            if (migration.activated()) {
                requireApplied(view, overlay, frozen, migration.activation(), migration.activatedAt(), frontier);
                final byte[] attemptRaw = Objects.requireNonNull(
                        view.projected(
                                ColumnFamily.META,
                                TargetMigrationActivationAttempt.keyFor(
                                        migration.plan().migrationId()),
                                overlay),
                        "original before-SDK intent");
                final var attempt = TargetMigrationActivationAttempt.decode(
                        TargetValueEnvelope.decode(attemptRaw, TargetMigrationActivationAttempt.VALUE_TYPE)
                                .payload());
                attempt.requirePlan(migration.plan());
                if (!Arrays.equals(
                        attempt.input().canonicalEnvelope(),
                        migration.activation().canonicalEnvelope())) {
                    throw new IllegalStateException("actual A changed original durable may-be-exposed frame");
                }
            }
        } else if (type == TargetMigrationOrigin.VALUE_TYPE) {
            final var origin = TargetMigrationOrigin.decode(payload);
            origin.requirePlan(migration.plan());
            if (!Arrays.equals(key, origin.key())) {
                throw new IllegalStateException("migration origin key changed");
            }
            frozen = origin.owner();
        } else if (type == TargetMigrationCustodyGuard.VALUE_TYPE) {
            final var guard = TargetMigrationCustodyGuard.decode(payload);
            requireGuard(migration.plan(), guard);
            if (!Arrays.equals(key, guard.key())) {
                throw new IllegalStateException("migration custody key changed");
            }
            frozen = guard.owner();
            if (guard.settled()) {
                requireApplied(view, overlay, migration.owner(), guard.settlement(), guard.settledAt(), frontier);
            }
        } else if (type == TargetMigrationPrefixRecord.VALUE_TYPE) {
            final var prefix = TargetMigrationPrefixRecord.decode(payload);
            prefix.requirePlan(migration.plan());
            if (!Arrays.equals(key, prefix.key())) {
                throw new IllegalStateException("migration prefix key changed");
            }
            prefix.mutation().requireAtOrBefore(frontier);
            frozen = migration.owner();
        } else if (type == TargetMigrationActivationAttempt.VALUE_TYPE) {
            final var attempt = TargetMigrationActivationAttempt.decode(payload);
            attempt.requirePlan(migration.plan());
            if (!Arrays.equals(key, attempt.key())) {
                throw new IllegalStateException("activation intent key changed");
            }
            attempt.preparedAt().requireAtOrBefore(frontier);
            frozen = migration.owner();
            if (migration.activated()
                    && !Arrays.equals(
                            attempt.input().canonicalEnvelope(),
                            migration.activation().canonicalEnvelope())) {
                throw new IllegalStateException("actual A differs from durable may-be-exposed activation intent");
            }
        } else {
            throw new IllegalArgumentException("unregistered migration Native type");
        }
        final byte[] ownerRaw = Objects.requireNonNull(
                view.projected(ColumnFamily.META, frozen.key(), overlay), "migration original fee owner");
        final var owner = TargetQuotaIncarnation.decodeForStore(
                frozen.key(),
                TargetValueEnvelope.decode(ownerRaw, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!owner.identity().equals(frozen.identity())
                || !owner.accounting().equals(frozen.accounting())
                || !owner.allocation().equals(frozen.allocation())) {
            throw new IllegalStateException("migration fee owner changed");
        }
        if (!owner.latestMutation().equals(migration.plan().baseline())
                || !frontier.equals(TargetMigrationRoot.baselineFor(migration.plan()))) {
            owner.latestMutation().requireAtOrBefore(frontier);
        }
        return new TargetRecordAccounting.Charge(
                owner,
                TargetRecordAccounting.resources(owner.accounting()
                        .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length)));
    }

    public static void requireApplied(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaIncarnation owner,
            SystemMutation input,
            TargetQuotaMutation appliedAt,
            TargetQuotaMutation frontier) {
        appliedAt.requireAtOrBefore(frontier);
        final byte[] firstKey =
                Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, input.systemMutationId());
        final byte[] firstRaw = Objects.requireNonNull(
                view.projected(ColumnFamily.DEDUPE, firstKey, overlay), "migration actual first");
        final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(firstRaw, TargetResultRecord.VALUE_TYPE)
                .payload());
        first.requireOwner(owner);
        final var expected = SystemMutationResult.from(
                input, ApplyStatus.APPLIED, StableCode.OK, appliedAt.source().canonicalBytes());
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || !first.mutation().equals(appliedAt)
                || !Arrays.equals(first.typedPayload(), expected.encode())) {
            throw new IllegalStateException("migration lost exact actual APPLIED first");
        }
        final byte[] positionKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                appliedAt.source().canonicalBytes());
        final byte[] positionRaw = Objects.requireNonNull(
                view.projected(ColumnFamily.DEDUPE, positionKey, overlay), "migration actual POS");
        final var position =
                TargetResultRecord.decode(TargetValueEnvelope.decode(positionRaw, TargetResultRecord.VALUE_TYPE)
                        .payload());
        position.requireFirst(first);
        position.requireOwner(owner);
        if (!position.mutation().equals(appliedAt) || position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM) {
            throw new IllegalStateException("migration lost actual Source POS");
        }
    }

    static Runnable authenticate(
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            TargetMigrationControlBody body,
            Authority authority) {
        TargetSourcePosition.requireBounded(source);
        Objects.requireNonNull(authority);
        if (!source.shardId().equals(body.shard())
                || source.brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                || control.kind() != body.request().operationKind()
                || !control.request().equals(body.request().operationRequest())) {
            throw new IllegalArgumentException("migration Source/registered purpose/retry boundary changed");
        }
        final PublicKey preparedKey = Objects.requireNonNull(
                authority.keys().resolve(control.signingKeyVersion(), source), "original Control key");
        final PublicKey markerKey = Objects.requireNonNull(
                authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source),
                "original Source key");
        if (!control.verifySignature(preparedKey) || !input.verifySignature(markerKey)) {
            throw new IllegalArgumentException("migration Control signature invalid");
        }
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), control.author().operationActorIdHash())
                || !Arrays.equals(author.second(), control.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), control.author().tenantResourceScopeHash())) {
            throw new IllegalArgumentException("migration original CONTROL author changed");
        }
        ControlOperationAuthorization.authorize(control, authority.actor(), authority.scopeProof());
        final var registered =
                authority.registrations().find(control.operationId()).orElseThrow();
        if (!Arrays.equals(registered.canonicalBytes(), control.canonicalBytes())) {
            throw new IllegalArgumentException("migration lacks original Prepared registration");
        }
        ControlTargetMutationBinding.validate(control, control.targets().getFirst(), input);
        authority.registrations().validateMutation(control, control.targets().getFirst(), input);
        final Runnable current = Objects.requireNonNull(
                authority.inputs().authenticate(control, input, source, body.request()),
                "original held migration authority");
        current.run();
        if (body.request() instanceof TargetMigrationActivateRequest activation
                && activation.rootIssuerAuthorization() != null) {
            final var statement = activation.rootIssuerAuthorization();
            statement.requirePlan(activation.plan());
            final var issuer =
                    authority.inputs().originalRootIssuer(statement, activation.plan(), control, input, source);
            current.run();
            if (issuer == null || !statement.verifySignature(issuer)) {
                throw new IllegalStateException(
                        "A2 Root issuer statement lacks original independent policy/signature/pin");
            }
            if (statement.schemaVersion() == 2) {
                final var readers = Objects.requireNonNull(
                        authority
                                .inputs()
                                .parentControlCreditReaders(statement, activation.plan(), control, input, source),
                        "original policy2 eligible readers");
                activation.plan().readerEvidence().requireContent(readers.canonicalBytes());
                readers.requireMigrationRootIssuerReaders();
                current.run();
            }
        }
        return current;
    }

    static void appendResults(
            TargetStoreBackend.Reader reader,
            List<TargetStoreBackend.Edit> edits,
            TargetQuotaIncarnation owner,
            SystemMutation input,
            SystemMutationResult outcome,
            TargetQuotaMutation stamp) {
        final TargetResultRecord.CreationAuthority results = (record, first) -> {
            record.requireOwner(owner);
            if (reader.get(ColumnFamily.DEDUPE, record.key()) != null
                    || !record.mutation().equals(stamp)
                    || record.allocation() != null) {
                throw new IllegalStateException("migration first/POS changed actual Source decision");
            }
            if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                record.requireFirst(Objects.requireNonNull(first));
            } else if (record.kind() != TargetResultRecord.Kind.SYSTEM
                    || first != null
                    || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                throw new IllegalStateException("unexpected migration result kind");
            }
        };
        final var first = TargetResultRecord.system(owner, outcome, stamp, null, results);
        final var position = TargetResultRecord.position(owner, first, stamp, results);
        edits.add(reader.replace(
                ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
        edits.add(reader.replace(
                ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
    }

    static TargetStoreBackend.CommitAuthority guarded(TargetStoreBackend.CommitAuthority authority, Runnable current) {
        return (store, scope, mutation) -> {
            final var guard = Objects.requireNonNull(authority.acquire(store, scope, mutation));
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    guard.requireCurrent();
                    current.run();
                    guard.requireCurrent();
                }

                @Override
                public void close() {
                    guard.close();
                }
            };
        };
    }
}
