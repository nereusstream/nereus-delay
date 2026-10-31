package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetProtocolActivationRecord;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloWriterContract;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Source-owned writer epoch rows, exact first/POS and frozen Root accounting in one original batch. */
public final class TargetSloWriterEpochStore {
    public static final class Prepared {
        private final TargetSloWriterEpochStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Runnable qualification;

        private Prepared(
                TargetSloWriterEpochStore owner,
                TargetStoreBackend.Prepared batch,
                SystemMutationResult result,
                Runnable qualification) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
            this.qualification = qualification;
        }
    }

    /** Minted only from actual guarded Native reads; no escaped Reader or caller-provided DTO is retained. */
    public static final class Applied {
        private final TargetSloWriterEpoch value;
        private final TargetSloWriterContract.Reference reference;
        private final TargetSloWriterOrigin.Loan loan;

        private Applied(Open actual) {
            this.value = actual.value();
            this.reference = value.reference();
            this.loan = actual.loan();
        }

        public TargetSloWriterEpoch value() {
            return value;
        }

        public TargetSloWriterContract.Reference reference() {
            return reference;
        }

        public void requireCurrent() {
            loan.requireCurrent();
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetSloWriterEpochStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains) {
        this.backend = Objects.requireNonNull(backend);
        this.scope = Objects.requireNonNull(scope);
        Bytes.requireLength(lineage, 16, "SLO writer Store lineage");
        if (scope.target() != null
                || Arrays.equals(lineage, new byte[16])
                || maximumCounters < 2
                || maximumDomains < 1
                || maximumDomains > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("SLO writer Store requires its original bounded Source scope");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    public Prepared prepareFirst(
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation input,
            SourcePosition source,
            TargetSloWriterEpochVerifier.Authority authority) {
        Objects.requireNonNull(control);
        Objects.requireNonNull(input);
        Objects.requireNonNull(authority);
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(input.shardId())
                || !scope.shard().equals(source.shardId())
                || input.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException("SLO writer input belongs to another Source scope");
        }
        final var outcome = new SystemMutationResult[1];
        final boolean[] applied = {false};
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("SLO writer first apply requires a later actual Source");
            }
            final var firstKey = firstKey(input.systemMutationId());
            final var positionKey = positionKey(source);
            absent(reader, ColumnFamily.DEDUPE, firstKey);
            absent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()),
                    source,
                    Bytes.sha256(input.canonicalEnvelope()));
            final var root = root(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    rootIdentity(reader),
                    reader.aggregate().mutation());
            stamp.requireAfter(reader.aggregate().mutation());
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            StableCode rejection = null;
            TargetSloWriterEpochVerifier.Change change = null;
            if (reader.closedIngressDeadlineThrough() >= input.retryUntilEpochMs()) {
                rejection = StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            } else {
                try {
                    change = TargetSloWriterEpochVerifier.verifyFirstApplication(
                            reader, scope, lineage, control, input, source, authority);
                } catch (CommandResolutionException denied) {
                    if (denied.stableCode() != StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                            && denied.stableCode() != StableCode.STALE_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                            && denied.stableCode() != StableCode.HARD_QUOTA_EXCEEDED) {
                        throw denied;
                    }
                    rejection = denied.stableCode();
                }
            }
            if (rejection == null) {
                final var selected = Objects.requireNonNull(change, "original SLO writer transition");
                final var epoch = selected.body().request().operationKind()
                                == ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH
                        ? TargetSloWriterEpoch.registered(input, stamp)
                        : Objects.requireNonNull(selected.retained()).closed(input, stamp);
                if (selected.body().request().operationKind()
                        == ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH) {
                    absent(reader, ColumnFamily.META, epoch.key());
                }
                edits.add(reader.replace(
                        ColumnFamily.META, epoch.key(), TargetSloWriterEpoch.VALUE_TYPE, epoch.canonicalBytes()));
                applied[0] = true;
            }
            outcome[0] = SystemMutationResult.from(
                    input,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection,
                    source.canonicalBytes());
            final TargetResultRecord.CreationAuthority results = (record, original) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("SLO writer first/POS changed its actual frozen Root");
                }
                absent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (original != null
                            || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), outcome[0].encode())) {
                        throw new IllegalStateException("SLO writer first changed its original decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (original == null
                            || !Arrays.equals(original.key(), firstKey)
                            || !Arrays.equals(record.key(), positionKey)
                            || !Arrays.equals(original.typedPayload(), outcome[0].encode())) {
                        throw new IllegalStateException("SLO writer POSITION lost its original first");
                    }
                    record.requireFirst(original);
                } else {
                    throw new IllegalStateException("SLO writer Control cannot create another result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome[0], stamp, null, results);
            final var position = TargetResultRecord.position(root, first, stamp, results);
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(
                    ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            if (applied[0]) {
                authority.materialization().requireCurrent();
            }
            return new TargetSourceAccounting(
                            scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .withScopeSource(input, authority.materialization().scopeFees())
                    .assemble(reader, edits);
        });
        return new Prepared(
                this, batch, outcome[0], applied[0] ? authority.materialization()::requireCurrent : () -> {});
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority actual) {
        Objects.requireNonNull(prepared);
        Objects.requireNonNull(actual);
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign SLO writer Source plan");
        }
        prepared.qualification.run();
        backend.commit(prepared.batch, (metadata, actualScope, mutation) -> {
            final var original = Objects.requireNonNull(actual.acquire(metadata, actualScope, mutation));
            return new TargetStoreBackend.CommitGuard() {
                public void requireCurrent() {
                    prepared.qualification.run();
                    original.requireCurrent();
                    prepared.qualification.run();
                }

                public void close() {
                    original.close();
                }
            };
        });
        return prepared.result;
    }

    /** Suppliers and read authority must perform already-held local checks and supply fresh finite read budgets. */
    public Applied applied(
            Supplier<BoundedReadBudget> freshReads,
            TargetStoreBackend.ReadAuthority authority,
            TargetSloWriterContract.Reference reference) {
        Objects.requireNonNull(freshReads);
        Objects.requireNonNull(authority);
        Objects.requireNonNull(reference);
        return new Applied(readOpen(freshReads, authority, reference));
    }

    private record Open(TargetSloWriterEpoch value, TargetSloWriterOrigin.Loan loan) {}

    private Open readOpen(
            Supplier<BoundedReadBudget> freshReads,
            TargetStoreBackend.ReadAuthority authority,
            TargetSloWriterContract.Reference reference) {
        return backend.guardedRead(
                Objects.requireNonNull(freshReads.get(), "fresh SLO writer read budget"),
                reader -> {
                    final var context = Objects.requireNonNull(
                            reader.writerOriginContext(),
                            "registered SLO writer signing requires the original protected prefix runtime");
                    final var epoch = Objects.requireNonNull(
                            readEpoch(reader, scope, lineage, reference), "actual registered SLO writer epoch");
                    if (epoch.closed()) {
                        throw new IllegalStateException("original SLO writer epoch is closed");
                    }
                    if (reader.closedIngressDeadlineThrough()
                            >= epoch.contract().maximumRetryUntil()) {
                        throw new IllegalStateException(
                                "original SLO writer deadline is already fenced for new signing");
                    }
                    requireOrigin(
                            reader,
                            List.of(),
                            scope,
                            lineage,
                            rootIdentity(reader),
                            epoch,
                            reader.aggregate().mutation());
                    return new Open(epoch, context.loan(epoch));
                },
                authority);
    }

    public static TargetSloWriterEpoch readEpoch(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloWriterContract.Reference reference) {
        return TargetSloWriterEpochVerifier.readEpoch(reader, scope, lineage, reference);
    }

    /** Complete local Native linkage; original public authorization is frozen independently before first apply. */
    public static void requireOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloWriterEpoch epoch,
            TargetQuotaMutation frontier) {
        final var context = view.writerOriginContext();
        if (context != null) {
            context.requireOrigin(view, overlay, scope, lineage, root, epoch, frontier);
            return;
        }
        requireFullOrigin(view, overlay, scope, lineage, root, epoch, frontier);
    }

    /** Independent cold verification. A hot prefix qualification is never evidence for this traversal. */
    public static void requireFullOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloWriterEpoch epoch,
            TargetQuotaMutation frontier) {
        Objects.requireNonNull(epoch);
        Objects.requireNonNull(frontier);
        final var owner = root(view, overlay, scope, lineage, root, frontier);
        requireFrontier(view, overlay, owner, frontier);
        TargetSloWriterEpoch current = epoch;
        while (true) {
            requireNodeOrigin(view, overlay, scope, lineage, root, owner, current, frontier);
            final var contract = current.contract();
            final var priorDigest = current.register().body().request().expectedPriorDigest();
            if (contract.epoch() == 1) {
                if (priorDigest != null) {
                    throw new IllegalStateException("first SLO writer epoch changed its predecessor");
                }
                return;
            }
            if (priorDigest == null) {
                throw new IllegalStateException("SLO writer epoch lost its predecessor digest");
            }
            final var previous = new TargetSloWriterContract.Reference(
                    contract.writerIdentityDigest(), contract.epoch() - 1, priorDigest);
            final var priorKey = previous.key(scope);
            final var prior = TargetSloWriterEpoch.decodeForStore(
                    priorKey,
                    payload(view, overlay, ColumnFamily.META, priorKey, TargetSloWriterEpoch.VALUE_TYPE),
                    scope,
                    lineage);
            if (!prior.reference().equals(previous)
                    || TargetQuotaMutation.increment(prior.contract().epoch()) != contract.epoch()) {
                throw new IllegalStateException("SLO writer epoch lost its exact prior applied REGISTER");
            }
            // A predecessor may close after this REGISTER; only its stable registration stamp orders epochs.
            current.register().appliedAt().requireAfter(prior.register().appliedAt());
            current = prior;
        }
    }

    /** One complete original node. Both preparation and hot checks use the cold predicate, without recursion. */
    static void requireNodeOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity identity,
            TargetQuotaIncarnation owner,
            TargetSloWriterEpoch current,
            TargetQuotaMutation frontier) {
        final var actual = TargetSloWriterEpoch.decodeForStore(
                current.key(),
                payload(view, overlay, ColumnFamily.META, current.key(), TargetSloWriterEpoch.VALUE_TYPE),
                scope,
                lineage);
        if (!Arrays.equals(actual.canonicalBytes(), current.canonicalBytes())) {
            throw new IllegalStateException("SLO writer epoch differs from its actual Native row");
        }
        current.mutation().requireAtOrBefore(frontier);
        final var contract = current.contract();
        contract.requireSource(frontier.source());
        contract.requireSource(current.register().appliedAt().source());
        if (!scope.equals(contract.scope())
                || !Arrays.equals(lineage, contract.lineage())
                || current.register().appliedAt().source().compareTo(contract.sourceAnchor()) <= 0) {
            throw new IllegalStateException("SLO writer epoch changed its original scope/lineage/Source anchor");
        }
        requireControlActivation(view, overlay, scope, lineage, identity, current.register(), frontier);
        requireSystemOrigin(
                view,
                overlay,
                owner,
                current.register().input(),
                current.register().appliedAt(),
                frontier);
        if (current.closed()) {
            requireControlActivation(view, overlay, scope, lineage, identity, current.close(), frontier);
            requireSystemOrigin(
                    view,
                    overlay,
                    owner,
                    current.close().input(),
                    current.close().appliedAt(),
                    frontier);
            final var close = current.close().body().request();
            requireCloseFence(
                    view,
                    overlay,
                    scope,
                    lineage,
                    identity,
                    current,
                    close.fence(),
                    close.fenceAt(),
                    current.close().appliedAt().source(),
                    frontier);
        }
    }

    private static void requireControlActivation(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloWriterEpoch.Control control,
            TargetQuotaMutation frontier) {
        final var key = TargetProtocolActivationRecord.key(control.body().tuple());
        final var activation = TargetProtocolActivationRecord.decodeForStore(
                key,
                payload(view, overlay, ColumnFamily.META, key, TargetProtocolActivationRecord.VALUE_TYPE),
                scope,
                lineage);
        TargetProtocolActivationStore.requireApplied(view, overlay, scope, lineage, root, activation, frontier);
        control.appliedAt().requireAfter(activation.appliedAt());
    }

    static void requireCloseFence(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            TargetSloWriterEpoch epoch,
            SystemMutation input,
            TargetQuotaMutation at,
            SourcePosition closeSource,
            TargetQuotaMutation frontier) {
        Objects.requireNonNull(input);
        Objects.requireNonNull(at);
        final var contract = epoch.contract();
        contract.requireSource(at.source());
        contract.requireSource(closeSource);
        at.requireAfter(epoch.register().appliedAt());
        at.requireAtOrBefore(frontier);
        if (at.source().compareTo(closeSource) >= 0 || input.type() != SystemMutationType.TIME_FENCE) {
            throw new IllegalStateException("SLO writer CLOSE lost its intervening original TIME_FENCE Source");
        }
        final var body = TargetTimeFenceBody.decode(input.canonicalBody());
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (!scope.shard().equals(body.shard())
                || body.retryUntil() != input.retryUntilEpochMs()
                || body.fenceKeyVersion() != input.signingKeyVersion()
                || !Arrays.equals(body.proofId(), input.logicalOperationIdentity())
                || author.kind() != AuthorIdentity.Kind.FENCE
                || body.closeThrough() < contract.maximumRetryUntil()) {
            throw new IllegalStateException("SLO writer CLOSE does not name the original complete deadline Fence");
        }
        final var owner = root(view, overlay, scope, lineage, root, frontier);
        requireSystemOrigin(view, overlay, owner, input, at, frontier);
        final var fenceRaw = view.projected(ColumnFamily.META, KeyCodec.metaFixed(4), overlay);
        if (fenceRaw == null
                || IngressFenceState.decode(
                                        TargetValueEnvelope.decode(fenceRaw, 1).payload())
                                .closedThroughEpochMs()
                        < contract.maximumRetryUntil()) {
            throw new IllegalStateException("SLO writer CLOSE lacks its actual cumulative ingress Fence");
        }
    }

    private static void requireSystemOrigin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaIncarnation root,
            SystemMutation input,
            TargetQuotaMutation at,
            TargetQuotaMutation frontier) {
        at.requireSourceApplied();
        at.requireAtOrBefore(frontier);
        if (!input.shardId().equals(root.identity().shard())
                || !Arrays.equals(at.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                || at.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()) {
            throw new IllegalStateException("SLO writer original System input lost its actual Source stamp");
        }
        final var first = result(view, overlay, firstKey(input.systemMutationId()));
        first.requireOwner(root);
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || !first.mutation().equals(at)
                || first.allocation() != null
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        input,
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        at.source().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("SLO writer dependency lost its exact original APPLIED OK first");
        }
        final var position = result(view, overlay, positionKey(at.source()));
        position.requireOwner(root);
        position.requireFirst(first);
        if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                || !position.mutation().equals(at)
                || position.allocation() != null) {
            throw new IllegalStateException("SLO writer dependency lost its original Root POSITION");
        }
    }

    static void requireFrontier(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaIncarnation root,
            TargetQuotaMutation frontier) {
        final var position = result(view, overlay, positionKey(frontier.source()));
        position.requireOwner(root);
        position.mutation().requireSourceApplied();
        position.mutation().requireAtOrBefore(frontier);
        if (!position.kind().position()
                || !Arrays.equals(
                        position.mutation().source().canonicalBytes(),
                        frontier.source().canonicalBytes())) {
            throw new IllegalStateException("SLO writer read frontier is not its actual Source POSITION");
        }
    }

    private static TargetQuotaIdentity rootIdentity(TargetStoreBackend.Reader reader) {
        return new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                reader.shardId(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
    }

    static TargetQuotaIncarnation root(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity identity,
            TargetQuotaMutation frontier) {
        if (scope.target() != null
                || identity.kind() != TargetQuotaIdentity.Kind.SHARD
                || !scope.shard().equals(identity.shard())
                || !scope.shard().equals(view.shardId())) {
            throw new IllegalStateException("SLO writer changed its actual Shard Root scope");
        }
        final var key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner = TargetQuotaIncarnation.decodeForStore(
                key,
                payload(view, overlay, ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),
                scope.tenantScope());
        if (!identity.equals(owner.identity()) || !Arrays.equals(lineage, owner.recoveryLineage())) {
            throw new IllegalStateException("SLO writer changed its frozen Source Root/lineage");
        }
        owner.latestMutation().requireAtOrBefore(frontier);
        return owner;
    }

    private static byte[] firstKey(byte[] id) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }

    private static byte[] positionKey(SourcePosition at) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, at.canonicalBytes());
    }

    private static TargetResultRecord result(
            TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay, byte[] key) {
        final var bytes = payload(view, overlay, ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE);
        final var result = TargetResultRecord.decode(bytes);
        result.requireStored(key, TargetResultRecord.VALUE_TYPE, bytes);
        return result;
    }

    private static byte[] payload(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family,
            byte[] key,
            int type) {
        return TargetValueEnvelope.decode(
                        Objects.requireNonNull(
                                view.projected(family, key, overlay), "actual original SLO writer Native dependency"),
                        type)
                .payload();
    }

    private static void absent(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("SLO writer first apply found an existing exact key");
        }
    }
}
