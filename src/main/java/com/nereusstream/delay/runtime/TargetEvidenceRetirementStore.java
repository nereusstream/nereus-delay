package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Atomic Native retirement intent/first/POSITION accounting under protected deployment Source approval. */
public final class TargetEvidenceRetirementStore {
    /**
     * The deployment must approve this exact SERVICE actor/key for retirement at the original Source,
     * exact authenticated Root/resource/closure/protection snapshot and the complete capacity reservation. Resolve outside
     * Native locks and retain its local protection through commit. Null is a proved denial; unknown throws.
     */
    @FunctionalInterface
    public interface Authority {
        Approval resolve(TargetQuotaScope scope, AuthorIdentity actor, SystemMutation intent, SourcePosition source);
    }

    /** The key is selected by trusted Source authority, never from the copy's serialized public keys. */
    public record Approval(PublicKey writerKey,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy, Runnable current,ProtocolTuple activatedTuple) {
        public Approval { Objects.requireNonNull(writerKey); Objects.requireNonNull(copy); Objects.requireNonNull(current); }
        /** Key/copy possession alone never activates body generation8. */
        public Approval(PublicKey key,OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy,Runnable current){
            this(key,copy,current,null);
        }
    }

    public static final class Prepared {
        private final TargetEvidenceRetirementStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Approval approval;
        private Prepared(TargetEvidenceRetirementStore owner, TargetStoreBackend.Prepared batch,
                SystemMutationResult result, Approval approval) {
            this.owner = owner; this.batch = batch; this.result = result; this.approval = approval;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters, maximumDomains;

    public TargetEvidenceRetirementStore(TargetStoreBackend backend, TargetQuotaScope scope, byte[] lineage,
            int maximumCounters, int maximumDomains) {
        this.backend = Objects.requireNonNull(backend); this.scope = Objects.requireNonNull(scope);
        Bytes.requireLength(lineage, 16, "retirement lineage");
        if (scope.target() != null || Arrays.equals(lineage, new byte[16]) || maximumCounters < 4
                || maximumDomains < 1 || maximumDomains > 64)
            throw new IllegalArgumentException("retirement Store requires bounded Shard/lineage accounting");
        this.lineage = Bytes.copy(lineage); this.maximumCounters = maximumCounters; this.maximumDomains = maximumDomains;
    }

    /** Caller routes retained first-result replay before this entry. No Source/reader activation is implicit. */
    public Prepared prepareFirst(com.nereusstream.delay.store.BoundedReadBudget budget, SystemMutation intent,
            SourcePosition source, Authority authority) {
        Objects.requireNonNull(authority); TargetSourcePosition.requireBounded(source);
        final var body = TargetEvidenceRetireIntentBody.decode(intent.canonicalBody());
        if (intent.type() != SystemMutationType.RESOURCE_RETIRE_INTENT || !scope.shard().equals(source.shardId())
                || !scope.shard().equals(intent.shardId()) || !body.shard().equals(scope.shard())
                || body.retryUntil() != intent.retryUntilEpochMs()
                || !Arrays.equals(body.logicalOperationIdentity(), intent.logicalOperationIdentity()))
            throw new IllegalArgumentException("retirement Source/operation mismatch");
        final boolean expired = source.brokerPersistenceTimeEpochMs() > intent.retryUntilEpochMs()
                || backend.prepareRead(budget, reader -> reader.closedIngressDeadlineThrough() >= intent.retryUntilEpochMs()).value();
        Approval approval = null;
        if (!expired && intent.authorIdentity().length <= (1 << 20)) {
            AuthorIdentity actor;
            try { actor = AuthorIdentity.decode(intent.authorIdentity()); }
            catch (IllegalArgumentException malformed) { actor = null; }
            if (actor != null && actor.kind() == AuthorIdentity.Kind.SERVICE) {
                approval = authority.resolve(scope, actor, intent, source);
                if (approval != null) approval.current().run();
            }
        }
        final var selected = approval;
        final boolean authorized = selected != null && intent.verifySignature(selected.writerKey());
        final boolean activated=authorized&&body.requiredTuple().equals(selected.activatedTuple());
        if (activated) {
            requireSame(body.resource().canonicalBytes(), selected.copy().image().resource().canonicalBytes());
            selected.copy().image().requireContext(body.context()); selected.current().run();
        }
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0)
                throw new IllegalStateException("first retirement needs an established strictly earlier Source");
            requireAbsent(reader, ColumnFamily.DEDUPE, resultKey(intent.systemMutationId()));
            requireAbsent(reader, ColumnFamily.DEDUPE, positionKey(source));
            final var stamp = new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()), source,
                    Bytes.sha256(intent.canonicalEnvelope()));
            final var root = sourceRoot(reader, stamp);
            final StableCode rejection = expired || reader.closedIngressDeadlineThrough() >= intent.retryUntilEpochMs()
                    ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                    : !authorized ? StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                    : !activated ? StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION : null;
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            if (rejection == null) {
                selected.current().run();
                edits.addAll(intentEdits(reader, scope, lineage, intent, stamp, selected.copy()));
            }
            final var outcome = SystemMutationResult.from(intent, rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection, source.canonicalBytes());
            final TargetResultRecord.CreationAuthority results = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null)
                    throw new IllegalStateException("retirement result changed its Source/root");
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null || !Arrays.equals(record.key(), resultKey(intent.systemMutationId()))
                            || !Arrays.equals(record.typedPayload(), outcome.encode()))
                        throw new IllegalStateException("retirement first differs from approved decision");
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null || !Arrays.equals(first.key(), resultKey(intent.systemMutationId()))
                            || !Arrays.equals(first.typedPayload(), outcome.encode()) || !Arrays.equals(record.key(), positionKey(source)))
                        throw new IllegalStateException("retirement POSITION lost its exact first");
                    record.requireFirst(first);
                } else throw new IllegalStateException("unexpected retirement result kind");
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, null, results);
            final var position = TargetResultRecord.position(root, first, stamp, results);
            edits.add(reader.replace(ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE, first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE, position.canonicalBytes()));
            result[0] = outcome;
            return new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(), maximumCounters, 1, maximumDomains)
                    .assemble(reader, edits);
        });
        return new Prepared(this, batch, result[0], selected);
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        if (Objects.requireNonNull(prepared).owner != this) throw new IllegalArgumentException("foreign retirement plan");
        final var actual = Objects.requireNonNull(authority);
        backend.commit(prepared.batch, (metadata, actualScope, mutation) -> {
            final var guard = Objects.requireNonNull(actual.acquire(metadata, actualScope, mutation));
            return new TargetStoreBackend.CommitGuard() {
                @Override public void requireCurrent() {
                    guard.requireCurrent();
                    if (prepared.approval != null) prepared.approval.current().run();
                    guard.requireCurrent();
                }
                @Override public void close() { guard.close(); }
            };
        });
        return prepared.result;
    }

    private TargetQuotaIncarnation sourceRoot(TargetStoreBackend.Reader reader, TargetQuotaMutation stamp) {
        final var id = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(), reader.aggregate().accountingIncarnation(), null, null);
        final byte[] key = id.key(); key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root = TargetQuotaIncarnation.decodeForStore(key, payload(reader, ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(), scope.tenantScope());
        if (!id.equals(root.identity()) || !Arrays.equals(root.recoveryLineage(), lineage))
            throw new IllegalStateException("retirement lost original Source root/lineage");
        root.latestMutation().requireAtOrBefore(reader.aggregate().mutation()); stamp.requireAfter(reader.aggregate().mutation());
        return root;
    }

    /**
     * Returns business edits only, for the same Source batch as APPLIED first/POSITION and exact accounting.
     * The copy receipt comes from actual Catalog authentication outside the Native lock. This entry neither
     * approves the Source author nor commits independently, and does not authorize physical deletion.
     */
    public static List<TargetStoreBackend.Edit> intentEdits(TargetStoreBackend.Reader reader,
            TargetQuotaScope scope, byte[] lineage, SystemMutation intent, TargetQuotaMutation stamp,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticated) {
        final var copy = Objects.requireNonNull(authenticated, "actual Catalog public-copy receipt").image();
        final var record = new TargetEvidenceRetiringRecord(intent, stamp, copy);
        final var body = record.body();
        if (scope.target() != null || !scope.shard().equals(intent.shardId())
                || !Arrays.equals(scope.tenantScope(), copy.resource().tenantScope())
                || !Arrays.equals(lineage, copy.resource().recoveryLineage()))
            throw new IllegalArgumentException("retiring intent changed actual Shard/tenant/lineage");
        final var cut = Objects.requireNonNull(reader.aggregate().mutation(), "established Native cut");
        body.context().preparedCut().requireAtOrBefore(cut); stamp.requireAfter(cut);
        // The logical cut survives cold reconstruction; the old physical Store incarnation/sequence need not.
        requireAbsent(reader, ColumnFamily.META, record.key());
        requireAbsent(reader, ColumnFamily.META, TargetEvidenceRetiringRecord.confirmedKey(copy.resource().exactIdentity().identityHash()));
        requireAbsent(reader, ColumnFamily.DEDUPE, resultKey(intent.systemMutationId()));
        requireAbsent(reader, ColumnFamily.DEDUPE, positionKey(stamp.source()));
        if (!(copy.resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery))
            throw new IllegalStateException("CONTROL/shared/staging Native retirement projection is not wired yet");
        final var owner = recovery.owner();
        requireSame(owner.canonicalBytes(), payload(reader, ColumnFamily.META, owner.key(), TargetRecoveryEvidenceOwner.VALUE_TYPE));
        final var reference = owner.referenceRecord();
        for (byte[] key : List.of(reference.key(), reference.attemptKey()))
            requireSame(reference.canonicalBytes(), payload(reader, ColumnFamily.META, key, TargetRecoveryEvidenceReference.VALUE_TYPE));
        final var proof = TargetRecoveryProof.decode(copy.signedObject()); owner.requireProof(proof);
        final var mapping = proof.mapping();
        requireSame(mapping.canonicalBytes(), payload(reader, ColumnFamily.META, mapping.key(), TargetKafkaReceiptMapping.VALUE_TYPE));
        final var original = TargetExternalEvidenceStore.readAdoption(reader, mapping, scope, lineage);
        requireSame(original.anchor().primaryOwner().canonicalBytes(), owner.primaryOwner().canonicalBytes());
        final byte[] attemptKey = Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT}, reference.attemptId());
        final var attempt = TargetQuotaAttemptBudget.decodeForStore(attemptKey,
                payload(reader, ColumnFamily.META, attemptKey, TargetQuotaAttemptBudget.VALUE_TYPE), scope.shard());
        reference.requireAttempt(attempt); attempt.mutation().requireAtOrBefore(cut);
        if (!owner.adoptedAt().equals(attempt.resolvedAt()) || !Arrays.equals(owner.admissionHash(), attempt.admissionDigest()))
            throw new IllegalStateException("retiring proof changed original resolved Attempt");
        final var sourceOwner = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                reader.aggregate().accountingIncarnation(), null, null);
        requireRetainedUses(reader, List.of(), scope, lineage, sourceOwner, record, cut);
        reader.requireWithinElapsedBudget();
        return List.of(reader.replace(ColumnFamily.META, record.key(), TargetEvidenceRetiringRecord.VALUE_TYPE, record.canonicalBytes()));
    }

    /** Shared Native contract over actual bytes, also used by immutable-image accounting; no Catalog I/O. */
    public static void requireRetainedUses(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity sourceOwner,
            TargetEvidenceRetiringRecord record, TargetQuotaMutation frontier) {
        if (sourceOwner.kind() != TargetQuotaIdentity.Kind.SHARD || !sourceOwner.shard().equals(scope.shard()))
            throw new IllegalArgumentException("retirement requires the actual Source accounting root");
        final var context = record.body().context();
        context.preparedCut().requireAtOrBefore(frontier);
        final byte[] rawFence = view.projected(ColumnFamily.META, KeyCodec.metaFixed(4), overlay);
        final var fence = rawFence == null ? new IngressFenceState(IngressFenceState.OPEN, null)
                : IngressFenceState.decode(TargetValueEnvelope.decode(rawFence, 1).payload());
        if (fence.closedThroughEpochMs() < context.fence().closedThroughEpochMs()
                || context.fence().proofId() != null && fence.proofId() == null)
            throw new IllegalStateException("retirement lost its committed prepared ingress fence");
        final var resource = record.copy().resource();
        final var adopted = resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery r ? r.owner() : null;
        if (adopted != null) adopted.adoptedAt().requireAtOrBefore(context.preparedCut());
        boolean adoptingFirst = false;
        for (var use : context.uses()) {
            final byte[] key = resultKey(use.id());
            final byte[] raw = view.projected(ColumnFamily.DEDUPE, key, overlay);
            if (use.conclusion() == TargetEvidenceRetirementContext.Conclusion.SOURCE_FENCED
                    && (context.fence().proofId() == null || context.fence().closedThroughEpochMs() < use.retryUntil()))
                throw new IllegalStateException("closed use has no prepared permanent Source fence");
            if (raw == null) {
                if (use.conclusion() != TargetEvidenceRetirementContext.Conclusion.SOURCE_FENCED)
                    throw new IllegalStateException("retirement lost a still-required sealed first result");
                continue;
            }
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload());
            first.requireStored(key, TargetResultRecord.VALUE_TYPE, first.canonicalBytes());
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM || !first.primaryIdentity().equals(sourceOwner)
                    || !Arrays.equals(result.mutationId(), use.id()) || !Arrays.equals(result.mutationHash(), use.hash())
                    || result.retryUntilEpochMs() != use.retryUntil()
                    || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION
                    || !Arrays.equals(result.appliedSourcePosition(), first.mutation().source().canonicalBytes()))
                throw new IllegalStateException("retirement sealed use changed its actual immutable first");
            first.mutation().requireAtOrBefore(frontier);
            final byte[] ownerKey = first.primaryIdentity().key(); ownerKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final var owner = TargetQuotaIncarnation.decodeForStore(ownerKey,
                    payload(view, overlay, ColumnFamily.META, ownerKey, TargetQuotaIncarnation.VALUE_TYPE), scope.shard(), scope.tenantScope());
            if (!Arrays.equals(owner.recoveryLineage(), lineage)) throw new IllegalStateException("sealed first changed its Source lineage");
            owner.latestMutation().requireAtOrBefore(frontier); first.requireOwner(owner);
            final byte[] positionKey = positionKey(first.mutation().source());
            final var position = TargetResultRecord.decode(payload(view, overlay, ColumnFamily.DEDUPE, positionKey, TargetResultRecord.VALUE_TYPE));
            position.requireStored(positionKey, TargetResultRecord.VALUE_TYPE, position.canonicalBytes());
            position.requireFirst(first); position.requireOwner(owner);
            if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM || !position.mutation().equals(first.mutation()))
                throw new IllegalStateException("retirement lost the sealed first's POSITION companion");
            if (use.conclusion() == TargetEvidenceRetirementContext.Conclusion.SOURCE_FENCED) {
                // A late expired first can appear after preparation; its permanent rejection does not reopen the use.
                first.mutation().requireAfter(context.preparedCut());
                if (result.applyStatus() != ApplyStatus.REJECTED || result.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED)
                    throw new IllegalStateException("prepared fenced absence became a possible successful adoption");
            } else {
                first.mutation().requireAtOrBefore(context.preparedCut());
                if (!Arrays.equals(first.digest(), use.firstDigest()) || !Arrays.equals(position.digest(), use.positionDigest())
                        || (result.applyStatus() == ApplyStatus.APPLIED)
                            != (use.conclusion() == TargetEvidenceRetirementContext.Conclusion.RETAINED_APPLIED))
                    throw new IllegalStateException("retirement changed its prepared first/POSITION conclusion");
            }
            if (adopted != null && Arrays.equals(use.id(), adopted.outcomeId())) {
                if (result.applyStatus() != ApplyStatus.APPLIED || result.mutationType() != adopted.outcomeType()
                        || !Arrays.equals(result.mutationHash(), adopted.outcomeHash()) || !first.mutation().equals(adopted.adoptedAt()))
                    throw new IllegalStateException("retiring proof lost its exact original adopting first");
                adoptingFirst = true;
            }
        }
        if (adopted != null && !adoptingFirst) throw new IllegalStateException("retiring proof lacks complete sealed adoption provenance");
    }

    private static byte[] resultKey(byte[] id) {
        return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }
    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }
    private static void requireAbsent(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        if (reader.get(family, key) != null) throw new IllegalStateException("retirement state0/first/POSITION already exists");
    }
    private static byte[] payload(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key, int type) {
        return payload(reader, List.of(), family, key, type);
    }
    private static byte[] payload(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family, byte[] key, int type) {
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family, key, overlay),
                "actual retained retirement dependency"), type).payload();
    }
    private static void requireSame(byte[] left, byte[] right) {
        if (!Arrays.equals(left, right)) throw new IllegalStateException("retirement changed original Native provenance");
    }
}
