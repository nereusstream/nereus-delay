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
        default SourceHistory history(TargetQuotaScope scope,AuthorIdentity actor,SystemMutation input,SourcePosition source){
            throw new IllegalStateException("original retirement Source history/pin is not configured");
        }
    }
    /** Authenticated original public inputs, held locally through the actual commit; embedded proof keys are not pins. */
    public record SourceHistory(TargetRetirementAuthorizationHistory.PublicProof proof,TargetRetirementAuthorizationHistory.Pin pin,Runnable current){
        public SourceHistory{Objects.requireNonNull(proof);Objects.requireNonNull(pin);Objects.requireNonNull(current);}
        public java.util.Optional<TargetRetirementAuthorizationHistory.Grant> select(TargetExternalEvidenceObjectResource resource,SystemMutation input,SourcePosition source){
            current.run();final var verified=proof.authenticate(pin);
            verified.requireCoverage(resource,source);
            if(input.authorIdentityLength()>1024){current.run();return java.util.Optional.empty();}
            final var selected=verified.select(resource,source,AuthorIdentity.decode(input.authorIdentity()),input.signingKeyVersion(),TargetRetirementAuthorizationHistory.Action.of(input.type()));
            current.run();return selected;
        }
    }

    /** The key is selected by trusted Source authority, never from the copy's serialized public keys. */
    public record Approval(PublicKey writerKey,
            OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy, Runnable current,ProtocolTuple activatedTuple,
            TargetRetirementAuthorizationHistory.Grant historyGrant) {
        public Approval { Objects.requireNonNull(writerKey); Objects.requireNonNull(copy); Objects.requireNonNull(current); }
        /** Key/copy possession alone never activates body generation8. */
        public Approval(PublicKey key,OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy,Runnable current){
            this(key,copy,current,null,null);
        }
        public Approval(PublicKey key,OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy,Runnable current,ProtocolTuple tuple){this(key,copy,current,tuple,null);}
    }

    public static final class Prepared {
        private final TargetEvidenceRetirementStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final Approval approval;
        private final SourceHistory history;
        private Prepared(TargetEvidenceRetirementStore owner, TargetStoreBackend.Prepared batch,
                SystemMutationResult result, Approval approval,SourceHistory history) {
            this.owner = owner; this.batch = batch; this.result = result; this.approval = approval;this.history=history;
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
        final boolean protocolReady=expired?false:backend.prepareRead(budget,
                reader->TargetProtocolActivationStore.isActivatedForRetirement(reader,scope,lineage,body.requiredTuple(),source,body.resource())).value();
        Approval approval = null;SourceHistory history=null;TargetRetirementAuthorizationHistory.Grant selectedGrant=null;
        if (!expired && protocolReady && intent.authorIdentity().length <= (1 << 20)) {
            AuthorIdentity actor;
            try { actor = AuthorIdentity.decode(intent.authorIdentity()); }
            catch (IllegalArgumentException malformed) { actor = null; }
            if(actor!=null){
                history=Objects.requireNonNull(authority.history(scope,actor,intent,source),"complete original Source history");
                selectedGrant=history.select(body.resource(),intent,source).orElse(null);
                final var candidate=history.proof();
                backend.prepareRead(budget,reader->{TargetRetirementSourceHistoryStore.currentTip(reader,scope,lineage,candidate);return Boolean.TRUE;});
                if(selectedGrant!=null&&intent.verifySignature(selectedGrant.writerKey())){
                    approval = authority.resolve(scope, actor, intent, source);
                    if (approval != null) approval.current().run();
                }
            }
        }
        final var selected = approval;
        final var retainedHistory=history;final var grant=selectedGrant;
        if(selected!=null&&(selected.historyGrant()==null||grant==null
                ||!Arrays.equals(selected.historyGrant().canonicalBytes(),grant.canonicalBytes())))
            throw new IllegalStateException("retirement approval changed authenticated original history grant");
        final boolean authorized = selected != null && grant!=null&&intent.verifySignature(selected.writerKey())
                &&grant.action()==TargetRetirementAuthorizationHistory.Action.RETIRE_INTENT
                &&grant.keyVersion()==intent.signingKeyVersion()&&Arrays.equals(grant.service().canonicalBytes(),intent.authorIdentity())
                &&Arrays.equals(grant.publicKey(),selected.writerKey().getEncoded());
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
            final boolean nativeReady=TargetProtocolActivationStore.isActivatedForRetirement(reader,scope,lineage,body.requiredTuple(),source,body.resource());
            if(nativeReady&&!expired&&retainedHistory==null)throw new IllegalStateException("original Source protocol/input changed; retain and retry");
            boolean sourceActivated=activated&&nativeReady;
            if(sourceActivated){
                if(grant==null)throw new IllegalStateException("retirement Source history omitted its exact Target activation binding");
                try{TargetProtocolActivationStore.requireHistoryBinding(reader,scope,lineage,grant,source);}
                catch(CommandResolutionException denied){if(denied.stableCode()!=StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION)throw denied;sourceActivated=false;}
            }
            final StableCode rejection = expired || reader.closedIngressDeadlineThrough() >= intent.retryUntilEpochMs()
                    ? StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED
                    : !nativeReady ? StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                    : !authorized ? StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                    : !sourceActivated ? StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION : null;
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            if(retainedHistory!=null&&nativeReady){
                retainedHistory.current().run();final var retained=new TargetRetirementSourceHistoryRecord(intent,stamp,retainedHistory.proof());
                final var previous=TargetRetirementSourceHistoryStore.currentTip(reader,scope,lineage,retainedHistory.proof());
                final var accepted=new TargetRetirementHistoryTipRecord(retained);
                if(previous!=null)accepted.requireAfter(previous);
                requireAbsent(reader,ColumnFamily.META,retained.key());
                edits.add(reader.replace(ColumnFamily.META,retained.key(),TargetRetirementSourceHistoryRecord.VALUE_TYPE,retained.canonicalBytes()));
                edits.add(reader.replace(ColumnFamily.META,accepted.key(),TargetRetirementHistoryTipRecord.VALUE_TYPE,accepted.canonicalBytes()));
            }
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
        return new Prepared(this, batch, result[0], selected,retainedHistory);
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
                    if(prepared.history!=null)prepared.history.current().run();
                    guard.requireCurrent();
                }
                @Override public void close() { guard.close(); }
            };
        });
        return prepared.result;
    }

    /** Actual retained Native intent/cut; this grants no provider, Floor, Pin or current-purpose permission. */
    public static final class AppliedIntent {
        private final TargetEvidenceRetiringRecord record;
        private final TargetQuotaMutation cut;
        private AppliedIntent(TargetEvidenceRetiringRecord record,TargetQuotaMutation cut){this.record=record;this.cut=cut;}
        public TargetEvidenceRetiringRecord record(){return record;}
        public TargetQuotaMutation cut(){return cut;}
    }
    public static AppliedIntent readAppliedIntent(TargetStoreBackend backend,com.nereusstream.delay.store.BoundedReadBudget budget,
            TargetQuotaScope scope,byte[] lineage,SystemMutation selected,TargetStoreBackend.ReadAuthority authority){
        if(scope.target()!=null||budget.maxBytes()==Long.MAX_VALUE||selected.type()!=SystemMutationType.RESOURCE_RETIRE_INTENT)
            throw new IllegalArgumentException("marker reservation requires bounded actual SHARD intent reads");
        final var body=TargetEvidenceRetireIntentBody.decode(selected.canonicalBody());
        return backend.guardedRead(budget,reader->{
            final var cut=reader.aggregate().mutation();cut.requireAtOrBefore(reader.sourceSequence(),reader.source());
            final var root=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
            final byte[] key=TargetEvidenceRetiringRecord.key(body.resource().exactIdentity().identityHash());
            final var retained=TargetEvidenceRetiringRecord.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetEvidenceRetiringRecord.VALUE_TYPE),scope.shard(),scope.tenantScope(),lineage);
            requireSame(selected.canonicalEnvelope(),retained.intent().canonicalEnvelope());
            requireRetiringState(reader,List.of(),scope,lineage,root,retained,cut);
            final var activation=TargetProtocolActivationStore.applied(reader,scope,lineage,body.requiredTuple());
            if(activation==null||!activation.body().payload().descriptor().supportsRetainedMarkerConfirmation())
                throw new IllegalStateException("marker reservation requires actual APPLIED descriptor5");
            final var resource=retained.copy().resource();
            if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery)
                requireRecoveryReferences(reader,List.of(),scope,lineage,recovery.owner());
            else if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                    ||resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor)
                requireNoRecoveryDependents(reader,List.of(),scope,lineage,controlOwner(resource),cut);
            reader.requireWithinElapsedBudget();return new AppliedIntent(retained,cut);
        },authority);
    }

    /** Same original state1 facts for live reads and immutable-image accounting, without Catalog/provider I/O. */
    public static TargetQuotaIncarnation requireRetiringState(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,TargetEvidenceRetiringRecord retiring,TargetQuotaMutation frontier){
        retiring.appliedAt().requireAtOrBefore(frontier);
        TargetProtocolActivationStore.requirePriorActivation(view,overlay,scope,lineage,sourceOwner,retiring.appliedAt(),frontier);
        TargetRetirementSourceHistoryStore.requireRetained(view,overlay,scope,lineage,sourceOwner,retiring.intent().systemMutationId(),retiring.appliedAt(),frontier);
        requireAppliedFirst(view,overlay,scope,lineage,sourceOwner,retiring.intent().systemMutationId(),retiring.intent().mutationHash(),
                SystemMutationType.RESOURCE_RETIRE_INTENT,retiring.appliedAt(),retiring.intent(),frontier);
        requireRetainedUses(view,overlay,scope,lineage,sourceOwner,retiring,frontier);
        final var origin=retiring.copy().resource().provenance();
        if(origin instanceof TargetExternalEvidenceObjectResource.Recovery recovery){
            final var owner=recovery.owner();
            requireSame(owner.canonicalBytes(),payload(view,overlay,ColumnFamily.META,owner.key(),TargetRecoveryEvidenceOwner.VALUE_TYPE));
            final var ref=owner.referenceRecord();
            for(byte[] key:List.of(ref.key(),ref.attemptKey()))
                requireSame(ref.canonicalBytes(),payload(view,overlay,ColumnFamily.META,key,TargetRecoveryEvidenceReference.VALUE_TYPE));
        }else if(origin instanceof TargetExternalEvidenceObjectResource.Protection p)
            requireSame(p.owner().canonicalBytes(),payload(view,overlay,ColumnFamily.META,p.owner().key(),TargetExternalEvidenceOwner.VALUE_TYPE));
        else if(origin instanceof TargetExternalEvidenceObjectResource.Anchor a)
            requireSame(a.owner().canonicalBytes(),payload(view,overlay,ColumnFamily.META,a.owner().key(),TargetExternalEvidenceOwner.VALUE_TYPE));
        final var frozen=retiring.feeOwner();final byte[] feeKey=frozen.identity().key();feeKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var fee=TargetQuotaIncarnation.decodeForStore(feeKey,payload(view,overlay,ColumnFamily.META,feeKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!fee.identity().equals(frozen.identity())||!Arrays.equals(fee.recoveryLineage(),lineage)||!fee.accounting().equals(frozen.accounting()))
            throw new IllegalStateException("retiring copy changed actual frozen fee owner/accounting");
        fee.latestMutation().requireAtOrBefore(frontier);return fee;
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
        if (copy.resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery) {
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
        }
        final var sourceOwner = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                reader.aggregate().accountingIncarnation(), null, null);
        requireRetainedUses(reader, List.of(), scope, lineage, sourceOwner, record, cut);
        reader.requireWithinElapsedBudget();
        return List.of(reader.replace(ColumnFamily.META, record.key(), TargetEvidenceRetiringRecord.VALUE_TYPE, record.canonicalBytes()));
    }

    /**
     * Business edits for one confirmed Source batch, never provider/purpose/Floor approval or a commit shortcut.
     * The caller must authenticate the original Root's actual conditional-operation/observation receipt before
     * calling, and append exact confirmation history/first/POSITION in this same accounting batch.
     */
    public static List<TargetStoreBackend.Edit> confirmationEdits(TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,byte[] lineage,SystemMutation confirmation,TargetQuotaMutation stamp){
        final var body=TargetEvidenceDeleteConfirmedBody.decode(confirmation.canonicalBody());
        final var resource=body.resource();
        if(scope.target()!=null||!scope.shard().equals(resource.shard())||!Arrays.equals(scope.tenantScope(),resource.tenantScope())
                ||!Arrays.equals(lineage,resource.recoveryLineage()))
            throw new IllegalArgumentException("confirmation changed actual Source SHARD/tenant/lineage");
        final var cut=Objects.requireNonNull(reader.aggregate().mutation(),"established confirmation Native cut");
        stamp.requireAfter(cut);
        final var sourceOwner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] key=TargetEvidenceRetiringRecord.key(resource.exactIdentity().identityHash());
        final var prior=TargetEvidenceRetiringRecord.decodeForStore(key,
                payload(reader,ColumnFamily.META,key,TargetEvidenceRetiringRecord.VALUE_TYPE),scope.shard(),scope.tenantScope(),lineage);
        prior.appliedAt().requireAtOrBefore(cut);
        TargetProtocolActivationStore.requirePriorActivation(reader,List.of(),scope,lineage,sourceOwner,prior.appliedAt(),cut);
        TargetRetirementSourceHistoryStore.requireRetained(reader,List.of(),scope,lineage,sourceOwner,prior.intent().systemMutationId(),prior.appliedAt(),cut);
        requireAppliedFirst(reader,List.of(),scope,lineage,sourceOwner,prior.intent().systemMutationId(),prior.intent().mutationHash(),
                SystemMutationType.RESOURCE_RETIRE_INTENT,prior.appliedAt(),prior.intent(),cut);
        requireRetainedUses(reader,List.of(),scope,lineage,sourceOwner,prior,cut);
        final var next=TargetEvidenceConfirmedRecord.fromRetiring(prior,confirmation,stamp);
        requireAbsent(reader,ColumnFamily.META,next.key());
        requireAbsent(reader,ColumnFamily.DEDUPE,resultKey(confirmation.systemMutationId()));
        requireAbsent(reader,ColumnFamily.DEDUPE,positionKey(stamp.source()));
        if(!TargetProtocolActivationStore.isActivatedForRetirement(reader,scope,lineage,body.requiredTuple(),stamp.source(),resource))
            throw new IllegalStateException("confirmation lacks actual prior Target capability");
        if(body.markerRetained()){
            TargetProtocolActivationStore.requireRetainedMarkerConfirmation(reader,List.of(),scope,lineage,sourceOwner,stamp,cut);
            prior.copy().requireMarker(body.markerEvidence().marker());
        }
        final var edits=new ArrayList<TargetStoreBackend.Edit>();
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery){
            final var owner=recovery.owner();
            requireSame(owner.canonicalBytes(),payload(reader,ColumnFamily.META,owner.key(),TargetRecoveryEvidenceOwner.VALUE_TYPE));
            requireRecoveryReferences(reader,List.of(),scope,lineage,owner);
            final var ref=owner.referenceRecord();
            edits.add(reader.replace(ColumnFamily.META,ref.key(),TargetRecoveryEvidenceReference.VALUE_TYPE,null));
            edits.add(reader.replace(ColumnFamily.META,ref.attemptKey(),TargetRecoveryEvidenceReference.VALUE_TYPE,null));
            edits.add(reader.replace(ColumnFamily.META,owner.key(),TargetRecoveryEvidenceOwner.VALUE_TYPE,null));
        }else if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                ||resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor){
            final var owner=controlOwner(resource);
            requireNoRecoveryDependents(reader,List.of(),scope,lineage,owner,cut);
            final var refs=requireControlReferences(reader,List.of(),scope,lineage,sourceOwner,owner,
                    resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor a?a:null,body.context().uses(),body.context().preparedCut(),cut);
            for(var ref:refs){
                edits.add(reader.replace(ColumnFamily.META,ref.key(),TargetExternalEvidenceReference.VALUE_TYPE,null));
                if(ref.kind()==TargetKafkaReceiptControlArtifact.ANCHOR)
                    edits.add(reader.replace(ColumnFamily.META,TargetExternalEvidenceStore.referenceByAttemptKey(ref.attemptId()),TargetExternalEvidenceReference.VALUE_TYPE,null));
            }
            edits.add(reader.replace(ColumnFamily.META,owner.key(),TargetExternalEvidenceOwner.VALUE_TYPE,null));
        }
        edits.add(reader.replace(ColumnFamily.META,prior.key(),TargetEvidenceRetiringRecord.VALUE_TYPE,null));
        edits.add(reader.replace(ColumnFamily.META,next.key(),TargetEvidenceConfirmedRecord.VALUE_TYPE,next.canonicalBytes()));
        reader.requireWithinElapsedBudget();return List.copyOf(edits);
    }

    /** Shared Native contract over actual bytes, also used by immutable-image accounting; no Catalog I/O. */
    public static void requireRetainedUses(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity sourceOwner,
            TargetEvidenceRetiringRecord record, TargetQuotaMutation frontier) {
        if(record.copy().resource().provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging)
            TargetProtocolActivationStore.requireCapacityOwnerStaging(view,overlay,scope,lineage,sourceOwner,record.appliedAt(),frontier);
        requireRetainedUses(view,overlay,scope,lineage,sourceOwner,record.copy().resource(),record.body().context(),frontier);
        if(record.copy().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                ||record.copy().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Anchor)
            requireControlAdoptions(view,overlay,scope,lineage,sourceOwner,record,frontier);
    }

    private static void requireRetainedUses(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity sourceOwner,
            TargetExternalEvidenceObjectResource resource,TargetEvidenceRetirementContext context,TargetQuotaMutation frontier) {
        if (sourceOwner.kind() != TargetQuotaIdentity.Kind.SHARD || !sourceOwner.shard().equals(scope.shard()))
            throw new IllegalArgumentException("retirement requires the actual Source accounting root");
        context.preparedCut().requireAtOrBefore(frontier);
        final byte[] rawFence = view.projected(ColumnFamily.META, KeyCodec.metaFixed(4), overlay);
        final var fence = rawFence == null ? new IngressFenceState(IngressFenceState.OPEN, null)
                : IngressFenceState.decode(TargetValueEnvelope.decode(rawFence, 1).payload());
        if (fence.closedThroughEpochMs() < context.fence().closedThroughEpochMs()
                || context.fence().proofId() != null && fence.proofId() == null)
            throw new IllegalStateException("retirement lost its committed prepared ingress fence");
        final var adopted = resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery r ? r.owner() : null;
        final boolean staging=resource.provenance() instanceof TargetExternalEvidenceObjectResource.StagingProvenance;
        final boolean control=resource.subtype()!=TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF;
        if(staging)requireStagingUnadopted(view,overlay,scope,lineage,sourceOwner,resource.subtype().wireValue(),
                resource.objectDigest(),resource.exactKey(),context.feeOwner(),frontier);
        if (adopted != null) adopted.adoptedAt().requireAtOrBefore(context.preparedCut());
        boolean adoptingFirst = false;
        for (var use : context.uses()) {
            if(staging&&use.conclusion()==TargetEvidenceRetirementContext.Conclusion.RETAINED_APPLIED)
                throw new IllegalStateException("staging retirement cannot claim an adopted sealed use");
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
                    || (control ? result.mutationType()!=SystemMutationType.TARGET_PUBLISH_ADMISSION
                        : result.mutationType()!=SystemMutationType.PUBLISH_OUTCOME&&result.mutationType()!=SystemMutationType.EVIDENCE_RESOLUTION)
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

    /** No logical Target owner is fabricated for staging; complete Native absence is additional to Root provenance. */
    static void requireStagingUnadopted(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,int subtype,byte[] objectDigest,String exactKey,
            TargetQuotaIncarnation frozen,TargetQuotaMutation frontier){
        final byte[] ownerKey=sourceOwner.key();ownerKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var actual=TargetQuotaIncarnation.decodeForStore(ownerKey,
                payload(view,overlay,ColumnFamily.META,ownerKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!frozen.identity().equals(sourceOwner)||!Arrays.equals(actual.recoveryLineage(),lineage)
                ||!frozen.accounting().equals(actual.accounting())||!frozen.allocation().equals(actual.allocation())
                ||frozen.draining()&&(!actual.draining()||!frozen.latestMutation().equals(actual.latestMutation())))
            throw new IllegalStateException("staging copy changed its actual frozen Source SHARD fee owner");
        actual.latestMutation().requireAtOrBefore(frontier);
        final boolean recovery=subtype==TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF.wireValue();
        final byte[] adoptedKey=recovery?TargetRecoveryEvidenceOwner.key(objectDigest)
                :TargetExternalEvidenceOwner.key(subtype,objectDigest);
        final byte[] adopted=view.projected(ColumnFamily.META,adoptedKey,overlay);
        final boolean ownedElsewhere;
        if(adopted==null)ownedElsewhere=false;
        else{
            final String adoptedExactKey=recovery?TargetRecoveryEvidenceOwner.decodeForStore(adoptedKey,
                    TargetValueEnvelope.decode(adopted,TargetRecoveryEvidenceOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope(),lineage).reference().exactKey()
                    :TargetExternalEvidenceOwner.decodeForStore(adoptedKey,
                        TargetValueEnvelope.decode(adopted,TargetExternalEvidenceOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope(),lineage).exactKey();
            if(adoptedExactKey.equals(exactKey))
                throw new IllegalStateException("staging object already has an actual Native adoption owner");
            // The digest index can name a different immutable physical copy. Its references retain that
            // exact owner's key; they cannot confer adoption on this Catalog object's different key.
            ownedElsewhere=true;
        }
        final byte tag=(byte)(recovery?TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG:TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG);
        byte[] lower={tag,TargetKeyCodec.KEY_FORMAT};final byte[] upper={tag,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=view.first(ColumnFamily.META,lower,upper,overlay);if(row==null)break;
            if(Arrays.compareUnsigned(row.key(),lower)<0||Arrays.compareUnsigned(row.key(),upper)>=0)
                throw new IllegalStateException("staging reference read escaped its finite range");
            lower=Bytes.concat(row.key(),new byte[]{0});
            final boolean matches;
            if(recovery){
                final var ref=TargetRecoveryEvidenceReference.decodeForStore(row.key(),
                        TargetValueEnvelope.decode(row.value(),TargetRecoveryEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
                matches=Arrays.equals(ref.proofDigest(),objectDigest);
            }else{
                final var ref=TargetExternalEvidenceReference.decodeForStore(row.key(),
                        TargetValueEnvelope.decode(row.value(),TargetExternalEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
                matches=ref.kind()==subtype&&Arrays.equals(ref.artifactDigest(),objectDigest);
            }
            if(matches&&!ownedElsewhere)throw new IllegalStateException("staging object still has an actual Native reference/Attempt alias");
        }
    }

    private static byte[] resultKey(byte[] id) {
        return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }
    /** Complete cold-path Native reference join; an incomplete range never proves shared-use closure. */
    private static void requireControlAdoptions(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,TargetEvidenceRetiringRecord retiring,
            TargetQuotaMutation frontier){
        final var resource=retiring.copy().resource();final var context=retiring.body().context();
        final var origin=resource.provenance();
        final var owner=origin instanceof TargetExternalEvidenceObjectResource.Protection p?p.owner()
                :((TargetExternalEvidenceObjectResource.Anchor)origin).owner();
        requireSame(owner.canonicalBytes(),payload(view,overlay,ColumnFamily.META,owner.key(),TargetExternalEvidenceOwner.VALUE_TYPE));
        owner.createdAt().requireAtOrBefore(context.preparedCut());
        final var artifact=TargetKafkaReceiptControlArtifact.decode(retiring.copy().signedObject());
        final var prepared=artifact.control();final var request=(TargetChannelControlRequest)prepared.request().branch();
        final var control=new ControlRef(prepared.operationId(),prepared.requestHash(),0);
        final byte[] eventKey=TargetKeyCodec.channelControl(control.logicalOperationIdentity(request.controlKind()));
        final var event=TargetChannelControlRecord.decodeForStore(eventKey,
                payload(view,overlay,ColumnFamily.META,eventKey,TargetChannelControlRecord.VALUE_TYPE),scope.shard());
        if(!Arrays.equals(event.body().request().canonicalBytes(),request.canonicalBytes())||!event.author().equals(prepared.author())
                ||!Arrays.equals(event.recoveryLineage(),lineage)
                ||!Arrays.equals(event.mutation().source().canonicalBytes(),owner.originalControlSource().canonicalBytes()))
            throw new IllegalStateException("retiring CONTROL object lost original APPLIED channel event");
        event.mutation().requireAtOrBefore(owner.createdAt());
        requireControlReferences(view,overlay,scope,lineage,sourceOwner,owner,
                origin instanceof TargetExternalEvidenceObjectResource.Anchor a?a:null,context.uses(),context.preparedCut(),frontier);
    }

    /** Shared by actual Native closure preflight and state1 accounting; every applied use must retain exact refs. */
    static List<TargetExternalEvidenceReference> requireControlReferences(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,TargetExternalEvidenceOwner owner,
            TargetExternalEvidenceObjectResource.Anchor anchor,List<TargetEvidenceRetirementContext.Use> uses,
            TargetQuotaMutation preparedCut,TargetQuotaMutation frontier){
        requireSame(owner.canonicalBytes(),payload(view,overlay,ColumnFamily.META,owner.key(),TargetExternalEvidenceOwner.VALUE_TYPE));
        owner.createdAt().requireAtOrBefore(preparedCut);
        final var applied=new java.util.HashMap<String,TargetEvidenceRetirementContext.Use>();
        for(var use:uses)if(use.conclusion()==TargetEvidenceRetirementContext.Conclusion.RETAINED_APPLIED)
            applied.put(Bytes.hex(use.id()),use);
        final var joined=new java.util.LinkedHashMap<String,TargetExternalEvidenceReference>();boolean creator=false;
        byte[] lower={TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG,TargetKeyCodec.KEY_FORMAT};
        final byte[] upper={TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=view.first(ColumnFamily.META,lower,upper,overlay);if(row==null)break;
            if(Arrays.compareUnsigned(row.key(),lower)<0||Arrays.compareUnsigned(row.key(),upper)>=0)
                throw new IllegalStateException("CONTROL reference read escaped its finite range");
            lower=Bytes.concat(row.key(),new byte[]{0});
            final var ref=TargetExternalEvidenceReference.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(),TargetExternalEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
            if(ref.kind()!=owner.kind()||!Arrays.equals(ref.artifactDigest(),owner.artifactDigest()))continue;
            // Scan aliases too. An orphan or changed alias cannot hide outside the primary kind range.
            requireSame(ref.canonicalBytes(),payload(view,overlay,ColumnFamily.META,ref.key(),TargetExternalEvidenceReference.VALUE_TYPE));
            if(ref.kind()==TargetKafkaReceiptControlArtifact.ANCHOR)
                requireSame(ref.canonicalBytes(),payload(view,overlay,ColumnFamily.META,
                        TargetExternalEvidenceStore.referenceByAttemptKey(ref.attemptId()),TargetExternalEvidenceReference.VALUE_TYPE));
            final byte[] mappingKey=Bytes.concat(new byte[]{TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG,TargetKeyCodec.KEY_FORMAT},ref.mappingId());
            final var mapping=TargetKafkaReceiptMapping.decodeForStore(mappingKey,
                    payload(view,overlay,ColumnFamily.META,mappingKey,TargetKafkaReceiptMapping.VALUE_TYPE),scope.shard());
            final var original=TargetExternalEvidenceStore.readMappingUse(view,overlay,mapping,scope,lineage,frontier,sourceOwner);
            final var attempt=original.attempt();
            if(attempt.phase()==TargetQuotaAttemptBudget.Phase.ADMITTED||attempt.phase()==TargetQuotaAttemptBudget.Phase.UNKNOWN)
                throw new IllegalStateException("CONTROL closure still has an unresolved Attempt needing its original recovery dependencies");
            final byte[] expectedDigest=owner.kind()==TargetKafkaReceiptControlArtifact.PROTECTION
                    ?mapping.admission().kafkaReceipt().retentionProtectionDigest():mapping.admission().kafkaReceipt().scanObservationDigest();
            final byte[] feeKey=owner.primaryOwner().identity().key();feeKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final var fee=TargetQuotaIncarnation.decodeForStore(feeKey,payload(view,overlay,ColumnFamily.META,feeKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
            fee.latestMutation().requireAtOrBefore(frontier);owner.createdAt().requireAtOrBefore(mapping.mutation());
            if(!ref.mutation().equals(mapping.mutation())||!Arrays.equals(ref.attemptId(),attempt.publishAttemptId())
                    ||!ref.referenceOwner().equals(attempt.primaryIdentity())||!ref.accounting().equals(attempt.accounting())
                    ||!Arrays.equals(expectedDigest,owner.artifactDigest())||!Arrays.equals(fee.recoveryLineage(),lineage)
                    ||!fee.accounting().equals(owner.primaryOwner().accounting())
                    ||!fee.identity().target().equals(attempt.primaryIdentity().target())
                    ||!owner.originalDomain().equals(mapping.admission().channel().context().domain()))
                throw new IllegalStateException("CONTROL reference changed its selected original owner/Admission/Attempt");
            mapping.mutation().requireAtOrBefore(preparedCut);
            final byte[] id=SystemMutation.computeSystemMutationId(scope.shard(),SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    mapping.admission().publishAttemptId(),mapping.mutationHash());
            final String identity=Bytes.hex(id);final var use=applied.get(identity);
            if(use==null||!Arrays.equals(use.hash(),mapping.mutationHash())
                    ||use.retryUntil()!=mapping.admission().retryUntilEpochMs())
                throw new IllegalStateException("retiring CONTROL object lacks complete sealed mapping references");
            final var previous=joined.putIfAbsent(identity,ref);
            if(previous!=null)requireSame(previous.canonicalBytes(),ref.canonicalBytes());
            if(mapping.mutation().equals(owner.createdAt())){
                if(owner.kind()==TargetKafkaReceiptControlArtifact.ANCHOR
                        &&(!Arrays.equals(owner.creatorAttemptId(),mapping.admission().publishAttemptId())
                            ||anchor!=null&&(!Arrays.equals(anchor.mappingId(),mapping.mappingId())
                                ||!Arrays.equals(anchor.admissionHash(),mapping.mutationHash()))))
                    throw new IllegalStateException("retiring anchor changed original creator Admission");
                creator=true;
            }
        }
        if(!creator||joined.size()!=applied.size())
            throw new IllegalStateException("retiring CONTROL object lost original adoption or a shared mapping/ref/alias");
        return List.copyOf(joined.values());
    }
    private static TargetExternalEvidenceOwner controlOwner(TargetExternalEvidenceObjectResource resource){
        return resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection p?p.owner()
                :((TargetExternalEvidenceObjectResource.Anchor)resource.provenance()).owner();
    }

    private static void requireRecoveryReferences(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetRecoveryEvidenceOwner owner){
        final var expected=owner.referenceRecord();final var found=new java.util.HashSet<String>();
        byte[] lower={TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG,TargetKeyCodec.KEY_FORMAT};
        final byte[] upper={TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=view.first(ColumnFamily.META,lower,upper,overlay);if(row==null)break;
            requireRange(row.key(),lower,upper);lower=Bytes.concat(row.key(),new byte[]{0});
            final var ref=TargetRecoveryEvidenceReference.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(),TargetRecoveryEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
            if(!Arrays.equals(ref.proofDigest(),owner.reference().proofDigest()))continue;
            requireSame(ref.canonicalBytes(),expected.canonicalBytes());
            found.add(Bytes.hex(row.key()));
        }
        if(found.size()!=2||!found.contains(Bytes.hex(expected.key()))||!found.contains(Bytes.hex(expected.attemptKey())))
            throw new IllegalStateException("retiring proof lost exact complete reference/Attempt alias");
    }

    /** A live adopted proof still needs its original CONTROL dependencies, even if its Attempt has settled. */
    private static void requireNoRecoveryDependents(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetExternalEvidenceOwner owner,TargetQuotaMutation frontier){
        byte[] lower={TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG,TargetKeyCodec.KEY_FORMAT};
        final byte[] upper={TargetKeyCodec.RECOVERY_EVIDENCE_OWNER_TAG,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=view.first(ColumnFamily.META,lower,upper,overlay);if(row==null)break;
            requireRange(row.key(),lower,upper);lower=Bytes.concat(row.key(),new byte[]{0});
            final var proof=TargetRecoveryEvidenceOwner.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(),TargetRecoveryEvidenceOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope(),lineage);
            proof.adoptedAt().requireAtOrBefore(frontier);
            final byte[] mappingKey=Bytes.concat(new byte[]{TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG,TargetKeyCodec.KEY_FORMAT},proof.reference().mappingId());
            final var mapping=TargetKafkaReceiptMapping.decodeForStore(mappingKey,
                    payload(view,overlay,ColumnFamily.META,mappingKey,TargetKafkaReceiptMapping.VALUE_TYPE),scope.shard());
            mapping.mutation().requireAtOrBefore(frontier);
            final byte[] digest=owner.kind()==TargetKafkaReceiptControlArtifact.PROTECTION
                    ?mapping.admission().kafkaReceipt().retentionProtectionDigest():mapping.admission().kafkaReceipt().scanObservationDigest();
            if(Arrays.equals(digest,owner.artifactDigest()))
                throw new IllegalStateException("CONTROL retirement still has an adopted recovery-proof dependency");
        }
    }
    private static void requireNoReferences(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetExternalEvidenceObjectResource resource){
        final boolean recovery=resource.subtype()==TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF;
        final byte tag=(byte)(recovery?TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG:TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG);
        byte[] lower={tag,TargetKeyCodec.KEY_FORMAT};final byte[] upper={tag,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=view.first(ColumnFamily.META,lower,upper,overlay);if(row==null)break;
            requireRange(row.key(),lower,upper);lower=Bytes.concat(row.key(),new byte[]{0});
            final boolean matches;
            if(recovery){
                final var ref=TargetRecoveryEvidenceReference.decodeForStore(row.key(),
                        TargetValueEnvelope.decode(row.value(),TargetRecoveryEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
                matches=Arrays.equals(ref.proofDigest(),resource.objectDigest());
            }else{
                final var ref=TargetExternalEvidenceReference.decodeForStore(row.key(),
                        TargetValueEnvelope.decode(row.value(),TargetExternalEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage);
                matches=ref.kind()==resource.subtype().wireValue()&&Arrays.equals(ref.artifactDigest(),resource.objectDigest());
            }
            if(matches)throw new IllegalStateException("confirmed object retained a reference/Attempt alias");
        }
    }
    private static void requireRange(byte[] key,byte[] lower,byte[] upper){
        if(Arrays.compareUnsigned(key,lower)<0||Arrays.compareUnsigned(key,upper)>=0)
            throw new IllegalStateException("retirement dependency read escaped its finite range");
    }

    /** Exact state2 invariant over actual bytes; neither a tombstone codec nor absence alone grants delete authority. */
    public static void requireConfirmedState(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,
            TargetEvidenceConfirmedRecord record,TargetQuotaMutation frontier){
        record.appliedAt().requireAtOrBefore(frontier);
        TargetProtocolActivationStore.requirePriorActivation(view,overlay,scope,lineage,sourceOwner,record.intentAppliedAt(),frontier);
        TargetRetirementSourceHistoryStore.requireRetained(view,overlay,scope,lineage,sourceOwner,record.body().intent().mutationId(),record.intentAppliedAt(),frontier);
        TargetRetirementSourceHistoryStore.requireRetained(view,overlay,scope,lineage,sourceOwner,record.confirmation().systemMutationId(),record.appliedAt(),frontier);
        final var body=record.body();final var resource=body.resource();
        if(body.markerRetained())TargetProtocolActivationStore.requireRetainedMarkerConfirmation(
                view,overlay,scope,lineage,sourceOwner,record.appliedAt(),frontier);
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.ReservedStaging)
            TargetProtocolActivationStore.requireCapacityOwnerStaging(view,overlay,scope,lineage,sourceOwner,record.intentAppliedAt(),frontier);
        if(scope.target()!=null||!scope.shard().equals(resource.shard())||!Arrays.equals(scope.tenantScope(),resource.tenantScope())
                ||!Arrays.equals(lineage,resource.recoveryLineage()))throw new IllegalStateException("confirmed resource changed actual Source scope");
        requireRetainedUses(view,overlay,scope,lineage,sourceOwner,resource,body.context(),frontier);
        if(view.projected(ColumnFamily.META,TargetEvidenceRetiringRecord.key(resource.exactIdentity().identityHash()),overlay)!=null)
            throw new IllegalStateException("confirmed object still has a retiring copy");
        if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery){
            if(view.projected(ColumnFamily.META,recovery.owner().key(),overlay)!=null)
                throw new IllegalStateException("confirmed proof retained its adopted owner");
            requireNoReferences(view,overlay,scope,lineage,resource);
        }else if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection
                ||resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor){
            final var owner=controlOwner(resource);
            if(view.projected(ColumnFamily.META,owner.key(),overlay)!=null)
                throw new IllegalStateException("confirmed CONTROL object retained its adopted owner");
            requireNoReferences(view,overlay,scope,lineage,resource);
            requireNoRecoveryDependents(view,overlay,scope,lineage,owner,frontier);
            boolean creator=false;
            for(var use:body.context().uses())if(use.conclusion()==TargetEvidenceRetirementContext.Conclusion.RETAINED_APPLIED){
                final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,resultKey(use.id()),TargetResultRecord.VALUE_TYPE));
                if(first.mutation().equals(owner.createdAt())){
                    if(resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor anchor
                            &&(!Arrays.equals(use.id(),anchor.admissionId())||!Arrays.equals(use.hash(),anchor.admissionHash())))
                        throw new IllegalStateException("confirmed anchor changed original creator Admission");
                    creator=true;
                }
            }
            if(!creator)throw new IllegalStateException("confirmed CONTROL object lost its original creator first");
        }
        requireAppliedFirst(view,overlay,scope,lineage,sourceOwner,body.intent().mutationId(),body.intent().mutationHash(),
                SystemMutationType.RESOURCE_RETIRE_INTENT,record.intentAppliedAt(),null,frontier);
        final var confirmation=record.confirmation();
        requireAppliedFirst(view,overlay,scope,lineage,sourceOwner,confirmation.systemMutationId(),confirmation.mutationHash(),
                SystemMutationType.RESOURCE_DELETE_CONFIRMED,record.appliedAt(),confirmation,frontier);
    }

    private static void requireAppliedFirst(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetQuotaIdentity sourceOwner,byte[] id,byte[] hash,
            SystemMutationType type,TargetQuotaMutation appliedAt,SystemMutation signed,TargetQuotaMutation frontier){
        final byte[] key=resultKey(id);final var first=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());
        final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(sourceOwner)||!first.mutation().equals(appliedAt)
                ||result.mutationType()!=type||result.applyStatus()!=ApplyStatus.APPLIED||result.stableCode()!=StableCode.OK
                ||!Arrays.equals(result.mutationId(),id)||!Arrays.equals(result.mutationHash(),hash)
                ||!Arrays.equals(result.appliedSourcePosition(),appliedAt.source().canonicalBytes())
                ||AuthorIdentity.decode(result.authorIdentity()).kind()!=AuthorIdentity.Kind.SERVICE
                ||signed!=null&&(result.retryUntilEpochMs()!=signed.retryUntilEpochMs()||!Arrays.equals(result.authorIdentity(),signed.authorIdentity())))
            throw new IllegalStateException("confirmed row lost its actual exact APPLIED Source first");
        final byte[] ownerKey=sourceOwner.key();ownerKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner=TargetQuotaIncarnation.decodeForStore(ownerKey,payload(view,overlay,ColumnFamily.META,ownerKey,TargetQuotaIncarnation.VALUE_TYPE),
                scope.shard(),scope.tenantScope());
        if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("confirmed first changed Source lineage");
        owner.latestMutation().requireAtOrBefore(frontier);first.requireOwner(owner);
        final byte[] positionKey=positionKey(appliedAt.source());
        final var position=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(owner);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(appliedAt))
            throw new IllegalStateException("confirmed row lost original Source POSITION");
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
