package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Same-Source atomic unused-reserve transfer; never a local GC ordinal or a physical release. */
public final class TargetResourceReclaimStore {
    public interface Authority {
        /** Complete independently pinned history, including a proved denial. Missing history must throw. */
        SourceHistory history(TargetQuotaScope scope, SystemMutation input, SourcePosition source);
        /**
         * Acquire outside Native locks. A valid grant without all original protections stays pending.
         * The hold must cover the original Worker/Store, actual published Catalog/Floor/ancestry,
         * active Pin and future-reader admission, fully authenticated images, and every reserved
         * writer (including Journal and late completions). No caller inventory or phase flag is proof.
         */
        Protection protect(TargetQuotaScope scope, SystemMutation input, SourcePosition source,
                TargetRetirementAuthorizationHistory.Grant grant);
    }

    /** Opaque receipt from the selected original Source and actual managed Catalog/image authentication. */
    public static final class Protection {
        private final com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ReclaimRead source;
        private final com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery;
        private final Runnable held;
        private final SystemMutation input;
        private Protection(com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ReclaimRead source,
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,Runnable held){
            this.source=source;this.recovery=recovery;this.held=held;input=source.entry().mutation();
        }
        /** Requires the original deployment hold to protect future-reader/Pin/image admission throughout commit. */
        public static Protection collect(com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ReclaimRead source,
                List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,BoundedReadBudget recoveryReads,
                long maximumInputBytes,Runnable originalHeld){
            Objects.requireNonNull(source);Objects.requireNonNull(originalHeld).run();source.requireCurrent();
            final var body=TargetResourceReclaimBody.decode(source.entry().mutation().canonicalBody());
            final var catalog=source.catalog();
            if(com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.recoveryProtectionInputWindowBytes(images)>maximumInputBytes)
                throw new IllegalStateException("Native recovery authentication exceeds original Source input reservation");
            final var recovery=catalog.captureSourceNativeRecoveryProtection(source,images,recoveryReads,maximumInputBytes,()->{source.requireCurrent();originalHeld.run();});
            final var result=new Protection(source,recovery,originalHeld);
            if(body.objectPayloadReclaim())source.requireClosedObject(result);else if(body.nativeRetainedReclaim())source.requireClosedNative(result);else if(body.inlinePayloadRelease())source.requireClosedInline(result);else source.requireClosedUnused(result);
            recovery.requireCurrent(recoveryReads,()->{source.requireCurrent();originalHeld.run();});
            result.requireCurrent();return result;
        }
        public void requireSource(TargetQuotaScope scope,byte[] lineage,SystemMutation actual,SourcePosition at){
            requireCurrent();final var original=source.entry();final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!scope.equals(body.scope())||!Arrays.equals(lineage,body.lineage())||!Arrays.equals(actual.canonicalEnvelope(),input.canonicalEnvelope())
                    ||!Arrays.equals(original.position().canonicalBytes(),at.canonicalBytes()))throw new IllegalArgumentException("Native reclaim protection changed original Source");
        }
        private void requireBackend(TargetStoreBackend backend){source.requireBackend(backend);}
        private com.nereusstream.delay.store.TargetCheckpointRootVerifier.UnusedReserveWriter writer(TargetQuotaAttemptBudget expected){
            return recovery.writers().stream().filter(w->Arrays.equals(w.expected().canonicalBytes(),expected.canonicalBytes())).findFirst()
                    .orElseThrow(()->new IllegalStateException("protected images omit exact expected attempt"));
        }
        private void requireUnusedReservation(TargetQuotaAttemptBudget expected,RecoveryFloorRef floor,TargetQuotaMutation release){
            requireCurrent();writer(expected);recovery.requireReleaseMutation(release);final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!body.floor().equals(floor)||!Arrays.equals(release.source().canonicalBytes(),source.entry().position().canonicalBytes())
                    ||!Arrays.equals(release.mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))throw new IllegalStateException("unused reservation changed protected Source/Floor");
        }
        private void requireInlineRelease(TargetResourceReclaimBody.InlinePayloadRelease expected,RecoveryFloorRef floor,TargetQuotaMutation release){
            requireCurrent();recovery.requireReleaseMutation(release);final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!recovery.inlinePayload()||!body.floor().equals(floor)||!Arrays.equals(release.source().canonicalBytes(),source.entry().position().canonicalBytes())
                    ||!Arrays.equals(release.mutationDigest(),Bytes.sha256(input.canonicalEnvelope()))
                    ||body.operations().stream().noneMatch(o->Arrays.equals(o.canonicalBytes(),expected.canonicalBytes())))
                throw new IllegalStateException("inline release changed protected Source/Floor/expected value");
        }
        private void requireNativeRelease(TargetResourceReclaimBody.Operation expected,RecoveryFloorRef floor,TargetQuotaMutation release){
            requireCurrent();recovery.requireReleaseMutation(release);final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!recovery.nativeRetained()||!body.floor().equals(floor)||!Arrays.equals(release.source().canonicalBytes(),source.entry().position().canonicalBytes())
                    ||!Arrays.equals(release.mutationDigest(),Bytes.sha256(input.canonicalEnvelope()))
                    ||body.operations().stream().noneMatch(o->Arrays.equals(o.canonicalBytes(),expected.canonicalBytes())))
                throw new IllegalStateException("Native retained release changed exact Source/Floor/value");
        }
        private void requireObjectStage(RecoveryFloorRef floor,TargetQuotaMutation decision){
            requireCurrent();recovery.requireReleaseMutation(decision);final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!recovery.objectPayload()||!body.floor().equals(floor)||!Arrays.equals(decision.source().canonicalBytes(),source.entry().position().canonicalBytes())
                    ||!Arrays.equals(decision.mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))throw new IllegalStateException("OBJECT changed exact held Source/Floor");
        }
        public void requireCurrent(){source.requireCurrent();held.run();recovery.requireHeldFor(source.catalog(),input);source.requireCurrent();}
        public long retainedBytes(){return Math.addExact(recovery.retainedBytes(),input.canonicalEnvelope().length);}
    }

    public record SourceHistory(TargetRetirementAuthorizationHistory.PublicProof proof,
            TargetRetirementAuthorizationHistory.Pin pin, Runnable current) {
        public SourceHistory { Objects.requireNonNull(proof); Objects.requireNonNull(pin); Objects.requireNonNull(current); }
        public Optional<TargetRetirementAuthorizationHistory.Grant> select(TargetResourceReclaimBody body,
                SystemMutation input, SourcePosition source) {
            current.run();
            final var verified=proof.authenticate(pin);
            verified.requireCoverage(body,source);
            if(input.authorIdentityLength()>1024)return Optional.empty();
            final AuthorIdentity actor;
            try { actor=AuthorIdentity.decode(input.authorIdentity()); }
            catch(IllegalArgumentException invalid){return Optional.empty();}
            final var selected=verified.select(body,source,actor,input.signingKeyVersion());
            current.run();return selected;
        }
    }

    public static final class Prepared {
        private final TargetResourceReclaimStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;
        private final SourceHistory history;
        private final Protection protection;
        private final SystemMutation input;
        private final SourcePosition source;
        private Prepared(TargetResourceReclaimStore owner,TargetStoreBackend.Prepared batch,SystemMutationResult result,
                SourceHistory history,Protection protection,SystemMutation input,SourcePosition source){
            this.owner=owner;this.batch=batch;this.result=result;this.history=history;
            this.protection=protection;this.input=input;this.source=source;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters,maximumDomains;

    public TargetResourceReclaimStore(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage,
            int maximumCounters,int maximumDomains){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);
        Bytes.requireLength(lineage,16,"reclaim lineage");
        if(scope.target()!=null||Arrays.equals(lineage,new byte[16])||maximumCounters<4
                ||maximumDomains<1||maximumDomains>64)throw new IllegalArgumentException("bounded Source reclaim accounting required");
        this.lineage=Bytes.copy(lineage);this.maximumCounters=maximumCounters;this.maximumDomains=maximumDomains;
    }

    /** Native-only rejection preflight; the Source runtime still checks retained-first before this read. */
    public boolean requiresInputs(BoundedReadBudget reads,SystemMutation input,SourcePosition source){
        final var body=body(input,source);
        return source.brokerPersistenceTimeEpochMs()<=input.retryUntilEpochMs()&&backend.prepareRead(reads,
                r->r.closedIngressDeadlineThrough()<input.retryUntilEpochMs()&&protocolReady(r,source,body)).value();
    }

    /** Retained-first replay must run before this entry and before any protection provider. */
    public Prepared prepareFirst(BoundedReadBudget reads,SystemMutation input,SourcePosition source,Authority authority){
        Objects.requireNonNull(authority);TargetSourcePosition.requireBounded(source);
        final var body=body(input,source);
        final boolean expired=source.brokerPersistenceTimeEpochMs()>input.retryUntilEpochMs()
                ||backend.prepareRead(reads,r->r.closedIngressDeadlineThrough()>=input.retryUntilEpochMs()).value();
        final boolean ready=!expired&&backend.prepareRead(reads,r->protocolReady(r,source,body)).value();
        final var history=ready?Objects.requireNonNull(authority.history(scope,input,source),"complete reclaim Source history"):null;
        final var grant=history==null?null:history.select(body,input,source).orElse(null);
        if(history!=null)backend.prepareRead(reads,r->{TargetRetirementSourceHistoryStore.currentTip(r,scope,lineage,history.proof());return Boolean.TRUE;});
        final boolean authorized=grant!=null&&input.verifySignature(grant.writerKey());
        final boolean expectedCurrent=authorized&&backend.prepareRead(reads,r->matchesExpected(r,body)).value();
        final var protection=expectedCurrent?Objects.requireNonNull(authority.protect(scope,input,source,grant),
                "actual original reclaim protection is unavailable"):null;
        if(protection!=null){protection.requireBackend(backend);protection.requireSource(scope,lineage,input,source);protection.requireCurrent();}
        final var result=new SystemMutationResult[1];
        final var batch=backend.prepare(reads,reader->{
            if(reader.source()==null||source.compareTo(reader.source())<=0)
                throw new IllegalStateException("reclaim first requires an established strictly earlier Source");
            requireAbsent(reader,ColumnFamily.DEDUPE,resultKey(input.systemMutationId()));
            requireAbsent(reader,ColumnFamily.DEDUPE,positionKey(source));
            final var stamp=new TargetQuotaMutation(TargetQuotaMutation.increment(reader.sourceSequence()),source,Bytes.sha256(input.canonicalEnvelope()));
            final var root=sourceRoot(reader,stamp);
            final boolean nativeReady=protocolReady(reader,source,body);
            if(nativeReady&&!expired&&history==null)throw new IllegalStateException("reclaim activation changed; retain and recollect original inputs");
            boolean bound=authorized&&nativeReady;
            if(bound){
                try {TargetProtocolActivationStore.requireHistoryBinding(reader,scope,lineage,grant,source);}
                catch(CommandResolutionException denied){if(denied.stableCode()!=StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION)throw denied;bound=false;}
            }
            StableCode rejection=expired||reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs()
                    ?StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED:!nativeReady?StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION
                    :!authorized?StableCode.UNAUTHORIZED_SYSTEM_MUTATION:!bound?StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION:null;
            final var edits=new ArrayList<TargetStoreBackend.Edit>();
            if(history!=null&&nativeReady){
                history.current().run();
                final var retained=new TargetRetirementSourceHistoryRecord(input,stamp,history.proof());
                final var previous=TargetRetirementSourceHistoryStore.currentTip(reader,scope,lineage,history.proof());
                final var tip=new TargetRetirementHistoryTipRecord(retained);
                if(previous!=null)tip.requireAfter(previous);
                requireAbsent(reader,ColumnFamily.META,retained.key());
                edits.add(reader.replace(ColumnFamily.META,retained.key(),TargetRetirementSourceHistoryRecord.VALUE_TYPE,retained.canonicalBytes()));
                edits.add(reader.replace(ColumnFamily.META,tip.key(),TargetRetirementHistoryTipRecord.VALUE_TYPE,tip.canonicalBytes()));
            }
            // Validate the entire bounded group before emitting any phase transition or refund.
            if(rejection==null&&!matchesExpected(reader,body))rejection=StableCode.STALE_SYSTEM_MUTATION;
            if(rejection==null&&protection==null)throw new IllegalStateException("expected reclaim value reappeared without original held protections");
            if(rejection==null){
                protection.requireSource(scope,lineage,input,source);protection.requireCurrent();
                if(body.objectPayloadReclaim()){
                    requireClosedObject(reader,scope,lineage,input,source,protection);protection.requireObjectStage(body.floor(),stamp);
                    final var expected=body.objectOwner();
                    if(body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadRetire||body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectRetire){
                        final var pending=new TargetPayloadObjectRetiringRecord(input,stamp);
                        requireAbsent(reader,ColumnFamily.META,pending.key());
                        edits.add(reader.replace(ColumnFamily.META,pending.key(),TargetPayloadObjectRetiringRecord.VALUE_TYPE,pending.canonicalBytes()));
                    }else{
                        final var intent=body.operations().getFirst() instanceof TargetResourceReclaimBody.ObjectPayloadConfirm confirmation?confirmation.intent():((TargetResourceReclaimBody.UncommittedObjectConfirm)body.operations().getFirst()).intent();
                        final var actual=TargetQuotaPayloadOwner.decodeForStore(expected.key(),payload(reader,ColumnFamily.META,expected.key(),TargetQuotaPayloadOwner.VALUE_TYPE),scope.shard(),scope.tenantScope());
                        final var released=actual.release(body.floor(),stamp,(before,after,floor)->{
                            if(!Arrays.equals(before.canonicalBytes(),expected.canonicalBytes())||after.phase()!=TargetQuotaPayloadOwner.Phase.RELEASED||!after.mutation().equals(stamp))throw new IllegalStateException("OBJECT changed frozen owner on release");
                            protection.requireObjectStage(floor,stamp);
                        });
                        if(!body.uncommittedObjectReclaim())edits.add(reader.replace(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()),TargetMessageRecord.VALUE_TYPE,null));
                        edits.add(reader.replace(ColumnFamily.META,intent.key(),TargetPayloadObjectRetiringRecord.VALUE_TYPE,null));
                        edits.add(reader.replace(ColumnFamily.META,released.key(),TargetQuotaPayloadOwner.VALUE_TYPE,released.canonicalBytes()));
                    }
                }else if(body.nativeRetainedReclaim()){
                    requireClosedNative(reader,scope,lineage,input,source,protection);
                    for(var operation:body.operations()){
                        if(operation instanceof TargetResourceReclaimBody.RetainedRecordDelete leaf){
                            leaf.expected().requireDeletion(body.floor(),stamp,(record,floor,deletion)->{
                                if(!Arrays.equals(record.canonicalBytes(),leaf.expected().canonicalBytes()))throw new IllegalStateException("Native deletion changed exact leaf");
                                protection.requireNativeRelease(leaf,floor,deletion);
                            });
                            edits.add(reader.replace(ColumnFamily.DEDUPE,leaf.expected().key(),TargetResultRecord.VALUE_TYPE,null));
                        }else if(operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation){
                            final var actual=TargetQuotaAttemptBudget.decodeForStore(allocation.expected().key(),payload(reader,ColumnFamily.META,allocation.expected().key(),TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
                            final var after=actual.releaseRetained(allocation.remaining(),body.floor(),stamp,(prior,kind,floor,remaining,decision)->{
                                if(kind!=TargetQuotaAttemptBudget.ReleaseKind.RETAINED_ALLOCATION||!Arrays.equals(prior.canonicalBytes(),allocation.expected().canonicalBytes())
                                        ||!remaining.equals(allocation.remaining()))throw new IllegalStateException("retained settlement changed frozen allocation basis");
                                protection.requireNativeRelease(allocation,floor,decision);
                            });
                            edits.add(reader.replace(ColumnFamily.META,after.key(),TargetQuotaAttemptBudget.VALUE_TYPE,after.canonicalBytes()));
                        }else if(operation instanceof TargetResourceReclaimBody.IncarnationDrain drain){
                            final var next=drain.expected().drain(stamp,(prior,after)->protection.requireNativeRelease(drain,body.floor(),stamp));
                            edits.add(reader.replace(ColumnFamily.META,next.key(),TargetQuotaIncarnation.VALUE_TYPE,next.canonicalBytes()));
                        }else if(operation instanceof TargetResourceReclaimBody.IncarnationRetire retire){
                            final var expected=retire.expected();
                            final var primary=TargetQuotaCounter.decodeForStore(expected.identity().key(),payload(reader,ColumnFamily.META,expected.identity().key(),TargetQuotaCounter.VALUE_TYPE),scope.shard());
                            final var mirror=TargetQuotaCounter.decodeForStore(expected.tenantIdentity().key(),payload(reader,ColumnFamily.META,expected.tenantIdentity().key(),TargetQuotaCounter.VALUE_TYPE),scope.shard());
                            final byte[] bookKey=TargetQuotaBookkeeping.keyFor(scope.shard());
                            final var book=TargetQuotaBookkeeping.decodeForStore(bookKey,payload(reader,ColumnFamily.META,bookKey,TargetQuotaBookkeeping.VALUE_TYPE),root.identity(),scope.tenantScope());
                            expected.requireRetirable(primary,mirror,book,reader.aggregate(),body.floor(),stamp,
                                    (owner,p,m,b,a,f,at)->protection.requireNativeRelease(retire,f,at));
                            edits.add(reader.replace(ColumnFamily.META,expected.key(),TargetQuotaIncarnation.VALUE_TYPE,null));
                            edits.add(reader.replace(ColumnFamily.META,retire.zeroGrant().key(),TargetQuotaGrantActivation.VALUE_TYPE,null));
                        }else throw new IllegalArgumentException("Native retained group changed family");
                    }
                }else if(body.inlinePayloadRelease()){
                    requireClosedInline(reader,scope,lineage,input,source,protection);
                    for(var operation:body.operations()){
                        final var inline=(TargetResourceReclaimBody.InlinePayloadRelease)operation;final var expected=inline.expected();
                        final var actual=TargetQuotaPayloadOwner.decodeForStore(expected.key(),
                                payload(reader,ColumnFamily.META,expected.key(),TargetQuotaPayloadOwner.VALUE_TYPE),scope.shard(),scope.tenantScope());
                        final var released=actual.release(body.floor(),stamp,(before,after,floor)->{
                            if(!Arrays.equals(before.canonicalBytes(),expected.canonicalBytes())||!after.mutation().equals(stamp)
                                    ||after.phase()!=TargetQuotaPayloadOwner.Phase.RELEASED)throw new IllegalStateException("inline release changed complete original owner");
                            protection.requireInlineRelease(inline,floor,stamp);protection.requireCurrent();
                        });
                        edits.add(reader.replace(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()),TargetMessageRecord.VALUE_TYPE,null));
                        edits.add(reader.replace(ColumnFamily.META,released.key(),TargetQuotaPayloadOwner.VALUE_TYPE,released.canonicalBytes()));
                    }
                }else{
                requireClosedUnused(reader,scope,lineage,input,source,protection);
                for(var operation:body.operations()){
                    final var expected=((TargetResourceReclaimBody.UnusedAttemptReserve)operation).expected();
                    final var actual=TargetQuotaAttemptBudget.decodeForStore(expected.key(),
                            payload(reader,ColumnFamily.META,expected.key(),TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
                    final var retained=actual.retainAfterCheckpoint(body.floor(),stamp,(attempt,kind,floor,allocated,release)->{
                        if(kind!=TargetQuotaAttemptBudget.ReleaseKind.UNUSED_RESERVATION||!allocated.equals(expected.allocated())
                                ||!Arrays.equals(attempt.canonicalBytes(),expected.canonicalBytes()))throw new IllegalStateException("reclaim changed closed operation");
                        protection.requireUnusedReservation(expected,floor,release);protection.requireCurrent();
                    });
                    edits.add(reader.replace(ColumnFamily.META,retained.key(),TargetQuotaAttemptBudget.VALUE_TYPE,retained.canonicalBytes()));
                }
                }
            }
            final var outcome=SystemMutationResult.from(input,rejection==null?ApplyStatus.APPLIED:ApplyStatus.REJECTED,
                    rejection==null?StableCode.OK:rejection,source.canonicalBytes());
            final TargetResultRecord.CreationAuthority results=(record,first)->{
                record.requireOwner(root);
                if(!record.mutation().equals(stamp)||record.allocation()!=null)throw new IllegalStateException("reclaim first changed Source accounting");
                requireAbsent(reader,ColumnFamily.DEDUPE,record.key());
                if(record.kind()==TargetResultRecord.Kind.SYSTEM){
                    if(first!=null||!Arrays.equals(record.key(),resultKey(input.systemMutationId()))||!Arrays.equals(record.typedPayload(),outcome.encode()))
                        throw new IllegalStateException("reclaim first changed approved decision");
                }else if(record.kind()==TargetResultRecord.Kind.POSITION_SYSTEM){
                    if(first==null||!Arrays.equals(first.key(),resultKey(input.systemMutationId()))||!Arrays.equals(first.typedPayload(),outcome.encode())
                            ||!Arrays.equals(record.key(),positionKey(source)))throw new IllegalStateException("reclaim POSITION lost original first");
                    record.requireFirst(first);
                }else throw new IllegalStateException("unexpected reclaim result kind");
            };
            final var first=TargetResultRecord.system(root,outcome,stamp,null,results);
            final var position=TargetResultRecord.position(root,first,stamp,results);
            edits.add(reader.replace(ColumnFamily.DEDUPE,first.key(),TargetResultRecord.VALUE_TYPE,first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE,position.key(),TargetResultRecord.VALUE_TYPE,position.canonicalBytes()));
            result[0]=outcome;
            return new TargetSourceAccounting(scope,lineage,source,stamp.mutationDigest(),maximumCounters,
                    TargetResourceReclaimBody.MAX_OPERATIONS,maximumDomains).assemble(reader,edits);
        });
        return new Prepared(this,batch,result[0],history,protection,input,source);
    }

    public SystemMutationResult commit(Prepared prepared,TargetStoreBackend.CommitAuthority authority){
        if(Objects.requireNonNull(prepared).owner!=this)throw new IllegalArgumentException("foreign reclaim plan");
        backend.commit(prepared.batch,(metadata,actualScope,mutation)->{
            final var guard=Objects.requireNonNull(authority.acquire(metadata,actualScope,mutation));
            return new TargetStoreBackend.CommitGuard(){
                public void requireCurrent(){
                    guard.requireCurrent();
                    if(prepared.history!=null)prepared.history.current().run();
                    if(prepared.protection!=null){prepared.protection.requireSource(scope,lineage,prepared.input,prepared.source);prepared.protection.requireCurrent();}
                    guard.requireCurrent();
                }
                public void close(){guard.close();}
            };
        });return prepared.result;
    }

    private TargetResourceReclaimBody body(SystemMutation input,SourcePosition source){
        if(input.type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM)throw new IllegalArgumentException("closed Target reclaim operation required");
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||!scope.shard().equals(source.shardId())
                ||!scope.shard().equals(input.shardId())||body.retryUntil()!=input.retryUntilEpochMs()
                ||!Arrays.equals(body.logicalOperationIdentity(),input.logicalOperationIdentity()))throw new IllegalArgumentException("reclaim Source/scope/lineage/identity mismatch");
        return body;
    }
    private boolean protocolReady(TargetStoreBackend.Reader reader,SourcePosition source,TargetResourceReclaimBody body){
        final var activation=TargetProtocolActivationStore.applied(reader,scope,lineage,body.requiredTuple());
        return activation!=null&&(body.duplicatePositionReclaim()?activation.body().payload().descriptor().supportsDuplicatePositionReclaim():body.incarnationReclaim()?activation.body().payload().descriptor().supportsIncarnationReclaim():body.anchorAllocationReclaim()?activation.body().payload().descriptor().supportsAnchorAllocationReclaim():body.uncommittedObjectReclaim()?activation.body().payload().descriptor().supportsUncommittedObjectPayloadReclaim():body.objectPayloadReclaim()?activation.body().payload().descriptor().supportsObjectPayloadReclaim():body.nativeRetainedReclaim()?activation.body().payload().descriptor().supportsNativeRetainedReclaim()
                :body.selfContainedInlinePayloadRelease()?activation.body().payload().descriptor().supportsSelfContainedInlinePayloadReclaim()
                :body.inlinePayloadRelease()?activation.body().payload().descriptor().supportsInlinePayloadReclaim()
                :activation.body().payload().descriptor().supportsResourceReclaim())&&activation.appliedAt().source().compareTo(source)<0;
    }
    private TargetQuotaIncarnation sourceRoot(TargetStoreBackend.Reader reader,TargetQuotaMutation stamp){
        final var id=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] key=id.key();key[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!id.equals(root.identity())||!Arrays.equals(root.recoveryLineage(),lineage))throw new IllegalStateException("reclaim changed original Source root");
        root.latestMutation().requireAtOrBefore(reader.aggregate().mutation());stamp.requireAfter(reader.aggregate().mutation());return root;
    }
    public static boolean matchesExpected(TargetStoreBackend.Reader reader,TargetResourceReclaimBody body){
        for(var operation:body.operations()){
            switch(operation){case TargetResourceReclaimBody.UnusedAttemptReserve unused->{
                final var expected=unused.expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
                if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetQuotaAttemptBudget.VALUE_TYPE).payload(),expected.canonicalBytes()))return false;
                expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
            }case TargetResourceReclaimBody.InlinePayloadRelease inline->{
                final var expected=inline.expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
                final byte[] primary=reader.get(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()));
                final byte[] terminal=reader.get(ColumnFamily.TERMINAL,TargetTerminalGenerationRecord.key(inline.locator()));
                if(raw==null||primary==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetQuotaPayloadOwner.VALUE_TYPE).payload(),expected.canonicalBytes())
                        ||terminal==null||!Arrays.equals(Bytes.sha256(TargetValueEnvelope.decode(primary,TargetMessageRecord.VALUE_TYPE).payload()),inline.primaryDigest())
                        ||!Arrays.equals(Bytes.sha256(TargetValueEnvelope.decode(terminal,TargetTerminalGenerationRecord.VALUE_TYPE).payload()),inline.terminalDigest()))return false;
                expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
            }case TargetResourceReclaimBody.ObjectPayloadRetire object->{
                if(!matchesObject(reader,object)||reader.get(ColumnFamily.META,TargetPayloadObjectRetiringRecord.key(object.expected().messageId()))!=null)return false;
            }case TargetResourceReclaimBody.UncommittedObjectRetire uncommitted->{
                if(!matchesUncommitted(reader,uncommitted)||reader.get(ColumnFamily.META,TargetPayloadObjectRetiringRecord.key(uncommitted.expected().messageId()))!=null)return false;
            }case TargetResourceReclaimBody.UncommittedObjectConfirm confirmation->{
                final var pending=confirmation.intent();final byte[] raw=reader.get(ColumnFamily.META,pending.key());
                if(!matchesUncommitted(reader,pending.uncommittedOperation())||raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),pending.canonicalBytes()))return false;
                pending.appliedAt().requireAtOrBefore(reader.aggregate().mutation());
            }case TargetResourceReclaimBody.ObjectPayloadConfirm confirmation->{
                final var pending=confirmation.intent();final byte[] raw=reader.get(ColumnFamily.META,pending.key());
                if(!matchesObject(reader,pending.operation())||raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),pending.canonicalBytes()))return false;
                pending.appliedAt().requireAtOrBefore(reader.aggregate().mutation());
            }case TargetResourceReclaimBody.RetainedRecordDelete leaf->{
                final var expected=leaf.expected();final byte[] raw=reader.get(ColumnFamily.DEDUPE,expected.key());
                if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetResultRecord.VALUE_TYPE).payload(),expected.canonicalBytes()))return false;
                expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
            }case TargetResourceReclaimBody.IncarnationDrain drain->{
                if(!sameNative(reader,drain.expected().key(),TargetQuotaIncarnation.VALUE_TYPE,drain.expected().canonicalBytes())
                        ||!sameNative(reader,drain.close().key(),TargetCloseRecord.VALUE_TYPE,drain.close().canonicalBytes()))return false;
            }case TargetResourceReclaimBody.IncarnationRetire retire->{
                if(!sameNative(reader,retire.expected().key(),TargetQuotaIncarnation.VALUE_TYPE,retire.expected().canonicalBytes())
                        ||!sameNative(reader,retire.zeroGrant().key(),TargetQuotaGrantActivation.VALUE_TYPE,retire.zeroGrant().canonicalBytes()))return false;
            }case TargetResourceReclaimBody.RetainedAllocationRelease allocation->{
                final var expected=allocation.expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
                if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetQuotaAttemptBudget.VALUE_TYPE).payload(),expected.canonicalBytes()))return false;
                expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
            }}
        }
        return true;
    }

    private static boolean sameNative(TargetStoreBackend.Reader reader,byte[] key,int type,byte[] expected){
        final byte[] raw=reader.get(ColumnFamily.META,key);return raw!=null&&Arrays.equals(TargetValueEnvelope.decode(raw,type).payload(),expected);
    }
    private static boolean matchesUncommitted(TargetStoreBackend.Reader reader,TargetResourceReclaimBody.UncommittedObjectRetire selected){
        final var expected=selected.expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
        if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetQuotaPayloadOwner.VALUE_TYPE).payload(),expected.canonicalBytes())
                ||reader.get(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()))!=null)return false;
        if(selected.facts()==null)return false;final var reservation=selected.facts().reservation();
        for(var key:List.of(reservation.key(),reservation.lookupKey())){final byte[] value=reader.get(ColumnFamily.ID,key);
            if(value==null||!Arrays.equals(TargetValueEnvelope.decode(value,TargetReservationRecord.VALUE_TYPE).payload(),reservation.canonicalBytes()))return false;}
        expected.mutation().requireAtOrBefore(reader.aggregate().mutation());return true;
    }
    private static boolean matchesObject(TargetStoreBackend.Reader reader,TargetResourceReclaimBody.ObjectPayloadRetire object){
        final var expected=object.expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
        final byte[] primary=reader.get(ColumnFamily.ID,TargetKeyCodec.message(expected.messageId()));
        final byte[] terminal=reader.get(ColumnFamily.TERMINAL,TargetTerminalGenerationRecord.key(object.locator()));
        if(raw==null||primary==null||terminal==null||!Arrays.equals(TargetValueEnvelope.decode(raw,TargetQuotaPayloadOwner.VALUE_TYPE).payload(),expected.canonicalBytes())
                ||!Arrays.equals(Bytes.sha256(TargetValueEnvelope.decode(primary,TargetMessageRecord.VALUE_TYPE).payload()),object.primaryDigest())
                ||!Arrays.equals(Bytes.sha256(TargetValueEnvelope.decode(terminal,TargetTerminalGenerationRecord.VALUE_TYPE).payload()),object.terminalDigest()))return false;
        expected.mutation().requireAtOrBefore(reader.aggregate().mutation());return true;
    }
    public static void requireClosedObject(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,SystemMutation input,SourcePosition source,Protection protection){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());protection.requireSource(scope,lineage,input,source);
        if(!body.objectPayloadReclaim()||!scope.equals(body.scope())||!Arrays.equals(lineage,body.lineage())||reader.source()==null
                ||source.compareTo(reader.source())<=0||!protection.recovery.objectPayload())throw new IllegalArgumentException("OBJECT closure changed actual Source context");
        TargetObjectPayloadReclaim.requireCurrent(reader,input,protection.recovery.objectPolicies(),()->{reader.requireWithinElapsedBudget();protection.requireCurrent();});
    }
    public static void requireClosedObjectDraft(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,SystemMutation input,
            com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.objectPayloadReclaim()||!scope.equals(body.scope())||!Arrays.equals(lineage,body.lineage())||!recovery.objectPayload()||reader.source()==null
                ||reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())throw new IllegalArgumentException("OBJECT draft changed actual context/window");
        if(body.floor().appliedSourcePosition().compareTo(reader.source())>0||Long.compareUnsigned(body.floor().includedMutationSequence(),reader.sourceSequence())>0)
            throw new IllegalStateException("OBJECT prefix has not replayed its Floor");
        TargetObjectPayloadReclaim.requireClosed(reader.auditLedger(),input,recovery.objectPolicies(),false,reader::requireWithinElapsedBudget);
    }
    /** Original Source calls this under its actual guarded read; apply repeats it in the atomic write view. */
    public static void requireClosedUnused(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,SourcePosition source,Protection protection){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        protection.requireSource(scope,lineage,input,source);
        if(!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||reader.source()==null||source.compareTo(reader.source())<=0)
            throw new IllegalArgumentException("unused-reserve closure changed selected Native Source");
        requireClosedUnusedInventory(reader,scope,lineage,body,protection::writer);
        protection.requireCurrent();
    }
    public static void requireClosedInline(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,SourcePosition source,Protection protection){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());protection.requireSource(scope,lineage,input,source);
        if(!body.inlinePayloadRelease()||!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)
                ||reader.source()==null||source.compareTo(reader.source())<=0)throw new IllegalArgumentException("inline closure changed original Native Source");
        if(!protection.recovery.inlinePayload())throw new IllegalArgumentException("inline Source lacks actual full Floor closure");
        TargetInlinePayloadReclaim.requireCurrent(reader,body,protection.recovery.inlinePolicies(),
                ()->{reader.requireWithinElapsedBudget();protection.requireCurrent();});
    }
    public static void requireClosedNative(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,SystemMutation input,SourcePosition source,Protection protection){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());protection.requireSource(scope,lineage,input,source);
        if(!body.nativeRetainedReclaim()||!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||reader.source()==null
                ||source.compareTo(reader.source())<=0||!protection.recovery.nativeRetained())throw new IllegalArgumentException("Native retained closure changed actual Source context");
        if(body.incarnationReclaim())TargetNativeRetainedReclaim.requireClosed(reader.auditLedger(),input,false,()->{reader.requireWithinElapsedBudget();protection.requireCurrent();});
        TargetNativeRetainedReclaim.requireCurrent(reader,input,()->{reader.requireWithinElapsedBudget();protection.requireCurrent();});
    }
    public static void requireClosedNativeDraft(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,SystemMutation input,
            com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.nativeRetainedReclaim()||!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||!recovery.nativeRetained()
                ||reader.source()==null||reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())throw new IllegalArgumentException("Native draft changed actual scope/protection/window");
        if(body.floor().appliedSourcePosition().compareTo(reader.source())>0||Long.compareUnsigned(body.floor().includedMutationSequence(),reader.sourceSequence())>0)
            throw new IllegalStateException("Native prefix has not replayed original Floor");
        TargetNativeRetainedReclaim.requireClosed(reader.auditLedger(),input,false,reader::requireWithinElapsedBudget);
    }
    private static void requireClosedInlineInventory(TargetStoreBackend.Reader reader,TargetResourceReclaimBody body,
            com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,Runnable current){
        if(!recovery.inlinePayload())throw new IllegalArgumentException("inline closure requires actual inline image qualification");
        if(body.floor().appliedSourcePosition().compareTo(reader.source())>0
                ||Long.compareUnsigned(body.floor().includedMutationSequence(),reader.sourceSequence())>0)
            throw new IllegalStateException("inline Native prefix has not replayed its declared Floor");
        final var dependencies=reader.auditLedger();
        TargetInlinePayloadReclaim.requireClosed(dependencies,body,recovery.inlinePolicies(),recovery.inlineSourceFacts(),false,current);
    }
    /** The pre-append check has no predicted Source position and grants no Source mutation authority. */
    public static void requireClosedInlineDraft(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,SystemMutation input,
            com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||reader.source()==null||!body.inlinePayloadRelease())
            throw new IllegalArgumentException("inline draft changed original Native scope");
        if(reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())throw new IllegalStateException("inline draft retry window is already closed");
        requireClosedInlineInventory(reader,body,recovery,reader::requireWithinElapsedBudget);
    }
    /** Pre-append validation has no nominated future Source position and grants no Native release authority. */
    public static void requireClosedUnusedDraft(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery){
        final var body=TargetResourceReclaimBody.decode(input.canonicalBody());
        if(!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage)||reader.source()==null)
            throw new IllegalArgumentException("Native reclaim draft changed actual Store scope");
        if(reader.closedIngressDeadlineThrough()>=input.retryUntilEpochMs())
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("new reclaim retry deadline is already closed by actual Source ingress fence");
        if(body.floor().appliedSourcePosition().compareTo(reader.source())>0
                ||Long.compareUnsigned(body.floor().includedMutationSequence(),reader.sourceSequence())>0)
            throw new IllegalStateException("active Native reclaim Store has not replayed its selected actual Floor");
        requireClosedUnusedInventory(reader,scope,lineage,body,expected->recovery.writers().stream()
                .filter(w->Arrays.equals(w.expected().canonicalBytes(),expected.canonicalBytes())).findFirst()
                .orElseThrow(()->new IllegalStateException("draft recovery copies omit selected full Budget")));
    }
    private static void requireClosedUnusedInventory(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            TargetResourceReclaimBody body,java.util.function.Function<TargetQuotaAttemptBudget,
                    com.nereusstream.delay.store.TargetCheckpointRootVerifier.UnusedReserveWriter> writers){
        if(body.inlinePayloadRelease())throw new IllegalArgumentException("unused reserve closure cannot authorize payload deletion");
        final var rootId=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] rootKey=rootId.key();rootKey[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(rootKey,payload(reader,ColumnFamily.META,rootKey,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!Arrays.equals(root.recoveryLineage(),lineage))throw new IllegalStateException("unused-reserve root lineage changed");
        final var attempts=new java.util.HashMap<String,TargetQuotaAttemptBudget>();
        for(var operation:body.operations()){
            final var expected=((TargetResourceReclaimBody.UnusedAttemptReserve)operation).expected();
            final var actual=TargetQuotaAttemptBudget.decodeForStore(expected.key(),payload(reader,ColumnFamily.META,expected.key(),TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
            if(!Arrays.equals(actual.canonicalBytes(),expected.canonicalBytes()))throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("protected Native Budget no longer equals full expected value");
            final var writer=writers.apply(expected);
            if(writer.admission().type()!=SystemMutationType.TARGET_PUBLISH_ADMISSION||writer.outcome().type()!=SystemMutationType.PUBLISH_OUTCOME)
                throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original writer is outside the proved BASELINE closure");
            final var admitted=TargetPublishAdmissionBody.decode(writer.admission().canonicalBody());
            final var outcome=PublishOutcomeBody.decode(writer.outcome().canonicalBody());
            if(writer.admission().type()!=SystemMutationType.TARGET_PUBLISH_ADMISSION||!admitted.materialized()||admitted.publication()==null
                    ||admitted.nativePublication()!=null||admitted.kafkaReceipt()!=null||admitted.channel().context().kind()!=ChannelKind.BASELINE_PRODUCER
                    ||writer.outcome().type()!=SystemMutationType.PUBLISH_OUTCOME||outcome.sideEffect()!=1&&outcome.sideEffect()!=2
                    ||outcome.recoveryProof()!=null||!expected.mutation().equals(expected.resolvedAt())
                    ||!Arrays.equals(expected.admissionDigest(),writer.admission().mutationHash())
                    ||!Arrays.equals(expected.publishAttemptId(),admitted.publishAttemptId())||!Arrays.equals(expected.publishAttemptId(),outcome.publishAttemptId())
                    ||!expected.locator().equals(admitted.locator())||expected.executionBytes()!=admitted.executionBytes()
                    ||!expected.commitment().equals(admitted.commitment())||!expected.allocated().equals(admitted.allocated())
                    ||AuthorIdentity.decode(writer.admission().authorIdentity()).kind()!=AuthorIdentity.Kind.OWNER
                    ||!AuthorIdentity.decode(writer.admission().authorIdentity()).asOwnerIdentity().equals(admitted.owner())
                    ||!Arrays.equals(writer.admission().authorIdentity(),writer.outcome().authorIdentity())
                    ||!Arrays.equals(admitted.outcomeTransfer(),outcome.transfer()))
                throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("Attempt is outside the proved ordinary BASELINE logical-writer closure");
            final var admissionFirst=requireAppliedFirst(reader,root,writer.admission(),writer.admissionSource());
            final var outcomeFirst=requireAppliedFirst(reader,root,writer.outcome(),writer.outcomeSource());
            if(!outcomeFirst.mutation().equals(expected.resolvedAt())||admissionFirst.mutation().source().compareTo(outcomeFirst.mutation().source())>=0)
                throw new IllegalStateException("unused reserve changed original definitive Source resolution");
            expected.mutation().requireCoveredByFloor(body.floor());
            if(reader.closedIngressDeadlineThrough()<Math.max(writer.admission().retryUntilEpochMs(),writer.outcome().retryUntilEpochMs()))
                throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Source ingress fence does not close both writer retry windows");
            attempts.put(Bytes.hex(expected.publishAttemptId()),expected);
        }
        // Complete actual bounded ranges: another generation/alias cannot hide the selected attempt.
        for(var family:List.of(ColumnFamily.ID,ColumnFamily.TERMINAL)){
            final byte tag=(byte)(family==ColumnFamily.ID?TargetKeyCodec.MESSAGE_TAG:TargetKeyCodec.TERMINAL_GENERATION_TAG);
            scan(reader,family,tag,row->{
                final TargetGenerationRuntimeIndex runtime;
                if(family==ColumnFamily.ID)runtime=TargetMessageRecord.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetMessageRecord.VALUE_TYPE).payload(),scope.shard()).runtime();
                else{
                    final var terminal=TargetTerminalGenerationRecord.decode(TargetValueEnvelope.decode(row.value(),TargetTerminalGenerationRecord.VALUE_TYPE).payload());
                    if(!Arrays.equals(terminal.key(),row.key())||!Arrays.equals(terminal.recoveryLineage(),lineage))throw new IllegalStateException("terminal alias changed original key/lineage");
                    runtime=terminal.runtime();
                }
                for(var ref:runtime.attemptObligations())if(attempts.containsKey(Bytes.hex(ref.publishAttemptId())))
                    throw new IllegalStateException("unused reserve still has an open Attempt or related alias");
            });
        }
        for(byte tag:new byte[]{TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG,TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG,TargetKeyCodec.RECOVERY_EVIDENCE_REFERENCE_TAG})
            scan(reader,ColumnFamily.META,tag,row->{
                final byte[] attempt=tag==TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG
                        ?TargetKafkaReceiptMapping.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetKafkaReceiptMapping.VALUE_TYPE).payload(),scope.shard()).admission().publishAttemptId()
                        :tag==TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG
                            ?TargetExternalEvidenceReference.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetExternalEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage).attemptId()
                            :TargetRecoveryEvidenceReference.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetRecoveryEvidenceReference.VALUE_TYPE).payload(),scope.shard(),lineage).attemptId();
                if(attempts.containsKey(Bytes.hex(attempt)))throw new IllegalStateException("BASELINE reserve has a mapping/proof obligation outside the supported closure");
            });
        reader.requireWithinElapsedBudget();
    }
    private static void scan(TargetStoreBackend.Reader reader,ColumnFamily family,byte tag,
            java.util.function.Consumer<com.nereusstream.delay.store.ShardStore.KeyValue> visitor){
        byte[] lower={tag,TargetKeyCodec.KEY_FORMAT};final byte[] upper={tag,(byte)(TargetKeyCodec.KEY_FORMAT+1)};
        while(true){
            final var row=reader.first(family,lower,upper,List.of());if(row==null)return;
            if(Arrays.compareUnsigned(row.key(),lower)<0||Arrays.compareUnsigned(row.key(),upper)>=0)throw new IllegalStateException("Native reclaim range escaped its bound");
            lower=Bytes.concat(row.key(),new byte[]{0});visitor.accept(row);reader.requireWithinElapsedBudget();
        }
    }
    private static TargetResultRecord requireAppliedFirst(TargetStoreBackend.Reader reader,TargetQuotaIncarnation root,SystemMutation input,SourcePosition source){
        final byte[] key=resultKey(input.systemMutationId());
        final var first=TargetResultRecord.decode(payload(reader,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());first.requireOwner(root);
        final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||first.allocation()!=null||result.applyStatus()!=ApplyStatus.APPLIED||result.stableCode()!=StableCode.OK
                ||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,ApplyStatus.APPLIED,StableCode.OK,source.canonicalBytes()).encode())
                ||!Arrays.equals(first.mutation().source().canonicalBytes(),source.canonicalBytes())||!Arrays.equals(first.mutation().mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))
            throw new IllegalStateException("unused reserve lacks exact original APPLIED first");
        final byte[] positionKey=positionKey(source);final var position=TargetResultRecord.decode(payload(reader,ColumnFamily.DEDUPE,positionKey,TargetResultRecord.VALUE_TYPE));
        position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(root);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(first.mutation()))throw new IllegalStateException("unused reserve lost original first POSITION");
        first.mutation().requireAtOrBefore(reader.aggregate().mutation());return first;
    }
    private static void requireAbsent(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key){
        if(reader.get(family,key)!=null)throw new IllegalStateException("reclaim first/history/POSITION already exists");
    }
    private static byte[] payload(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(family,key),"actual reclaim Native dependency"),type).payload();
    }
    private static byte[] resultKey(byte[] id){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},id);}
    private static byte[] positionKey(SourcePosition source){return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},source.canonicalBytes());}
}
