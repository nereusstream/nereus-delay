package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded Native candidates, never a release or a substitute for original authenticated Admission/Outcome bytes. */
public final class TargetResourceReclaimDiscovery {
    public enum Stop { RANGE_END, PAGE_LIMIT, READ_BUDGET }
    public static final class StaleCursor extends IllegalStateException {
        private static final long serialVersionUID=1L;
        private StaleCursor(){super("reclaim scan belongs to another physical Store view");}
    }
    public static final class Cursor {
        private final TargetResourceReclaimDiscovery owner;
        private final byte[] storeIncarnation,after;
        private final int kind;
        private Cursor(TargetResourceReclaimDiscovery owner,byte[] storeIncarnation,byte[] after,int kind){
            this.owner=owner;this.storeIncarnation=Bytes.copy(storeIncarnation);this.after=after==null?null:Bytes.copy(after);this.kind=kind;
        }
    }
    public static final class Candidate {
        private final TargetResourceReclaimDiscovery owner;
        private final TargetQuotaAttemptBudget expected;
        private final TargetResourceReclaimBody.Operation operation;
        private final TargetResultRecord admission,outcome;
        private Candidate(TargetResourceReclaimDiscovery owner,TargetQuotaAttemptBudget expected,
                TargetResultRecord admission,TargetResultRecord outcome){
            this.owner=owner;this.expected=expected;this.admission=admission;this.outcome=outcome;
            operation=new TargetResourceReclaimBody.UnusedAttemptReserve(expected);
        }
        private Candidate(TargetResourceReclaimDiscovery owner,TargetResourceReclaimBody.InlinePayloadRelease operation){
            this.owner=owner;this.operation=operation;expected=null;admission=null;outcome=null;
        }
        private Candidate(TargetResourceReclaimDiscovery owner,TargetResourceReclaimBody.Operation operation){
            this.owner=owner;this.operation=operation;expected=null;admission=null;outcome=null;
        }
        public TargetQuotaAttemptBudget expected(){if(expected==null)throw new IllegalStateException("inline candidate has no unused attempt Budget");return expected;}
        public TargetResourceReclaimBody.Operation operation(){return operation;}
        public boolean inlinePayload(){return operation instanceof TargetResourceReclaimBody.InlinePayloadRelease;}
        public boolean nativeRetained(){return operation instanceof TargetResourceReclaimBody.RetainedRecordDelete||operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease;}
        public TargetQuotaIdentity primaryIdentity(){return switch(operation){
            case TargetResourceReclaimBody.InlinePayloadRelease p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.UnusedAttemptReserve p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.RetainedRecordDelete p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.RetainedAllocationRelease p->p.expected().primaryIdentity();};}
        public ProtocolTuple requiredTuple(){return nativeRetained()?ProtocolTuple.targetNativeRetainedReclaim():inlinePayload()?ProtocolTuple.targetSelfContainedInlinePayloadReclaim():ProtocolTuple.targetResourceReclaim();}
        public TargetResultRecord admission(){return admission;}
        public TargetResultRecord outcome(){return outcome;}
    }
    public record Page(List<Candidate> candidates,Cursor continuation,Stop stop){
        public Page{candidates=List.copyOf(candidates);Objects.requireNonNull(continuation);Objects.requireNonNull(stop);}
        /** End of this traversal only; concurrent earlier inserts are picked up when the caller starts the next sweep. */
        public boolean complete(){return stop==Stop.RANGE_END;}
    }
    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    public TargetResourceReclaimDiscovery(TargetStoreBackend backend,TargetQuotaScope scope,byte[] lineage){
        this.backend=Objects.requireNonNull(backend);this.scope=Objects.requireNonNull(scope);
        Bytes.requireLength(lineage,16,"reclaim discovery lineage");
        if(scope.target()!=null||Arrays.equals(lineage,new byte[16]))throw new IllegalArgumentException("reclaim discovery requires original Source scope");
        this.lineage=Bytes.copy(lineage);
    }
    /**
     * Rows, not matches, consume the bound. Each candidate has its own current Native proof and is rechecked
     * before handoff, so unrelated native writes need not restart the entire traversal. No page or traversal
     * proves a consistent whole-Store absence; a range end tells the caller to wrap to the first key.
     */
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority){
        return scan(reads,previous,maximumRows,authority,true,false);
    }
    /** Family selection only; discovery confers no tuple, signing, protection or deletion permission. */
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline){
        return scan(reads,previous,maximumRows,authority,unused,inline,false);
    }
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained){
        if(maximumRows<1||maximumRows>TargetResourceReclaimBody.MAX_OPERATIONS||previous!=null&&previous.owner!=this||!unused&&!inline&&!nativeRetained)
            throw new IllegalArgumentException("invalid bounded reclaim scan or foreign continuation");
        return backend.guardedRead(reads,reader->{
            final var incarnation=reader.metadata().storeIncarnation();
            if(previous!=null&&!Arrays.equals(previous.storeIncarnation,incarnation))throw new StaleCursor();
            final var candidates=new ArrayList<Candidate>();
            int kind=previous==null?(unused?1:inline?2:3):previous.kind;
            byte[] after=previous==null?null:previous.after;
            if(kind==1&&!unused||kind==2&&!inline||kind>=3&&!nativeRetained){kind=unused?1:inline?2:3;after=null;}
            int count=0;
            while(count<maximumRows){
                final int tag=switch(kind){case 1,5->TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG;case 2->TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG;
                    case 3->TargetKeyCodec.RESULT_QUERY_TAG;case 4->TargetKeyCodec.RESULT_POSITION_TAG;case 6->TargetKeyCodec.EVIDENCE_CONFIRMED_TAG;default->throw new IllegalStateException("unknown reclaim cursor family");};
                final var family=kind==3||kind==4?ColumnFamily.DEDUPE:ColumnFamily.META;
                final byte[] upper={(byte)tag,TargetKeyCodec.KEY_FORMAT+1};
                final byte[] lower=after==null?new byte[]{(byte)tag,TargetKeyCodec.KEY_FORMAT}:Bytes.concat(after,new byte[]{0});
                try{
                    final var row=reader.first(family,lower,upper,List.of());
                    if(row==null){
                        reader.requireWithinElapsedBudget();
                        final int next=kind==1?(inline?2:nativeRetained?3:0):kind==2?(nativeRetained?3:0):kind<6?kind+1:0;
                        if(next!=0){
                            kind=next;after=null;
                            if(!candidates.isEmpty())return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.PAGE_LIMIT);
                            continue;
                        }
                        return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.RANGE_END);
                    }
                    final var selected=kind==1?unused(reader,row):kind==2?inline(reader,row):nativeRetained(reader,row,kind);
                    reader.requireWithinElapsedBudget();count++;
                    if(selected!=null)candidates.add(selected);
                    after=row.key();
                    // A legacy opaque policy stays retained without blocking eligible neighbours in one atomic group.
                    // The Source protocol and explicit preparation APIs still support up to32 INLINE operations.
                    if(kind>=2&&selected!=null)return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.PAGE_LIMIT);
                }catch(ReadIncompleteException exhausted){return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.READ_BUDGET);}
            }
            return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.PAGE_LIMIT);
        },authority);
    }
    private Candidate nativeRetained(TargetStoreBackend.Reader reader,ShardStore.KeyValue row,int kind){
        if(kind==3||kind==4){
            final var record=TargetResultRecord.decode(TargetValueEnvelope.decode(row.value(),TargetResultRecord.VALUE_TYPE).payload());
            record.requireStored(row.key(),TargetResultRecord.VALUE_TYPE,record.canonicalBytes());
            scope.requireRoute(record.primaryIdentity().shard(),record.tenantScope());
            if(!Arrays.equals(record.recoveryLineage(),lineage))throw new IllegalStateException("Native record discovery changed lineage");
            record.mutation().requireAtOrBefore(reader.aggregate().mutation());
            if(kind==3&&record.kind()!=TargetResultRecord.Kind.RESULT||kind==4&&record.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED)return null;
            if(reader.source().compareTo(record.mutation().source())<=0)return null;
            return new Candidate(this,new TargetResourceReclaimBody.RetainedRecordDelete(record));
        }
        if(kind==5){
            final var budget=TargetQuotaAttemptBudget.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetQuotaAttemptBudget.VALUE_TYPE).payload(),scope.shard());
            scope.requireRoute(budget.locator().messageId().routingId().shardId(),budget.tenantScope());
            if(!Arrays.equals(budget.recoveryLineage(),lineage))throw new IllegalStateException("zero retained Budget discovery changed lineage");
            budget.mutation().requireAtOrBefore(reader.aggregate().mutation());
            return budget.phase()==TargetQuotaAttemptBudget.Phase.RETAINED&&budget.allocated().isZero()
                    ?new Candidate(this,new TargetResourceReclaimBody.RetainedAllocationRelease(budget,List.of())):null;
        }
        final var receipt=TargetEvidenceConfirmedRecord.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetEvidenceConfirmedRecord.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope(),lineage);
        receipt.appliedAt().requireAtOrBefore(reader.aggregate().mutation());
        if(!(receipt.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery))return null;
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},recovery.owner().reference().attemptId());
        final var budget=TargetQuotaAttemptBudget.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
        if(budget.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED||!budget.allocated().covers(recovery.owner().adoptedCharge()))return null;
        return new Candidate(this,new TargetResourceReclaimBody.RetainedAllocationRelease(budget,List.of(receipt)));
    }
    private Candidate unused(TargetStoreBackend.Reader reader,ShardStore.KeyValue row){
        final var expected=TargetQuotaAttemptBudget.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetQuotaAttemptBudget.VALUE_TYPE).payload(),scope.shard());
        scope.requireRoute(expected.locator().messageId().routingId().shardId(),expected.tenantScope());
        if(!Arrays.equals(expected.recoveryLineage(),lineage))throw new IllegalStateException("reclaim candidate changed original lineage");
        expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
        Candidate selected=null;
        if(expected.phase()==TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR&&expected.mutation().equals(expected.resolvedAt())){
            final var root=root(reader);
            final byte[] admissionId=SystemMutation.computeSystemMutationId(scope.shard(),SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    expected.publishAttemptId(),expected.admissionDigest());
            final var admission=first(reader,root,admissionId);
            final var position=position(reader,root,expected.resolvedAt());
            final var outcome=first(reader,root,position.logicalId());position.requireFirst(outcome);
            final var admitted=SystemMutationResult.decode(admission.typedPayload());
            final var resolved=SystemMutationResult.decode(outcome.typedPayload());
            if(admitted.mutationType()!=SystemMutationType.TARGET_PUBLISH_ADMISSION
                    ||!Arrays.equals(admitted.mutationHash(),expected.admissionDigest())
                    ||admission.mutation().source().compareTo(outcome.mutation().source())>=0
                    ||!outcome.mutation().equals(expected.resolvedAt()))throw new IllegalStateException("resolved reclaim candidate lost original Source firsts");
            if(resolved.mutationType()==SystemMutationType.PUBLISH_OUTCOME
                    &&AuthorIdentity.decode(admitted.authorIdentity()).kind()==AuthorIdentity.Kind.OWNER
                    &&Arrays.equals(admitted.authorIdentity(),resolved.authorIdentity())
                    &&reader.closedIngressDeadlineThrough()>=Math.max(admitted.retryUntilEpochMs(),resolved.retryUntilEpochMs())){
                // Baseline/channel/evidence, full images, aliases and admission are proved before a signed request escapes preparation.
                final var admissionPosition=position(reader,root,admission.mutation());admissionPosition.requireFirst(admission);
                selected=new Candidate(this,expected,admission,outcome);
            }
        }
        return selected;
    }
    private Candidate inline(TargetStoreBackend.Reader reader,ShardStore.KeyValue row){
        final var owner=TargetQuotaPayloadOwner.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetQuotaPayloadOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
        if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("inline discovery changed original lineage");
        owner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if(owner.kind()!=TargetQuotaPayloadOwner.Kind.INLINE||owner.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED)return null;
        final byte[] key=TargetKeyCodec.message(owner.messageId()),raw=reader.get(ColumnFamily.ID,key);
        if(raw==null)throw new IllegalStateException("retained inline candidate lost primary Message");
        final byte[] primary=TargetValueEnvelope.decode(raw,TargetMessageRecord.VALUE_TYPE).payload();
        final var message=TargetMessageRecord.decodeForStore(key,primary,scope.shard());owner.requireMessagePayload(message);
        if(!message.runtime().terminal()||!message.runtime().attemptObligations().isEmpty())return null;
        final byte[] terminalKey=TargetTerminalGenerationRecord.key(message.locator());
        final byte[] terminalBytes=payload(reader,ColumnFamily.TERMINAL,terminalKey,TargetTerminalGenerationRecord.VALUE_TYPE);
        final var terminal=TargetTerminalGenerationRecord.decode(terminalBytes);terminal.requireOwner(owner);
        if(!Arrays.equals(terminal.key(),terminalKey)||!Arrays.equals(terminal.recoveryLineage(),lineage)
                ||terminal.stateVersion()!=message.stateVersion()||!Arrays.equals(terminal.runtime().canonicalBytes(),message.runtime().canonicalBytes()))
            throw new IllegalStateException("inline candidate lost exact current terminal metadata");
        terminal.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return new Candidate(this,new TargetResourceReclaimBody.InlinePayloadRelease(owner,message.locator(),message.stateVersion(),Bytes.sha256(primary),Bytes.sha256(terminalBytes)));
    }
    public void requireCandidate(Candidate candidate){
        if(Objects.requireNonNull(candidate).owner!=this)throw new IllegalArgumentException("reclaim candidate came from another original Store");
    }
    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader){
        final var identity=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final byte[] key=identity.key();key[0]=TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var root=TargetQuotaIncarnation.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetQuotaIncarnation.VALUE_TYPE),scope.shard(),scope.tenantScope());
        if(!identity.equals(root.identity())||!Arrays.equals(root.recoveryLineage(),lineage))throw new IllegalStateException("reclaim candidate changed Source root");
        root.latestMutation().requireAtOrBefore(reader.aggregate().mutation());return root;
    }
    private static TargetResultRecord first(TargetStoreBackend.Reader reader,TargetQuotaIncarnation root,byte[] id){
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},id);
        final var first=record(reader,root,key);final var result=SystemMutationResult.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM||first.allocation()!=null||result.applyStatus()!=ApplyStatus.APPLIED||result.stableCode()!=StableCode.OK
                ||!Arrays.equals(result.mutationId(),id)||!Arrays.equals(result.appliedSourcePosition(),first.mutation().source().canonicalBytes()))
            throw new IllegalStateException("resolved reclaim candidate lost exact APPLIED first");
        return first;
    }
    private static TargetResultRecord position(TargetStoreBackend.Reader reader,TargetQuotaIncarnation root,TargetQuotaMutation stamp){
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},stamp.source().canonicalBytes());
        final var result=record(reader,root,key);
        if(result.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!result.mutation().equals(stamp))throw new IllegalStateException("reclaim candidate lost original Source POSITION");
        return result;
    }
    private static TargetResultRecord record(TargetStoreBackend.Reader reader,TargetQuotaIncarnation root,byte[] key){
        final var record=TargetResultRecord.decode(payload(reader,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        record.requireStored(key,TargetResultRecord.VALUE_TYPE,record.canonicalBytes());record.requireOwner(root);
        record.mutation().requireAtOrBefore(reader.aggregate().mutation());return record;
    }
    private static byte[] payload(TargetStoreBackend.Reader reader,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(reader.get(family,key),"actual reclaim candidate dependency"),type).payload();
    }
}
