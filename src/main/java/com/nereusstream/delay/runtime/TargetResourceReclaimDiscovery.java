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
    /** Exact native selection before the original held Source/commit-proof input has been loaded. */
    public record ObjectSelection(TargetQuotaPayloadOwner expected,TargetMessageLocator locator,long stateVersion,
            byte[] primaryDigest,byte[] terminalDigest){
        public ObjectSelection{primaryDigest=Bytes.copy(primaryDigest);terminalDigest=Bytes.copy(terminalDigest);}
        @Override public byte[] primaryDigest(){return Bytes.copy(primaryDigest);}
        @Override public byte[] terminalDigest(){return Bytes.copy(terminalDigest);}
    }
    public static final class Candidate {
        private final TargetResourceReclaimDiscovery owner;
        private final TargetQuotaAttemptBudget expected;
        private final TargetResourceReclaimBody.Operation operation;
        private final TargetResultRecord admission,outcome;
        private final ObjectSelection objectSelection;
        private final TargetPayloadObjectRetiringRecord objectIntent;
        private Candidate(TargetResourceReclaimDiscovery owner,TargetQuotaAttemptBudget expected,
                TargetResultRecord admission,TargetResultRecord outcome){
            this.owner=owner;this.expected=expected;this.admission=admission;this.outcome=outcome;
            operation=new TargetResourceReclaimBody.UnusedAttemptReserve(expected);objectSelection=null;objectIntent=null;
        }
        private Candidate(TargetResourceReclaimDiscovery owner,TargetResourceReclaimBody.InlinePayloadRelease operation){
            this.owner=owner;this.operation=operation;expected=null;admission=null;outcome=null;objectSelection=null;objectIntent=null;
        }
        private Candidate(TargetResourceReclaimDiscovery owner,TargetResourceReclaimBody.Operation operation){
            this.owner=owner;this.operation=operation;expected=null;admission=null;outcome=null;objectSelection=null;objectIntent=null;
        }
        private Candidate(TargetResourceReclaimDiscovery owner,ObjectSelection selection,TargetPayloadObjectRetiringRecord pending){
            this.owner=owner;objectSelection=selection;objectIntent=pending;operation=null;expected=null;admission=null;outcome=null;
        }
        public boolean objectPayload(){return objectSelection!=null||objectIntent!=null||operation instanceof TargetResourceReclaimBody.ObjectPayloadRetire||operation instanceof TargetResourceReclaimBody.ObjectPayloadConfirm||uncommittedObject();}
        public boolean uncommittedObject(){return operation instanceof TargetResourceReclaimBody.UncommittedObjectRetire||operation instanceof TargetResourceReclaimBody.UncommittedObjectConfirm||objectIntent!=null&&objectIntent.body().uncommittedObjectReclaim();}
        public Candidate withUncommittedConfirmation(TargetResourceReclaimBody.UncommittedObjectConfirm confirmation){
            if(!Arrays.equals(objectIntent().canonicalBytes(),confirmation.intent().canonicalBytes()))throw new IllegalArgumentException("uncommitted confirmation changed actual pending intent");
            return new Candidate(owner,confirmation);
        }
        public boolean needsObjectProof(){return objectSelection!=null;}
        public boolean pendingObject(){return objectIntent!=null;}
        public ObjectSelection objectSelection(){return Objects.requireNonNull(objectSelection,"OBJECT intent selection required");}
        public TargetPayloadObjectRetiringRecord objectIntent(){return Objects.requireNonNull(objectIntent,"actual pending OBJECT intent required");}
        public Candidate withNativeOrigin(TargetObjectPayloadOrigin origin){
            final var value=objectSelection();return new Candidate(owner,new TargetResourceReclaimBody.ObjectPayloadRetire(value.expected(),value.locator(),value.stateVersion(),value.primaryDigest(),value.terminalDigest(),origin));
        }
        public Candidate withObjectProof(CanonicalPayloadCommitProof proof){
            final var value=objectSelection();
            return new Candidate(owner,new TargetResourceReclaimBody.ObjectPayloadRetire(value.expected(),value.locator(),value.stateVersion(),value.primaryDigest(),value.terminalDigest(),proof));
        }
        public Candidate withObjectConfirmation(TargetResourceReclaimBody.ObjectPayloadConfirm confirmation){
            if(!Arrays.equals(objectIntent().canonicalBytes(),confirmation.intent().canonicalBytes()))throw new IllegalArgumentException("OBJECT confirmation changed actual selected Native intent");
            return new Candidate(owner,confirmation);
        }
        public TargetQuotaAttemptBudget expected(){if(expected==null)throw new IllegalStateException("inline candidate has no unused attempt Budget");return expected;}
        public TargetResourceReclaimBody.Operation operation(){return Objects.requireNonNull(operation,"original OBJECT inputs must be authenticated/materialized first");}
        public boolean inlinePayload(){return operation instanceof TargetResourceReclaimBody.InlinePayloadRelease;}
        public boolean incarnationLifecycle(){return operation instanceof TargetResourceReclaimBody.IncarnationDrain||operation instanceof TargetResourceReclaimBody.IncarnationRetire;}
        public boolean nativeRetained(){return incarnationLifecycle()||operation instanceof TargetResourceReclaimBody.RetainedRecordDelete||operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease;}
        public TargetQuotaIdentity primaryIdentity(){if(objectSelection!=null)return objectSelection.expected().primaryIdentity();if(objectIntent!=null)return objectIntent.expectedOwner().primaryIdentity();return switch(operation){
            case TargetResourceReclaimBody.InlinePayloadRelease p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.ObjectPayloadRetire p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.ObjectPayloadConfirm p->p.intent().operation().expected().primaryIdentity();
            case TargetResourceReclaimBody.UncommittedObjectRetire p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.UncommittedObjectConfirm p->p.intent().expectedOwner().primaryIdentity();
            case TargetResourceReclaimBody.UnusedAttemptReserve p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.RetainedRecordDelete p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.RetainedAllocationRelease p->p.expected().primaryIdentity();
            case TargetResourceReclaimBody.IncarnationDrain p->p.expected().identity();
            case TargetResourceReclaimBody.IncarnationRetire p->p.expected().identity();};}
        public ProtocolTuple requiredTuple(){return operation instanceof TargetResourceReclaimBody.RetainedRecordDelete d&&d.expected().kind()==TargetResultRecord.Kind.POSITION_COMMAND?ProtocolTuple.targetDuplicatePositionReclaim():incarnationLifecycle()?ProtocolTuple.targetIncarnationReclaim():operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease a&&a.includesAnchor()?ProtocolTuple.targetAnchorAllocationReclaim():uncommittedObject()?ProtocolTuple.targetUncommittedObjectReclaim():objectPayload()?ProtocolTuple.targetObjectPayloadReclaim():nativeRetained()?ProtocolTuple.targetNativeRetainedReclaim():inlinePayload()?ProtocolTuple.targetSelfContainedInlinePayloadReclaim():ProtocolTuple.targetResourceReclaim();}
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
            boolean unused,boolean inline,boolean nativeRetained){return scan(reads,previous,maximumRows,authority,unused,inline,nativeRetained,false);}
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained,boolean object){return scan(reads,previous,maximumRows,authority,unused,inline,nativeRetained,object,false);}
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted){
        return scan(reads,previous,maximumRows,authority,unused,inline,nativeRetained,object,uncommitted,false);
    }
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted,boolean anchorAllocation){
        return scan(reads,previous,maximumRows,authority,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,false);
    }
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted,boolean anchorAllocation,boolean incarnationLifecycle){
        return scan(reads,previous,maximumRows,authority,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,false);
    }
    public Page scan(BoundedReadBudget reads,Cursor previous,int maximumRows,TargetStoreBackend.ReadAuthority authority,
            boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted,boolean anchorAllocation,boolean incarnationLifecycle,boolean duplicatePosition){
        if(maximumRows<1||maximumRows>TargetResourceReclaimBody.MAX_OPERATIONS||previous!=null&&previous.owner!=this||!unused&&!inline&&!nativeRetained&&!object&&!uncommitted&&!anchorAllocation&&!incarnationLifecycle&&!duplicatePosition)
            throw new IllegalArgumentException("invalid bounded reclaim scan or foreign continuation");
        return backend.guardedRead(reads,reader->{
            final var incarnation=reader.metadata().storeIncarnation();
            if(previous!=null&&!Arrays.equals(previous.storeIncarnation,incarnation))throw new StaleCursor();
            final var candidates=new ArrayList<Candidate>();
            int kind=previous==null?nextKind(0,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,duplicatePosition):previous.kind;
            byte[] after=previous==null?null:previous.after;
            if(!allowedKind(kind,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,duplicatePosition)){kind=nextKind(0,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,duplicatePosition);after=null;}
            int count=0;
            while(count<maximumRows){
                final int tag=switch(kind){case 1,5->TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG;case 2,7,9->TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG;
                    case 3->TargetKeyCodec.RESULT_QUERY_TAG;case 4,12->TargetKeyCodec.RESULT_POSITION_TAG;case 6->TargetKeyCodec.EVIDENCE_CONFIRMED_TAG;case 8,10->TargetKeyCodec.PAYLOAD_OBJECT_RETIRING_TAG;case 11->TargetKeyCodec.QUOTA_INCARNATION_TAG;default->throw new IllegalStateException("unknown reclaim cursor family");};
                final var family=kind==3||kind==4||kind==12?ColumnFamily.DEDUPE:ColumnFamily.META;
                final byte[] upper={(byte)tag,TargetKeyCodec.KEY_FORMAT+1};
                final byte[] lower=after==null?new byte[]{(byte)tag,TargetKeyCodec.KEY_FORMAT}:Bytes.concat(after,new byte[]{0});
                try{
                    final var row=reader.first(family,lower,upper,List.of());
                    if(row==null){
                        reader.requireWithinElapsedBudget();
                        final int next=nextKind(kind,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,duplicatePosition);
                        if(next!=0){
                            kind=next;after=null;
                            if(!candidates.isEmpty())return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.PAGE_LIMIT);
                            continue;
                        }
                        return new Page(candidates,new Cursor(this,incarnation,after,kind),Stop.RANGE_END);
                    }
                    final var selected=kind==11?incarnation(reader,row):kind==1?unused(reader,row):kind==2?inline(reader,row):kind>=7&&kind<=10?object(reader,row,kind):nativeRetained(reader,row,kind,nativeRetained,anchorAllocation);
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
    private static boolean allowedKind(int kind,boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted,boolean anchorAllocation,boolean incarnationLifecycle,boolean duplicatePosition){
        return kind==1?unused:kind==2?inline:kind>=3&&kind<=5?nativeRetained:kind==6?nativeRetained||anchorAllocation:kind==7||kind==8?object:kind==9||kind==10?uncommitted:kind==11?incarnationLifecycle:kind==12?duplicatePosition:false;
    }
    private static int nextKind(int prior,boolean unused,boolean inline,boolean nativeRetained,boolean object,boolean uncommitted,boolean anchorAllocation,boolean incarnationLifecycle,boolean duplicatePosition){
        for(int next=prior+1;next<=12;next++)if(allowedKind(next,unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnationLifecycle,duplicatePosition))return next;return 0;
    }
    private Candidate incarnation(TargetStoreBackend.Reader reader,ShardStore.KeyValue row){
        final var owner=TargetQuotaIncarnation.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetQuotaIncarnation.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
        if(owner.identity().kind()!=TargetQuotaIdentity.Kind.TARGET)return null;
        if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("incarnation discovery changed original lineage");
        owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());
        if(!owner.draining()){
            final byte[] key=TargetKeyCodec.close(owner.identity().target()),raw=reader.get(ColumnFamily.META,key);
            if(raw==null)return null;
            final var close=TargetCloseRecord.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetCloseRecord.VALUE_TYPE).payload(),scope.shard(),lineage);
            if(!close.ownerIdentity().equals(owner.identity()))return null;
            return new Candidate(this,new TargetResourceReclaimBody.IncarnationDrain(owner,close));
        }
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG,TargetKeyCodec.KEY_FORMAT},owner.scope().keySuffix());
        final byte[] raw=reader.get(ColumnFamily.META,key);if(raw==null)return null;
        final var grant=TargetQuotaGrantActivation.decodeForStore(key,TargetValueEnvelope.decode(raw,TargetQuotaGrantActivation.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
        if(!grant.grant().limit().isZero()||grant.allocation()==null||!grant.allocation().identity().equals(owner.identity()))return null;
        final var primary=TargetQuotaCounter.decodeForStore(owner.identity().key(),payload(reader,ColumnFamily.META,owner.identity().key(),TargetQuotaCounter.VALUE_TYPE),scope.shard());
        final var mirror=TargetQuotaCounter.decodeForStore(owner.tenantIdentity().key(),payload(reader,ColumnFamily.META,owner.tenantIdentity().key(),TargetQuotaCounter.VALUE_TYPE),scope.shard());
        if(!primary.usage().equals(owner.ownContribution())||!mirror.usage().equals(owner.tenantContribution()))return null;
        return new Candidate(this,new TargetResourceReclaimBody.IncarnationRetire(owner,grant));
    }
    private Candidate object(TargetStoreBackend.Reader reader,ShardStore.KeyValue row,int kind){
        if(kind==8||kind==10){
            final var intent=TargetPayloadObjectRetiringRecord.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),scope,lineage);
            if((kind==10)!=intent.body().uncommittedObjectReclaim())return null;
            TargetObjectPayloadReclaim.requirePending(reader,List.of(),intent,reader.aggregate().mutation());
            return new Candidate(this,null,intent);
        }
        final var owner=TargetQuotaPayloadOwner.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetQuotaPayloadOwner.VALUE_TYPE).payload(),scope.shard(),scope.tenantScope());
        if(!Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("OBJECT discovery changed original lineage");
        owner.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if(kind==9){
            if(owner.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||owner.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED||owner.committedPayload()!=null
                    ||reader.get(ColumnFamily.META,TargetPayloadObjectRetiringRecord.key(owner.messageId()))!=null)return null;
            final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESERVATION_TAG,TargetKeyCodec.KEY_FORMAT},owner.messageId().bytes());
            final var reservation=TargetReservationRecord.decode(payload(reader,ColumnFamily.ID,key,TargetReservationRecord.VALUE_TYPE));reservation.requireOwner(owner);
            if(reservation.status()!=PayloadReservationStatus.EXPIRED&&reservation.status()!=PayloadReservationStatus.ABANDONED)return null;
            if(!Arrays.equals(reservation.canonicalBytes(),payload(reader,ColumnFamily.ID,reservation.lookupKey(),TargetReservationRecord.VALUE_TYPE))
                    ||reader.get(ColumnFamily.ID,TargetKeyCodec.message(owner.messageId()))!=null)throw new IllegalStateException("uncommitted discovery lost terminal reservation projections/absent Message");
            return new Candidate(this,new TargetResourceReclaimBody.UncommittedObjectRetire(owner,null));
        }
        if(owner.kind()!=TargetQuotaPayloadOwner.Kind.OBJECT||owner.phase()!=TargetQuotaPayloadOwner.Phase.RETAINED||owner.committedPayload()==null
                ||reader.get(ColumnFamily.META,TargetPayloadObjectRetiringRecord.key(owner.messageId()))!=null)return null;
        final byte[] key=TargetKeyCodec.message(owner.messageId());final byte[] primary=payload(reader,ColumnFamily.ID,key,TargetMessageRecord.VALUE_TYPE);
        final var message=TargetMessageRecord.decodeForStore(key,primary,scope.shard());owner.requireMessagePayload(message);
        if(!message.runtime().terminal()||!message.runtime().attemptObligations().isEmpty())return null;
        final byte[] terminalKey=TargetTerminalGenerationRecord.key(message.locator());
        final byte[] terminalBytes=payload(reader,ColumnFamily.TERMINAL,terminalKey,TargetTerminalGenerationRecord.VALUE_TYPE);
        final var terminal=TargetTerminalGenerationRecord.decode(terminalBytes);terminal.requireOwner(owner);terminal.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if(!Arrays.equals(terminal.key(),terminalKey)||!Arrays.equals(terminal.recoveryLineage(),lineage)||terminal.stateVersion()!=message.stateVersion()
                ||!Arrays.equals(terminal.runtime().canonicalBytes(),message.runtime().canonicalBytes()))throw new IllegalStateException("OBJECT discovery lost exact terminal metadata");
        return new Candidate(this,new ObjectSelection(owner,message.locator(),message.stateVersion(),Bytes.sha256(primary),Bytes.sha256(terminalBytes)),null);
    }
    private Candidate nativeRetained(TargetStoreBackend.Reader reader,ShardStore.KeyValue row,int kind,boolean nativeRetained,boolean anchorAllocation){
        if(kind==3||kind==4||kind==12){
            final var record=TargetResultRecord.decode(TargetValueEnvelope.decode(row.value(),TargetResultRecord.VALUE_TYPE).payload());
            record.requireStored(row.key(),TargetResultRecord.VALUE_TYPE,record.canonicalBytes());
            scope.requireRoute(record.primaryIdentity().shard(),record.tenantScope());
            if(!Arrays.equals(record.recoveryLineage(),lineage))throw new IllegalStateException("Native record discovery changed lineage");
            record.mutation().requireAtOrBefore(reader.aggregate().mutation());
            if(kind==3&&record.kind()!=TargetResultRecord.Kind.RESULT||kind==4&&record.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED||kind==12&&record.kind()!=TargetResultRecord.Kind.POSITION_COMMAND)return null;
            if(reader.source().compareTo(record.mutation().source())<=0)return null;
            if(kind==12){
                final byte[] firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_COMMAND_TAG,TargetKeyCodec.KEY_FORMAT},record.logicalId());
                final var original=TargetResultRecord.decode(payload(reader,ColumnFamily.DEDUPE,firstKey,TargetResultRecord.VALUE_TYPE));
                original.requireStored(firstKey,TargetResultRecord.VALUE_TYPE,original.canonicalBytes());record.requireFirst(original);
                if(record.mutation().equals(original.mutation()))return null;
                record.mutation().requireAfter(original.mutation());
            }
            if(TargetSloObservationSamples.hasReference(reader,List.of(),record.key()))return null;
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
        final byte[] attempt;
        if(receipt.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Recovery recovery){
            if(!nativeRetained)return null;attempt=recovery.owner().reference().attemptId();
        }else if(receipt.body().resource().provenance() instanceof TargetExternalEvidenceObjectResource.Anchor anchor){
            if(!anchorAllocation)return null;attempt=anchor.owner().creatorAttemptId();
        }else return null;
        final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT},attempt);
        final var budget=TargetQuotaAttemptBudget.decodeForStore(key,payload(reader,ColumnFamily.META,key,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
        if(budget.phase()!=TargetQuotaAttemptBudget.Phase.RETAINED||!budget.allocated().covers(TargetResourceReclaimBody.RetainedAllocationRelease.confirmedShare(receipt)))return null;
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
