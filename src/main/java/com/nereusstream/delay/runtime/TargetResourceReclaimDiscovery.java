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
        private Cursor(TargetResourceReclaimDiscovery owner,byte[] storeIncarnation,byte[] after){
            this.owner=owner;this.storeIncarnation=Bytes.copy(storeIncarnation);this.after=after==null?null:Bytes.copy(after);
        }
    }
    public static final class Candidate {
        private final TargetResourceReclaimDiscovery owner;
        private final TargetQuotaAttemptBudget expected;
        private final TargetResultRecord admission,outcome;
        private Candidate(TargetResourceReclaimDiscovery owner,TargetQuotaAttemptBudget expected,
                TargetResultRecord admission,TargetResultRecord outcome){
            this.owner=owner;this.expected=expected;this.admission=admission;this.outcome=outcome;
        }
        public TargetQuotaAttemptBudget expected(){return expected;}
        public TargetResultRecord admission(){return admission;}
        public TargetResultRecord outcome(){return outcome;}
    }
    public record Page(List<Candidate> candidates,Cursor continuation,Stop stop){
        public Page{candidates=List.copyOf(candidates);Objects.requireNonNull(continuation);Objects.requireNonNull(stop);}
        /** End of this traversal only; concurrent earlier inserts are picked up when the caller starts the next sweep. */
        public boolean complete(){return stop==Stop.RANGE_END;}
    }
    private static final byte[] LOWER={TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT};
    private static final byte[] UPPER={TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,TargetKeyCodec.KEY_FORMAT+1};
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
        if(maximumRows<1||maximumRows>TargetResourceReclaimBody.MAX_OPERATIONS||previous!=null&&previous.owner!=this)
            throw new IllegalArgumentException("invalid bounded reclaim scan or foreign continuation");
        return backend.guardedRead(reads,reader->{
            final var incarnation=reader.metadata().storeIncarnation();
            if(previous!=null&&!Arrays.equals(previous.storeIncarnation,incarnation))throw new StaleCursor();
            final var candidates=new ArrayList<Candidate>();
            byte[] after=previous==null?null:previous.after;
            byte[] lower=after==null?LOWER:Bytes.concat(after,new byte[]{0});
            for(int count=0;count<maximumRows;count++){
                try{
                    final var row=reader.first(ColumnFamily.META,lower,UPPER,List.of());
                    if(row==null){reader.requireWithinElapsedBudget();return new Page(candidates,new Cursor(this,incarnation,after),Stop.RANGE_END);}
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
                    reader.requireWithinElapsedBudget();
                    if(selected!=null)candidates.add(selected);
                    after=row.key();lower=Bytes.concat(after,new byte[]{0});
                }catch(ReadIncompleteException exhausted){return new Page(candidates,new Cursor(this,incarnation,after),Stop.READ_BUDGET);}
            }
            return new Page(candidates,new Cursor(this,incarnation,after),Stop.PAGE_LIMIT);
        },authority);
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
