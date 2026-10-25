package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual Command-batch sampling; missing certification evidence keeps the original Source unacknowledged. */
public final class TargetSloObservationSamples {
    private TargetSloObservationSamples() {}

    /** Already-held original budget/load/reservation/collector protections, acquired outside Store locks. */
    public interface Authority {
        void requireSample(TargetSloObservationGeneration generation, TargetSloObservationSample sample,
                long maximumRetainedBytes);
        void requireCurrent();
    }
    public static final Authority UNAVAILABLE = new Authority() {
        public void requireSample(TargetSloObservationGeneration generation, TargetSloObservationSample sample, long bytes) {
            throw new IllegalStateException("original Target certification sample reservation/custody authority unavailable");
        }
        public void requireCurrent() { throw new IllegalStateException("original Target certification sample hold unavailable"); }
    };
    public record Plan(List<TargetStoreBackend.Edit> edits, Runnable qualification) {
        public Plan { edits = List.copyOf(edits); Objects.requireNonNull(qualification); }
    }

    /** A new expired physical POSITION has a completed Command sync, without resurrecting a logical first. */
    public static Plan expiredCommand(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            PreparedCommand command,TargetQuotaMutation stamp,TargetResultRecord position,
            java.util.function.Supplier<Authority> authority){
        final var state=TargetSloObservationVerifier.readState(reader,scope,lineage);
        if(state==null||!state.active())return new Plan(List.of(),()->{});
        final var generation=Objects.requireNonNull(TargetSloObservationVerifier.readGeneration(reader,scope,lineage,state.generation()),"original expired Command SLO generation");
        state.requireGeneration(generation);generation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        generation.policy().requireSource(stamp.source());stamp.requireAfter(generation.mutation());
        final var declaration=generation.policy().declarations().stream()
                .filter(d->d.objective().name()==SloObjectiveName.COMMAND_APPLIED_LATENCY).findFirst().orElse(null);
        if(declaration==null)return new Plan(List.of(),()->{});
        if(position.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED||!position.mutation().equals(stamp))
            throw new IllegalStateException("expired Command Start lost its new original physical POSITION");
        position.requireExpiredCommand(command);
        final var logicalKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_COMMAND_TAG,TargetKeyCodec.KEY_FORMAT},command.commandId().bytes());
        final var queryKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_QUERY_TAG,TargetKeyCodec.KEY_FORMAT},command.commandId().bytes());
        if(reader.get(ColumnFamily.DEDUPE,logicalKey)!=null||reader.get(ColumnFamily.DEDUPE,queryKey)!=null
                ||reader.get(ColumnFamily.DEDUPE,position.key())!=null)throw new IllegalStateException("expired Command Start cannot rebind existing logical or physical evidence");
        final var sample=new TargetSloObservationSample(scope,lineage,generation.generation(),generation.enable().input().systemMutationId(),
                generation.enable().appliedAt(),generation.policy().digest(),declaration,position,
                SloObservationOutbox.open(SloAuthoritativeStartFactory.commandApplied(declaration.objective(),stamp.source())),stamp);
        return append(reader,generation,List.of(sample),stamp,Objects.requireNonNull(authority.get(),"original expired Command sample authority"));
    }

    /** Only a first business decision reaches this path. Logical retry/replay never creates or rebinds a sample. */
    public static Plan command(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            PreparedCommand command, TargetQuotaMutation stamp, List<TargetStoreBackend.Edit> business, Authority authority) {
        final var state=TargetSloObservationVerifier.readState(reader,scope,lineage);
        if(state==null||!state.active())return new Plan(List.of(),()->{});
        final var generation=Objects.requireNonNull(TargetSloObservationVerifier.readGeneration(reader,scope,lineage,state.generation()),"original Target SLO generation");
        state.requireGeneration(generation);generation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        generation.policy().requireSource(stamp.source());stamp.requireAfter(generation.mutation());
        final var declaration=generation.policy().declarations().stream().filter(d->d.objective().name()==SloObjectiveName.COMMAND_APPLIED_LATENCY).findFirst().orElse(null);
        final var due=generation.policy().declarations().stream().filter(d->d.objective().name()==SloObjectiveName.DUE_ADMISSION_LAG&&d.path()==SloPath.ORDINARY_MANAGED).findFirst().orElse(null);
        final var firstKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_COMMAND_TAG,TargetKeyCodec.KEY_FORMAT},command.commandId().bytes());
        if(reader.get(ColumnFamily.DEDUPE,firstKey)!=null)throw new IllegalStateException("SLO birth cannot rebind an existing Command event");
        final var first=result(reader,business,firstKey);
        final var original=CommandDedupeRecord.decode(first.typedPayload());
        if(first.kind()!=TargetResultRecord.Kind.COMMAND||!first.mutation().equals(stamp)||!Arrays.equals(original.commandHash(),command.commandHash())
                ||!original.protocolTuple().equals(command.protocolTuple())||!Arrays.equals(stamp.mutationDigest(),command.canonicalFrameDigest()))throw new IllegalStateException("SLO birth lost original Command Source");
        final var samples=new ArrayList<TargetSloObservationSample>();
        if(declaration!=null)samples.add(new TargetSloObservationSample(scope,lineage,generation.generation(),generation.enable().input().systemMutationId(),
                generation.enable().appliedAt(),generation.policy().digest(),declaration,first,
                SloObservationOutbox.open(SloAuthoritativeStartFactory.commandApplied(declaration.objective(),stamp.source())),stamp));
        for(var edit:business){
            if(edit.family()!=ColumnFamily.ID||edit.key()[0]!=TargetKeyCodec.MESSAGE_TAG||edit.after()==null)continue;
            final var message=TargetMessageRecord.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.after(),TargetMessageRecord.VALUE_TYPE).payload(),scope.shard());
            if(!Arrays.equals(message.scheduleSource().canonicalBytes(),stamp.source().canonicalBytes()))continue;
            if(edit.before()!=null){
                final var before=TargetMessageRecord.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.before(),TargetMessageRecord.VALUE_TYPE).payload(),scope.shard());
                if(before.locator().generation()==message.locator().generation())continue;
            }
            if(message.nativeDeliveryPolicy()!=NativeDeliveryPolicy.FORBID){
                if(generation.policy().declarations().stream().anyMatch(d->d.objective().name()==SloObjectiveName.DUE_ADMISSION_LAG))
                    throw new IllegalStateException("native-capable Message eligibility requires its original qualified Source observation producer");
                continue;
            }
            if(due==null)continue;
            final var eligibility=TargetSloMessageEligibility.from(message);
            if(original.result().applyStatus()!=ApplyStatus.APPLIED||original.result().generation()!=message.locator().generation()
                    ||original.result().stateVersion()!=message.stateVersion())throw new IllegalStateException("eligibility lost accepted original Message birth result");
            samples.add(new TargetSloObservationSample(scope,lineage,generation.generation(),generation.enable().input().systemMutationId(),generation.enable().appliedAt(),
                    generation.policy().digest(),due,first,SloObservationOutbox.open(eligibility.start(due)),stamp,null,eligibility));
        }
        return append(reader,generation,samples,stamp,authority);
    }

    /** Admission is positive completion material, never the ALL_ACCEPTED population's birth authority. */
    public static Plan admission(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            SystemMutation input,TargetQuotaMutation stamp,List<TargetStoreBackend.Edit> business,
            SystemMutationResult outcome,Authority authority){
        if(outcome.applyStatus()!=ApplyStatus.APPLIED||outcome.stableCode()!=StableCode.OK)return new Plan(List.of(),()->{});
        final var state=TargetSloObservationVerifier.readState(reader,scope,lineage);
        if(state==null||!state.active())return new Plan(List.of(),()->{});
        final var body=TargetPublishAdmissionBody.decode(input.canonicalBody());
        if(body.attemptNo()!=1)return new Plan(List.of(),()->{});
        final var generation=Objects.requireNonNull(TargetSloObservationVerifier.readGeneration(reader,scope,lineage,state.generation()),"original due generation");
        state.requireGeneration(generation);
        if(generation.policy().declarations().stream().noneMatch(d->d.objective().name()==SloObjectiveName.DUE_ADMISSION_LAG))return new Plan(List.of(),()->{});
        if(!body.materialized())throw new IllegalStateException("due observation cannot classify an unmaterialized Admission");
        final var key=TargetKeyCodec.message(body.locator().messageId());
        final var message=TargetMessageRecord.decodeForStore(key,payload(reader,List.of(),ColumnFamily.ID,key,TargetMessageRecord.VALUE_TYPE),scope.shard());
        if(message.scheduleSource().compareTo(generation.enable().appliedAt().source())<=0)return new Plan(List.of(),()->{}); // No current-generation backfill.
        if(body.nativePublication()!=null||message.nativeDeliveryPolicy()!=NativeDeliveryPolicy.FORBID)
            throw new IllegalStateException("due Admission requires the separately authenticated original native eligibility binding");
        final var due=generation.policy().declarations().stream().filter(d->d.objective().name()==SloObjectiveName.DUE_ADMISSION_LAG&&d.path()==SloPath.ORDINARY_MANAGED).findFirst().orElse(null);
        if(due==null)return new Plan(List.of(),()->{});
        final var positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},message.scheduleSource().canonicalBytes());
        final var prefix=TargetSloObservationSample.referencePrefix(positionKey);
        final var upper=prefixUpper(prefix);byte[] lower=prefix;
        for(int count=0;count<TargetSloObservationPolicy.MAX_DECLARATIONS;count++){
            final var row=reader.first(ColumnFamily.META,lower,upper,List.of());
            if(row==null)break;
            final var sample=TargetSloObservationSample.decodeForStore(row.key(),TargetValueEnvelope.decode(row.value(),TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
            if(sample.eligibility()!=null&&sample.generation()==generation.generation()&&sample.eligibility().locator().equals(body.locator())
                    &&sample.eligibility().deliverAt()==body.publication().deliverAtEpochMs()&&Arrays.equals(sample.declaration().canonicalBytes(),due.canonicalBytes()))
                return new Plan(List.of(),()->{});
            lower=Bytes.concat(row.key(),new byte[]{0});
        }
        throw new IllegalStateException("Admission cannot create a missing ALL_ACCEPTED eligibility Start or change its observation generation");
    }

    private static Plan append(TargetStoreBackend.Reader reader,TargetSloObservationGeneration generation,
            List<TargetSloObservationSample> samples,TargetQuotaMutation stamp,Authority authority){
        if(samples.isEmpty())return new Plan(List.of(),()->{});
        final var before=generation.counts();
        var reservation=TargetSloObservationReservations.requireReserved(generation);
        long actual=0;
        for(var sample:samples){
            actual=Math.addExact(actual,sample.storedBytes());
            reservation=reservation.plus(TargetSloObservationReservations.forSample(generation,sample,false));
        }
        final var counts=new TargetSloObservationGeneration.Counts(Math.addExact(before.issued(),samples.size()),
                Math.addExact(before.outboxRecords(),samples.size()),Math.addExact(before.outboxBytes(),actual),
                before.custodied(),before.eventRecords(),before.eventBytes(),reservation);
        // Actual rows and every remaining mandatory Final/custody share the original frozen finite caps.
        final var next=generation.withCounts(counts,stamp);
        final long maximum=Math.addExact(actual,Math.subtractExact(reservation.bytes(),before.reservation().bytes()));
        final var held=Objects.requireNonNull(authority);held.requireCurrent();
        final var edits=new ArrayList<TargetStoreBackend.Edit>();
        for(var sample:samples){
            held.requireSample(generation,sample,maximum);held.requireCurrent();
            for(var key:sample.keys()){
                if(reader.get(ColumnFamily.META,key)!=null)throw new IllegalStateException("SLO event already has its immutable original binding");
                edits.add(reader.replace(ColumnFamily.META,key,TargetSloObservationSample.VALUE_TYPE,sample.canonicalBytes()));
            }
        }
        edits.add(reader.replace(ColumnFamily.META,next.key(),TargetSloObservationGeneration.VALUE_TYPE,next.canonicalBytes()));
        return new Plan(edits,held::requireCurrent);
    }

    /** Independent causal joins for every actual primary/reference value, including full cold ledger charging. */
    public static void requireOrigin(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity root, TargetSloObservationSample sample,
            TargetQuotaMutation frontier) {
        sample.mutation().requireAtOrBefore(frontier);
        final var generationKey = TargetSloObservationGeneration.key(scope,sample.generation());
        final var generation = TargetSloObservationGeneration.decodeForStore(generationKey,
                payload(view,overlay,ColumnFamily.META,generationKey,TargetSloObservationGeneration.VALUE_TYPE),scope,lineage);
        sample.mutation().requireAtOrBefore(generation.mutation());
        if (!Arrays.equals(generation.enable().input().systemMutationId(), sample.enableId())
                || !generation.enable().appliedAt().equals(sample.enabledAt()) || !Arrays.equals(generation.policy().digest(), sample.policyDigest())
                || generation.policy().declarations().stream().noneMatch(d -> Arrays.equals(d.canonicalBytes(),sample.declaration().canonicalBytes()))
                || generation.stop()!=null && sample.first().mutation().source().compareTo(generation.stop().appliedAt().source())>=0) {
            throw new IllegalStateException("Target SLO sample changed original effective generation/declaration");
        }
        TargetSloObservationStore.requireOrigin(view,overlay,scope,lineage,root,generation,frontier);
        final boolean written = overlay.stream().anyMatch(e->e.family()==ColumnFamily.META&&sample.keys().stream().anyMatch(k->Arrays.equals(k,e.key())));
        final var priorRaw=view.projected(ColumnFamily.META,sample.key(),List.of());
        final boolean born=written&&priorRaw==null;
        if(written && (!sample.mutation().equals(frontier)
                || born&&view.projected(ColumnFamily.DEDUPE,sample.first().key(),List.of())!=null
                || overlay.stream().noneMatch(e->e.family()==ColumnFamily.META&&Arrays.equals(e.key(),generationKey)))) {
            throw new IllegalStateException("Target SLO Start must share the actual new first/generation Source batch; no repair backfill");
        }
        final var actual = result(view,overlay,sample.first().key());
        if (!Arrays.equals(actual.canonicalBytes(),sample.first().canonicalBytes())) throw new IllegalStateException("Target SLO sample lost protected original first");
        final var position=result(view,overlay,sample.positionKey());
        if(actual.kind()==TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED){
            if(!Arrays.equals(position.canonicalBytes(),actual.canonicalBytes()))throw new IllegalStateException("expired Command sample changed its sole physical POSITION");
        }else position.requireFirst(actual);
        if(sample.eligibility()!=null){
            final var birth=sample.eligibility();final var accepted=CommandDedupeRecord.decode(actual.typedPayload()).result();
            if(accepted.applyStatus()!=ApplyStatus.APPLIED||accepted.generation()!=birth.locator().generation()||accepted.stateVersion()!=birth.stateVersion()
                    ||accepted.messageStatus()!=MessageStatus.SCHEDULED)throw new IllegalStateException("due eligibility lost original accepted Message birth");
            if(born){
                final var key=TargetKeyCodec.message(birth.locator().messageId());
                final var message=TargetMessageRecord.decodeForStore(key,payload(view,overlay,ColumnFamily.ID,key,TargetMessageRecord.VALUE_TYPE),scope.shard());
                if(!Arrays.equals(TargetSloMessageEligibility.from(message).canonicalBytes(),birth.canonicalBytes()))
                    throw new IllegalStateException("due Start is not the exact original Message birth projection");
            }
        }
        if(sample.admission()!=null){
            final var body=TargetPublishAdmissionBody.decode(sample.admission().canonicalBody());
            final var budgetKey=sample.admissionBudgetKey();
            final var budget=TargetQuotaAttemptBudget.decodeForStore(budgetKey,payload(view,overlay,ColumnFamily.META,budgetKey,TargetQuotaAttemptBudget.VALUE_TYPE),scope.shard());
            if(!budget.locator().equals(body.locator())||!Arrays.equals(budget.admissionDigest(),sample.admission().mutationHash())
                    ||!Arrays.equals(budget.recoveryLineage(),lineage))throw new IllegalStateException("Target due sample lost its original Admission Budget");
            budget.mutation().requireAtOrBefore(frontier);
        }
        for(var key:sample.keys()) if(!Arrays.equals(payload(view,overlay,ColumnFamily.META,key,TargetSloObservationSample.VALUE_TYPE),sample.canonicalBytes())) {
            throw new IllegalStateException("Target SLO sample lost exact retained primary/Source reference projection");
        }
        if(sample.observation().finalObservation()==null){
            if(!sample.mutation().equals(sample.first().mutation()))throw new IllegalStateException("open SLO Start changed birth mutation without original Source event");
        }else{
            final var eventKey=TargetSloObservationEventRecord.key(sample.observation().sampleId(),sample.mutation().sequence());
            final var event=TargetSloObservationEventRecord.decodeForStore(eventKey,payload(view,overlay,ColumnFamily.META,eventKey,TargetSloObservationEventRecord.VALUE_TYPE),scope,lineage);
            TargetSloObservationEventStore.requireOrigin(view,overlay,scope,lineage,root,event,frontier);
            if(!sample.mutation().equals(event.appliedAt())||!Arrays.equals(event.after().canonicalBytes(),sample.observation().canonicalBytes()))throw new IllegalStateException("SLO Final lost its actual latest Source merge edge");
            if(written){
                if(priorRaw==null)throw new IllegalStateException("SLO Final cannot invent original Start");
                final var prior=TargetSloObservationSample.decodeForStore(sample.key(),TargetValueEnvelope.decode(priorRaw,TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
                if(!Arrays.equals(prior.bindingDigest(),sample.bindingDigest())||!Arrays.equals(prior.observation().recordDigest(),event.beforeDigest())
                        ||!Arrays.equals(prior.observation().mergeFinal(event.body().finalObservation(),sample.declaration().objective().direction()).canonicalBytes(),sample.observation().canonicalBytes()))
                    throw new IllegalStateException("SLO Final changed immutable binding or conservative original merge");
            }
        }
    }

    /** Derives actual and remaining mandatory duties from exact before/after rows, including old-version custody. */
    public static void requireGenerationDelta(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetSloObservationGeneration generation,TargetQuotaMutation frontier) {
        if(overlay.isEmpty())return; // The full cold fold independently reconstructs every actual and reserved duty.
        final var changed=overlay.stream().filter(e->e.family()==ColumnFamily.META&&Arrays.equals(e.key(),generation.key())).findFirst().orElse(null);
        if(changed==null)return;
        final byte[] priorRaw=view.projected(ColumnFamily.META,generation.key(),List.of());
        final var prior=priorRaw==null?null:TargetSloObservationGeneration.decodeForStore(generation.key(),TargetValueEnvelope.decode(priorRaw,TargetSloObservationGeneration.VALUE_TYPE).payload(),scope,lineage);
        final var base=prior==null?TargetSloObservationGeneration.Counts.empty():prior.counts();
        final boolean reserved=generation.counts().reservation()!=null;
        if(prior!=null&&(base.reservation()!=null)!=reserved)throw new IllegalStateException("SLO Source mutation cannot upgrade or discard an original completion reservation");
        long added=0,bytes=0,eventRecords=0,eventBytes=0,custodied=0;
        final var touched=new java.util.LinkedHashMap<String,byte[]>();
        for(var edit:overlay){
            if(edit.family()!=ColumnFamily.META)continue;
            if(edit.key()[0]==TargetKeyCodec.SLO_SAMPLE_TAG){
                if(edit.after()==null)throw new IllegalStateException("SLO observation cannot delete retained sample");
                final var sample=TargetSloObservationSample.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.after(),TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
                if(sample.generation()!=generation.generation())continue;
                if(!sample.mutation().equals(frontier))throw new IllegalStateException("SLO sample edit changed actual Source mutation");
                touched.put(Bytes.hex(sample.key()),sample.key());bytes=Math.addExact(bytes,sample.storedBytes());
                if(edit.before()==null)added=Math.addExact(added,1);
                else bytes=Math.subtractExact(bytes,TargetSloObservationSample.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.before(),TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage).storedBytes());
            }else if(edit.key()[0]==TargetKeyCodec.SLO_EVENT_TAG){
                if(edit.before()!=null||edit.after()==null)throw new IllegalStateException("original SLO event history is immutable");
                final var event=TargetSloObservationEventRecord.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.after(),TargetSloObservationEventRecord.VALUE_TYPE).payload(),scope,lineage);
                if(event.body().generation()!=generation.generation())continue;
                if(!event.appliedAt().equals(frontier))throw new IllegalStateException("SLO event changed Source stamp");
                eventRecords=Math.addExact(eventRecords,1);eventBytes=Math.addExact(eventBytes,event.storedBytes());
            }else if(edit.key()[0]==TargetKeyCodec.SLO_CUSTODY_TAG){
                if(edit.before()!=null||edit.after()==null)throw new IllegalStateException("original SLO custody history is immutable");
                final var receipt=TargetSloCustodyRecord.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.after(),TargetSloCustodyRecord.VALUE_TYPE).payload(),scope,lineage);
                if(receipt.sample().generation()!=generation.generation())continue;
                if(!receipt.appliedAt().equals(frontier))throw new IllegalStateException("SLO custody changed Source stamp");
                touched.put(Bytes.hex(receipt.sample().key()),receipt.sample().key());
                eventRecords=Math.addExact(eventRecords,1);eventBytes=Math.addExact(eventBytes,receipt.storedBytes());
            }
        }
        var beforeDuties=TargetSloObservationGeneration.Counts.Reservation.empty();
        var afterDuties=TargetSloObservationGeneration.Counts.Reservation.empty();
        for(var key:touched.values()){
            final var oldRaw=view.projected(ColumnFamily.META,key,List.of());
            final var newRaw=Objects.requireNonNull(view.projected(ColumnFamily.META,key,overlay),"SLO event requires retained current sample");
            final var next=TargetSloObservationSample.decodeForStore(key,TargetValueEnvelope.decode(newRaw,TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
            final boolean afterCustody=TargetSloObservationReservations.currentCustodied(view,overlay,scope,lineage,next);
            if(afterCustody)custodied=Math.addExact(custodied,1);
            if(reserved)afterDuties=afterDuties.plus(TargetSloObservationReservations.forSample(generation,next,afterCustody));
            if(oldRaw!=null){
                final var old=TargetSloObservationSample.decodeForStore(key,TargetValueEnvelope.decode(oldRaw,TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
                final boolean beforeCustody=TargetSloObservationReservations.currentCustodied(view,List.of(),scope,lineage,old);
                if(beforeCustody)custodied=Math.subtractExact(custodied,1);
                if(reserved)beforeDuties=beforeDuties.plus(TargetSloObservationReservations.forSample(Objects.requireNonNull(prior),old,beforeCustody));
            }
        }
        if(!reserved&&!touched.isEmpty())throw new IllegalStateException("legacy SLO generation has no persistent completion reservation for new sample/event/custody writes");
        final var reservation=reserved?(prior==null?TargetSloObservationGeneration.Counts.Reservation.empty():base.reservation()).minus(beforeDuties).plus(afterDuties):null;
        final var expected=new TargetSloObservationGeneration.Counts(Math.addExact(base.issued(),added),Math.addExact(base.outboxRecords(),added),
                Math.addExact(base.outboxBytes(),bytes),Math.addExact(base.custodied(),custodied),Math.addExact(base.eventRecords(),eventRecords),Math.addExact(base.eventBytes(),eventBytes),reservation);
        if(!expected.equals(generation.counts())||!generation.mutation().equals(frontier)
                ||touched.isEmpty()&&!generation.mutation().equals(generation.control().appliedAt())||eventRecords!=0&&touched.isEmpty())
            throw new IllegalStateException("SLO generation totals/reservations do not match complete original sample/event/custody edits");
    }

    /** Any retained sample keeps its original first/POS protected, independently of collector acknowledgement. */
    public static void requireUnreferenced(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] resultKey){
        if(hasReference(view,overlay,resultKey)) {
            throw new IllegalStateException("Target retained Source record still protects certification samples");
        }
    }
    public static boolean hasReference(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] resultKey){
        final var prefix=TargetSloObservationSample.referencePrefix(resultKey);
        return view.first(ColumnFamily.META,prefix,prefixUpper(prefix),overlay)!=null;
    }
    public static void requireUnreferenced(TargetCheckpointDependencies dependencies,byte[] resultKey){
        if(dependencies.sloSamples().stream().anyMatch(s->s.references(resultKey))) {
            throw new IllegalStateException("original Source record still has retained Target certification samples");
        }
    }
    private static byte[] prefixUpper(byte[] prefix){
        final byte[] upper=Bytes.copy(prefix);for(int i=upper.length-1;i>=0;i--){if(++upper[i]!=0)break;}return upper;
    }
    private static TargetResultRecord result(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key){
        final var record=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        record.requireStored(key,TargetResultRecord.VALUE_TYPE,record.canonicalBytes());return record;
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"original Target SLO dependency"),type).payload();
    }
}
