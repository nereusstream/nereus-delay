package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetScopeFeeAuthority;
import com.nereusstream.delay.runtime.TargetScopeNativeRows;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reconstructs v2 settlement from the bounded actual image, never from the persisted Usage claim. */
public final class TargetScopeNativeUsageAudit {
    private TargetScopeNativeUsageAudit() {}

    /** Compact facts from one actual physical row; no extra retained copy of its large typed payload. */
    public record Row(byte[] key, int valueType, int payloadBytes, long generation,
            TargetSloControlScopeHeader.Reference directScope, TargetSloWriterContract.Reference writer,
            TargetQuotaMutation bornAt, TargetQuotaMutation mutation, boolean primarySample) {
        public Row {
            key=Bytes.copy(key); Objects.requireNonNull(mutation).requireSourceApplied();
            if(!TargetScopeNativeRows.nativeType(valueType)||payloadBytes<0||key.length==0)
                throw new IllegalArgumentException("invalid actual Scope Native fact");
            if(bornAt!=null)bornAt.requireAtOrBefore(mutation);
        }
        @Override public byte[] key(){return Bytes.copy(key);}
    }

    public static Row head(byte[] key,int bytes,TargetSloObservationState value){
        return new Row(key,TargetSloObservationState.VALUE_TYPE,bytes,value.generation(),null,null,null,value.mutation(),false);
    }
    public static Row generation(byte[] key,int bytes,TargetSloObservationGeneration value){
        return new Row(key,TargetSloObservationGeneration.VALUE_TYPE,bytes,value.generation(),value.enable().body().admissionScope(),
                null,value.enable().appliedAt(),value.mutation(),false);
    }
    public static Row sample(byte[] key,int bytes,TargetSloObservationSample value){
        return new Row(key,TargetSloObservationSample.VALUE_TYPE,bytes,value.generation(),null,null,
                value.first().mutation(),value.mutation(),value.primary(key));
    }
    public static Row event(byte[] key,int bytes,TargetSloObservationEventRecord value){
        return new Row(key,TargetSloObservationEventRecord.VALUE_TYPE,bytes,0,null,value.body().writerEpoch(),
                value.appliedAt(),value.appliedAt(),false);
    }
    public static Row custody(byte[] key,int bytes,TargetSloCustodyRecord value){
        return new Row(key,TargetSloCustodyRecord.VALUE_TYPE,bytes,0,null,value.body().writerEpoch(),
                value.appliedAt(),value.appliedAt(),false);
    }
    public static Row writer(byte[] key,int bytes,TargetSloWriterEpoch value){
        return new Row(key,TargetSloWriterEpoch.VALUE_TYPE,bytes,0,value.register().body().admissionScope(),null,
                value.register().appliedAt(),value.mutation(),false);
    }

    private static final class Rebuilt {
        final TargetScopeControlObligation original;
        CapacityVector converted=CapacityVector.empty();
        long occurrences,samples,writers,generations;
        TargetQuotaMutation latest;
        Rebuilt(TargetScopeControlObligation original){this.original=original;latest=original.fundedAt();}
        void touch(TargetQuotaMutation actual){
            actual.requireSourceApplied();original.requireSource(actual.source());
            if(Long.compareUnsigned(actual.sequence(),latest.sequence())>0){actual.requireAfter(latest);latest=actual;}
            else actual.requireAtOrBefore(latest);
        }
        void record(TargetSloScopeNativePlan.NativeClass kind,byte[] key,int bytes,CapacityVector actual){
            original.definition().plan().requireNativeRecord(kind,key.length,bytes);
            converted=converted.add(actual);
        }
    }

    /**
     * Invoke only after generic original Control/event authentication. All fee materials must already be held;
     * requireScope must protect the complete billed facts and original Source prefix, including lifetime facts.
     * No mutable Store or external I/O is consulted while reconstructing a historical cut.
     */
    public static void require(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> controls,TargetScopeFeeAuthority fees,
            BoundedReadBudget budget,Runnable guard){
        Objects.requireNonNull(dependencies);Objects.requireNonNull(controls);Objects.requireNonNull(budget);
        Objects.requireNonNull(guard).run();
        final Map<String,Rebuilt> rebuilt=new HashMap<>();
        for(var scope:dependencies.scopeObligations())if(scope.settlementEnabled()){
            if(fees==null)throw unavailable("settlement-v2 checkpoint lacks original Scope fee authority");
            before(budget,guard);fees.requireCurrent();fees.requireScope(scope);fees.requireCurrent();after(budget,guard);
            scope.requireRoot(dependencies.root().root());
            rebuilt.put(token(scope.reference()),new Rebuilt(scope));
        }
        if(rebuilt.isEmpty())return;
        final Map<Long,TargetSloObservationGeneration> generations=new HashMap<>();
        for(var generation:dependencies.sloGenerations()){
            if(generations.put(generation.generation(),generation)!=null)
                throw new IllegalStateException("duplicate original Scope generation");
        }
        for(var row:dependencies.scopeNativeRows()){
            after(budget,guard);
            final var origin=origin(dependencies,generations,row);
            final var payer=selected(dependencies,rebuilt,origin);
            if(payer==null)continue;
            final var born=row.bornAt()!=null?row.bornAt():requireGeneration(generations,row.generation()).enable().appliedAt();
            requireFundedBefore(payer.original,born);
            payer.record(kind(row),row.key(),row.payloadBytes(),payer.original.accounting().recordCharge(
                    TargetQuotaAccounting.RecordClass.STATE,row.key().length,row.payloadBytes()));
            payer.touch(row.mutation());
            switch(row.valueType()){
                case TargetSloObservationSample.VALUE_TYPE -> {if(row.primarySample())payer.samples=Math.incrementExact(payer.samples);}
                case TargetSloWriterEpoch.VALUE_TYPE -> payer.writers=Math.incrementExact(payer.writers);
                case TargetSloObservationGeneration.VALUE_TYPE -> payer.generations=Math.incrementExact(payer.generations);
                default -> { }
            }
        }
        // A replaced head leaves no physical old row. Its removal still advances the original ENABLE payer.
        final var ordered=new ArrayList<>(generations.values());
        ordered.sort((a,b)->a.enable().appliedAt().source().compareTo(b.enable().appliedAt().source()));
        for(int i=1;i<ordered.size();i++){
            after(budget,guard);
            final var prior=ordered.get(i-1);
            final var payer=selected(dependencies,rebuilt,prior.enable().body().admissionScope());
            if(payer!=null)payer.touch(ordered.get(i).enable().appliedAt());
        }
        requireOccurrences(dependencies,controls,fees,rebuilt,budget,guard);
        for(var value:rebuilt.values()){
            after(budget,guard);
            final var actual=value.original;
            var expected=TargetScopeControlObligation.fund(actual.definition(),actual.funding().input(),
                    actual.fundedAt(),actual.bootstrapResultsCharge());
            if(!value.latest.equals(actual.fundedAt()))expected=expected.settled(value.converted,value.occurrences,
                    value.samples,value.writers,value.generations,value.latest);
            if(!Arrays.equals(expected.usage().canonicalBytes(),actual.usage().canonicalBytes())
                    ||!expected.frozenFutureNative().equals(actual.frozenFutureNative())
                    ||!expected.metadataCeiling().equals(actual.metadataCeiling())
                    ||!expected.parentCommitment().equals(actual.parentCommitment())
                    ||!expected.remainingFuture().equals(actual.remainingFuture()))
                throw new IllegalStateException("Native64 Usage/F0/M0/C0 differs from complete actual Source/Native fees");
        }
        fees.requireCurrent();after(budget,guard);
    }

    private static void requireOccurrences(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> controls,TargetScopeFeeAuthority fees,
            Map<String,Rebuilt> rebuilt,BoundedReadBudget budget,Runnable guard){
        final Map<String,TargetCheckpointDependencies.ControlImage> inputs=new HashMap<>();
        for(var input:controls)if(inputs.put(Bytes.hex(input.image().systemMutationId()),input)!=null)
            throw new IllegalArgumentException("duplicate original Source fee input");
        final Map<String,TargetResultRecord> firsts=new HashMap<>();
        for(var first:dependencies.controlResults()){
            firsts.put(Bytes.hex(first.logicalId()),first);
            final var input=Objects.requireNonNull(inputs.get(Bytes.hex(first.logicalId())),"Scope fee lost original first input inventory");
            if(requiresOccurrenceInput(input.image()))dependencies.requireSystemPosition(first);
        }
        // sourceCandidate only needs an exact retained writer row; no full-image/raw-frame copy is built.
        final Map<String,TargetSloWriterEpoch> writers=new HashMap<>();
        for(var writer:dependencies.sloWriterEpochs())writers.put(Bytes.hex(writer.key()),writer);
        final TargetRecordAccounting.View view=new TargetRecordAccounting.View(){
            @Override public ShardId shardId(){return dependencies.root().metadata().shardId();}
            @Override public byte[] projected(ColumnFamily family,byte[] key,List<TargetStoreBackend.Edit> overlay){
                if(family!=ColumnFamily.META||!overlay.isEmpty())throw new IllegalArgumentException("invalid cold Scope writer join");
                final var row=writers.get(Bytes.hex(key));
                return row==null?null:TargetValueEnvelope.encode(TargetSloWriterEpoch.VALUE_TYPE,row.canonicalBytes());
            }
        };
        for(var position:dependencies.resultRecords()){
            if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM)continue;
            final var first=firsts.get(Bytes.hex(position.logicalId()));
            if(first==null)continue;
            final var original=Objects.requireNonNull(inputs.get(Bytes.hex(first.logicalId())),"Scope fee lost complete original first frame");
            if(!requiresOccurrenceInput(original.image()))continue;
            before(budget,guard);
            final var input=Arrays.equals(position.mutation().mutationDigest(),Bytes.sha256(original.image().canonicalEnvelope()))
                    ?original.image():Objects.requireNonNull(fees.occurrenceInput(original.image(),first,position),"exact Source occurrence frame");
            if(!budget.tryCharge(position.mutation().source().canonicalBytes().length,input.encodeFrame().length))throw budget.incomplete();
            // Authenticate the exact occurrence digest/logical first before selecting any candidate payer.
            new TargetScopeFeeAuthority.Occurrence(original.image(),input,first,position,null);
            if(!input.shardId().equals(dependencies.root().metadata().shardId())
                    ||!position.primaryIdentity().equals(dependencies.root().root().identity())
                    ||!position.accounting().equals(dependencies.root().root().accounting())
                    ||!Arrays.equals(position.tenantScope(),dependencies.root().root().tenantScope())
                    ||!Arrays.equals(position.recoveryLineage(),dependencies.root().root().recoveryLineage()))
                throw new IllegalStateException("Scope fee occurrence crosses original Root/Source/accounting");
            final var candidate=TargetScopeNativeRows.sourceCandidate(view,List.of(),dependencies.root().root().scope(),
                    dependencies.root().root().recoveryLineage(),input);
            if(!candidate.occurrenceRequired()){after(budget,guard);continue;}
            final var obligation=candidate.reference()==null?null:dependencies.scopeObligation(candidate.reference());
            // v1 retains its unchanged zero-only bookkeeping and never acquires a retroactive v2 payer.
            if(obligation!=null&&!obligation.settlementEnabled()){after(budget,guard);continue;}
            if(obligation!=null)requireFundedBefore(obligation,position.mutation());
            fees.requireCurrent();
            final var payment=Objects.requireNonNull(fees.requireOccurrence(new TargetScopeFeeAuthority.Occurrence(
                    original.image(),input,first,position,obligation)),"original physical Source fee payment");
            fees.requireCurrent();after(budget,guard);
            if(payment==TargetScopeFeeAuthority.Payment.INVALID_SHARED){
                if(first.appliedFirst()&&position.mutation().equals(first.mutation()))
                    throw unavailable("shared invalid-input payment cannot fund an APPLIED scoped Native first");
                continue;
            }
            if(obligation==null||candidate.reference()==null)
                throw unavailable("funded Source occurrence lost its unique original Native64 payer");
            final var payer=Objects.requireNonNull(rebuilt.get(token(candidate.reference())),"original v2 Source payer");
            final var plan=obligation.definition().plan();
            plan.requireSource(position.mutation().source());
            if(input.encodeFrame().length>plan.maximumSourceFrameBytes())
                throw unavailable("Source occurrence exceeds its original frozen frame bound");
            payer.record(TargetSloScopeNativePlan.NativeClass.POSITION,position.key(),position.canonicalBytes().length,position.recordCharge());
            if(position.mutation().equals(first.mutation())){
                if(!first.primaryIdentity().equals(obligation.definition().originalRoot().identity())
                        ||!first.accounting().equals(obligation.accounting()))
                    throw new IllegalStateException("Scope first changed its original accounting owner");
                payer.record(TargetSloScopeNativePlan.NativeClass.FIRST,first.key(),first.canonicalBytes().length,first.recordCharge());
            }
            payer.occurrences=Math.incrementExact(payer.occurrences);payer.touch(position.mutation());
        }
    }

    private static boolean requiresOccurrenceInput(SystemMutation input){
        if(TargetSloObservationControlBody.isControl(input))
            return TargetSloObservationControlBody.decode(input.canonicalBody()).admissionScope()!=null;
        if(TargetSloWriterControlBody.isControl(input))
            return TargetSloWriterControlBody.decode(input.canonicalBody()).admissionScope()!=null;
        return input.type()==SystemMutationType.TARGET_SLO_OBSERVATION
                &&TargetSloObservationEventBody.decode(input.canonicalBody()).writerEpoch()!=null;
    }

    private static TargetSloControlScopeHeader.Reference origin(TargetCheckpointDependencies dependencies,
            Map<Long,TargetSloObservationGeneration> generations,Row row){
        return switch(row.valueType()){
            case TargetSloObservationState.VALUE_TYPE,TargetSloObservationSample.VALUE_TYPE ->
                    requireGeneration(generations,row.generation()).enable().body().admissionScope();
            case TargetSloObservationGeneration.VALUE_TYPE,TargetSloWriterEpoch.VALUE_TYPE -> row.directScope();
            case TargetSloObservationEventRecord.VALUE_TYPE,TargetSloCustodyRecord.VALUE_TYPE -> {
                if(row.writer()==null)yield null;
                final var writer=dependencies.sloWriterEpoch(row.writer());
                if(writer==null)throw unavailable("actual modern Native event lost its exact original writer REGISTER");
                yield writer.register().body().admissionScope();
            }
            default -> throw new IllegalArgumentException("unknown actual Scope Native fact");
        };
    }
    private static TargetSloObservationGeneration requireGeneration(Map<Long,TargetSloObservationGeneration> values,long generation){
        final var value=values.get(generation);
        if(value==null)throw unavailable("Scope Native row lost its original ENABLE generation");
        return value;
    }
    private static Rebuilt selected(TargetCheckpointDependencies dependencies,Map<String,Rebuilt> rebuilt,
            TargetSloControlScopeHeader.Reference reference){
        if(reference==null)return null;
        final var scope=dependencies.scopeObligation(reference);
        if(scope==null)throw unavailable("actual scoped Native fact lost its original Native64");
        return scope.settlementEnabled()?Objects.requireNonNull(rebuilt.get(token(reference))):null;
    }
    private static TargetSloScopeNativePlan.NativeClass kind(Row row){
        return switch(row.valueType()){
            case TargetSloObservationState.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.HEAD;
            case TargetSloObservationGeneration.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.GENERATION;
            case TargetSloObservationSample.VALUE_TYPE -> row.primarySample()?TargetSloScopeNativePlan.NativeClass.SAMPLE_PRIMARY:TargetSloScopeNativePlan.NativeClass.SAMPLE_PROJECTION;
            case TargetSloObservationEventRecord.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.EVENT;
            case TargetSloCustodyRecord.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.CUSTODY;
            case TargetSloWriterEpoch.VALUE_TYPE -> TargetSloScopeNativePlan.NativeClass.WRITER_EPOCH;
            default -> throw new IllegalArgumentException("unknown Scope Native class");
        };
    }
    private static void requireFundedBefore(TargetScopeControlObligation scope,TargetQuotaMutation actual){
        try{actual.requireAfter(scope.fundedAt());scope.requireSource(actual.source());}
        catch(IllegalArgumentException|IllegalStateException invalid){throw unavailable("Scope was not actually funded before this original Source/Native birth");}
    }
    private static String token(TargetSloControlScopeHeader.Reference reference){return Bytes.hex(reference.canonicalBytes());}
    private static void before(BoundedReadBudget budget,Runnable guard){guard.run();if(!budget.beforeRead())throw budget.incomplete();}
    private static void after(BoundedReadBudget budget,Runnable guard){guard.run();if(!budget.beforeTimedWork())throw budget.incomplete();}
    private static TargetSourceAuthorityPreparation.InputUnavailable unavailable(String message){return new TargetSourceAuthorityPreparation.InputUnavailable(message);}
}
