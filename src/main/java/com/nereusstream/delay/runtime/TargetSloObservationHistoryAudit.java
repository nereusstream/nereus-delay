package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Independent retained mode-history completeness; this never certifies SLO sample coverage. */
public final class TargetSloObservationHistoryAudit {
    private TargetSloObservationHistoryAudit() {}

    public static void requireComplete(List<TargetSloObservationState> states,
            List<TargetSloObservationGeneration> generations, TargetQuotaScope scope, byte[] lineage,
            TargetQuotaMutation frontier) {
        if (states.isEmpty() && generations.isEmpty()) return;
        if (states.size() != 1 || generations.isEmpty()) throw new IllegalStateException("Target SLO history lacks its sole current head or generations");
        final var ordered = new TreeMap<Long, TargetSloObservationGeneration>(Long::compareUnsigned);
        for (var generation : generations) {
            if (!scope.equals(generation.policy().scope()) || !Arrays.equals(lineage, generation.policy().lineage())
                    || ordered.putIfAbsent(generation.generation(), generation) != null) throw new IllegalStateException("Target SLO retained generations changed scope or repeat an ordinal");
            generation.mutation().requireAtOrBefore(frontier);
            generation.policy().requireSource(frontier.source());
            if (Long.compareUnsigned(generation.generation(), generation.policy().limits().maximumConcurrentGenerations()) > 0) {
                throw new IllegalStateException("Target SLO generation exceeded its original retained-generation window");
            }
        }
        long expected = 1;
        TargetSloObservationGeneration previous = null;
        for (var generation : ordered.values()) {
            if (generation.generation() != expected) throw new IllegalStateException("Target SLO retained generation history has a gap");
            if (previous != null) {
                if (previous.active() || !Arrays.equals(TargetSloObservationState.from(previous).digest(), generation.enable().body().request().expectedPriorDigest())) {
                    throw new IllegalStateException("Target SLO enable lost its complete stopped predecessor");
                }
                generation.enable().appliedAt().requireAfter(previous.control().appliedAt());
            }
            previous = generation;
            expected = TargetQuotaMutation.increment(expected);
        }
        final var state = states.getFirst();
        state.requireGeneration(Objects.requireNonNull(previous));
        state.mutation().requireAtOrBefore(frontier);
    }

    /** Every applied mode input and every stored endpoint must correlate, including when all mode rows are missing. */
    public static void requireControlInputs(List<TargetSloObservationGeneration> generations,
            List<TargetCheckpointDependencies.ControlImage> images, List<TargetResultRecord> firsts) {
        final var pending = new HashMap<String, TargetSloObservationGeneration.Control>();
        for (var generation : generations) {
            put(pending, generation.enable());
            if (generation.stop() != null) put(pending, generation.stop());
        }
        final var actual = new HashMap<String, TargetResultRecord>();
        for (var first : firsts) actual.put(Bytes.hex(first.logicalId()), first);
        for (var image : images) {
            if (!TargetSloObservationControlBody.isControl(image.image())) continue;
            final var key = Bytes.hex(image.image().systemMutationId());
            final var first = Objects.requireNonNull(actual.get(key), "original Target SLO Control first");
            final var endpoint = pending.remove(key);
            if (first.appliedFirst()) {
                if (endpoint == null || !Arrays.equals(endpoint.input().canonicalEnvelope(), image.image().canonicalEnvelope())
                        || !Arrays.equals(endpoint.appliedAt().source().canonicalBytes(), image.source().canonicalBytes())) {
                    throw new IllegalStateException("APPLIED Target SLO Control lost its exact retained generation endpoint");
                }
            } else if (endpoint != null) throw new IllegalStateException("rejected Target SLO Control appears as an applied generation");
        }
        if (!pending.isEmpty()) throw new IllegalStateException("Target SLO generation has missing original Source Control inputs");
    }

    private static void put(HashMap<String, TargetSloObservationGeneration.Control> values, TargetSloObservationGeneration.Control control) {
        if (values.putIfAbsent(Bytes.hex(control.input().systemMutationId()), control) != null) throw new IllegalStateException("duplicate Target SLO generation Control endpoint");
    }

    /** Actual retained firsts positively enumerate required Command Starts; counts are independently rebuilt. */
    public static void requireSamples(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts) {
        final var controls=new TreeMap<SourcePosition,TargetSloObservationGeneration>();
        final var counts=new HashMap<Long,long[]>();
        final var latest=new HashMap<Long,TargetQuotaMutation>();
        final var pending=new HashMap<String,TargetSloObservationSample>();
        final var dueFirsts=new HashMap<String,TargetSloObservationSample>();
        final var eligibilityFirsts=new HashMap<String,TargetSloObservationSample>();
        for(var generation:generations){
            controls.put(generation.enable().appliedAt().source(),generation);
            counts.put(generation.generation(),new long[2]); latest.put(generation.generation(),generation.control().appliedAt());
        }
        for(var sample:samples){
            final var usage=Objects.requireNonNull(counts.get(sample.generation()),"Target SLO sample lost original generation");
            if(pending.putIfAbsent(Bytes.hex(sample.key()),sample)!=null)throw new IllegalStateException("duplicate Target SLO sample identity");
            if(sample.admission()!=null&&dueFirsts.putIfAbsent(Bytes.hex(sample.first().key()),sample)!=null)throw new IllegalStateException("Admission event was bound more than once");
            if(sample.eligibility()!=null&&eligibilityFirsts.putIfAbsent(Bytes.hex(sample.first().key()),sample)!=null)throw new IllegalStateException("Message birth eligibility was bound more than once");
            usage[0]=Math.addExact(usage[0],1);usage[1]=Math.addExact(usage[1],sample.storedBytes());
            final var at=latest.get(sample.generation());
            if(Long.compareUnsigned(sample.mutation().sequence(),at.sequence())>0)latest.put(sample.generation(),sample.mutation());
        }
        for(var first:firsts){
            if(first.kind()==TargetResultRecord.Kind.SYSTEM){
                final var sample=dueFirsts.remove(Bytes.hex(first.key()));
                if(sample!=null){
                    final var selected=controls.lowerEntry(first.mutation().source());
                    if(selected==null||selected.getValue().generation()!=sample.generation()||!Arrays.equals(first.canonicalBytes(),sample.first().canonicalBytes())) {
                        throw new IllegalStateException("due sample changed its original effective Admission first");
                    }
                    pending.remove(Bytes.hex(sample.key()));
                }
                continue;
            }
            if(first.kind()!=TargetResultRecord.Kind.COMMAND)continue;
            final var eligibility=eligibilityFirsts.remove(Bytes.hex(first.key()));
            if(eligibility!=null){
                if(!Arrays.equals(first.canonicalBytes(),eligibility.first().canonicalBytes()))throw new IllegalStateException("eligibility changed original birth first");
                pending.remove(Bytes.hex(eligibility.key()));
            }
            final var selected=controls.lowerEntry(first.mutation().source());
            if(selected==null)continue;
            final var generation=selected.getValue();
            if(generation.stop()!=null&&first.mutation().source().compareTo(generation.stop().appliedAt().source())>=0)continue;
            final var declaration=generation.policy().declarations().stream().filter(d->d.objective().name()==SloObjectiveName.COMMAND_APPLIED_LATENCY).findFirst().orElse(null);
            if(declaration==null)continue;
            final var start=SloAuthoritativeStartFactory.commandApplied(declaration.objective(),first.mutation().source());
            final var sample=pending.remove(Bytes.hex(TargetSloObservationSample.key(start.sampleId())));
            if(sample==null||sample.generation()!=generation.generation()||!Arrays.equals(sample.first().canonicalBytes(),first.canonicalBytes())
                    ||!Arrays.equals(sample.observation().start().canonicalBytes(),start.canonicalBytes())) {
                throw new IllegalStateException("Source-applied certification Command lost its required original Start/binding");
            }
        }
        if(!pending.isEmpty()||!dueFirsts.isEmpty()||!eligibilityFirsts.isEmpty())throw new IllegalStateException("Target SLO sample has no eligible original business first");
        for(var generation:generations){
            final var usage=counts.get(generation.generation());
            final var expected=new TargetSloObservationGeneration.Counts(usage[0],usage[0],usage[1],0);
            if(!expected.equals(generation.counts())||!latest.get(generation.generation()).equals(generation.mutation())) {
                throw new IllegalStateException("Target SLO generation disagrees with complete actual retained sample fold");
            }
        }
    }

    /** Legacy Admission-only binding correlation is not an ALL_ACCEPTED population oracle. */
    public static void requireAdmissionInputs(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetCheckpointDependencies.AdmissionImage> inputs){
        final var pending=new HashMap<String,TargetSloObservationSample>();
        for(var sample:samples)if(sample.admission()!=null)pending.put(Bytes.hex(sample.admission().systemMutationId()),sample);
        if(pending.isEmpty())return;
        for(var input:inputs){
            final var sample=pending.remove(Bytes.hex(input.image().systemMutationId()));
            if(sample!=null&&(!Arrays.equals(sample.admission().canonicalEnvelope(),input.image().canonicalEnvelope())
                    ||!Arrays.equals(sample.first().mutation().source().canonicalBytes(),input.source().canonicalBytes())))
                throw new IllegalStateException("legacy Admission binding changed exact original input");
        }
        if(!pending.isEmpty())throw new IllegalStateException("legacy due sample lost its original Admission image");
    }

    /** Current durable Message generations positively require their original ordinary eligibility Start, even without Admission. */
    public static void requireEligibilityMessages(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetMessageRecord> messages){
        final var controls=new TreeMap<SourcePosition,TargetSloObservationGeneration>();
        for(var generation:generations)controls.put(generation.enable().appliedAt().source(),generation);
        final var births=new HashMap<String,TargetSloObservationSample>();
        for(var sample:samples)if(sample.eligibility()!=null){
            final var locator=sample.eligibility().locator();
            final var token=Bytes.hex(Bytes.concat(locator.messageId().bytes(),Bytes.u32beBits(locator.generation())));
            if(births.putIfAbsent(token,sample)!=null)throw new IllegalStateException("duplicate ordinary eligibility birth");
        }
        for(var message:messages){
            if(message.nativeDeliveryPolicy()!=NativeDeliveryPolicy.FORBID)continue;
            final var selected=controls.lowerEntry(message.scheduleSource());if(selected==null)continue;
            final var generation=selected.getValue();
            if(generation.stop()!=null&&message.scheduleSource().compareTo(generation.stop().appliedAt().source())>=0)continue;
            final var declaration=generation.policy().declarations().stream().filter(d->d.objective().name()==SloObjectiveName.DUE_ADMISSION_LAG&&d.path()==SloPath.ORDINARY_MANAGED).findFirst().orElse(null);
            if(declaration==null)continue;
            final var token=Bytes.hex(Bytes.concat(message.locator().messageId().bytes(),Bytes.u32beBits(message.locator().generation())));
            final var sample=births.get(token);
            if(sample==null||sample.generation()!=generation.generation()||!sample.eligibility().locator().equals(message.locator())
                    ||sample.eligibility().deliverAt()!=message.deliverAtEpochMs()||sample.eligibility().expireAt()!=message.expireAtEpochMs()
                    ||!Arrays.equals(sample.eligibility().source().canonicalBytes(),message.scheduleSource().canonicalBytes())
                    ||!Arrays.equals(sample.declaration().canonicalBytes(),declaration.canonicalBytes()))
                throw new IllegalStateException("durable ordinary eligibility is missing its original ALL_ACCEPTED Start, independently of Admission");
        }
    }
}
