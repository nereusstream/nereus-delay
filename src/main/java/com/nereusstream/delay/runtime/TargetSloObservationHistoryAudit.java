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
            if (image.image().type()!=SystemMutationType.APPLY_SHARD_CONTROL || !TargetSloObservationControlBody.isControl(image.image())) continue;
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

    /** Actual firsts, complete immutable history and exact current custody independently reconstruct every duty. */
    public static void requireSamples(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts) {
        requireSamples(generations,samples,firsts,List.of(),List.of());
    }
    public static void requireSamples(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts,List<TargetSloObservationEventRecord> events) {
        requireSamples(generations,samples,firsts,events,List.of());
    }
    public static void requireSamples(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts,List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies) {
        requireSamples(generations,samples,firsts,events,custodies,List.of());
    }
    /** Writer rows must be the actual Native62 material collected and independently authenticated by the checkpoint. */
    public static void requireSamples(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts,List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies,List<TargetSloWriterEpoch> writers) {
        final var writerRows=new HashMap<String,TargetSloWriterEpoch>();
        for(var writer:writers)if(writerRows.putIfAbsent(Bytes.hex(writer.key()),writer)!=null)
            throw new IllegalStateException("duplicate SLO writer epoch in actual checkpoint history");
        for(var generation:generations)requirePolicyWriter(generation,writerRows);
        requireEventHistory(generations,samples,firsts,events,custodies,writerRows);
        final var controls=new TreeMap<SourcePosition,TargetSloObservationGeneration>();
        final var byGeneration=new HashMap<Long,TargetSloObservationGeneration>();
        final var counts=new HashMap<Long,long[]>();
        final var latest=new HashMap<Long,TargetQuotaMutation>();
        final var pending=new HashMap<String,TargetSloObservationSample>();
        final var dueFirsts=new HashMap<String,TargetSloObservationSample>();
        final var eligibilityFirsts=new HashMap<String,TargetSloObservationSample>();
        final var custodyByKey=new HashMap<String,TargetSloCustodyRecord>();
        for(var custody:custodies)if(custodyByKey.putIfAbsent(Bytes.hex(custody.key()),custody)!=null)throw new IllegalStateException("duplicate SLO custody row");
        for(var generation:generations){
            controls.put(generation.enable().appliedAt().source(),generation);byGeneration.put(generation.generation(),generation);
            counts.put(generation.generation(),new long[7]);latest.put(generation.generation(),generation.control().appliedAt());
        }
        for(var sample:samples){
            final var generation=Objects.requireNonNull(byGeneration.get(sample.generation()),"Target SLO sample lost original generation");
            final var usage=counts.get(sample.generation());
            if(pending.putIfAbsent(Bytes.hex(sample.key()),sample)!=null)throw new IllegalStateException("duplicate Target SLO sample identity");
            if(sample.admission()!=null&&dueFirsts.putIfAbsent(Bytes.hex(sample.first().key()),sample)!=null)throw new IllegalStateException("Admission event was bound more than once");
            if(sample.eligibility()!=null&&eligibilityFirsts.putIfAbsent(Bytes.hex(sample.first().key()),sample)!=null)throw new IllegalStateException("Message birth eligibility was bound more than once");
            usage[0]=Math.addExact(usage[0],1);usage[1]=Math.addExact(usage[1],sample.storedBytes());
            final var custody=custodyByKey.get(Bytes.hex(TargetSloCustodyRecord.key(sample.observation().sampleId(),sample.digest())));
            final boolean currentCustodied=custody!=null;
            if(currentCustodied){
                if(!Arrays.equals(custody.sample().canonicalBytes(),sample.canonicalBytes()))throw new IllegalStateException("SLO custody changed exact current sample bytes");
                usage[4]=Math.addExact(usage[4],1);
            }
            if(generation.counts().reservation()!=null){
                final var reservation=TargetSloObservationReservations.forSample(generation,sample,currentCustodied);
                usage[5]=Math.addExact(usage[5],reservation.records());usage[6]=Math.addExact(usage[6],reservation.bytes());
            }else if(currentCustodied)throw new IllegalStateException("legacy SLO generation cannot pretend to have reserved custody");
            latest(latest,sample.generation(),sample.mutation());
        }
        for(var event:events){
            final var usage=Objects.requireNonNull(counts.get(event.body().generation()),"SLO event lost original generation");
            usage[2]=Math.addExact(usage[2],1);usage[3]=Math.addExact(usage[3],event.storedBytes());
            latest(latest,event.body().generation(),event.appliedAt());
        }
        for(var custody:custodies){
            final var usage=Objects.requireNonNull(counts.get(custody.sample().generation()),"SLO custody lost original generation");
            usage[2]=Math.addExact(usage[2],1);usage[3]=Math.addExact(usage[3],custody.storedBytes());
            latest(latest,custody.sample().generation(),custody.appliedAt());
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
            if(first.kind()!=TargetResultRecord.Kind.COMMAND&&first.kind()!=TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED)continue;
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
            final var reservation=generation.counts().reservation()==null?null:new TargetSloObservationGeneration.Counts.Reservation(usage[5],usage[6]);
            final var expected=new TargetSloObservationGeneration.Counts(usage[0],usage[0],usage[1],usage[4],usage[2],usage[3],reservation);
            if(!expected.equals(generation.counts())||!latest.get(generation.generation()).equals(generation.mutation()))
                throw new IllegalStateException("Target SLO generation disagrees with complete actual/reserved sample/event/custody fold");
        }
    }
    private static void latest(HashMap<Long,TargetQuotaMutation> values,long generation,TargetQuotaMutation at){
        final var before=Objects.requireNonNull(values.get(generation),"SLO event lost original generation");
        if(Long.compareUnsigned(at.sequence(),before.sequence())>0)values.put(generation,at);
        else if(at.sequence()==before.sequence()&&!at.equals(before))throw new IllegalStateException("SLO history changed one actual Source mutation");
    }

    private record HistoryEntry(SystemMutation input,TargetQuotaMutation appliedAt,
            TargetSloObservationEventRecord finalEvent,TargetSloCustodyRecord custody) {}

    /** Every APPLIED type15 first must name exactly one immutable Final or custody event. */
    private static void requireEventHistory(List<TargetSloObservationGeneration> generations,
            List<TargetSloObservationSample> samples,List<TargetResultRecord> firsts,List<TargetSloObservationEventRecord> events,
            List<TargetSloCustodyRecord> custodies,HashMap<String,TargetSloWriterEpoch> writers){
        final var bySample=new HashMap<String,TargetSloObservationSample>();
        final var byGeneration=new HashMap<Long,TargetSloObservationGeneration>();
        final var histories=new HashMap<String,TreeMap<Long,HistoryEntry>>();
        final var pending=new HashMap<String,HistoryEntry>();
        for(var generation:generations)byGeneration.put(generation.generation(),generation);
        for(var sample:samples)bySample.put(Bytes.hex(sample.observation().sampleId()),sample);
        final var rows=new java.util.ArrayList<HistoryEntry>();
        for(var event:events)rows.add(new HistoryEntry(event.input(),event.appliedAt(),event,null));
        for(var custody:custodies)rows.add(new HistoryEntry(custody.input(),custody.appliedAt(),null,custody));
        for(var row:rows){
            final var sampleId=row.finalEvent()!=null?row.finalEvent().body().sampleId():row.custody().sample().observation().sampleId();
            final var sample=Objects.requireNonNull(bySample.get(Bytes.hex(sampleId)),"orphaned SLO Source event/custody");
            final var generation=Objects.requireNonNull(byGeneration.get(sample.generation()),"SLO event original generation");
            requireEventWriter(generation,TargetSloObservationEventBody.decode(row.input().canonicalBody()),
                    row.input(),row.appliedAt(),writers);
            generation.policy().requireSource(row.appliedAt().source());row.appliedAt().requireAfter(sample.first().mutation());
            if(row.finalEvent()!=null){
                final var event=row.finalEvent();
                if(event.body().generation()!=sample.generation()||!Arrays.equals(event.body().bindingDigest(),sample.bindingDigest()))throw new IllegalStateException("SLO Final changed original sample binding");
                event.appliedAt().requireAtOrBefore(sample.mutation());
                if(event.body().evidence()!=null)event.body().evidence().requireObservation(sample,event.incoming());
            }else if(row.custody().sample().generation()!=sample.generation()
                    ||!Arrays.equals(row.custody().sample().bindingDigest(),sample.bindingDigest()))throw new IllegalStateException("SLO custody changed original sample binding");
            if(pending.putIfAbsent(Bytes.hex(row.input().systemMutationId()),row)!=null
                    ||histories.computeIfAbsent(Bytes.hex(sampleId),ignored->new TreeMap<>(Long::compareUnsigned))
                        .putIfAbsent(row.appliedAt().sequence(),row)!=null)throw new IllegalStateException("duplicate SLO Source history edge");
        }
        for(var first:firsts){
            if(first.kind()!=TargetResultRecord.Kind.SYSTEM)continue;
            final var result=SystemMutationResult.decode(first.typedPayload());
            if(result.mutationType()!=SystemMutationType.TARGET_SLO_OBSERVATION)continue;
            final var row=pending.remove(Bytes.hex(result.mutationId()));
            if(first.appliedFirst()){
                if(row==null||!first.mutation().equals(row.appliedAt())
                        ||!Arrays.equals(first.typedPayload(),SystemMutationResult.from(row.input(),ApplyStatus.APPLIED,StableCode.OK,row.appliedAt().source().canonicalBytes()).encode()))
                    throw new IllegalStateException("APPLIED SLO Source first lost its exact immutable Final/custody event");
            }else if(row!=null)throw new IllegalStateException("rejected SLO event has an applied history edge");
        }
        if(!pending.isEmpty())throw new IllegalStateException("SLO event lost original first result");
        for(var sample:samples){
            var folded=SloObservationOutbox.open(sample.observation().start());var latest=sample.first().mutation();
            var version=new TargetSloObservationSample(sample.scope(),sample.lineage(),sample.generation(),sample.enableId(),sample.enabledAt(),
                    sample.policyDigest(),sample.declaration(),sample.first(),folded,latest,sample.admission(),sample.eligibility());
            final var versions=new HashMap<String,TargetSloObservationSample>();versions.put(Bytes.hex(version.digest()),version);
            final var history=histories.get(Bytes.hex(sample.observation().sampleId()));
            if(history!=null)for(var row:history.values()){
                if(row.finalEvent()!=null){
                    final var event=row.finalEvent();
                    if(!Arrays.equals(folded.recordDigest(),event.beforeDigest()))throw new IllegalStateException("SLO Source merge history has a missing predecessor");
                    event.appliedAt().requireAfter(latest);
                    event.requireIncomingFor(version);
                    folded=folded.mergeFinal(event.incoming(),sample.declaration().objective().direction());
                    if(!Arrays.equals(folded.canonicalBytes(),event.after().canonicalBytes()))throw new IllegalStateException("SLO Source merge edge changes conservative v1 semantics");
                    latest=event.appliedAt();
                    version=new TargetSloObservationSample(sample.scope(),sample.lineage(),sample.generation(),sample.enableId(),sample.enabledAt(),
                            sample.policyDigest(),sample.declaration(),sample.first(),folded,latest,sample.admission(),sample.eligibility());
                    if(versions.putIfAbsent(Bytes.hex(version.digest()),version)!=null)throw new IllegalStateException("SLO Source merge repeated one full sample version");
                }else{
                    final var exported=row.custody().sample();
                    final var known=versions.get(Bytes.hex(exported.digest()));
                    if(known==null||!Arrays.equals(known.canonicalBytes(),exported.canonicalBytes()))throw new IllegalStateException("SLO custody lacks its exact earlier exported sample version");
                    exported.mutation().requireAtOrBefore(row.appliedAt());
                }
            }
            if(!latest.equals(sample.mutation())||!Arrays.equals(version.canonicalBytes(),sample.canonicalBytes()))
                throw new IllegalStateException("SLO sample lost complete original Source event history");
        }
    }

    private static TargetSloWriterEpoch requirePolicyWriter(TargetSloObservationGeneration generation,
            HashMap<String,TargetSloWriterEpoch> writers){
        final var policy=generation.policy();
        if(!policy.requiresWriterEpoch())return null;
        final var reference=policy.requiredWriterEpoch();
        final var writer=Objects.requireNonNull(writers.get(Bytes.hex(reference.key(policy.scope()))),
                "Policy2 lost its actual original Native62 writer epoch");
        final var contract=writer.contract();
        if(!writer.reference().equals(reference)||!contract.scope().equals(policy.scope())
                ||!Arrays.equals(contract.lineage(),policy.lineage()))
            throw new IllegalStateException("Policy2 changed its exact original applied writer contract");
        contract.requireSource(generation.enable().appliedAt().source());
        generation.enable().appliedAt().requireAfter(writer.register().appliedAt());
        return writer;
    }

    private static void requireEventWriter(TargetSloObservationGeneration generation,TargetSloObservationEventBody body,
            SystemMutation input,TargetQuotaMutation appliedAt,HashMap<String,TargetSloWriterEpoch> writers){
        final var policy=generation.policy();
        if(body.writerEpoch()==null){
            if(policy.requiresWriterEpoch()||!Arrays.equals(input.authorIdentity(),policy.service().canonicalBytes())
                    ||input.signingKeyVersion()!=policy.keyVersion())
                throw new IllegalStateException("legacy SLO Source event changed its original Policy1 writer");
            return;
        }
        final var writer=Objects.requireNonNull(writers.get(Bytes.hex(body.writerEpoch().key(policy.scope()))),
                "SLO body5 lost its actual original Native62 writer epoch");
        final var contract=writer.contract();
        if(!writer.reference().equals(body.writerEpoch())||!policy.scope().equals(contract.scope())
                ||!Arrays.equals(policy.lineage(),contract.lineage())
                ||!body.scope().equals(contract.scope())||!Arrays.equals(body.lineage(),contract.lineage())
                ||!Arrays.equals(input.authorIdentity(),contract.service().canonicalBytes())
                ||input.signingKeyVersion()!=contract.keyVersion()||input.retryUntilEpochMs()>contract.maximumRetryUntil()
                ||input.encodeFrame().length>contract.limits().maximumFrameBytes()
                ||appliedAt.source().brokerPersistenceTimeEpochMs()>input.retryUntilEpochMs())
            throw new IllegalStateException("SLO body5 changed its original registered writer or finite frame/deadline bounds");
        contract.requireSource(appliedAt.source());
        appliedAt.requireAfter(writer.register().appliedAt());
        if(writer.closed()){
            final var fenceAt=writer.close().body().request().fenceAt();
            if(appliedAt.source().compareTo(fenceAt.source())>=0)
                throw new IllegalStateException("SLO body5 follows its original writer's actual closure Fence");
            appliedAt.requireAtOrBefore(fenceAt);
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
