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

    /** Only a first business decision reaches this path. Logical retry/replay never creates or rebinds a sample. */
    public static Plan command(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            PreparedCommand command, TargetQuotaMutation stamp, List<TargetStoreBackend.Edit> business, Authority authority) {
        final var state = TargetSloObservationVerifier.readState(reader, scope, lineage);
        if (state == null || !state.active()) return new Plan(List.of(), () -> {});
        final var generation = Objects.requireNonNull(TargetSloObservationVerifier.readGeneration(reader, scope, lineage, state.generation()), "original Target SLO generation");
        state.requireGeneration(generation); generation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        generation.policy().requireSource(stamp.source()); stamp.requireAfter(generation.mutation());
        final var declaration = generation.policy().declarations().stream()
                .filter(d -> d.objective().name() == SloObjectiveName.COMMAND_APPLIED_LATENCY).findFirst().orElse(null);
        if (declaration == null) return new Plan(List.of(), () -> {});
        final var firstKey = Bytes.concat(new byte[]{TargetKeyCodec.RESULT_COMMAND_TAG, TargetKeyCodec.KEY_FORMAT}, command.commandId().bytes());
        if (reader.get(ColumnFamily.DEDUPE, firstKey) != null) throw new IllegalStateException("Target SLO first Command found an existing event");
        final var first = result(reader, business, firstKey);
        if (first.kind() != TargetResultRecord.Kind.COMMAND || !first.mutation().equals(stamp)) throw new IllegalStateException("Target SLO Command changed its first Source decision");
        final var original = CommandDedupeRecord.decode(first.typedPayload());
        if (!Arrays.equals(original.commandHash(), command.commandHash()) || !original.protocolTuple().equals(command.protocolTuple())
                || !Arrays.equals(stamp.mutationDigest(), command.canonicalFrameDigest())) throw new IllegalStateException("Target SLO Command lost exact original input");
        final var sample = new TargetSloObservationSample(scope, lineage, generation.generation(),
                generation.enable().input().systemMutationId(), generation.enable().appliedAt(), generation.policy().digest(),
                declaration, first, SloObservationOutbox.open(SloAuthoritativeStartFactory.commandApplied(declaration.objective(), stamp.source())), stamp);
        final var limits = generation.policy().limits();
        if (sample.maximumRecordBytes() > limits.maximumRecordBytes()) throw new IllegalStateException("Target certification cannot retain the bounded Final for this original Start");
        final var before = generation.counts();
        final long reservedBefore = Math.addExact(before.outboxBytes(), Math.multiplyExact(before.outboxRecords(),TargetSloObservationSample.MAX_FUTURE_STORED_GROWTH));
        final long reservedAfter = Math.addExact(reservedBefore, sample.maximumStoredBytes());
        if (before.outboxRecords() >= limits.maximumOutboxRecords() || reservedAfter > limits.maximumOutboxBytes()) {
            throw new IllegalStateException("Target certification outbox lacks its original finite sample/Final reservation");
        }
        final var held = Objects.requireNonNull(authority); held.requireCurrent();
        held.requireSample(generation, sample, sample.maximumStoredBytes()); held.requireCurrent();
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        for (var key : sample.keys()) {
            if (reader.get(ColumnFamily.META, key) != null) throw new IllegalStateException("Target SLO event/sample was already bound; no new generation substitution");
            edits.add(reader.replace(ColumnFamily.META, key, TargetSloObservationSample.VALUE_TYPE, sample.canonicalBytes()));
        }
        final var counts = new TargetSloObservationGeneration.Counts(Math.addExact(before.issued(),1),
                Math.addExact(before.outboxRecords(),1), Math.addExact(before.outboxBytes(),sample.storedBytes()), before.custodied());
        final var next = generation.withCounts(counts, stamp);
        edits.add(reader.replace(ColumnFamily.META,next.key(),TargetSloObservationGeneration.VALUE_TYPE,next.canonicalBytes()));
        return new Plan(edits, held::requireCurrent);
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
        if(written && (!sample.mutation().equals(frontier)
                || view.projected(ColumnFamily.DEDUPE,sample.first().key(),List.of())!=null
                || overlay.stream().noneMatch(e->e.family()==ColumnFamily.META&&Arrays.equals(e.key(),generationKey)))) {
            throw new IllegalStateException("Target SLO Start must share the actual new first/generation Source batch; no repair backfill");
        }
        final var actual = result(view,overlay,sample.first().key());
        if (!Arrays.equals(actual.canonicalBytes(),sample.first().canonicalBytes())) throw new IllegalStateException("Target SLO sample lost protected original first");
        result(view,overlay,sample.positionKey()).requireFirst(actual);
        for(var key:sample.keys()) if(!Arrays.equals(payload(view,overlay,ColumnFamily.META,key,TargetSloObservationSample.VALUE_TYPE),sample.canonicalBytes())) {
            throw new IllegalStateException("Target SLO sample lost exact retained primary/Source reference projection");
        }
        // Finals require their own original Source event/custody chain; this Start writer cannot manufacture one.
        if(sample.observation().finalObservation()!=null || !sample.mutation().equals(sample.first().mutation())) {
            throw new IllegalStateException("Target SLO Final Source event is not wired yet");
        }
    }

    /** Hot delta derived from actual sample edits, never from caller-supplied generation totals. */
    public static void requireGenerationDelta(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,byte[] lineage,TargetSloObservationGeneration generation,TargetQuotaMutation frontier) {
        if(overlay.isEmpty()) return; // The complete cold fold independently derives totals below.
        final var changed=overlay.stream().filter(e->e.family()==ColumnFamily.META&&Arrays.equals(e.key(),generation.key())).findFirst().orElse(null);
        if(changed==null) return;
        final byte[] priorRaw=view.projected(ColumnFamily.META,generation.key(),List.of());
        final var prior=priorRaw==null?null:TargetSloObservationGeneration.decodeForStore(generation.key(),TargetValueEnvelope.decode(priorRaw,TargetSloObservationGeneration.VALUE_TYPE).payload(),scope,lineage);
        final var base=prior==null?TargetSloObservationGeneration.Counts.empty():prior.counts();
        long added=0,bytes=0;
        for(var edit:overlay){
            if(edit.family()!=ColumnFamily.META||edit.key()[0]!=TargetKeyCodec.SLO_SAMPLE_TAG)continue;
            if(edit.after()==null)throw new IllegalStateException("Target SLO Start cannot delete retained samples");
            final var sample=TargetSloObservationSample.decodeForStore(edit.key(),TargetValueEnvelope.decode(edit.after(),TargetSloObservationSample.VALUE_TYPE).payload(),scope,lineage);
            if(sample.generation()!=generation.generation())continue;
            if(edit.before()!=null || !sample.mutation().equals(frontier))throw new IllegalStateException("Target SLO Start changed an already bound event");
            added=Math.addExact(added,1);bytes=Math.addExact(bytes,sample.storedBytes());
        }
        final var expected=new TargetSloObservationGeneration.Counts(Math.addExact(base.issued(),added),Math.addExact(base.outboxRecords(),added),Math.addExact(base.outboxBytes(),bytes),base.custodied());
        if(!expected.equals(generation.counts()) || !generation.mutation().equals(frontier)
                || added==0 && !generation.mutation().equals(generation.control().appliedAt())) {
            throw new IllegalStateException("Target SLO generation totals do not match original actual sample/Control edits");
        }
    }

    /** Any retained sample keeps its original first/POS protected, independently of collector acknowledgement. */
    public static void requireUnreferenced(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] resultKey){
        if(hasReference(view,overlay,resultKey)) {
            throw new IllegalStateException("Target retained Source record still protects certification samples");
        }
    }
    public static boolean hasReference(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] resultKey){
        final var prefix=TargetSloObservationSample.referencePrefix(resultKey);
        final byte[] upper=Bytes.copy(prefix);
        for(int i=upper.length-1;i>=0;i--){if(++upper[i]!=0)break;}
        return view.first(ColumnFamily.META,prefix,upper,overlay)!=null;
    }
    public static void requireUnreferenced(TargetCheckpointDependencies dependencies,byte[] resultKey){
        if(dependencies.sloSamples().stream().anyMatch(s->s.references(resultKey))) {
            throw new IllegalStateException("original Source record still has retained Target certification samples");
        }
    }
    private static TargetResultRecord result(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,byte[] key){
        final var record=TargetResultRecord.decode(payload(view,overlay,ColumnFamily.DEDUPE,key,TargetResultRecord.VALUE_TYPE));
        record.requireStored(key,TargetResultRecord.VALUE_TYPE,record.canonicalBytes());return record;
    }
    private static byte[] payload(TargetRecordAccounting.View view,List<TargetStoreBackend.Edit> overlay,ColumnFamily family,byte[] key,int type){
        return TargetValueEnvelope.decode(Objects.requireNonNull(view.projected(family,key,overlay),"original Target SLO dependency"),type).payload();
    }
}
