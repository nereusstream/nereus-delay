package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Original post-sync observation, or a qualified deadline observation; external time/history trust is mandatory. */
public record TargetSloFinalEvidence(Kind kind,TrustedUtcIntervalEvidence observedAt,TargetQuotaMutation inspectedAt,
        SystemMutation admission,TargetQuotaMutation admissionAt) {
    public enum Kind {
        COMMAND_SYNC(1),ADMISSION_SYNC(2),TIMEOUT(3);
        private final int wire;Kind(int wire){this.wire=wire;}
        public int wireValue(){return wire;}
        static Kind fromWire(int wire){for(var kind:values())if(kind.wire==wire)return kind;throw new IllegalArgumentException("unknown SLO endpoint kind");}
    }
    public static final int MAX_CANONICAL_BYTES=TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES
            +2*TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES+128;
    private static final byte[] DOMAIN=Bytes.utf8("nereus-delay-target-slo-final-evidence\0");
    public TargetSloFinalEvidence {
        Objects.requireNonNull(kind);Objects.requireNonNull(observedAt);Objects.requireNonNull(inspectedAt).requireSourceApplied();
        TargetNativePolicySnapshot.requireBoundedTime(observedAt);
        if((kind==Kind.ADMISSION_SYNC)!=(admission!=null)||(admission==null)!=(admissionAt==null))
            throw new IllegalArgumentException("SLO Admission endpoint requires its exact input and sync stamp");
        if(admission!=null){
            admissionAt.requireSourceApplied();admissionAt.requireAtOrBefore(inspectedAt);
            if(admission.type()!=SystemMutationType.TARGET_PUBLISH_ADMISSION
                    ||admission.canonicalEnvelope().length>TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES
                    ||!Arrays.equals(Bytes.sha256(admission.canonicalEnvelope()),admissionAt.mutationDigest()))
                throw new IllegalArgumentException("SLO endpoint changed original Admission Source input");
        }
    }
    public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
        CanonicalProtobuf.uint32(out,1,1);CanonicalProtobuf.uint32(out,2,kind.wireValue());
        CanonicalProtobuf.bytes(out,3,observedAt.canonicalBytes());CanonicalProtobuf.bytes(out,4,inspectedAt.canonicalBytes());
        if(admission!=null){CanonicalProtobuf.bytes(out,5,admission.canonicalEnvelope());CanonicalProtobuf.bytes(out,6,admissionAt.canonicalBytes());}
    });}
    public byte[] digest(){return Bytes.sha256(DOMAIN,canonicalBytes());}
    /** Reconstructs exact v1 timing/outcome fields; never trusts supplied durations or a decision-time proxy. */
    public SloSampleFinal observation(TargetSloObservationSample sample,long revision){
        requireSample(sample);
        final var start=sample.observation().start();final var objective=sample.declaration().objective();
        final long lower=Math.max(0,Math.subtractExact(observedAt.earliestEpochMs(),start.start().latestEpochMs()));
        final long upper=Math.max(0,Math.subtractExact(observedAt.latestEpochMs(),start.start().earliestEpochMs()));
        final var outcome=kind==Kind.TIMEOUT?SloFinalOutcome.BAD_TIMEOUT:upper<=objective.threshold()?SloFinalOutcome.SUCCESS
                :lower>objective.threshold()?SloFinalOutcome.BAD_DEFINITIVE:SloFinalOutcome.BAD_UNCERTAIN;
        return new SloSampleFinal(start.sampleId(),start.startDigest(),outcome,SloThresholdUnit.MILLISECONDS,lower,upper,null,
                new SloTimeEndpoint(SloTimeEndpointKind.TRUSTED_OBSERVATION,observedAt.earliestEpochMs(),observedAt.latestEpochMs(),
                        Bytes.sha256(observedAt.canonicalBytes())),digest(),revision);
    }
    public void requireObservation(TargetSloObservationSample sample,SloSampleFinal finalObservation){
        if(!Arrays.equals(observation(sample,finalObservation.observationRevision()).canonicalBytes(),finalObservation.canonicalBytes()))
            throw new IllegalArgumentException("SLO Final differs from its original qualified endpoint/time evidence");
    }
    private void requireSample(TargetSloObservationSample sample){
        Objects.requireNonNull(sample);sample.first().mutation().requireAtOrBefore(inspectedAt);
        if(!sample.scope().shard().equals(inspectedAt.source().shardId()))throw new IllegalArgumentException("SLO observed cut changed Shard");
        if(sample.declaration().objective().direction()!=SloThresholdDirection.AT_MOST||sample.declaration().objective().unit()!=SloThresholdUnit.MILLISECONDS)
            throw new IllegalArgumentException("unsupported Target SLO Final objective units");
        if(kind==Kind.COMMAND_SYNC){
            if(sample.declaration().objective().name()!=SloObjectiveName.COMMAND_APPLIED_LATENCY||sample.admission()!=null||sample.eligibility()!=null)
                throw new IllegalArgumentException("Command sync cannot complete a due eligibility");
        }else if(kind==Kind.ADMISSION_SYNC){
            final var body=TargetPublishAdmissionBody.decode(admission.canonicalBody());
            if(!body.materialized())throw new IllegalArgumentException("SLO Admission sync requires original materialized business event");
            final var eligibility=sample.eligibility();
            final var path=body.nativePublication()==null?SloPath.ORDINARY_MANAGED:SloPath.MANAGED_PULSAR_HANDOFF;
            final long actionAt=body.nativePublication()==null?body.publication().deliverAtEpochMs():body.nativePublication().actionAtEpochMs();
            if(!body.materialized()||body.attemptNo()!=1||sample.declaration().objective().name()!=SloObjectiveName.DUE_ADMISSION_LAG
                    ||sample.declaration().path()!=path||sample.observation().start().start().earliestEpochMs()!=actionAt
                    ||eligibility!=null&&!eligibility.locator().equals(body.locator())
                    ||eligibility==null&&(sample.admission()==null||!Arrays.equals(sample.admission().canonicalEnvelope(),admission.canonicalEnvelope())))
                throw new IllegalArgumentException("Admission sync changed original due event/path/generation");
            sample.first().mutation().requireAtOrBefore(admissionAt);
        }else{
            final var deadline=sample.observation().start().timeoutAtEpochMs();
            if(deadline==null||observedAt.earliestEpochMs()<deadline)throw new IllegalArgumentException("SLO timeout lacks qualified original deadline observation");
        }
    }
    public static TargetSloFinalEvidence decode(byte[] encoded){
        final var f=TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,6,false,"Target SLO Final evidence");
        final boolean admission=f.size()==6;
        QueryCodecSupport.requireNumbers(f,admission?new int[]{1,2,3,4,5,6}:new int[]{1,2,3,4},"Target SLO Final evidence");
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=1)throw new IllegalArgumentException("unknown SLO Final evidence schema");
        final var value=new TargetSloFinalEvidence(Kind.fromWire(QueryCodecSupport.uint32(f.get(1),2)),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.bytes(f.get(2),3)),TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3),4)),
                admission?SystemMutation.decodeTargetEnvelope(QueryCodecSupport.bytes(f.get(4),5),SystemMutationType.TARGET_PUBLISH_ADMISSION):null,
                admission?TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5),6)):null);
        QueryCodecSupport.requireCanonical(encoded,value.canonicalBytes(),"Target SLO Final evidence");return value;
    }
}
