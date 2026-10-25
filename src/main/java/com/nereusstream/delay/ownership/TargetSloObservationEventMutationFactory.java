package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Freezes one original SERVICE observation for the existing bounded System Source handoff. */
public final class TargetSloObservationEventMutationFactory {
    private final PrivateKey key;
    private final long maximumFrameBytes;
    public TargetSloObservationEventMutationFactory(PrivateKey key,long maximumFrameBytes){
        this.key=Objects.requireNonNull(key);
        if(maximumFrameBytes<=0||maximumFrameBytes==Long.MAX_VALUE)throw new IllegalArgumentException("SLO event needs a finite Source frame bound");
        this.maximumFrameBytes=maximumFrameBytes;
    }
    /** The Source verifier independently checks original endpoint/time/history and retained resource authority. */
    public Prepared prepare(TargetSloObservationGeneration generation,TargetSloObservationSample sample,
            SloSampleFinal observation,long retryUntil){
        Objects.requireNonNull(generation);Objects.requireNonNull(sample);Objects.requireNonNull(observation);
        if(sample.generation()!=generation.generation()||!sample.scope().equals(generation.policy().scope())
                ||!Arrays.equals(sample.lineage(),generation.policy().lineage())
                ||!Arrays.equals(sample.policyDigest(),generation.policy().digest()))throw new IllegalArgumentException("SLO event changed original frozen sample generation");
        observation.validateAgainst(sample.observation().start());
        return sign(generation,new TargetSloObservationEventBody(sample.scope(),sample.lineage(),retryUntil,sample.generation(),
                sample.observation().sampleId(),sample.bindingDigest(),observation));
    }
    public Prepared prepareObserved(TargetSloObservationGeneration generation,TargetSloObservationSample sample,
            TargetSloFinalEvidence evidence,long retryUntil){
        Objects.requireNonNull(generation);Objects.requireNonNull(sample);Objects.requireNonNull(evidence);
        if(sample.generation()!=generation.generation()||!Arrays.equals(sample.policyDigest(),generation.policy().digest()))
            throw new IllegalArgumentException("observed Final changed frozen sample generation");
        final var prior=sample.observation().finalObservation();
        final long revision=prior==null?1:TargetQuotaMutation.increment(prior.observationRevision());
        final var observation=evidence.observation(sample,revision);
        return sign(generation,new TargetSloObservationEventBody(sample.scope(),sample.lineage(),retryUntil,sample.generation(),
                sample.observation().sampleId(),sample.bindingDigest(),observation,evidence));
    }
    private Prepared sign(TargetSloObservationGeneration generation,TargetSloObservationEventBody body){
        final var retryUntil=body.retryUntil();
        final var image=SystemMutation.signed(body.scope().shard(),SystemMutationType.TARGET_SLO_OBSERVATION,retryUntil,
                body.logicalIdentity(),body.canonicalBytes(),generation.policy().service().canonicalBytes(),generation.policy().keyVersion(),key);
        if(image.encodeFrame().length>maximumFrameBytes||image.canonicalEnvelope().length>TargetSloObservationEventRecord.MAX_INPUT_BYTES)
            throw new IllegalArgumentException("SLO event exceeds original Source frame budget");
        return Prepared.restore(image);
    }
    public static final class Prepared {
        private final SystemMutation mutation;
        private Prepared(SystemMutation mutation){this.mutation=mutation;}
        public SystemMutation mutation(){return mutation;}
        /** Retained exact bytes survive uncertain append; no new event ID, signature or allocated Source position. */
        public static Prepared restore(SystemMutation image){
            Objects.requireNonNull(image);final var body=TargetSloObservationEventBody.decode(image.canonicalBody());
            if(image.type()!=SystemMutationType.TARGET_SLO_OBSERVATION||!image.shardId().equals(body.scope().shard())
                    ||image.retryUntilEpochMs()!=body.retryUntil()||!Arrays.equals(image.logicalOperationIdentity(),body.logicalIdentity())
                    ||AuthorIdentity.decode(image.authorIdentity()).kind()!=AuthorIdentity.Kind.SERVICE
                    ||image.canonicalEnvelope().length>TargetSloObservationEventRecord.MAX_INPUT_BYTES)
                throw new IllegalArgumentException("recovered SLO event changed exact original image");
            return new Prepared(image);
        }
    }
}
