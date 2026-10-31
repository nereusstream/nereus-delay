package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.SloSampleFinal;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetSloCustodyReceipt;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloFinalEvidence;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloWriterContract;
import com.nereusstream.delay.runtime.TargetSloWriterEpochStore.Applied;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;

/** Freezes one original SERVICE observation for the existing bounded System Source handoff. */
public final class TargetSloObservationEventMutationFactory {
    private final PrivateKey key;
    private final long maximumFrameBytes;

    public TargetSloObservationEventMutationFactory(PrivateKey key, long maximumFrameBytes) {
        this.key = Objects.requireNonNull(key);
        if (maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("SLO event needs a finite Source frame bound");
        }
        this.maximumFrameBytes = maximumFrameBytes;
    }
    /** The Source verifier independently checks original endpoint/time/history and retained resource authority. */
    public Prepared prepare(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            SloSampleFinal observation,
            long retryUntil) {
        Objects.requireNonNull(generation);
        Objects.requireNonNull(sample);
        Objects.requireNonNull(observation);
        requireLegacy(generation);
        requireGeneration(generation, sample);
        observation.validateAgainst(sample.observation().start());
        return sign(
                generation,
                new TargetSloObservationEventBody(
                        sample.scope(),
                        sample.lineage(),
                        retryUntil,
                        sample.generation(),
                        sample.observation().sampleId(),
                        sample.bindingDigest(),
                        observation));
    }

    public Prepared prepareObserved(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            TargetSloFinalEvidence evidence,
            long retryUntil) {
        Objects.requireNonNull(generation);
        Objects.requireNonNull(sample);
        Objects.requireNonNull(evidence);
        requireLegacy(generation);
        requireGeneration(generation, sample);
        evidence.requireSample(sample);
        return sign(
                generation,
                new TargetSloObservationEventBody(
                        sample.scope(),
                        sample.lineage(),
                        retryUntil,
                        sample.generation(),
                        sample.observation().sampleId(),
                        sample.bindingDigest(),
                        null,
                        evidence));
    }

    public Prepared prepareCustody(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            TargetSloCustodyReceipt receipt,
            long retryUntil) {
        requireLegacy(Objects.requireNonNull(generation));
        requireGeneration(generation, Objects.requireNonNull(sample));
        Objects.requireNonNull(receipt)
                .requireSample(Objects.requireNonNull(generation), Objects.requireNonNull(sample));
        return sign(generation, TargetSloObservationEventBody.custody(sample, receipt, retryUntil));
    }

    public Prepared prepareObserved(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            TargetSloFinalEvidence evidence,
            long retryUntil,
            Applied writer) {
        requireGeneration(Objects.requireNonNull(generation), Objects.requireNonNull(sample));
        Objects.requireNonNull(evidence).requireSample(sample);
        Objects.requireNonNull(writer);
        return sign(
                writer,
                new TargetSloObservationEventBody(
                        sample.scope(),
                        sample.lineage(),
                        retryUntil,
                        sample.generation(),
                        sample.observation().sampleId(),
                        sample.bindingDigest(),
                        null,
                        evidence,
                        null,
                        null,
                        writer.reference()));
    }

    public Prepared prepareCustody(
            TargetSloObservationGeneration generation,
            TargetSloObservationSample sample,
            TargetSloCustodyReceipt receipt,
            long retryUntil,
            Applied writer) {
        requireGeneration(Objects.requireNonNull(generation), Objects.requireNonNull(sample));
        Objects.requireNonNull(receipt).requireSample(generation, sample);
        Objects.requireNonNull(writer);
        return sign(writer, TargetSloObservationEventBody.custody(sample, receipt, retryUntil, writer.reference()));
    }

    private static void requireLegacy(TargetSloObservationGeneration generation) {
        if (generation.policy().requiresWriterEpoch()) {
            throw new IllegalArgumentException("epoch-qualified SLO policy requires an actual registered writer epoch");
        }
    }

    private static void requireGeneration(
            TargetSloObservationGeneration generation, TargetSloObservationSample sample) {
        if (sample.generation() != generation.generation()
                || !sample.scope().equals(generation.policy().scope())
                || !Arrays.equals(sample.lineage(), generation.policy().lineage())
                || !Arrays.equals(sample.policyDigest(), generation.policy().digest())) {
            throw new IllegalArgumentException("SLO event changed original frozen sample generation");
        }
    }

    private Prepared sign(TargetSloObservationGeneration generation, TargetSloObservationEventBody body) {
        final var retryUntil = body.retryUntil();
        final var image = SystemMutation.signed(
                body.scope().shard(),
                SystemMutationType.TARGET_SLO_OBSERVATION,
                retryUntil,
                body.logicalIdentity(),
                body.canonicalBytes(),
                generation.policy().service().canonicalBytes(),
                generation.policy().keyVersion(),
                key);
        if (image.encodeFrame().length > maximumFrameBytes
                || image.canonicalEnvelope().length > maximumInputBytes(body)) {
            throw new IllegalArgumentException("SLO event exceeds original Source frame budget");
        }
        return Prepared.restore(image);
    }

    private Prepared sign(Applied writer, TargetSloObservationEventBody body) {
        writer.requireCurrent();
        final var contract = requireWriter(writer, body);
        final var image = SystemMutation.signed(
                body.scope().shard(),
                SystemMutationType.TARGET_SLO_OBSERVATION,
                body.retryUntil(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                contract.service().canonicalBytes(),
                contract.keyVersion(),
                key);
        if (image.encodeFrame().length > maximumFrameBytes
                || image.encodeFrame().length > contract.limits().maximumFrameBytes()
                || image.canonicalEnvelope().length > maximumInputBytes(body)) {
            throw new IllegalArgumentException("SLO event exceeds its actual registered writer frame budget");
        }
        writer.requireCurrent();
        return new Prepared(Prepared.restore(image).mutation, writer);
    }

    static TargetSloWriterContract requireWriter(Applied writer, TargetSloObservationEventBody body) {
        final var value = Objects.requireNonNull(writer).value();
        final var contract = value.contract();
        if (body.writerEpoch() == null
                || !body.scope().equals(contract.scope())
                || !Arrays.equals(body.lineage(), contract.lineage())
                || !Arrays.equals(
                        body.writerEpoch().canonicalBytes(), writer.reference().canonicalBytes())
                || !Arrays.equals(
                        value.reference().canonicalBytes(), writer.reference().canonicalBytes())
                || contract.epoch() != writer.reference().epoch()
                || !Arrays.equals(
                        contract.writerIdentityDigest(), writer.reference().writerIdentityDigest())
                || body.retryUntil() > contract.maximumRetryUntil()) {
            throw new IllegalArgumentException("SLO frame changed its actual registered writer contract");
        }
        if (body.isCustody()) {
            contract.requireSource(body.exportedSample().mutation().source());
        } else {
            contract.requireSource(body.evidence().inspectedAt().source());
        }
        return contract;
    }

    static void requirePreparedWriter(Prepared prepared, TargetSloObservationGeneration generation) {
        final var image = prepared.mutation();
        final var body = TargetSloObservationEventBody.decode(image.canonicalBody());
        final AuthorIdentity service;
        final int keyVersion;
        if (body.writerEpoch() == null) {
            requireLegacy(generation);
            if (prepared.writerEpoch() != null) {
                throw new IllegalStateException("legacy SLO frame cannot acquire writer epoch qualification");
            }
            service = generation.policy().service();
            keyVersion = generation.policy().keyVersion();
        } else {
            final var writer = Objects.requireNonNull(
                    prepared.writerEpoch(), "fresh SLO frame lost its original registered writer token");
            // Retained bytes keep their original minted registration even after it closes. New
            // signing and append admission have their own current checks; exact-first reconciliation
            // must remain possible without promoting these bytes to a later epoch.
            final var contract = requireWriter(writer, body);
            service = contract.service();
            keyVersion = contract.keyVersion();
            if (image.encodeFrame().length > contract.limits().maximumFrameBytes()) {
                throw new IllegalArgumentException("SLO frame exceeds its registered writer envelope");
            }
        }
        if (!Arrays.equals(image.authorIdentity(), service.canonicalBytes())
                || image.signingKeyVersion() != keyVersion) {
            throw new IllegalArgumentException("SLO Source frame changed its actual writer SERVICE/key");
        }
    }

    private static int maximumInputBytes(TargetSloObservationEventBody body) {
        return body.isCustody()
                ? body.writerEpoch() == null
                        ? TargetSloCustodyRecord.MAX_V1_INPUT_BYTES
                        : TargetSloCustodyRecord.MAX_INPUT_BYTES
                : body.writerEpoch() != null
                        ? TargetSloObservationEventRecord.MAX_INPUT_BYTES
                        : body.isSourceAssigned()
                                ? TargetSloObservationEventRecord.MAX_LEGACY_INPUT_BYTES
                                : TargetSloObservationEventRecord.MAX_V1_INPUT_BYTES;
    }

    public static final class Prepared {
        private final SystemMutation mutation;
        private final Applied writerEpoch;

        private Prepared(SystemMutation mutation) {
            this(mutation, null);
        }

        private Prepared(SystemMutation mutation, Applied writerEpoch) {
            this.mutation = mutation;
            this.writerEpoch = writerEpoch;
        }

        public SystemMutation mutation() {
            return mutation;
        }

        Applied writerEpoch() {
            return writerEpoch;
        }
        /** Retained exact bytes survive uncertain append; no new event ID, signature or allocated Source position. */
        public static Prepared restore(SystemMutation image) {
            Objects.requireNonNull(image);
            final var body = TargetSloObservationEventBody.decode(image.canonicalBody());
            if (image.type() != SystemMutationType.TARGET_SLO_OBSERVATION
                    || !image.shardId().equals(body.scope().shard())
                    || image.retryUntilEpochMs() != body.retryUntil()
                    || !Arrays.equals(image.logicalOperationIdentity(), body.logicalIdentity())
                    || AuthorIdentity.decode(image.authorIdentity()).kind() != AuthorIdentity.Kind.SERVICE
                    || image.canonicalEnvelope().length > maximumInputBytes(body)) {
                throw new IllegalArgumentException("recovered SLO event changed exact original image");
            }
            return new Prepared(image);
        }
    }
}
