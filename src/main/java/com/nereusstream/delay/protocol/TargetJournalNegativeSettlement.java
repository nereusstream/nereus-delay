package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Closed Journal settlement binding. Parsing it never authenticates Source history or permits a writer. */
public final class TargetJournalNegativeSettlement {
    public static final int VERSION = 1;
    public static final int MAX_FRAME_BYTES = 2 * TargetPublishAdmissionBody.MAX_SUPPORTED_BYTES
            + TargetResultRecord.MAX_AUTHOR_BYTES + 8192;
    public static final int MAX_CANONICAL_BYTES = 2 * MAX_FRAME_BYTES
            + 2 * TargetSourcePosition.MAX_CANONICAL_BYTES + 2 * TargetResultRecord.MAX_CANONICAL_BYTES
            + MAX_FRAME_BYTES + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-journal-negative-settlement\0");
    private final byte[] mappingId;
    private final SystemMutation admission;
    private final SourcePosition admissionSource;
    private final TargetResultRecord admissionFirst;
    private final SystemMutation outcome;
    private final SourcePosition outcomeSource;
    private final TargetResultRecord outcomeFirst;
    private final PulsarRecordTemplate template;
    private final byte[] digest;

    public TargetJournalNegativeSettlement(byte[] mappingId, SystemMutation admission, SourcePosition admissionSource,
            TargetResultRecord admissionFirst, SystemMutation outcome, SourcePosition outcomeSource,
            TargetResultRecord outcomeFirst, PulsarRecordTemplate template) {
        Bytes.requireLength(mappingId, 32, "mappingId");
        this.mappingId = Bytes.copy(mappingId);
        this.admission = Objects.requireNonNull(admission, "admission");
        this.admissionSource = TargetSourcePosition.requireBounded(admissionSource);
        this.admissionFirst = Objects.requireNonNull(admissionFirst, "admissionFirst");
        this.outcome = Objects.requireNonNull(outcome, "outcome");
        this.outcomeSource = TargetSourcePosition.requireBounded(outcomeSource);
        this.outcomeFirst = Objects.requireNonNull(outcomeFirst, "outcomeFirst");
        this.template = Objects.requireNonNull(template, "template");
        if (template.canonicalBytes().length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("negative settlement template exceeds its byte bound");
        }
        if (admission.encodeFrame().length > MAX_FRAME_BYTES || outcome.encodeFrame().length > MAX_FRAME_BYTES
                || admissionFirst.canonicalBytes().length > TargetResultRecord.MAX_CANONICAL_BYTES
                || outcomeFirst.canonicalBytes().length > TargetResultRecord.MAX_CANONICAL_BYTES
                || admission.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || outcome.type() != SystemMutationType.PUBLISH_OUTCOME
                        && outcome.type() != SystemMutationType.EVIDENCE_RESOLUTION
                || !admission.shardId().equals(outcome.shardId())
                || !admission.shardId().equals(admissionSource.shardId())
                || !outcome.shardId().equals(outcomeSource.shardId())
                || !admissionSource.sameSourceIdentity(outcomeSource)
                || admissionSource.compareTo(outcomeSource) >= 0) {
            throw new IllegalArgumentException("negative settlement changes its bounded Source images/identity/order");
        }
        final var admitted = TargetPublishAdmissionBody.decode(admission.canonicalBody());
        final var body = outcomeBody();
        if (!admitted.materialized() || body.sideEffect() != 2
                || !Arrays.equals(admitted.publishAttemptId(), body.publishAttemptId())
                || !Arrays.equals(body.transfer(), admitted.outcomeTransfer())
                || !Arrays.equals(admissionFirst.recoveryLineage(), outcomeFirst.recoveryLineage())) {
            throw new IllegalArgumentException("negative settlement changes its frozen Admission/Outcome/lineage");
        }
        if (SystemMutationResult.decode(admissionFirst.typedPayload()).stableCode() != StableCode.OK
                || !admissionFirst.primaryIdentity().equals(outcomeFirst.primaryIdentity())) {
            throw new IllegalArgumentException("negative settlement changes its accepted Admission/root owner");
        }
        requireFirst(admission, admissionSource, admissionFirst);
        requireFirst(outcome, outcomeSource, outcomeFirst);
        final var evidence = PublishEvidence.decode(body.evidence());
        if (admitted.nativePublication() != null) {
            evidence.requireNativeTargetNotPublishedBinding(admitted.nativePublication());
        } else { evidence.requireOrdinaryTargetNotPublishedBinding(admitted.publication()); }
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("negative settlement exceeds its canonical bound");
        }
    }

    private static void requireFirst(SystemMutation image, SourcePosition source, TargetResultRecord first) {
        final var result = SystemMutationResult.decode(first.typedPayload());
        if (first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
                || result.applyStatus() != ApplyStatus.APPLIED || result.mutationType() != image.type()
                || !Arrays.equals(result.mutationId(), image.systemMutationId())
                || !Arrays.equals(result.mutationHash(), image.mutationHash())
                || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                || result.retryUntilEpochMs() != image.retryUntilEpochMs()
                || !Arrays.equals(result.appliedSourcePosition(), source.canonicalBytes())
                || !Arrays.equals(first.mutation().source().canonicalBytes(), source.canonicalBytes())
                || !Arrays.equals(first.mutation().mutationDigest(), Bytes.sha256(image.canonicalEnvelope()))) {
            throw new IllegalArgumentException("negative settlement has a conflicting APPLIED Source first result");
        }
    }

    public byte[] mappingId() { return Bytes.copy(mappingId); }
    public SystemMutation admission() { return admission; }
    public SourcePosition admissionSource() { return admissionSource; }
    public TargetResultRecord admissionFirst() { return admissionFirst; }
    public SystemMutation outcome() { return outcome; }
    public SourcePosition outcomeSource() { return outcomeSource; }
    public TargetResultRecord outcomeFirst() { return outcomeFirst; }
    public PulsarRecordTemplate template() { return template; }
    public byte[] digest() { return Bytes.copy(digest); }
    public PublishOutcomeBody outcomeBody() {
        return outcome.type() == SystemMutationType.EVIDENCE_RESOLUTION
                ? PublishOutcomeBody.decodeEvidenceResolution(outcome.canonicalBody())
                : PublishOutcomeBody.decode(outcome.canonicalBody());
    }

    /** Full immutable correlation only; external Source/history and writer authority remain mandatory. */
    public void requireMapping(com.nereusstream.delay.adapter.PulsarAttemptJournal.Mapping mapping) {
            final var admission = TargetPublishAdmissionBody.decode(admission().canonicalBody());
            final var nativeBinding = admission.nativePublication();
            final var ordinary = admission.publication();
            final var physical = nativeBinding != null ? nativeBinding.physical() : ordinary.physical();
            final var expectedTemplate = nativeBinding != null
                    ? com.nereusstream.delay.adapter.PulsarPreparedRecordFactory.targetTemplate(nativeBinding, template().payload())
                    : com.nereusstream.delay.adapter.PulsarPreparedRecordFactory.targetTemplate(ordinary, template().payload());
            if (!mapping.producer().isTarget() || !mapping.isCurrentGeneration()
                    || !Arrays.equals(mapping.mappingId(), mappingId())
                    || !Arrays.equals(mapping.producer().targetChannel().canonicalBytes(), admission.channel().canonicalBytes())
                    || !mapping.producer().equals(com.nereusstream.delay.adapter.PulsarAttemptJournal.ProducerKey.target(admission.channel(), physical))
                    || !Arrays.equals(mapping.publishAttemptId(), admission.publishAttemptId())
                    || !mapping.delayMessageId().equals(admission.locator().messageId())
                    || mapping.generation() != admission.locator().generation()
                    || !Arrays.equals(mapping.sourcePosition(), admissionSource().canonicalBytes())
                    || mapping.deliveryContract() != expectedTemplate.deliveryContract()
                    || !expectedTemplate.equals(template())
                    || !Arrays.equals(mapping.recordTemplateHash(), expectedTemplate.recordTemplateHash())
                    || !Arrays.equals(mapping.preparedPublishHash(), nativeBinding != null
                            ? nativeBinding.preparedPublishHash() : ordinary.preparedPublishHash())
                    || !Arrays.equals(mapping.artifactGenerationSetDigest(), template().artifactGenerationSetDigest())) {
                throw new IllegalArgumentException("negative settlement changes its exact Journal mapping/template/Admission");
            }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, mappingId);
            CanonicalProtobuf.bytes(out, 3, admission.encodeFrame());
            CanonicalProtobuf.bytes(out, 4, admissionSource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, admissionFirst.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, outcome.encodeFrame());
            CanonicalProtobuf.bytes(out, 7, outcomeSource.canonicalBytes());
            CanonicalProtobuf.bytes(out, 8, outcomeFirst.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, template.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 10, digest); });
    }

    public static TargetJournalNegativeSettlement decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 10, false, "Journal negative settlement");
        QueryCodecSupport.requireNumbers(fields, new int[] {1,2,3,4,5,6,7,8,9,10}, "Journal negative settlement");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Journal negative settlement version");
        }
        final byte[] admission = QueryCodecSupport.bytes(fields.get(2), 3);
        final byte[] outcome = QueryCodecSupport.bytes(fields.get(5), 6);
        if (admission.length > MAX_FRAME_BYTES || outcome.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("negative settlement Source frame bound exceeded");
        }
        final var result = new TargetJournalNegativeSettlement(QueryCodecSupport.fixed(fields.get(1), 2, 32),
                SystemMutation.decodeFrame(admission), TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(fields.get(4), 5)), SystemMutation.decodeFrame(outcome),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(6), 7)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(fields.get(7), 8)),
                PulsarRecordTemplate.decode(QueryCodecSupport.bytes(fields.get(8), 9)));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(9), 10, 32))) {
            throw new IllegalArgumentException("negative settlement digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Journal negative settlement");
        return result;
    }
}
