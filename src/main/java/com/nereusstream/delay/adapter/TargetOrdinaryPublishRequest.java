package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import java.util.Arrays;
import java.util.Objects;

/** Actual-Admission-derived ordinary SDK input; it carries no synthetic Lane and no physical ownership permission. */
public final class TargetOrdinaryPublishRequest {
    private final TargetPublishAdmissionStore.Applied admission;
    private final PayloadForPublish payload;
    private final ResolvedPayload resolved;
    private final ArtifactGenerationSet artifacts;

    private TargetOrdinaryPublishRequest(
            TargetPublishAdmissionStore.Applied admission,
            PayloadForPublish payload,
            ResolvedPayload resolved,
            ArtifactGenerationSet artifacts) {
        this.admission = Objects.requireNonNull(admission, "actual applied Admission");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.resolved = Objects.requireNonNull(resolved, "resolved");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        final var publication = Objects.requireNonNull(admission.body().publication(), "ordinary v4 publication");
        final var message = admission.message();
        publication.requirePayload(resolved.bytes());
        if (message.runtime().currentWorkKind() != CurrentSendWorkKind.PUBLISHING
                || !publication.locator().equals(message.locator())
                || message.stateVersion() != TargetQueueState.nextRevision(publication.claimedMessageVersion())
                || !Arrays.equals(message.runtime().publishAttemptId(), publication.publishAttemptId())
                || payload.length() != publication.payloadLength()
                || !Arrays.equals(payload.payloadSha256(), publication.payloadSha256())
                || !Arrays.equals(artifacts.setDigest(), publication.artifactGenerationSetDigest())) {
            throw new IllegalArgumentException("ordinary SDK input changes its newly applied Message/payload/Artifact");
        }
        if ((admission.body().kafkaReceipt() == null) != (admission.kafkaMapping() == null)
                || (publication.channel().context().kind()
                                == com.nereusstream.delay.protocol.ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                        && admission.kafkaMapping() == null)) {
            throw new IllegalArgumentException("ordinary K2 SDK input lacks its actual Source mapping");
        }
        if (admission.kafkaMapping() != null) {
            admission.kafkaMapping().requireOriginal(admission.image(), admission.source());
        }
    }

    public static TargetOrdinaryPublishRequest fromApplied(
            TargetPublishAdmissionStore.Applied admission,
            PayloadForPublish payload,
            ResolvedPayload resolved,
            ArtifactGenerationSet artifacts) {
        return new TargetOrdinaryPublishRequest(admission, payload, resolved, artifacts);
    }

    public TargetPublishAdmissionStore.Applied applied() {
        return admission;
    }

    public TargetOrdinaryPublicationBinding publication() {
        return admission.body().publication();
    }

    public SourcePosition source() {
        return admission.source();
    }

    public PayloadForPublish payload() {
        return payload;
    }

    public ResolvedPayload resolved() {
        return resolved;
    }

    public ArtifactGenerationSet artifacts() {
        return artifacts;
    }

    public long physicalByteCharge() {
        final long ordinary = Math.addExact(
                Math.addExact(publication().canonicalBytes().length, payload.canonicalBytes().length),
                Math.addExact(resolved.length(), source().canonicalBytes().length));
        final var mapping = admission.kafkaMapping();
        return mapping == null
                ? ordinary
                : Math.addExact(
                        ordinary,
                        Math.addExact(
                                mapping.canonicalBytes().length,
                                Math.addExact(
                                        com.nereusstream.delay.protocol.TargetKafkaReceiptWire.key(mapping).length,
                                        com.nereusstream.delay.protocol.TargetKafkaReceiptWire.value(mapping).length)));
    }
}
