package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import java.util.Objects;

/** Preloaded exact durable objects authenticated outside the native Store view; no Broker or catalog I/O in apply. */
public record TargetKafkaReceiptExternalEvidence(
        TargetKafkaReceiptControlArtifact protection,
        String protectionKey,
        TargetKafkaReceiptControlArtifact anchor,
        String anchorKey) {
    public TargetKafkaReceiptExternalEvidence {
        Objects.requireNonNull(protection, "original protected artifact");
        Objects.requireNonNull(protectionKey, "complete object key");
        Objects.requireNonNull(anchor, "original anchor artifact");
        Objects.requireNonNull(anchorKey, "complete anchor key");
        if (protection.kind() != TargetKafkaReceiptControlArtifact.PROTECTION
                || anchor.kind() != TargetKafkaReceiptControlArtifact.ANCHOR) {
            throw new IllegalArgumentException("K2 external materialization requires both exact immutable objects");
        }
    }

    public void requireAdmission(TargetPublishAdmissionBody body) {
        anchor.requireAnchorBinding(protection, Objects.requireNonNull(body.kafkaReceipt(), "original v6 binding"));
        protection.protection().requireChannel(body.channel());
        if (!body.publication()
                .capabilityProfile()
                .equals(protection.protection().capability())) {
            throw new IllegalArgumentException("external objects change original materialized capability");
        }
    }
}
