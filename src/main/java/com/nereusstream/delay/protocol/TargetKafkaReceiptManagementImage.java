package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed topic-wide deployment authorization in the original CONTROL root, shared across slots and Owners. */
public record TargetKafkaReceiptManagementImage(
        BrokerResourceIdentity topic,
        String physicalTopic,
        ProfileSemanticEnvelope capability,
        byte[] controlScopeRef,
        byte[] managementEpoch,
        List<String> managementPrincipals,
        List<String> workerPrincipals,
        byte[] deploymentRootDigest,
        byte[] configuredCredentialFingerprint,
        byte[] deploymentEvidence) {
    public static final int MAX_CANONICAL_BYTES = 8192;

    public TargetKafkaReceiptManagementImage {
        Objects.requireNonNull(topic, "exact receipt topic");
        Objects.requireNonNull(capability, "original capability");
        controlScopeRef = TargetCompatibilityCodec.assigned(controlScopeRef, 32, "management control scope");
        managementEpoch = TargetCompatibilityCodec.assigned(managementEpoch, 16, "persistent management epoch");
        deploymentRootDigest =
                TargetCompatibilityCodec.assigned(deploymentRootDigest, 32, "source-bound deployment root");
        configuredCredentialFingerprint = TargetCompatibilityCodec.assigned(
                configuredCredentialFingerprint, 32, "configured management credentials");
        managementPrincipals = principals(managementPrincipals);
        workerPrincipals = principals(workerPrincipals);
        deploymentEvidence = Bytes.copy(
                Objects.requireNonNull(deploymentEvidence, "actual deployment/permission isolation evidence"));
        if (topic.kind() != BrokerResourceIdentity.Kind.KAFKA
                || physicalTopic == null
                || physicalTopic.length() > 249
                || !physicalTopic.matches("[a-zA-Z0-9._-]+")
                || physicalTopic.equals(".")
                || physicalTopic.equals("..")
                || !java.util.Collections.disjoint(managementPrincipals, workerPrincipals)
                || capability.profileKind() != ProfileKind.DELIVERY_CAPABILITY
                || !(capability.body() instanceof DeliveryCapabilitySemantic c)
                || c.outcomeCapability() != OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT
                || !topic.equals(c.evidenceResource())
                || deploymentEvidence.length == 0
                || deploymentEvidence.length > 2048) {
            throw new IllegalArgumentException("invalid closed receipt deployment/root/capability image");
        }
    }

    @Override
    public byte[] controlScopeRef() {
        return Bytes.copy(controlScopeRef);
    }

    @Override
    public byte[] managementEpoch() {
        return Bytes.copy(managementEpoch);
    }

    @Override
    public byte[] deploymentRootDigest() {
        return Bytes.copy(deploymentRootDigest);
    }

    @Override
    public byte[] configuredCredentialFingerprint() {
        return Bytes.copy(configuredCredentialFingerprint);
    }

    @Override
    public byte[] deploymentEvidence() {
        return Bytes.copy(deploymentEvidence);
    }

    public byte[] topicIdentity() {
        return Bytes.sha256(Bytes.utf8("nereus-delay-target-kafka-receipt-managed-topic\0"), topic.canonicalBytes());
    }

    public byte[] canonicalBytes() {
        final var bytes = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, topic.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, Bytes.utf8(physicalTopic));
            CanonicalProtobuf.bytes(out, 4, capability.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, controlScopeRef);
            CanonicalProtobuf.bytes(out, 6, managementEpoch);
            for (var principal : managementPrincipals) {
                CanonicalProtobuf.bytes(out, 7, Bytes.utf8(principal));
            }
            for (var principal : workerPrincipals) {
                CanonicalProtobuf.bytes(out, 8, Bytes.utf8(principal));
            }
            CanonicalProtobuf.bytes(out, 9, deploymentRootDigest);
            CanonicalProtobuf.bytes(out, 10, configuredCredentialFingerprint);
            CanonicalProtobuf.bytes(out, 11, deploymentEvidence);
        });
        if (bytes.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("receipt management image exceeds bound");
        }
        return bytes;
    }

    public static TargetKafkaReceiptManagementImage decode(byte[] value) {
        final var f = TargetCompatibilityCodec.read(value, MAX_CANONICAL_BYTES, 73, true, "receipt management image");
        if (f.size() < 11 || QueryCodecSupport.uint(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("invalid management image schema");
        }
        final var managers = new java.util.ArrayList<String>();
        final var workers = new java.util.ArrayList<String>();
        int i = 6;
        while (i < f.size() && f.get(i).number() == 7) {
            managers.add(text(QueryCodecSupport.bytes(f.get(i++), 7)));
        }
        while (i < f.size() && f.get(i).number() == 8) {
            workers.add(text(QueryCodecSupport.bytes(f.get(i++), 8)));
        }
        if (i + 3 != f.size()) {
            throw new IllegalArgumentException("unknown management image fields");
        }
        final var image = new TargetKafkaReceiptManagementImage(
                BrokerResourceIdentity.decode(QueryCodecSupport.nested(f.get(1), 2)),
                text(QueryCodecSupport.bytes(f.get(2), 3)),
                ProfileSemanticEnvelope.decode(QueryCodecSupport.nested(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                QueryCodecSupport.fixed(f.get(5), 6, 16),
                managers,
                workers,
                QueryCodecSupport.fixed(f.get(i), 9, 32),
                QueryCodecSupport.fixed(f.get(i + 1), 10, 32),
                QueryCodecSupport.bytes(f.get(i + 2), 11));
        QueryCodecSupport.requireCanonical(value, image.canonicalBytes(), "receipt management image");
        return image;
    }

    private static List<String> principals(List<String> list) {
        Objects.requireNonNull(list, "authenticated exact principals");
        if (list.isEmpty() || list.size() > 32) {
            throw new IllegalArgumentException("receipt principal count exceeds bound");
        }
        final var sorted = new java.util.TreeSet<String>();
        for (var s : list) {
            if (s == null || !s.startsWith("User:") || s.equals("User:*") || s.length() > 128 || s.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("receipt principal is not an exact finite identity");
            }
            if (!sorted.add(s)) {
                throw new IllegalArgumentException("duplicate receipt principal");
            }
        }
        return List.copyOf(sorted);
    }

    private static String text(byte[] value) {
        final var result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(value, result.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid management UTF-8");
        }
        return result;
    }
}
