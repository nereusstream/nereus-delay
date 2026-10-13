package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptProtection;
import java.util.Arrays;
import java.util.Objects;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;

/** Dedicated receipt scope in the existing registered CONTROL/key trust domain; no new root or Worker issuer. */
public final class TargetKafkaReceiptProtectionTrust {
    @FunctionalInterface
    public interface HistoricalActor {
        ControlAuthorizationContext resolve(PreparedControlOperation operation, SourcePosition originalAuthorization);
    }
    @FunctionalInterface
    public interface ReceiptScope {
        void requireCovered(TargetKafkaReceiptProtection protection, PreparedControlOperation operation,
                ControlAuthorizationContext actor, SourcePosition originalAuthorization);
        default void requireManagementCovered(com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage image,
                PreparedControlOperation operation, ControlAuthorizationContext actor, SourcePosition at) {
            throw new IllegalStateException("whole-topic deployment CONTROL scope is not authenticated");
        }
    }
    /** Actual immutable capability prerequisite and uninterrupted management/retirement protection. */
    @FunctionalInterface
    public interface Backing {
        void requireCurrent(TargetKafkaReceiptProtection protection, PreparedControlOperation originalControl,
                SourcePosition originalAuthorization);
    }
    private final ControlTargetRegistrationAuthority registrations;
    private final TargetChannelControlVerifier.KeyAuthority keys;
    private final HistoricalActor actors;
    private final ReceiptScope scope;
    private final Backing backing;

    public TargetKafkaReceiptProtectionTrust(ControlTargetRegistrationAuthority registrations,
            TargetChannelControlVerifier.KeyAuthority keys, HistoricalActor actors, ReceiptScope scope, Backing backing) {
        this.registrations = Objects.requireNonNull(registrations, "original CONTROL registrations");
        this.keys = Objects.requireNonNull(keys, "original historical CONTROL keys");
        this.actors = Objects.requireNonNull(actors, "original authenticated actor history");
        this.scope = Objects.requireNonNull(scope, "explicit receipt scope");
        this.backing = Objects.requireNonNull(backing, "actual management/capability backing");
    }

    /** Can run before a management action; authorization covers this receipt, rather than just its business Target. */
    public void requirePublisher(TargetKafkaReceiptProtection image, PreparedControlOperation prepared, SourcePosition at) {
        final var actor = Objects.requireNonNull(actors.resolve(prepared, at), "original authenticated CONTROL actor");
        final var registered = registrations.find(prepared.operationId()).orElseThrow(
                () -> new IllegalStateException("receipt issuer has no original registered CONTROL operation"));
        if (!Arrays.equals(registered.canonicalBytes(), prepared.canonicalBytes())
                || !actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR)) {
            throw new IllegalArgumentException("receipt protection issuer is not the registered PLATFORM_OPERATOR");
        }
        ControlOperationAuthorization.authorize(prepared, actor, ignored -> true);
        final var key = keys.resolve(prepared.signingKeyVersion(), at);
        if (key == null || !prepared.verifySignature(key)) {
            throw new IllegalArgumentException("original receipt CONTROL key/signature is unauthorized");
        }
        scope.requireCovered(image, prepared, actor, at);
    }

    public void requireTrusted(TargetKafkaReceiptControlArtifact artifact,
            TargetKafkaReceiptControlArtifact protection) {
        if (protection.kind() != TargetKafkaReceiptControlArtifact.PROTECTION) {
            throw new IllegalArgumentException("receipt trust requires its original protection artifact");
        }
        requireArtifactHistory(artifact, protection);
        backing.requireCurrent(protection.protection(), protection.control(), protection.authorizedAt());
    }

    /** Original scope/signatures remain checkable on retained BROKEN evidence; this grants no current SEND/absence right. */
    public void requireArtifactHistory(TargetKafkaReceiptControlArtifact artifact, TargetKafkaReceiptControlArtifact protection) {
        requirePublisher(protection.protection(), protection.control(), protection.authorizedAt());
        final var management = TargetKafkaReceiptControlArtifact.decode(protection.protection().managementAuthorityImage());
        requireManagementHistory(management);
        final var deployed = com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.decode(management.image());
        final var retained = protection.protection();
        if (!deployed.topic().kafka().authenticatedClusterId().equals(retained.resource().authenticatedClusterId())
                || !deployed.topic().kafka().nativeTopicUuid().equals(retained.resource().receiptTopicUuid())
                || !deployed.physicalTopic().equals(retained.physicalTopic())
                || !deployed.capability().ref().equals(retained.capability())
                || !Arrays.equals(deployed.controlScopeRef(), retained.controlScopeRef())
                || !Arrays.equals(deployed.managementEpoch(), retained.managementEpoch())) {
            throw new IllegalArgumentException("retained protection changes its original topic-management scope/epoch");
        }
        final var key = keys.resolve(artifact.control().signingKeyVersion(), artifact.authorizedAt());
        if (key == null || !artifact.verifySignature(key) || !protection.verifySignature(key)
                || !Arrays.equals(artifact.control().canonicalBytes(), protection.control().canonicalBytes())
                || !Arrays.equals(artifact.authorizedAt().canonicalBytes(), protection.authorizedAt().canonicalBytes())) {
            throw new IllegalArgumentException("receipt artifact changes its original authorized CONTROL signer/use");
        }
    }

    /** Key-possession preflight before any policy mutation; a known historical prepared image alone is insufficient. */
    public void requireIssuerSignature(TargetKafkaReceiptControlArtifact artifact) {
        final var key = keys.resolve(artifact.control().signingKeyVersion(), artifact.authorizedAt());
        if (key == null || !artifact.verifySignature(key)) {
            throw new IllegalArgumentException("receipt operator does not possess its authorized original CONTROL signing key");
        }
    }

    public void requireManagementIssuer(TargetKafkaReceiptControlArtifact artifact, ControlAuthorizationContext currentActor) {
        if (artifact.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT
                || !Objects.requireNonNull(currentActor, "current authenticated operator").roleSet().contains(ControlRole.PLATFORM_OPERATOR)) {
            throw new IllegalArgumentException("topic management requires its current authenticated PLATFORM_OPERATOR");
        }
        final var registered = registrations.find(artifact.control().operationId()).orElseThrow();
        if (!Arrays.equals(registered.canonicalBytes(), artifact.control().canonicalBytes())) {
            throw new IllegalArgumentException("management issuer lacks its exact original registered CONTROL operation");
        }
        ControlOperationAuthorization.authorize(artifact.control(), currentActor, ignored -> true);
        requireIssuerSignature(artifact);
        scope.requireManagementCovered(com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.decode(artifact.image()),
                artifact.control(), currentActor, artifact.authorizedAt());
    }

    public void requireManagementHistory(TargetKafkaReceiptControlArtifact artifact) {
        final var originalActor = Objects.requireNonNull(actors.resolve(artifact.control(), artifact.authorizedAt()), "original deployment author");
        requireManagementIssuer(artifact, originalActor);
    }

    public static byte[] signFailure(TargetKafkaReceiptControlArtifact original, byte[] evidence, PrivateKey key) {
        try {
            final var signer = Signature.getInstance("Ed25519");
            signer.initSign(key); signer.update(failureInput(original, evidence)); return signer.sign();
        } catch (GeneralSecurityException failure) { throw new IllegalArgumentException("cannot sign receipt management failure", failure); }
    }

    public void requireFailure(TargetKafkaReceiptControlArtifact original, byte[] evidence, byte[] signature) {
        if (original.kind() == TargetKafkaReceiptControlArtifact.MANAGEMENT) {
            requireManagementHistory(original);
        } else {
            requirePublisher(original.protection(), original.control(), original.authorizedAt());
        }
        com.nereusstream.delay.protocol.Bytes.requireLength(signature, 64, "CONTROL failure signature");
        try {
            final var verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(Objects.requireNonNull(keys.resolve(original.control().signingKeyVersion(), original.authorizedAt()),
                    "original CONTROL failure key"));
            verifier.update(failureInput(original, evidence));
            if (!verifier.verify(signature)) { throw new IllegalArgumentException("receipt failure lacks its authorized CONTROL signature"); }
        } catch (GeneralSecurityException failure) { throw new IllegalArgumentException("cannot verify receipt management failure", failure); }
    }

    private static byte[] failureInput(TargetKafkaReceiptControlArtifact original, byte[] evidence) {
        com.nereusstream.delay.protocol.Bytes.requireLength(evidence, 32, "management failure digest");
        return com.nereusstream.delay.protocol.Bytes.concat(
                com.nereusstream.delay.protocol.Bytes.utf8("nereus-delay-target-kafka-receipt-protection-broken\0"),
                original.digest(), evidence, com.nereusstream.delay.protocol.Bytes.sha256(original.control().canonicalBytes()));
    }
}
