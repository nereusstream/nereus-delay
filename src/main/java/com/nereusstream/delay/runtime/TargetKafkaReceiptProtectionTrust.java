package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Action;
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
    /** Original Root-owned registered history; exported keys remain public material, never new trust pins. */
    @FunctionalInterface
    public interface PublicHistory {
        byte[] export(TargetKafkaReceiptControlArtifact artifact, TargetKafkaReceiptControlArtifact protection);
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
    private final TargetReceiptCurrentManagement current;
    private final PublicHistory publicHistory;

    public TargetKafkaReceiptProtectionTrust(ControlTargetRegistrationAuthority registrations,
            TargetChannelControlVerifier.KeyAuthority keys, HistoricalActor actors, ReceiptScope scope, Backing backing) {
        this(registrations, keys, actors, scope, backing, null);
    }

    public TargetKafkaReceiptProtectionTrust(ControlTargetRegistrationAuthority registrations,
            TargetChannelControlVerifier.KeyAuthority keys, HistoricalActor actors, ReceiptScope scope, Backing backing,
            TargetReceiptCurrentManagement current) {
        this(registrations, keys, actors, scope, backing, current, null);
    }

    public TargetKafkaReceiptProtectionTrust(ControlTargetRegistrationAuthority registrations,
            TargetChannelControlVerifier.KeyAuthority keys, HistoricalActor actors, ReceiptScope scope, Backing backing,
            TargetReceiptCurrentManagement current, PublicHistory publicHistory) {
        this.current = current;
        this.publicHistory = publicHistory;
        this.registrations = Objects.requireNonNull(registrations, "original CONTROL registrations");
        this.keys = Objects.requireNonNull(keys, "original historical CONTROL keys");
        this.actors = Objects.requireNonNull(actors, "original authenticated actor history");
        this.scope = Objects.requireNonNull(scope, "explicit receipt scope");
        this.backing = Objects.requireNonNull(backing, "actual management/capability backing");
    }

    /** Authenticates original CONTROL history without substituting a current operator or management permission. */
    public byte[] historicalPublicInputs(TargetKafkaReceiptControlArtifact artifact,
            TargetKafkaReceiptControlArtifact protection) {
        requireArtifactHistory(artifact, protection);
        final byte[] inputs = Objects.requireNonNull(publicHistory, "original CONTROL public history is not configured")
                .export(artifact, protection);
        if (inputs == null || inputs.length == 0
                || inputs.length > com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES)
            throw new IllegalStateException("original CONTROL public history exceeds its finite copy bound");
        return com.nereusstream.delay.protocol.Bytes.copy(inputs);
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
        final var management = TargetKafkaReceiptControlArtifact.decode(protection.protection().managementAuthorityImage());
        try (var permission = currentManagement(management, Action.OBSERVE)) {
            permission.requireCurrent();
            backing.requireCurrent(protection.protection(), protection.control(), protection.authorizedAt());
            permission.requireCurrent();
        }
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

    /** Original evidence remains authenticatable even when no current operator/proof is available. */
    public void requireManagementHistory(TargetKafkaReceiptControlArtifact artifact) {
        if (artifact.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT)
            throw new IllegalArgumentException("management history requires the original MANAGEMENT artifact");
        final var prepared = artifact.control();
        final var actor = Objects.requireNonNull(actors.resolve(prepared, artifact.authorizedAt()), "original deployment author");
        final var registered = registrations.find(prepared.operationId()).orElseThrow();
        if (!Arrays.equals(registered.canonicalBytes(), prepared.canonicalBytes())
                || !actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR))
            throw new IllegalArgumentException("original management authorization is not registered PLATFORM_OPERATOR");
        ControlOperationAuthorization.authorize(prepared, actor, ignored -> true);
        requireIssuerSignature(artifact);
        scope.requireManagementCovered(com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.decode(artifact.image()),
                prepared, actor, artifact.authorizedAt());
    }

    /** A current action uses its own consistent authenticated policy view, never the historical author projection. */
    public TargetReceiptCurrentManagement.Permit currentManagement(TargetKafkaReceiptControlArtifact original, Action action) {
        requireManagementHistory(original);
        if (current == null) throw new TargetReceiptCurrentManagement.Denied("current whole-topic authority is not configured");
        return current.acquire(com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage.decode(original.image()), action);
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
