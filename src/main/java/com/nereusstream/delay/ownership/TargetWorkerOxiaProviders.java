package com.nereusstream.delay.ownership;

import com.nereusstream.delay.route.OxiaRouteAuthoritySession;
import com.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.OxiaSyncPayloadProofTrustSetCatalog;
import com.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackend;
import com.nereusstream.delay.runtime.OxiaSyncRetryPolicyCatalog;
import com.nereusstream.delay.runtime.OxiaSyncSourceProfileCatalog;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend;
import java.security.PublicKey;
import java.text.Normalizer;
import java.util.List;
import java.util.Objects;

/** Actual session-bound registry composition; the trusted process root retains ownership of both original clients. */
public final class TargetWorkerOxiaProviders {
    public record Prefixes(String routes, String assignments, String capabilities, String operations,
            String registrations, String credentials, String sourceProfiles, String retries, String payloadTrusts) {
        public Prefixes {
            final var values = List.of(routes, assignments, capabilities, operations, registrations,
                    credentials, sourceProfiles, retries, payloadTrusts);
            for (var value : values) {
                if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
                        || value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 1024
                        || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
                    throw new IllegalArgumentException("Oxia provider prefix is not a bounded canonical namespace");
                }
            }
            for (int i = 0; i < values.size(); i++) for (int j = i + 1; j < values.size(); j++) {
                if (values.get(i).equals(values.get(j)) || values.get(i).startsWith(values.get(j) + '/')
                        || values.get(j).startsWith(values.get(i) + '/')) {
                    throw new IllegalArgumentException("distinct Oxia provider namespaces overlap");
                }
            }
        }
    }

    private final OxiaSyncOwnerLeaseBackend.ClientHandle session;
    private final OxiaSignedRouteSnapshotProvider routes;
    private final OxiaOwnerLeaseStore leases;
    private final OxiaSyncWorkerAssignmentBackend assignments;
    private final OxiaSyncProtocolCapabilityBackend capabilities;
    private final OxiaControlOperationAuthority operations;
    private final OxiaControlTargetRegistrationAuthority registrations;
    private final OxiaSyncProfileCatalogBackend credentials;
    private final OxiaSyncSourceProfileCatalog sourceProfiles;
    private final OxiaSyncRetryPolicyCatalog retries;
    private final OxiaSyncPayloadProofTrustSetCatalog payloadTrusts;

    /**
     * Continues the actual configured clients. They must outlive Source/checkpoint tasks and final Owner drain;
     * this provider set does not independently close a shared client or claim native termination.
     */
    public static TargetWorkerOxiaProviders fromSessions(OxiaSyncOwnerLeaseBackend.ClientHandle ownerSession,
            OxiaRouteAuthoritySession routeSession, Prefixes prefixes, PublicKey originalRouteKey,
            TrustedClock time, CredentialAttestationTrustSet credentialTrust, long maximumCredentialLeaseMs,
            long maximumAttestationAgeMs, int maximumTrustSetBytes) {
        Objects.requireNonNull(ownerSession, "actual owned Oxia session").backend().requireConfiguredRequestTimeout();
        Objects.requireNonNull(routeSession, "actual Route session").requireConfiguredRequestTimeout();
        ownerSession.backend().requireOriginalConnectedClient(ownerSession.client());
        routeSession.sessionIdentity();
        if (maximumCredentialLeaseMs < 1 || maximumCredentialLeaseMs == Long.MAX_VALUE
                || maximumAttestationAgeMs < 1 || maximumAttestationAgeMs == Long.MAX_VALUE
                || maximumTrustSetBytes < 1 || maximumTrustSetBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Oxia provider values require explicit finite bounds");
        }
        return new TargetWorkerOxiaProviders(ownerSession, routeSession, Objects.requireNonNull(prefixes),
                Objects.requireNonNull(originalRouteKey), Objects.requireNonNull(time), Objects.requireNonNull(credentialTrust),
                maximumCredentialLeaseMs, maximumAttestationAgeMs, maximumTrustSetBytes);
    }

    public static TargetWorkerOxiaProviders fromSessions(OxiaSyncOwnerLeaseBackend.ClientHandle owner,
            OxiaRouteAuthoritySession route, Prefixes prefixes, PublicKey key, TrustedClock time,
            CredentialAttestationTrustSet trust, long leaseMs, long attestationMs, int trustBytes,
            java.util.concurrent.ThreadPoolExecutor executor,
            OxiaSignedRouteSnapshotProvider.RefreshLimits routeLimits, java.util.function.LongSupplier nanos) {
        owner.backend().requireConfiguredRequestTimeout(); route.requireConfiguredRequestTimeout();
        owner.backend().requireOriginalConnectedClient(owner.client()); route.sessionIdentity();
        return new TargetWorkerOxiaProviders(owner, route, prefixes, key, time, trust, leaseMs, attestationMs,
                trustBytes, executor, routeLimits, nanos);
    }

    private TargetWorkerOxiaProviders(OxiaSyncOwnerLeaseBackend.ClientHandle session,
            OxiaRouteAuthoritySession routeSession, Prefixes prefixes, PublicKey routeKey, TrustedClock time,
            CredentialAttestationTrustSet trust, long leaseMs, long attestationMs, int trustBytes) {
        this(session, routeSession, prefixes, routeKey, time, trust, leaseMs, attestationMs, trustBytes, null, null, null);
    }

    private TargetWorkerOxiaProviders(OxiaSyncOwnerLeaseBackend.ClientHandle session,
            OxiaRouteAuthoritySession routeSession, Prefixes prefixes, PublicKey routeKey, TrustedClock time,
            CredentialAttestationTrustSet trust, long leaseMs, long attestationMs, int trustBytes,
            java.util.concurrent.ThreadPoolExecutor executor, OxiaSignedRouteSnapshotProvider.RefreshLimits limits,
            java.util.function.LongSupplier nanos) {
        this.session = session;
        routes = executor == null ? new OxiaSignedRouteSnapshotProvider(routeSession, prefixes.routes(), routeKey, time)
                : new OxiaSignedRouteSnapshotProvider(routeSession, prefixes.routes(), routeKey, time, executor, limits, nanos);
        leases = new OxiaOwnerLeaseStore(session.backend());
        assignments = new OxiaSyncWorkerAssignmentBackend(session, prefixes.assignments());
        capabilities = new OxiaSyncProtocolCapabilityBackend(session, prefixes.capabilities());
        operations = new OxiaControlOperationAuthority(new OxiaSyncControlOperationBackend(session, prefixes.operations()));
        registrations = new OxiaControlTargetRegistrationAuthority(new OxiaSyncControlTargetRegistrationBackend(session, prefixes.registrations()));
        credentials = new OxiaSyncProfileCatalogBackend(session, prefixes.credentials(), leaseMs, attestationMs, trust);
        sourceProfiles = new OxiaSyncSourceProfileCatalog(session, prefixes.sourceProfiles());
        retries = new OxiaSyncRetryPolicyCatalog(session, prefixes.retries());
        payloadTrusts = new OxiaSyncPayloadProofTrustSetCatalog(session, prefixes.payloadTrusts(), trustBytes);
    }

    public OxiaSignedRouteSnapshotProvider routes() { return routes; }
    public OxiaOwnerLeaseStore leases() { return leases; }
    public OxiaSyncWorkerAssignmentBackend assignments() { return assignments; }
    public OxiaSyncProtocolCapabilityBackend capabilities() { return capabilities; }
    public OxiaControlOperationAuthority operations() { return operations; }
    public OxiaControlTargetRegistrationAuthority registrations() { return registrations; }
    public OxiaSyncProfileCatalogBackend credentials() { return credentials; }
    public OxiaSyncSourceProfileCatalog sourceProfiles() { return sourceProfiles; }
    public OxiaSyncRetryPolicyCatalog retries() { return retries; }
    public OxiaSyncPayloadProofTrustSetCatalog payloadTrusts() { return payloadTrusts; }
    public byte[] sessionIdentity() { return session.sessionIdentity(); }

    /** Original captured SDK association, available before any Catalog/session-validation RPC can fail. */
    public boolean hasManagedRegistryInputOwner(){return session.backend().hasManagedClientInputOwner();}
    public void retainClosedRegistryClientInputs(TargetDestinationSdkBudget.Client input){session.backend().retainClosedClientInputUntilTermination(input);}
    public void retainClosedRegistryClientInputs(TargetDestinationSdkBudget.Client input,TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission){
        session.backend().retainClosedClientInputUntilTermination(input,admission);
    }

    /** Uses the original owned connected session, which outlives the process receipt management dependency. */
    public com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog receiptProtectionCatalog(
            String prefix, com.nereusstream.delay.runtime.TargetKafkaReceiptProtectionTrust trust,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.Limits limits) {
        return receiptProtectionCatalog(prefix,trust,limits,null);
    }
    public com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog receiptProtectionCatalog(
            String prefix, com.nereusstream.delay.runtime.TargetKafkaReceiptProtectionTrust trust,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.Limits limits,
            TargetCatalogRootAuthority authority) {
        session.backend().requireOriginalConnectedClient(session.client());
        return new com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog(session, prefix, trust, limits,authority);
    }

    /** One per-Shard authority backs both intents and Catalog/Floor; do not construct a second catalog record. */
    public OxiaSyncCheckpointPublicationBackend checkpointPublication(String exactShardPrefix, CheckpointManifestLimits limits) {
        session.backend().requireOriginalConnectedClient(session.client());
        return new OxiaSyncCheckpointPublicationBackend(session, exactShardPrefix, Objects.requireNonNull(limits));
    }
}
