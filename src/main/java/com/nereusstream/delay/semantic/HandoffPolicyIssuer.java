package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.PrivateKey;
import java.util.Objects;

/** Issues bounded signed policy snapshots; it never mutates the current head. */
public final class HandoffPolicyIssuer {
    private final PrivateKey issuerKey;
    private final int issuerKeyGeneration;
    private final ArtifactGenerationSet artifacts;
    private final long maximumLeaseMs;

    public HandoffPolicyIssuer(
            final PrivateKey issuerKey,
            final int issuerKeyGeneration,
            final ArtifactGenerationSet artifacts,
            final long maximumLeaseMs) {
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
        if (issuerKeyGeneration <= 0 || maximumLeaseMs <= 0) {
            throw new IllegalArgumentException("invalid handoff policy issuer bounds");
        }
        this.issuerKeyGeneration = issuerKeyGeneration;
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.maximumLeaseMs = maximumLeaseMs;
    }

    public HandoffPolicySnapshot issue(
            final byte[] policyScopeDigest,
            final long generation,
            final HandoffPolicyMode mode,
            final long effectiveLeadMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final int allowedPathBits,
            final TrustedUtcIntervalEvidence issuedAt) {
        Objects.requireNonNull(issuedAt, "issuedAt");
        if (validUntilEpochMs <= validFromEpochMs || validUntilEpochMs - validFromEpochMs > maximumLeaseMs) {
            throw new IllegalArgumentException("handoff policy lease exceeds issuer bound");
        }
        return HandoffPolicySnapshot.create(
                policyScopeDigest,
                generation,
                mode,
                effectiveLeadMs,
                validFromEpochMs,
                validUntilEpochMs,
                allowedPathBits,
                issuedAt,
                issuerKeyGeneration,
                artifacts.setDigest(),
                issuerKey);
    }

    public HandoffPolicySnapshot issueDisabled(
            final byte[] policyScopeDigest,
            final long generation,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final TrustedUtcIntervalEvidence issuedAt) {
        return issue(
                policyScopeDigest,
                generation,
                HandoffPolicyMode.DISABLED,
                0,
                validFromEpochMs,
                validUntilEpochMs,
                0,
                issuedAt);
    }

    /** Issues and CAS-publishes one self-contained policy head. */
    public HandoffPolicyAuthority.Publication issueAndPublish(
            final HandoffPolicyAuthority authority,
            final byte[] policyScopeDigest,
            final long expectedOxiaVersion,
            final long generation,
            final HandoffPolicyMode mode,
            final long effectiveLeadMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final int allowedPathBits,
            final long effectiveDisabledAfterEpochMs,
            final TrustedUtcIntervalEvidence issuedAt) {
        Objects.requireNonNull(authority, "authority");
        final HandoffPolicySnapshot snapshot = issue(
                policyScopeDigest,
                generation,
                mode,
                effectiveLeadMs,
                validFromEpochMs,
                validUntilEpochMs,
                allowedPathBits,
                issuedAt);
        final HandoffPolicyHead head =
                new HandoffPolicyHead(policyScopeDigest, generation, mode, snapshot, effectiveDisabledAfterEpochMs);
        return authority.publish(policyScopeDigest, expectedOxiaVersion, head);
    }
}
