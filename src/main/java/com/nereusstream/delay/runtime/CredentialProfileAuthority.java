package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CredentialUseKind;
import com.nereusstream.delay.protocol.CredentialUseLease;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

/** Profile catalog plus the bounded lease-issuance operation used at activation. */
public interface CredentialProfileAuthority extends ProfileCatalog {
    CredentialUseLease issueCredentialUseLease(
            ProfileRef profile,
            CredentialUseKind kind,
            byte[] holderScopeDigest,
            long expectedSecretGeneration,
            byte[] expectedBindingDigest,
            byte[] resolvedCredentialFingerprintDigest,
            TrustedUtcIntervalEvidence issuedAt,
            long validUntilEpochMs,
            long expectedHeadRevision);
}
