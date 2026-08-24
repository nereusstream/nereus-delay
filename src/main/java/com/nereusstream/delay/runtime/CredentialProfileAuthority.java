package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CredentialUseKindV1;
import com.nereusstream.delay.protocol.CredentialUseLeaseV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

/** Profile catalog plus the bounded lease-issuance operation used at activation. */
public interface CredentialProfileAuthority extends ProfileCatalog {
    CredentialUseLeaseV1 issueCredentialUseLease(
            ProfileRefV1 profile,
            CredentialUseKindV1 kind,
            byte[] holderScopeDigest,
            long expectedSecretGeneration,
            byte[] expectedBindingDigest,
            byte[] resolvedCredentialFingerprintDigest,
            TrustedUtcIntervalEvidence issuedAt,
            long validUntilEpochMs,
            long expectedHeadRevision);
}
