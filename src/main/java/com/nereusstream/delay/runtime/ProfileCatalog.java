package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CredentialBindingHeadV1;
import com.nereusstream.delay.protocol.CredentialBindingProtectionV1;
import com.nereusstream.delay.protocol.CredentialBindingV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;

/**
 * Exact lookup seam for immutable Profile semantics and private credential
 * generations.
 *
 * <p>Implementations must resolve only the exact Profile/reference bytes or
 * return {@code null}. This seam does not authorize a Profile at a Shard Log
 * position, authenticate a control actor, or replace Oxia CAS/Owner Lease
 * authority; source-ordered first-binding markers remain shard-local state.</p>
 */
public interface ProfileCatalog {
    ProfileSemanticEnvelopeV1 resolve(ProfileRefV1 reference);

    CredentialBindingV1 resolveBinding(ProfileRefV1 profile, long secretGeneration);

    CredentialBindingHeadV1 resolveHead(ProfileRefV1 profile);

    CredentialBindingProtectionV1 resolveProtection(ProfileRefV1 profile, long secretGeneration);
}
