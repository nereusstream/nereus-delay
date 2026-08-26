package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;

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
    ProfileSemanticEnvelope resolve(ProfileRef reference);

    CredentialBinding resolveBinding(ProfileRef profile, long secretGeneration);

    CredentialBindingHead resolveHead(ProfileRef profile);

    CredentialBindingProtection resolveProtection(ProfileRef profile, long secretGeneration);
}
