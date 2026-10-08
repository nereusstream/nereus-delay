package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.runtime.ProfileCatalog;

/** Explicit empty catalog fixture; any I/O means the supposedly empty image inventory was incomplete. */
public final class TargetCheckpointSemanticSnapshotTestBridge {
    private TargetCheckpointSemanticSnapshotTestBridge() {}

    public static ProfileCatalog emptyProfiles() {
        return new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(ProfileRef reference) {
                throw new AssertionError("unexpected Profile");
            }

            @Override
            public CredentialBinding resolveBinding(ProfileRef profile, long generation) {
                throw new AssertionError("unexpected binding");
            }

            @Override
            public CredentialBindingHead resolveHead(ProfileRef profile) {
                throw new AssertionError("unexpected head");
            }

            @Override
            public CredentialBindingProtection resolveProtection(ProfileRef profile, long generation) {
                throw new AssertionError("unexpected protection");
            }
        };
    }
}
