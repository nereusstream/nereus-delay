package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandType;
import io.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;

import java.security.PublicKey;
import java.util.Objects;

/**
 * Bounded inline AUTO_FAST request. The managed command is always prepared
 * first; the optional native candidate is only an immutable, caller-supplied
 * capability snapshot and never performs transport I/O.
 */
public final class AutoFastSchedule {
    private final PreparedCommand managedCommand;
    private final NativeCandidate nativeCandidate;

    private AutoFastSchedule(final PreparedCommand managedCommand, final NativeCandidate nativeCandidate) {
        this.managedCommand = Objects.requireNonNull(managedCommand, "managedCommand");
        if (managedCommand.type() != CommandType.SCHEDULE) {
            throw new IllegalArgumentException("AUTO_FAST requires a ScheduleV1 command");
        }
        // This is the local zero-I/O strictness fence. It also validates that
        // a legacy body cannot be smuggled into the fallback branch.
        CommandCodec.encodeFrameV1(managedCommand);
        this.nativeCandidate = nativeCandidate;
    }

    /** Uses the managed branch and does not offer a native candidate. */
    public static AutoFastSchedule managed(final PreparedCommand managedCommand) {
        return new AutoFastSchedule(managedCommand, null);
    }

    /** Offers one immutable native candidate; selection remains local and pre-I/O. */
    public static AutoFastSchedule withNativeCandidate(final PreparedCommand managedCommand,
                                                        final NativeCandidate nativeCandidate) {
        return new AutoFastSchedule(managedCommand, Objects.requireNonNull(nativeCandidate, "nativeCandidate"));
    }

    public PreparedCommand managedCommand() {
        return managedCommand;
    }

    public NativeCandidate nativeCandidate() {
        return nativeCandidate;
    }

    /**
     * Immutable native input. The profile envelopes and signed snapshot are
     * inputs to local eligibility checks; production authority remains in the
     * profile catalog, Oxia, Broker guard, and credential issuer.
     */
    public static final class NativeCandidate {
        private final ProfileSemanticEnvelopeV1 destinationProfile;
        private final ProfileSemanticEnvelopeV1 capabilityProfile;
        private final PulsarBrokerResourceIdentityV1 target;
        private final int physicalPartition;
        private final byte[] inlinePayload;
        private final PulsarMetadataV1 metadata;
        private final Long eventTimeEpochMs;
        private final long deliverAtEpochMs;
        private final long nativeDelayBudgetMs;
        private final NativeCapabilitySnapshotV1 capabilitySnapshot;
        private final PublicKey issuerKey;
        private final boolean directTargetAuthority;

        public NativeCandidate(final ProfileSemanticEnvelopeV1 destinationProfile,
                               final ProfileSemanticEnvelopeV1 capabilityProfile,
                               final PulsarBrokerResourceIdentityV1 target,
                               final int physicalPartition, final byte[] inlinePayload,
                               final PulsarMetadataV1 metadata, final Long eventTimeEpochMs,
                               final long deliverAtEpochMs, final long nativeDelayBudgetMs,
                               final NativeCapabilitySnapshotV1 capabilitySnapshot,
                               final PublicKey issuerKey, final boolean directTargetAuthority) {
            this.destinationProfile = Objects.requireNonNull(destinationProfile, "destinationProfile");
            this.capabilityProfile = Objects.requireNonNull(capabilityProfile, "capabilityProfile");
            this.target = Objects.requireNonNull(target, "target");
            if (physicalPartition < 0) {
                throw new IllegalArgumentException("physicalPartition must be non-negative");
            }
            this.physicalPartition = physicalPartition;
            this.inlinePayload = Bytes.copy(Objects.requireNonNull(inlinePayload, "inlinePayload"));
            this.metadata = Objects.requireNonNull(metadata, "metadata");
            if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
                throw new IllegalArgumentException("eventTime must be non-negative");
            }
            if (deliverAtEpochMs < 0 || nativeDelayBudgetMs <= 0) {
                throw new IllegalArgumentException("native timing bounds must be non-negative/positive");
            }
            this.eventTimeEpochMs = eventTimeEpochMs;
            this.deliverAtEpochMs = deliverAtEpochMs;
            this.nativeDelayBudgetMs = nativeDelayBudgetMs;
            this.capabilitySnapshot = Objects.requireNonNull(capabilitySnapshot, "capabilitySnapshot");
            this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
            this.directTargetAuthority = directTargetAuthority;
        }

        public ProfileSemanticEnvelopeV1 destinationProfile() {
            return destinationProfile;
        }

        public ProfileSemanticEnvelopeV1 capabilityProfile() {
            return capabilityProfile;
        }

        public PulsarBrokerResourceIdentityV1 target() {
            return target;
        }

        public int physicalPartition() {
            return physicalPartition;
        }

        public byte[] inlinePayload() {
            return Bytes.copy(inlinePayload);
        }

        public PulsarMetadataV1 metadata() {
            return metadata;
        }

        public Long eventTimeEpochMs() {
            return eventTimeEpochMs;
        }

        public long deliverAtEpochMs() {
            return deliverAtEpochMs;
        }

        public long nativeDelayBudgetMs() {
            return nativeDelayBudgetMs;
        }

        public NativeCapabilitySnapshotV1 capabilitySnapshot() {
            return capabilitySnapshot;
        }

        public PublicKey issuerKey() {
            return issuerKey;
        }

        public boolean directTargetAuthority() {
            return directTargetAuthority;
        }
    }
}
