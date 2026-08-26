package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.StableCode;
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
        try {
            this.managedCommand = Objects.requireNonNull(managedCommand, "managedCommand");
            if (managedCommand.type() != CommandType.SCHEDULE) {
                throw PreparationFailure.of(StableCode.INVALID_COMMAND);
            }
            // This is the local zero-I/O strictness fence. It also validates
            // that a legacy body cannot be smuggled into the fallback branch.
            CommandCodec.encodeManagedFrame(managedCommand);
        } catch (PreparationFailure failure) {
            throw failure;
        } catch (RuntimeException invalidCommand) {
            throw PreparationFailure.of(StableCode.INVALID_COMMAND, invalidCommand);
        }
        this.nativeCandidate = nativeCandidate;
    }

    /** Uses the managed branch and does not offer a native candidate. */
    public static AutoFastSchedule managed(final PreparedCommand managedCommand) {
        return new AutoFastSchedule(managedCommand, null);
    }

    /** Offers one immutable native candidate; selection remains local and pre-I/O. */
    public static AutoFastSchedule withNativeCandidate(
            final PreparedCommand managedCommand, final NativeCandidate nativeCandidate) {
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
        private final ProfileSemanticEnvelope destinationProfile;
        private final ProfileSemanticEnvelope capabilityProfile;
        private final PulsarBrokerResourceIdentity target;
        private final int physicalPartition;
        private final byte[] inlinePayload;
        private final PulsarMetadata metadata;
        private final Long eventTimeEpochMs;
        private final long deliverAtEpochMs;
        private final long nativeDelayBudgetMs;
        private final NativeCapabilitySnapshot capabilitySnapshot;
        private final PublicKey issuerKey;
        private final boolean directTargetAuthority;

        public NativeCandidate(
                final ProfileSemanticEnvelope destinationProfile,
                final ProfileSemanticEnvelope capabilityProfile,
                final PulsarBrokerResourceIdentity target,
                final int physicalPartition,
                final byte[] inlinePayload,
                final PulsarMetadata metadata,
                final Long eventTimeEpochMs,
                final long deliverAtEpochMs,
                final long nativeDelayBudgetMs,
                final NativeCapabilitySnapshot capabilitySnapshot,
                final PublicKey issuerKey,
                final boolean directTargetAuthority) {
            this.destinationProfile = require(destinationProfile, "destinationProfile");
            this.capabilityProfile = require(capabilityProfile, "capabilityProfile");
            this.target = require(target, "target");
            // Physical partitions are Registry uint32 values. Preserve the
            // complete raw bit pattern; profile/routing validation below is
            // responsible for deciding whether this value is authorized.
            this.physicalPartition = physicalPartition;
            this.inlinePayload = Bytes.copy(require(inlinePayload, "inlinePayload"));
            this.metadata = require(metadata, "metadata");
            if (eventTimeEpochMs != null && eventTimeEpochMs < 0) {
                throw PreparationFailure.of(StableCode.INVALID_DELIVERY_WINDOW);
            }
            if (deliverAtEpochMs < 0 || nativeDelayBudgetMs <= 0) {
                throw PreparationFailure.of(StableCode.INVALID_DELIVERY_WINDOW);
            }
            this.eventTimeEpochMs = eventTimeEpochMs;
            this.deliverAtEpochMs = deliverAtEpochMs;
            this.nativeDelayBudgetMs = nativeDelayBudgetMs;
            this.capabilitySnapshot = require(capabilitySnapshot, "capabilitySnapshot");
            this.issuerKey = require(issuerKey, "issuerKey");
            this.directTargetAuthority = directTargetAuthority;
        }

        private static <T> T require(final T value, final String name) {
            if (value == null) {
                throw PreparationFailure.of(StableCode.INVALID_COMMAND);
            }
            return value;
        }

        public ProfileSemanticEnvelope destinationProfile() {
            return destinationProfile;
        }

        public ProfileSemanticEnvelope capabilityProfile() {
            return capabilityProfile;
        }

        public PulsarBrokerResourceIdentity target() {
            return target;
        }

        public int physicalPartition() {
            return physicalPartition;
        }

        public byte[] inlinePayload() {
            return Bytes.copy(inlinePayload);
        }

        public PulsarMetadata metadata() {
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

        public NativeCapabilitySnapshot capabilitySnapshot() {
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
