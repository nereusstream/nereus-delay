package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Actual Source/Owner and immutable Profile/credential checks for destination SDK opening, not SEND permission. */
public final class TargetDestinationActivation implements Runnable {
    @FunctionalInterface
    public interface CredentialMaterial { void requireConfigured(CredentialBinding original); }
    private final TargetWorkerShardRuntime worker;
    private final OwnerIdentity owner;
    private final TargetChannelIdentity channel;
    private final CanonicalTargetPartition physical;
    private final ProfileCatalog profiles;
    private final CredentialAttestationTrustSet trust;
    private final CredentialMaterial material;
    private final Supplier<BoundedReadBudget> reads;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final LongSupplier clock;
    private LocalBrokerMtlsResolver.Receipt verifiedMaterial;

    public TargetDestinationActivation(TargetWorkerShardRuntime worker, OwnerIdentity owner, TargetChannelIdentity channel,
            CanonicalTargetPartition physical, ProfileCatalog profiles, CredentialAttestationTrustSet trust,
            CredentialMaterial material, Supplier<BoundedReadBudget> reads, Supplier<TrustedUtcIntervalEvidence> time,
            LongSupplier clock) {
        this.worker=Objects.requireNonNull(worker); this.owner=Objects.requireNonNull(owner);
        this.channel=Objects.requireNonNull(channel); this.physical=Objects.requireNonNull(physical);
        this.profiles=Objects.requireNonNull(profiles); this.trust=Objects.requireNonNull(trust);
        this.material=Objects.requireNonNull(material); this.reads=Objects.requireNonNull(reads);
        this.time=Objects.requireNonNull(time); this.clock=Objects.requireNonNull(clock);
    }

    public TargetDestinationActivation(TargetWorkerShardRuntime worker, OwnerIdentity owner, TargetChannelIdentity channel,
            CanonicalTargetPartition physical, ProfileCatalog profiles, CredentialAttestationTrustSet trust,
            LocalBrokerMtlsResolver.Receipt material, Supplier<BoundedReadBudget> reads,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier clock) {
        this(worker, owner, channel, physical, profiles, trust,
                Objects.requireNonNull(material, "actual resolved mTLS material")::requireBinding, reads, time, clock);
        verifiedMaterial = material;
    }

    public LocalBrokerMtlsResolver.Receipt verifiedMaterial() {
        return Objects.requireNonNull(verifiedMaterial, "Target production opening requires resolved mTLS material");
    }

    /** Local SEND check: no file/catalog/secret resolution and no change to the original Source ownership gate. */
    public void requireLoadedMaterial(TargetChannelIdentity actual) {
        channel.requireExactFrozenIdentity(actual);
        verifiedMaterial().requireChannel(actual, Objects.requireNonNull(time.get(), "current trusted clock interval"));
    }

    @Override public void run() {
        worker.requirePublicationOwner(owner, clock);
        final var current=worker.requirePublicationChannel(channel, true, reads.get(), clock);
        if(!Arrays.equals(current.canonicalBytes(), physical.canonicalBytes())) {
            throw new IllegalStateException("destination SDK changes the actual Source Target");
        }
        channel.credentialLease().requireActiveAt(Objects.requireNonNull(time.get(), "trusted clock interval"));
        final var binding = TargetDestinationCredentialChecks.requireBinding(channel, physical, profiles, trust);
        material.requireConfigured(binding);
        worker.requirePublicationOwner(owner, clock);
        final var afterMaterial = worker.requirePublicationChannel(channel, true, reads.get(), clock);
        if (!Arrays.equals(afterMaterial.canonicalBytes(), physical.canonicalBytes())) {
            throw new IllegalStateException("destination SDK changes the actual Source Target after credential loading");
        }
        channel.credentialLease().requireActiveAt(Objects.requireNonNull(time.get(), "current trusted clock interval"));
        if (verifiedMaterial != null) { requireLoadedMaterial(channel); }
    }
}
