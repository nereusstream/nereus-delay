package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Actual Registry/Oxia/Worker/Profile-backed receipt and original anchor provider for first SEND and cold recovery. */
public final class KafkaClientArtifactTargetReceiptProvider implements KafkaClientArtifactTargetReceiptReader.Protection,
        KafkaClientArtifactTargetTransactionalFactory.Activation, KafkaClientArtifactTargetReceiptAnchor.Protection {
    private final TargetWorkerShardRuntime worker;
    private final OwnerIdentity owner;
    private final boolean newWork;
    private final TargetChannelIdentity channel;
    private final OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
    private final TargetKafkaReceiptControlArtifact protection;
    private final ProfileCatalog profiles;
    private final CredentialAttestationTrustSet credentialTrust;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final LongSupplier ownerClock;
    private final byte[] sdkJar;
    private final CredentialMaterial credentials;
    private final Supplier<com.nereusstream.delay.store.BoundedReadBudget> adoptionReads;
    private com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt;
    private volatile TargetWorkerShardRuntime.PublicationReads nativeReads;

    void bindNativeReads(TargetWorkerShardRuntime.PublicationReads original) {
        original.requireBinding(worker, channel);
        synchronized (this) {
            if (nativeReads != null && nativeReads != original) throw new IllegalStateException("original SDK read ownership cannot be replaced");
            nativeReads = original;
        }
    }

    public KafkaClientArtifactTargetReceiptProvider(TargetWorkerShardRuntime worker, OwnerIdentity owner,
            TargetChannelIdentity channel, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            byte[] originalProtectionDigest, com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource,
            ProfileCatalog profiles, CredentialAttestationTrustSet credentialTrust,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt material,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock, byte[] activatedSdkJarSha256,
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> adoptionReads, boolean newWork) {
        this(worker, owner, channel, catalog, originalProtectionDigest, resource, profiles, credentialTrust,
                material::requireBinding, time, ownerClock, activatedSdkJarSha256, adoptionReads, newWork);
        credentialReceipt = Objects.requireNonNull(material, "actual resolved K2 mTLS material");
        requireLoadedMaterial(channel);
    }

    @Override public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() { return credentialReceipt; }
    @Override public void requireLoadedMaterial(TargetChannelIdentity actual) {
        channel.requireExactFrozenIdentity(actual);
        if (credentialReceipt == null) { throw new IllegalStateException("new K2 service requires resolved mTLS material"); }
        credentialReceipt.requireChannel(actual, time.get());
    }

    /** Actual resolved private transport material must match the exact frozen binding/fingerprint, not a mutable alias. */
    @FunctionalInterface
    public interface CredentialMaterial { void requireConfigured(CredentialBinding original); }

    /** Implemented by the CONTROL management entry; the Worker never receives its issuer key. */
    @FunctionalInterface
    public interface AnchorPublisher {
        CompletableFuture<TargetKafkaReceiptBinding> publish(KafkaClientArtifactTargetReceiptAnchor.Observation actual,
                String targetPhysicalTopic);
    }

    public KafkaClientArtifactTargetReceiptProvider(TargetWorkerShardRuntime worker, OwnerIdentity owner,
            TargetChannelIdentity channel, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            byte[] originalProtectionDigest, com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource,
            ProfileCatalog profiles, CredentialAttestationTrustSet credentialTrust, CredentialMaterial credentials,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock, byte[] activatedSdkJarSha256,
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> adoptionReads) {
        this(worker,owner,channel,catalog,originalProtectionDigest,resource,profiles,credentialTrust,credentials,time,ownerClock,
                activatedSdkJarSha256,adoptionReads,true);
    }

    public KafkaClientArtifactTargetReceiptProvider(TargetWorkerShardRuntime worker, OwnerIdentity owner,
            TargetChannelIdentity channel, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            byte[] originalProtectionDigest, com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource,
            ProfileCatalog profiles, CredentialAttestationTrustSet credentialTrust, CredentialMaterial credentials,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock, byte[] activatedSdkJarSha256,
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> adoptionReads, boolean newWork) {
        this.newWork=newWork;
        this.worker = Objects.requireNonNull(worker, "actual Worker");
        this.owner = Objects.requireNonNull(owner, "actual configured Owner");
        this.channel = Objects.requireNonNull(channel, "current registered channel");
        this.catalog = Objects.requireNonNull(catalog, "actual Registry receipt catalog");
        this.profiles = Objects.requireNonNull(profiles, "actual immutable Profile/credential catalog");
        this.credentialTrust = Objects.requireNonNull(credentialTrust, "original credential-attestation trust");
        this.credentials = Objects.requireNonNull(credentials, "actual configured private SDK material");
        this.adoptionReads = Objects.requireNonNull(adoptionReads,"finite actual Source adoption read budgets");
        this.time = Objects.requireNonNull(time, "trusted clock policy");
        this.ownerClock = Objects.requireNonNull(ownerClock, "Source Owner clock");
        Bytes.requireLength(activatedSdkJarSha256, 32, "eligible-reader activated SDK JAR");
        sdkJar = Bytes.copy(activatedSdkJarSha256);
        if (!worker.shardId().equals(channel.context().sourceShard())) { throw new IllegalArgumentException("receipt provider belongs to another Shard"); }
        requireWorker();
        protection = catalog.requireActive(resource, channel.context().evidenceGeneration(), originalProtectionDigest);
        protection.protection().requireChannel(channel);
        if(newWork)worker.requirePublicationChannel(channel,true,adoptionReads.get(),ownerClock);
        else worker.requireRetainedPublicationChannel(channel,adoptionReads.get(),ownerClock);
        requireCurrentChannel();
    }

    public com.nereusstream.delay.protocol.TargetKafkaReceiptProtection protection() { return protection.protection(); }

    /** Real sample, original CONTROL signature and exact durable reread all precede returning the v6 binding. */
    public CompletableFuture<Optional<TargetKafkaReceiptBinding>> prepareBinding(String targetPhysicalTopic,
            Map<String, Object> readerConfiguration, AnchorPublisher publisher, Duration timeout, Executor io) {
        final var p = protection.protection();
        final var access = nativeReads;
        requireCurrentChannel(access);
        return KafkaClientArtifactTargetReceiptAnchor.sample(readerConfiguration, p.resource(), p.physicalTopic(),
                p.physicalPartition(), this, timeout, io).thenCompose(sample -> {
                    if (sample.isEmpty()) { return CompletableFuture.completedFuture(Optional.empty()); }
                    requireCurrentChannel(access);
                    return Objects.requireNonNull(publisher, "actual CONTROL anchor publication").publish(
                            sample.orElseThrow(), targetPhysicalTopic).thenApply(binding -> {
                                requireBinding(binding, access); return Optional.of(binding);
                            });
                });
    }

    /** Cold startup reloads original two digests; it never samples a replacement end for an existing mapping. */
    public TargetKafkaReceiptControlArtifact requireBinding(TargetKafkaReceiptBinding binding) {
        return requireBinding(binding, null);
    }
    private TargetKafkaReceiptControlArtifact requireBinding(TargetKafkaReceiptBinding binding,
            TargetWorkerShardRuntime.PublicationReads access) {
        requireWorker(access);
        catalog.requireActive(binding.receiptResource(), protection.protection().evidenceGeneration(), binding.retentionProtectionDigest());
        final var anchor = catalog.requireAnchor(protection, binding);
        requireCurrentChannel(access);
        return anchor;
    }

    @Override
    public void requireCurrent(TargetKafkaReceiptMapping mapping, long nextOffset) {
        final var admitted = mapping.admission();
        if (!Arrays.equals(channel.context().producerIdentity(), admitted.channel().context().producerIdentity())
                || nextOffset < admitted.kafkaReceipt().scanStartOffset()
                || !admitted.publication().capabilityProfile().equals(protection.protection().capability())) {
            throw new IllegalArgumentException("receipt recovery changes frozen producer/range/capability");
        }
        reconcileAdoption(mapping, adoptionReads.get(), nativeReads);
    }

    @Override public void requireAdoption(TargetKafkaReceiptMapping mapping) {
        reconcileAdoption(mapping, adoptionReads.get(), nativeReads);
    }

    @Override
    public OwnerIdentity requireCurrent(TargetChannelIdentity requested, TargetKafkaReceiptBinding receipt, byte[] actualSdkJar) {
        channel.requireExactFrozenIdentity(requested);
        final var access = nativeReads;
        final CanonicalTargetPartition current;
        if (!newWork) current = retainedChannel(access);
        else if (access == null) current = worker.requirePublicationChannel(channel,true,adoptionReads.get(),ownerClock);
        else current = access.requireOpenChannel(channel,adoptionReads.get(),ownerClock);
        if(!current.id().equals(channel.context().target()))throw new IllegalStateException("SDK opening Target changed actual Source identity");
        requireSdk(actualSdkJar);
        requireBinding(receipt, access);
        return requireWorker(access);
    }

    @Override
    public byte[] requireProtected(com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource, String topic, int partition) {
        final var access = nativeReads;
        requireWorker(access); requireResource(resource, topic, partition);
        catalog.requireActive(resource, protection.protection().evidenceGeneration(), protection.digest());
        requireCurrentChannel(access);
        return protection.digest();
    }
    @Override
    public void requireCurrent(byte[] digest, com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource,
            String topic, int partition, byte[] actualSdkJar) {
        if (!Arrays.equals(digest, protection.digest())) { throw new IllegalArgumentException("anchor protection was replaced"); }
        requireSdk(actualSdkJar); requireProtected(resource, topic, partition);
    }

    /** Snapshot lookup runs before the Store view; the apply callback only compares exact preloaded signed bytes and charges. */
    public TargetPublishAdmissionVerifier.MaterializationAuthority materialization(
            com.nereusstream.delay.protocol.SystemMutation prepared, com.nereusstream.delay.protocol.SourcePosition source) {
        final var body = TargetPublishAdmissionBody.decode(prepared.canonicalBody());
        requireBinding(Objects.requireNonNull(body.kafkaReceipt(), "original v6 receipt binding"));
        protection.protection().requireChannel(body.channel());
        if (!body.publication().capabilityProfile().equals(protection.protection().capability())
                || !source.shardId().equals(worker.shardId()) || source.compareTo(protection.authorizedAt()) <= 0) {
            throw new IllegalArgumentException("Admission changes original protected CONTROL/capability source use");
        }
        final byte[] actualBody = body.canonicalBytes();
        final byte[] actualSource = source.canonicalBytes();
        final var material = new com.nereusstream.delay.runtime.TargetKafkaReceiptExternalEvidence(protection,
                catalog.protectionObjectKey(protection.protection()), requireBinding(body.kafkaReceipt()),
                catalog.anchorObjectKey(body.kafkaReceipt().scanObservationDigest()));
        catalog.requirePrepared(protection, material.anchor(), prepared);
        final byte[] preparedFrame = prepared.encodeFrame();
        return new TargetPublishAdmissionVerifier.MaterializationAuthority() {
            @Override public void requirePrepared(com.nereusstream.delay.protocol.SystemMutation candidate) {
                if (!Arrays.equals(preparedFrame, candidate.encodeFrame())) {
                    throw new IllegalArgumentException("Source Admission differs from its durable prepared ID/hash/retry binding");
                }
            }
            @Override public void requireAuthorized(TargetPublishAdmissionBody candidate,
                    com.nereusstream.delay.protocol.SourcePosition at) {
                if (!Arrays.equals(actualBody, candidate.canonicalBytes()) || !Arrays.equals(actualSource, at.canonicalBytes())) {
                    throw new IllegalArgumentException("Admission differs from its protected original evidence snapshot");
                }
            }
            @Override public com.nereusstream.delay.runtime.TargetKafkaReceiptExternalEvidence externalEvidence(
                    TargetPublishAdmissionBody candidate, com.nereusstream.delay.protocol.SourcePosition at) {
                requireAuthorized(candidate, at); return material;
            }
        };
    }

    /** Same original frame stays retained on UNKNOWN; native APPLIED adoption is reconciled before any first SEND. */
    public com.nereusstream.delay.ownership.TargetPulsarPublishRuntime.ReceiptPublications publications(
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads, Executor io) {
        Objects.requireNonNull(reads, "finite native adoption read budget");
        if(!(io instanceof java.util.concurrent.ThreadPoolExecutor pool))throw new IllegalArgumentException("receipt publication requires actual bounded I/O pool");
        return new com.nereusstream.delay.ownership.TargetPulsarPublishRuntime.ReceiptPublications() {
            @Override public CompletableFuture<Void> bind(com.nereusstream.delay.protocol.SystemMutation prepared) {
                return com.nereusstream.delay.ownership.OwnedCheckpointIo.<Void>submit(guard -> {
                    guard.run();final var body = TargetPublishAdmissionBody.decode(prepared.canonicalBody());
                    final var anchor = requireBinding(body.kafkaReceipt());
                    if (!body.owner().equals(requireWorker())) { throw new IllegalStateException("prepared receipt use has another live Owner"); }
                    catalog.bindPrepared(protection, anchor, prepared); requireWorker();guard.run();return null;
                },()->{},pool).receipt();
            }
            @Override public CompletableFuture<Void> adopt(com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied actual) {
                return com.nereusstream.delay.ownership.OwnedCheckpointIo.<Void>submit(guard->{guard.run();
                    reconcileAdoption(actual.kafkaMapping(),reads.get(),nativeReads);guard.run();return null;},()->{},pool).receipt();
            }
        };
    }

    /** Cold restart reconciles from immutable native first-result/owner/ref facts, without re-SEND or a new anchor. */
    public void reconcileAdoption(TargetKafkaReceiptMapping mapping, com.nereusstream.delay.store.BoundedReadBudget reads) {
        reconcileAdoption(mapping, reads, null);
    }
    private void reconcileAdoption(TargetKafkaReceiptMapping mapping, com.nereusstream.delay.store.BoundedReadBudget reads,
            TargetWorkerShardRuntime.PublicationReads access) {
        requireBinding(mapping.admission().kafkaReceipt(), access);
        Objects.requireNonNull(reads, "bounded adoption reads");
        final var actual = access == null ? worker.readExternalEvidenceAdoption(reads, mapping, ownerClock)
                : access.readAdoption(reads, mapping, ownerClock);
        catalog.adopt(actual); requireWorker(access);
    }

    /** Anchor reservation consumption uses exact external bytes; the shared protection is independently owned once by Source. */
    public CapacityVector evidenceCharge(TargetKafkaReceiptBinding binding,
            com.nereusstream.delay.protocol.TargetQuotaAccounting frozenAccounting) {
        final var anchor = catalog.requireAnchor(protection, binding);
        return frozenAccounting.externalEvidenceCharge(catalog.anchorObjectKey(anchor.digest()).getBytes(
                java.nio.charset.StandardCharsets.UTF_8).length, anchor.canonicalBytes().length);
    }

    private OwnerIdentity requireWorker() { return worker.requirePublicationOwner(owner, ownerClock); }
    private OwnerIdentity requireWorker(TargetWorkerShardRuntime.PublicationReads access) {
        return access == null ? requireWorker() : access.requireOwner(owner, ownerClock);
    }
    private com.nereusstream.delay.protocol.CanonicalTargetPartition retainedChannel(TargetWorkerShardRuntime.PublicationReads access) {
        return access == null ? worker.requireRetainedPublicationChannel(channel,adoptionReads.get(),ownerClock)
                : access.requireRetainedChannel(channel,adoptionReads.get(),ownerClock);
    }
    private void requireCurrentChannel() { requireCurrentChannel(null); }
    private void requireCurrentChannel(TargetWorkerShardRuntime.PublicationReads access) {
        requireWorker(access);
        final var physical = retainedChannel(access);
        channel.credentialLease().requireActiveAt(Objects.requireNonNull(time.get(), "actual trusted clock interval"));
        final var binding = com.nereusstream.delay.ownership.TargetDestinationCredentialChecks.requireBinding(
                channel, physical, profiles, credentialTrust);
        credentials.requireConfigured(binding);
        requireSdk(KafkaClientArtifactTargetReceiptReader.requireSdkIdentity());
        requireWorker(access);
        final var afterMaterial = retainedChannel(access);
        if (!Arrays.equals(physical.canonicalBytes(), afterMaterial.canonicalBytes())) {
            throw new IllegalStateException("receipt SDK resource changed after credential loading");
        }
        channel.credentialLease().requireActiveAt(Objects.requireNonNull(time.get(), "current trusted clock interval"));
        if (credentialReceipt != null) { requireLoadedMaterial(channel); }
    }
    private void requireSdk(byte[] actual) {
        if (!Arrays.equals(sdkJar, actual)) { throw new IllegalStateException("receipt reader/producer differs from the activated actual SDK artifact"); }
    }
    private void requireResource(com.nereusstream.delay.protocol.KafkaReceiptSlotResource resource, String topic, int partition) {
        final var p = protection.protection();
        if (!resource.equals(p.resource()) || !topic.equals(p.physicalTopic()) || partition != p.physicalPartition()) {
            throw new IllegalArgumentException("receipt provider changed exact registered resource/geometry");
        }
    }
}
