package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded public Registry value collection on the same live cut as original Control/Outcome/Admission history. */
public final class TargetCheckpointRegistryInputs {
    public enum Status {
        LOADING,
        ACCEPTED,
        READ_YIELD,
        COMPLETE,
        FAILED
    }

    /** Providers return promptly and own finite I/O deadlines, original-use authority, immutable pins and cleanup. */
    public record Providers(
            java.util.function.Function<
                            TargetCheckpointDependencies.ProfileUse, CompletableFuture<ProfileSemanticEnvelope>>
                    profiles,
            java.util.function.Function<TargetCheckpointDependencies.RetryUse, CompletableFuture<RetryPolicySemantic>>
                    retries,
            java.util.function.Function<PayloadProofTrustSetRef, CompletableFuture<PayloadProofTrustSetSemantic>>
                    trusts,
            java.util.function.Function<
                            TargetCheckpointDependencies.RetryUse,
                            CompletableFuture<com.nereusstream.delay.runtime.RetryPolicyCatalog.Publication>>
                    retryPublications) {
        public Providers(
                java.util.function.Function<
                                TargetCheckpointDependencies.ProfileUse, CompletableFuture<ProfileSemanticEnvelope>>
                        profiles,
                java.util.function.Function<
                                TargetCheckpointDependencies.RetryUse, CompletableFuture<RetryPolicySemantic>>
                        retries,
                java.util.function.Function<PayloadProofTrustSetRef, CompletableFuture<PayloadProofTrustSetSemantic>>
                        trusts) {
            this(profiles, retries, trusts, null);
        }

        public Providers {
            Objects.requireNonNull(profiles, "profiles");
            Objects.requireNonNull(retries, "retries");
            Objects.requireNonNull(trusts, "trusts");
        }

        /** Connects existing catalogs; caller owns finite RPC/executor bounds and held original-use/pin authority. */
        public static Providers fromOxiaCatalogs(
                com.nereusstream.delay.runtime.OxiaSyncSourceProfileCatalog profiles,
                com.nereusstream.delay.runtime.OxiaSyncRetryPolicyCatalog retries,
                com.nereusstream.delay.runtime.OxiaSyncPayloadProofTrustSetCatalog trusts,
                java.time.Duration timeout,
                Runnable protection,
                java.util.concurrent.ThreadPoolExecutor executor) {
            Objects.requireNonNull(profiles, "profiles");
            Objects.requireNonNull(retries, "retries");
            Objects.requireNonNull(trusts, "trusts");
            Objects.requireNonNull(timeout, "timeout");
            Objects.requireNonNull(protection, "protection");
            Objects.requireNonNull(executor, "executor");
            return new Providers(
                    use -> profiles.resolveAsync(use.reference(), use.source(), timeout, protection, executor),
                    use -> retries.resolveAsync(use.reference(), use.source(), timeout, protection, executor),
                    reference -> trusts.resolveAsync(reference, timeout, protection, executor),
                    use -> retries.resolvePublicationAsync(
                            use.reference(), use.source(), timeout, protection, executor));
        }
    }

    public record Limits(int maximumUses, long maximumResponseBytes) {
        public Limits {
            if (maximumUses <= 0
                    || maximumUses == Integer.MAX_VALUE
                    || maximumResponseBytes <= 0
                    || maximumResponseBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Registry collection requires finite positive bounds");
            }
        }
    }

    public record Turn(Status status, Throwable failure) {}

    public record Values(
            List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries,
            List<PayloadProofTrustSetSemantic> trusts,
            List<com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.TerminalPolicyImage> terminalPolicies) {
        public Values(
                List<ProfileSemanticEnvelope> profiles,
                List<RetryPolicySemantic> retries,
                List<PayloadProofTrustSetSemantic> trusts) {
            this(profiles, retries, trusts, List.of());
        }

        public Values {
            profiles = List.copyOf(profiles);
            retries = List.copyOf(retries);
            trusts = List.copyOf(trusts);
            terminalPolicies = List.copyOf(terminalPolicies);
        }
    }

    private final TargetWorkerShardRuntime worker;
    private final TargetCheckpointControlInputs.Cut cut;
    private final Supplier<BoundedReadBudget> reads;
    private final LongSupplier ownerClock;
    private final Runnable protection;
    private final Providers providers;
    private final Limits limits;
    private final TreeMap<String, TargetCheckpointDependencies.ProfileUse> profileUses = new TreeMap<>();
    private final Set<String> seenProfiles = new HashSet<>();
    private final List<TargetCheckpointDependencies.RetryUse> retryUses;
    private final List<PayloadProofTrustSetRef> trustUses;
    private final TreeMap<String, ProfileSemanticEnvelope> profiles = new TreeMap<>();
    private final TreeMap<String, RetryPolicySemantic> retries = new TreeMap<>();
    private final TreeMap<String, com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.TerminalPolicyImage>
            terminalPolicies = new TreeMap<>();
    private final TreeMap<String, PayloadProofTrustSetSemantic> trusts = new TreeMap<>();
    private CompletableFuture<?> loading;
    private TargetCheckpointDependencies.ProfileUse selectedProfile;
    private int retryIndex;
    private int trustIndex;
    private int uses;
    private long responseBytes;
    private Throwable failure;
    private boolean complete;

    /** Complete dependencies must extend this exact captured live root, including all original Admission images. */
    public TargetCheckpointRegistryInputs(
            TargetWorkerShardRuntime worker,
            TargetCheckpointControlInputs.Cut cut,
            TargetCheckpointDependencies dependencies,
            Supplier<BoundedReadBudget> reads,
            LongSupplier ownerClock,
            Runnable protection,
            Providers providers,
            Limits limits) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.cut = Objects.requireNonNull(cut, "cut");
        if (Objects.requireNonNull(dependencies, "dependencies").root()
                        != cut.dependencies().root()
                || !dependencies.admissionImagesComplete()) {
            throw new IllegalArgumentException("Registry inputs require the same complete live checkpoint root");
        }
        this.reads = Objects.requireNonNull(reads, "reads");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.providers = Objects.requireNonNull(providers, "providers");
        this.limits = Objects.requireNonNull(limits, "limits");
        retryUses = dependencies.retryUses();
        trustUses = dependencies.trustSets();
        uses = Math.addExact(retryUses.size(), trustUses.size());
        if (uses > limits.maximumUses()) {
            throw new IllegalArgumentException("Registry use count exceeds its bound");
        }
        dependencies.profileUses().forEach(this::addProfile);
    }

    public synchronized Turn runTurn() {
        if (failure != null) {
            return new Turn(Status.FAILED, failure);
        }
        try {
            requireCurrent();
            if (complete) {
                return new Turn(Status.COMPLETE, null);
            }
            if (selectedProfile == null && !profileUses.isEmpty()) {
                selectedProfile = profileUses.pollFirstEntry().getValue();
            }
            if (selectedProfile == null && retryIndex == retryUses.size() && trustIndex == trustUses.size()) {
                complete = true;
                return new Turn(Status.COMPLETE, null);
            }
            if (loading == null) {
                loading = selectedProfile != null
                        ? providers.profiles().apply(selectedProfile)
                        : retryIndex < retryUses.size()
                                ? providers.retryPublications() == null
                                        ? providers.retries().apply(retryUses.get(retryIndex))
                                        : providers.retryPublications().apply(retryUses.get(retryIndex))
                                : providers.trusts().apply(trustUses.get(trustIndex));
                Objects.requireNonNull(loading, "Registry future");
                requireCurrent();
                return new Turn(Status.LOADING, null);
            }
            if (!loading.isDone()) {
                return new Turn(Status.LOADING, null);
            }
            final Object value = Objects.requireNonNull(loading.join(), "Registry semantic value");
            if (!CheckpointInputFuture.ioExited(loading)) {
                return new Turn(Status.LOADING, null);
            }
            requireCurrent();
            if (selectedProfile != null) {
                final var profile = (ProfileSemanticEnvelope) value;
                if (!selectedProfile.reference().equals(profile.ref())) {
                    throw new IllegalStateException("Registry Profile differs from its original use");
                }
                charge(profile.canonicalBytes().length);
                profiles.put(Bytes.hex(profile.ref().canonicalBytes()), profile);
                if (profile.body() instanceof DestinationProfileSemantic destination) {
                    addProfile(new TargetCheckpointDependencies.ProfileUse(
                            destination.deliveryCapability(), selectedProfile.source()));
                }
                selectedProfile = null;
            } else if (retryIndex < retryUses.size()) {
                final var publication = providers.retryPublications() == null
                        ? null
                        : (com.nereusstream.delay.runtime.RetryPolicyCatalog.Publication) value;
                final var retry = publication == null ? (RetryPolicySemantic) value : publication.retry();
                final var use = retryUses.get(retryIndex);
                if (!use.reference().matches(retry)) {
                    throw new IllegalStateException("Registry Retry Policy differs from its original use");
                }
                charge(retry.canonicalBytes().length);
                if (publication != null && publication.terminal() != null) {
                    publication.terminal().requireBinding(retry);
                    charge(publication.terminal().canonicalBytes().length);
                    terminalPolicies.put(
                            Bytes.hex(use.canonicalBytes()),
                            new com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.TerminalPolicyImage(
                                    use, publication.terminal()));
                }
                retries.put(Bytes.hex(use.reference().canonicalBytes()), retry);
                retryIndex++;
            } else {
                final var trust = (PayloadProofTrustSetSemantic) value;
                if (!trustUses.get(trustIndex).equals(trust.ref())) {
                    throw new IllegalStateException("Registry Trust Set differs from its exact reference");
                }
                charge(trust.canonicalBytes().length);
                trusts.put(Bytes.hex(trust.ref().canonicalBytes()), trust);
                trustIndex++;
            }
            loading = null;
            requireCurrent();
            return new Turn(Status.ACCEPTED, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.READ_YIELD, null);
        } catch (RuntimeException invalid) {
            failure = invalid.getCause() == null ? invalid : invalid.getCause();
            return new Turn(Status.FAILED, failure);
        }
    }

    public synchronized Optional<Values> result() {
        if (failure != null || !complete) {
            return Optional.empty();
        }
        try {
            requireCurrent();
            return Optional.of(new Values(
                    List.copyOf(profiles.values()),
                    List.copyOf(retries.values()),
                    List.copyOf(trusts.values()),
                    List.copyOf(terminalPolicies.values())));
        } catch (ReadIncompleteException incomplete) {
            throw incomplete;
        } catch (RuntimeException invalid) {
            failure = invalid;
            return Optional.empty();
        }
    }

    public synchronized void retryCleanup() {
        CheckpointInputFuture.retryCleanup(loading);
    }

    public synchronized void requestStop() {
        CheckpointInputFuture.requestStop(loading);
    }

    public synchronized boolean ioExited() {
        return CheckpointInputFuture.ioExited(loading);
    }

    public synchronized Throwable pendingIoFailure() {
        return CheckpointInputFuture.ownedFailure(loading);
    }

    private void addProfile(TargetCheckpointDependencies.ProfileUse use) {
        final String key = Bytes.hex(use.canonicalBytes());
        if (seenProfiles.add(key)) {
            uses = Math.addExact(uses, 1);
            if (uses > limits.maximumUses()) {
                throw new IllegalArgumentException("Registry use count exceeds its bound");
            }
            profileUses.put(key, use);
        }
    }

    private void charge(int bytes) {
        responseBytes = Math.addExact(responseBytes, bytes);
        if (responseBytes > limits.maximumResponseBytes()) {
            throw new IllegalArgumentException("Registry response bytes exceed their bound");
        }
    }

    private void requireCurrent() {
        protection.run();
        worker.requireCheckpointControlInputsCurrent(
                Objects.requireNonNull(reads.get(), "read budget"), cut, ownerClock);
        protection.run();
    }
}
