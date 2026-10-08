package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.TargetCheckpointAdmissionInputs;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.RetryPolicyCatalog;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Local dependencies from a complete Target ledger fold; external authentication and pins remain required. */
public final class TargetCheckpointDependencies {
    public record StoredControl(ColumnFamily family, byte[] key, int valueType, byte[] payload) {
        public StoredControl {
            Objects.requireNonNull(family, "family");
            key = Bytes.copy(key);
            payload = Bytes.copy(payload);
        }

        @Override
        public byte[] key() { return Bytes.copy(key); }

        @Override
        public byte[] payload() { return Bytes.copy(payload); }
    }

    /** Captures the original first result, independently of the current UNKNOWN/Outcome/Floor Budget stamp. */
    public record Admission(TargetQuotaAttemptBudget attempt, TargetResultRecord first) {
        public Admission {
            Objects.requireNonNull(attempt, "attempt");
            Objects.requireNonNull(first, "first");
        }
    }

    /** Each use retains its original visibility point, including old bindings after later policy publication. */
    public record RetryUse(RetryPolicyRef reference, SourcePosition source) {
        public RetryUse {
            Objects.requireNonNull(reference, "reference");
            TargetSourcePosition.requireBounded(source);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, reference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, source.canonicalBytes());
            });
        }

        /** Exact semantics must have been visible at this use, independently of a later checkpoint cut. */
        public RetryPolicySemantic resolve(RetryPolicyCatalog catalog) {
            final var semantic = Objects.requireNonNull(catalog, "catalog").resolve(reference, source);
            if (semantic == null || !reference.matches(semantic)) {
                throw new IllegalStateException("Target Retry Policy is not visible at its original Source use");
            }
            return semantic;
        }
    }

    private final TargetCheckpointRootVerifier.RootProof root;
    private final List<StoredControl> controls;
    private final List<Admission> admissions;
    private final List<ProfileRef> profiles;
    private final List<RetryPolicyRef> retryPolicies;
    private final List<RetryUse> retryUses;
    private final List<PayloadProofTrustSetRef> trustSets;
    private final List<byte[]> objectProfileHashes;
    private final List<byte[]> artifactDigests;
    private final boolean admissionImagesComplete;

    private TargetCheckpointDependencies(Builder builder, List<Admission> admissions, boolean complete) {
        root = builder.root;
        controls = List.copyOf(builder.controls.values());
        this.admissions = List.copyOf(admissions);
        profiles = List.copyOf(builder.profiles.values());
        retryPolicies = List.copyOf(builder.retries.values());
        retryUses = List.copyOf(builder.retryUses.values());
        trustSets = List.copyOf(builder.trusts.values());
        objectProfileHashes = copies(builder.objectHashes.values().stream().toList());
        artifactDigests = copies(builder.artifacts.values().stream().toList());
        admissionImagesComplete = complete;
    }

    public TargetCheckpointRootVerifier.RootProof root() { return root; }
    public List<StoredControl> controls() { return controls; }
    public List<Admission> admissions() { return admissions; }
    public List<ProfileRef> profiles() { return profiles; }
    public List<RetryPolicyRef> retryPolicies() { return retryPolicies; }
    public List<RetryUse> retryUses() { return retryUses; }
    public List<PayloadProofTrustSetRef> trustSets() { return trustSets; }
    public List<byte[]> objectProfileHashes() { return copies(objectProfileHashes); }
    public List<byte[]> artifactDigests() { return copies(artifactDigests); }
    public boolean admissionImagesComplete() { return admissionImagesComplete; }

    /**
     * Reads every original use under one finite budget and caller-owned protection guard. Invoke outside Store locks;
     * providers own I/O deadlines. Never replace an original visibility point with a later checkpoint position.
     */
    public List<RetryPolicySemantic> resolveRetryPolicies(final RetryPolicyCatalog catalog,
            final BoundedReadBudget budget, final Runnable protectionGuard) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        final var values = new TreeMap<String, RetryPolicySemantic>();
        for (final var use : retryUses) {
            protectionGuard.run();
            if (!budget.beforeRead()) { throw budget.incomplete(); }
            final var semantic = use.resolve(catalog);
            protectionGuard.run();
            if (!budget.tryCharge(use.canonicalBytes().length, semantic.canonicalBytes().length)
                    || !budget.beforeTimedWork()) { throw budget.incomplete(); }
            values.put(Bytes.hex(use.reference().canonicalBytes()), semantic);
        }
        protectionGuard.run();
        return List.copyOf(values.values());
    }

    /** Original signed snapshot input; this DTO alone carries no history or authentication rights. */
    public record AdmissionImage(SourcePosition source, SystemMutation image) {
        public AdmissionImage {
            TargetSourcePosition.requireBounded(source);
            Objects.requireNonNull(image, "image");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, source.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, image.encodeFrame());
            });
        }
    }

    /** Joins every retained attempt to its original image; incomplete or stale references cannot prove closure. */
    public TargetCheckpointDependencies withAdmissionInputs(
            final List<TargetCheckpointAdmissionInputs.Input> inputs, final long maximumFrameBytes) {
        Objects.requireNonNull(inputs, "inputs");
        if (maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE || inputs.size() != admissions.size()) {
            throw new IllegalArgumentException("Target semantic closure needs finite bytes and every Admission input");
        }
        final var expected = new TreeMap<String, TargetQuotaAttemptBudget>();
        admissions.forEach(admission -> expected.put(Bytes.hex(admission.attempt().publishAttemptId()),
                admission.attempt()));
        final var images = new java.util.ArrayList<AdmissionImage>();
        for (final var input : inputs) {
            final var attempt = expected.get(Bytes.hex(input.reference().attempt().publishAttemptId()));
            if (attempt == null || !Arrays.equals(attempt.canonicalBytes(),
                    input.reference().attempt().canonicalBytes())) {
                throw new IllegalArgumentException("Target semantic closure has an extra or stale Budget reference");
            }
            input.reference().requireImage(input.image(), input.reference().source());
            images.add(new AdmissionImage(input.reference().source(), input.image()));
        }
        return withAdmissionImages(images, maximumFrameBytes);
    }

    /** Offline restore correlates original first results and frozen Budgets after the full physical ledger audit. */
    public TargetCheckpointDependencies withAdmissionImages(
            final List<AdmissionImage> inputs, final long maximumFrameBytes) {
        Objects.requireNonNull(inputs, "inputs");
        if (maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE || inputs.size() != admissions.size()) {
            throw new IllegalArgumentException(
                    "Target semantic closure needs finite bytes and every Admission image");
        }
        final Map<String, Admission> pending = new TreeMap<>();
        for (final var admission : admissions) {
            pending.put(Bytes.hex(admission.attempt().publishAttemptId()), admission);
        }
        final var builder = new Builder(this);
        long bytes = 0;
        for (final var input : inputs) {
            final var image = input.image();
            final var body = TargetPublishAdmissionBody.decode(image.canonicalBody());
            final var retained = pending.remove(Bytes.hex(body.publishAttemptId()));
            if (retained == null) {
                throw new IllegalArgumentException("Target semantic closure has an extra or duplicate Admission");
            }
            final var attempt = retained.attempt();
            final var first = SystemMutationResult.decode(retained.first().typedPayload());
            if (image.type() != com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || !Arrays.equals(retained.first().mutation().source().canonicalBytes(),
                        input.source().canonicalBytes())
                    || !Arrays.equals(Bytes.sha256(image.canonicalEnvelope()),
                        retained.first().mutation().mutationDigest())
                    || !Arrays.equals(first.mutationId(), image.systemMutationId())
                    || !Arrays.equals(first.mutationHash(), image.mutationHash())
                    || !Arrays.equals(first.authorIdentity(), image.authorIdentity())
                    || first.retryUntilEpochMs() != image.retryUntilEpochMs()
                    || !body.locator().equals(attempt.locator())
                    || !body.owner().equals(AuthorIdentity.decode(first.authorIdentity()).asOwnerIdentity())
                    || body.executionBytes() != attempt.executionBytes()
                    || !body.commitment().equals(attempt.commitment())
                    || !attempt.commitment().covers(body.allocated())
                    || attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                            && !body.allocated().equals(attempt.allocated())) {
                throw new IllegalArgumentException("Target semantic closure changes its original Admission proof");
            }
            bytes = Math.addExact(bytes, image.encodeFrame().length);
            if (bytes > maximumFrameBytes) {
                throw new IllegalArgumentException("Target semantic Admission frames exceed their byte bound");
            }
            builder.admissionImage(image);
        }
        return new TargetCheckpointDependencies(builder, admissions, true);
    }

    /** Deterministic local version inventory, not an authenticated control snapshot or publication authorization. */
    public byte[] semanticInputsDigest() {
        final byte[] fields = CanonicalProtobuf.message(out -> {
            for (final var profile : profiles) { CanonicalProtobuf.bytes(out, 1, profile.canonicalBytes()); }
            for (final var retry : retryPolicies) { CanonicalProtobuf.bytes(out, 2, retry.canonicalBytes()); }
            for (final var trust : trustSets) { CanonicalProtobuf.bytes(out, 3, trust.canonicalBytes()); }
            for (final var hash : objectProfileHashes) { CanonicalProtobuf.bytes(out, 4, hash); }
            for (final var digest : artifactDigests) { CanonicalProtobuf.bytes(out, 5, digest); }
            for (final var use : retryUses) { CanonicalProtobuf.bytes(out, 6, use.canonicalBytes()); }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-target-checkpoint-semantic-inputs\0"), fields);
    }

    /** Exact stored control inputs; providers must authenticate them and resolve all referenced immutable values. */
    public byte[] storedControlsDigest() {
        final byte[] fields = CanonicalProtobuf.message(out -> {
            for (final var control : controls) {
                CanonicalProtobuf.bytes(out, 1, CanonicalProtobuf.message(entry -> {
                    CanonicalProtobuf.bytes(entry, 1, Bytes.utf8(control.family().rocksName()));
                    CanonicalProtobuf.bytes(entry, 2, control.key());
                    CanonicalProtobuf.uint32(entry, 3, control.valueType());
                    CanonicalProtobuf.bytes(entry, 4, control.payload());
                }));
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-target-checkpoint-stored-controls\0"), fields);
    }

    private static List<byte[]> copies(List<byte[]> values) { return values.stream().map(Bytes::copy).toList(); }

    static final class Builder {
        private final TargetCheckpointRootVerifier.RootProof root;
        private final Map<String, StoredControl> controls = new TreeMap<>();
        private final Map<String, ProfileRef> profiles = new TreeMap<>();
        private final Map<String, RetryPolicyRef> retries = new TreeMap<>();
        private final Map<String, RetryUse> retryUses = new TreeMap<>();
        private final Map<String, PayloadProofTrustSetRef> trusts = new TreeMap<>();
        private final Map<String, byte[]> objectHashes = new TreeMap<>();
        private final Map<String, byte[]> artifacts = new TreeMap<>();

        Builder(TargetCheckpointRootVerifier.RootProof root) { this.root = root; }

        Builder(TargetCheckpointDependencies prior) {
            this(prior.root);
            for (final var control : prior.controls) { control(control.family(), control.key(),
                    control.valueType(), control.payload()); }
            prior.profiles.forEach(this::profile);
            prior.retryPolicies.forEach(this::retry);
            prior.retryUses.forEach(this::retryUse);
            prior.trustSets.forEach(this::trust);
            prior.objectProfileHashes.forEach(this::objectHash);
            prior.artifactDigests.forEach(this::artifact);
        }

        void control(ColumnFamily family, byte[] key, int type, byte[] payload) {
            controls.put(family.rocksName() + '/' + Bytes.hex(key), new StoredControl(family, key, type, payload));
        }

        void binding(TargetScheduleBinding binding) {
            profile(binding.intent().profile());
            retryUse(new RetryUse(binding.intent().retryPolicy(), binding.bindingSource()));
            if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
                final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
                profile(prepare.objectStoreProfile());
                trust(prepare.trustSet());
            } else if (!binding.intent().hasInlinePayload()) {
                profile(binding.intent().committedPayload().objectStoreProfile());
            }
        }

        void membership(TargetMembershipGrant grant) { profile(grant.memberProfile()); }

        void nativeScope(TargetNativePolicyScope scope) { artifact(scope.artifacts().digest()); }

        void admissionImage(SystemMutation image) {
            final var publication = TargetPublishAdmissionBody.decode(image.canonicalBody()).publication();
            if (publication == null) {
                throw new IllegalArgumentException(
                        "unmaterialized Target Admission requires protected historical closure");
            }
            profile(publication.destinationProfile());
            profile(publication.capabilityProfile());
            profile(publication.channel().credentialLease().profile());
            artifact(publication.artifactGenerationSetDigest());
        }

        void profile(ProfileRef profile) {
            final String identity = Bytes.hex(profile.profileId()) + '/' + unsigned(profile.version());
            final var old = profiles.putIfAbsent(identity, profile);
            if (old != null && !old.equals(profile)) {
                throw new IllegalArgumentException("conflicting immutable Target Profile version");
            }
        }

        void retry(RetryPolicyRef retry) {
            final String identity = Bytes.hex(retry.policyId()) + '/' + unsigned(retry.version());
            final var old = retries.putIfAbsent(identity, retry);
            if (old != null && !old.equals(retry)) {
                throw new IllegalArgumentException("conflicting immutable Target Retry Policy version");
            }
        }

        void retryUse(RetryUse use) {
            retry(use.reference());
            if (!root.metadata().shardId().equals(use.source().shardId())) {
                throw new IllegalArgumentException("Target Retry Policy use belongs to another Source Shard");
            }
            retryUses.put(Bytes.hex(use.canonicalBytes()), use);
        }

        void trust(PayloadProofTrustSetRef trust) {
            final var old = trusts.putIfAbsent(unsigned(trust.version()), trust);
            if (old != null && !old.equals(trust)) {
                throw new IllegalArgumentException("conflicting immutable Target Payload Trust Set version");
            }
        }

        void objectHash(byte[] hash) { objectHashes.put(Bytes.hex(hash), Bytes.copy(hash)); }
        void artifact(byte[] digest) { artifacts.put(Bytes.hex(digest), Bytes.copy(digest)); }

        TargetCheckpointDependencies finish(List<Admission> admissions) {
            return new TargetCheckpointDependencies(this, admissions, admissions.isEmpty());
        }

        private static String unsigned(long value) { return Bytes.hex(Bytes.u64beBits(value)); }
    }
}
