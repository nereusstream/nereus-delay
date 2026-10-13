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
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.RetryPolicyCatalog;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
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

    /** Derived original use from actual image records; never substitutes the later checkpoint Source. */
    public record ProfileUse(ProfileRef reference, SourcePosition source) {
        public ProfileUse {
            Objects.requireNonNull(reference, "reference");
            TargetSourcePosition.requireBounded(source);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, reference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, source.canonicalBytes());
            });
        }
    }

    private final TargetCheckpointRootVerifier.RootProof root;
    private final List<StoredControl> controls;
    private final List<TargetResultRecord> controlResults;
    private final List<TargetResultRecord> outcomeResults;
    private final List<Admission> admissions;
    private final List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings;
    private final List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence;
    private final List<ProfileRef> profiles;
    private final List<ProfileUse> profileUses;
    private final List<TargetScheduleBinding> bindings;
    private final List<RetryPolicyRef> retryPolicies;
    private final List<RetryUse> retryUses;
    private final List<PayloadProofTrustSetRef> trustSets;
    private final List<byte[]> objectProfileHashes;
    private final List<byte[]> artifactDigests;
    private final boolean admissionImagesComplete;

    private TargetCheckpointDependencies(Builder builder, List<Admission> admissions, boolean complete) {
        root = builder.root;
        controls = List.copyOf(builder.controls.values());
        controlResults = List.copyOf(builder.controlResults.values());
        outcomeResults = List.copyOf(builder.outcomeResults.values());
        this.admissions = List.copyOf(admissions);
        kafkaMappings = List.copyOf(builder.kafkaMappings.values());
        externalEvidence = List.copyOf(builder.externalEvidence.values());
        profiles = List.copyOf(builder.profiles.values());
        profileUses = List.copyOf(builder.profileUses.values());
        bindings = List.copyOf(builder.bindings.values());
        retryPolicies = List.copyOf(builder.retries.values());
        retryUses = List.copyOf(builder.retryUses.values());
        trustSets = List.copyOf(builder.trusts.values());
        objectProfileHashes = copies(builder.objectHashes.values().stream().toList());
        artifactDigests = copies(builder.artifacts.values().stream().toList());
        admissionImagesComplete = complete;
    }

    public TargetCheckpointRootVerifier.RootProof root() { return root; }
    public List<StoredControl> controls() { return controls; }
    /** Every retained APPLY_SHARD_CONTROL first result, including durable rejections. */
    public List<TargetResultRecord> controlResults() { return controlResults; }
    public List<ControlReference> controlReferences() {
        return controlResults.stream().map(ControlReference::new).toList();
    }
    public List<TargetResultRecord> outcomeResults() { return outcomeResults; }
    public List<OutcomeReference> outcomeReferences() {
        return outcomeResults.stream().map(OutcomeReference::new).toList();
    }
    public List<Admission> admissions() { return admissions; }
    public List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings() { return kafkaMappings; }
    public List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence() { return externalEvidence; }

    /** The actual signed catalog object; neither copied lengths nor Target aggregate counters can satisfy this lookup. */
    @FunctionalInterface
    public interface ExternalEvidenceAuthority {
        com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact resolve(
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner original);
    }
    public record ExternalEvidenceImage(String exactKey, com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact artifact) {
        public ExternalEvidenceImage { Objects.requireNonNull(exactKey, "exact external key"); Objects.requireNonNull(artifact, "actual artifact"); }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out,1,Bytes.utf8(exactKey)); CanonicalProtobuf.bytes(out,2,artifact.canonicalBytes());
        }); }
        public static ExternalEvidenceImage decode(byte[] bytes) {
            final var fields = com.nereusstream.delay.protocol.QueryCodecSupport.read(bytes, "external checkpoint evidence");
            com.nereusstream.delay.protocol.QueryCodecSupport.requireNumbers(fields,new int[]{1,2},"external checkpoint evidence");
            final var image = new ExternalEvidenceImage(new String(com.nereusstream.delay.protocol.QueryCodecSupport.bytes(fields.get(0),1),
                    java.nio.charset.StandardCharsets.UTF_8), com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.decode(
                    com.nereusstream.delay.protocol.QueryCodecSupport.nested(fields.get(1),2)));
            com.nereusstream.delay.protocol.QueryCodecSupport.requireCanonical(bytes,image.canonicalBytes(),"external checkpoint evidence");return image;
        }
    }
    public List<ExternalEvidenceImage> resolveExternalEvidence(ExternalEvidenceAuthority authority,
            BoundedReadBudget budget, Runnable guard) {
        if (!externalEvidence.isEmpty()) { Objects.requireNonNull(authority,"authenticated retained external catalog lookup"); }
        final var values = new java.util.ArrayList<ExternalEvidenceImage>();
        for (var owner : externalEvidence) {
            beforeAuthentication(budget,guard);
            final var actual = Objects.requireNonNull(authority.resolve(owner),"original actual external object");
            guard.run(); owner.requireObject(actual.kind(),actual.digest(),owner.exactKey(),actual.canonicalBytes());
            if (!budget.tryCharge(owner.exactKey().getBytes(java.nio.charset.StandardCharsets.UTF_8).length,
                    actual.canonicalBytes().length)) { throw budget.incomplete(); }
            values.add(new ExternalEvidenceImage(owner.exactKey(),actual)); afterAuthentication(budget,guard);
        }
        requireExternalEvidenceImages(values); return List.copyOf(values);
    }
    public void requireExternalEvidenceImages(List<ExternalEvidenceImage> images) {
        final var byKey = new TreeMap<String,ExternalEvidenceImage>();
        for (var image : images) { if(byKey.put(image.exactKey(),image)!=null)throw new IllegalArgumentException("duplicate external catalog object"); }
        if(byKey.size()!=externalEvidence.size())throw new IllegalStateException("checkpoint lacks complete actual external evidence bytes");
        for(var owner:externalEvidence){final var image=Objects.requireNonNull(byKey.get(owner.exactKey()),"original retained external key");
            owner.requireObject(image.artifact().kind(),image.artifact().digest(),image.exactKey(),image.artifact().canonicalBytes());
            if(!Arrays.equals(owner.originalControlDigest(),Bytes.sha256(image.artifact().control().canonicalBytes()))
                    ||!Arrays.equals(owner.originalControlSource().canonicalBytes(),image.artifact().authorizedAt().canonicalBytes()))
                throw new IllegalStateException("external checkpoint evidence changed original CONTROL/Source responsibility");
        }
    }

    public List<ProfileRef> profiles() { return profiles; }
    public List<ProfileUse> profileUses() { return profileUses; }
    public TargetScheduleBinding requireBinding(TargetMessageLocator locator) {
        final var binding = bindings.stream().filter(value -> Arrays.equals(
                value.digest(), locator.scheduleBindingDigest()))
                .findFirst().orElseThrow(() -> new IllegalStateException("Outcome lacks original Schedule binding"));
        binding.requireLocator(locator);
        return binding;
    }
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

    /** Original signed Source input; a matching local digest does not authenticate its issuer or history. */
    public record ControlImage(SourcePosition source, SystemMutation image) {
        public ControlImage {
            TargetSourcePosition.requireBounded(source);
            Objects.requireNonNull(image, "image");
            if (image.type() != SystemMutationType.APPLY_SHARD_CONTROL || !source.shardId().equals(image.shardId())) {
                throw new IllegalArgumentException("Target control image has another Source/type");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, source.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, image.encodeFrame());
            });
        }
    }

    /** Exact original input locator derived only from this complete actual ledger fold. No SEND/GC authority. */
    public static final class ControlReference {
        private final TargetResultRecord first;

        private ControlReference(TargetResultRecord first) { this.first = first; }

        public SourcePosition source() { return first.mutation().source(); }
        public TargetResultRecord first() { return first; }
        public byte[] envelopeDigest() { return first.mutation().mutationDigest(); }

        public SystemMutation requireImage(SystemMutation image, SourcePosition actualSource) {
            Objects.requireNonNull(image, "image");
            TargetSourcePosition.requireBounded(actualSource);
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (image.type() != SystemMutationType.APPLY_SHARD_CONTROL
                    || !Arrays.equals(actualSource.canonicalBytes(), source().canonicalBytes())
                    || !Arrays.equals(Bytes.sha256(image.canonicalEnvelope()), envelopeDigest())
                    || !Arrays.equals(result.mutationId(), image.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), image.mutationHash())
                    || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                    || !Arrays.equals(result.appliedSourcePosition(), actualSource.canonicalBytes())
                    || result.retryUntilEpochMs() != image.retryUntilEpochMs()) {
                throw new IllegalArgumentException("Target Control input differs from its exact first result/Source");
            }
            return image;
        }
    }

    public record OutcomeImage(SourcePosition source, SystemMutation image) {
        public OutcomeImage {
            TargetSourcePosition.requireBounded(source);
            Objects.requireNonNull(image, "image");
            if (image.type() != SystemMutationType.PUBLISH_OUTCOME
                    && image.type() != SystemMutationType.EVIDENCE_RESOLUTION
                    || !source.shardId().equals(image.shardId())) {
                throw new IllegalArgumentException("Target Outcome image has another Source/type");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, source.canonicalBytes());
                CanonicalProtobuf.bytes(out, 2, image.encodeFrame());
            });
        }
    }

    /** Original frame locator from an actual complete ledger fold, including durable rejections. */
    public static final class OutcomeReference {
        private final TargetResultRecord first;
        private OutcomeReference(TargetResultRecord first) { this.first = first; }
        public TargetResultRecord first() { return first; }
        public SourcePosition source() { return first.mutation().source(); }
        public byte[] envelopeDigest() { return first.mutation().mutationDigest(); }

        public SystemMutation requireImage(SystemMutation image, SourcePosition actualSource) {
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (image == null || image.type() != result.mutationType()
                    || !Arrays.equals(actualSource.canonicalBytes(), source().canonicalBytes())
                    || !Arrays.equals(Bytes.sha256(image.canonicalEnvelope()), envelopeDigest())
                    || !Arrays.equals(result.mutationId(), image.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), image.mutationHash())
                    || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                    || !Arrays.equals(result.appliedSourcePosition(), actualSource.canonicalBytes())
                    || result.retryUntilEpochMs() != image.retryUntilEpochMs()) {
                throw new IllegalArgumentException("Target Outcome input differs from its exact first result/Source");
            }
            return image;
        }
    }

    /** Must authenticate original history/Owner-service/key/tuple/time and exact evidence/Admission obligations. */
    @FunctionalInterface
    public interface OutcomeImageAuthority {
        java.security.PublicKey authenticate(OutcomeImage input, TargetResultRecord first);
    }

    List<OutcomeImage> correlateOutcomeImages(List<OutcomeImage> inputs, long maximumFrameBytes) {
        if (maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE || inputs.size() != outcomeResults.size()) {
            throw new IllegalArgumentException("Target Outcome closure needs finite bytes and every original input");
        }
        final var pending = new TreeMap<String, OutcomeReference>();
        outcomeReferences().forEach(ref -> pending.put(Bytes.hex(
                SystemMutationResult.decode(ref.first().typedPayload()).mutationId()), ref));
        final var sorted = new TreeMap<String, OutcomeImage>();
        long bytes = 0;
        for (var input : inputs) {
            final String id = Bytes.hex(input.image().systemMutationId());
            final var expected = pending.remove(id);
            if (expected == null) { throw new IllegalArgumentException("extra or duplicate original Outcome"); }
            expected.requireImage(input.image(), input.source());
            bytes = Math.addExact(bytes, input.image().encodeFrame().length);
            if (bytes > maximumFrameBytes) { throw new IllegalArgumentException("Outcome frame bound exceeded"); }
            sorted.put(id, input);
        }
        return List.copyOf(sorted.values());
    }

    public List<OutcomeImage> requireOutcomeImages(List<OutcomeImage> inputs, long maximumFrameBytes,
            BoundedReadBudget budget, OutcomeImageAuthority authority, Runnable protection) {
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(protection, "protection").run();
        final var checked = correlateOutcomeImages(inputs, maximumFrameBytes);
        final var results = new TreeMap<String, TargetResultRecord>();
        outcomeResults.forEach(first -> results.put(Bytes.hex(
                SystemMutationResult.decode(first.typedPayload()).mutationId()), first));
        for (var input : checked) {
            beforeAuthentication(budget, protection);
            if (!budget.tryCharge(input.source().canonicalBytes().length, input.image().encodeFrame().length)) {
                throw budget.incomplete();
            }
            final var first = results.get(Bytes.hex(input.image().systemMutationId()));
            final var key = authority.authenticate(input, first);
            afterAuthentication(budget, protection);
            if (SystemMutationResult.decode(first.typedPayload()).applyStatus()
                    == com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                    && (key == null || !input.image().verifySignature(key))) {
                throw new IllegalArgumentException("original APPLIED Outcome lacks authorized signature/evidence");
            }
        }
        afterAuthentication(budget, protection);
        return checked;
    }

    /**
     * Authenticates original Source history/protection outside Store locks. APPLIED requires its authorized issuer
     * key/Control author; a durable rejection may retain an invalid signature/author and return no issuer key.
     */
    @FunctionalInterface
    public interface ControlImageAuthority {
        java.security.PublicKey authenticate(ControlImage input, TargetResultRecord first);
    }

    /**
     * Joins every retained Control first result to its original signed envelope under finite bytes/time/records.
     * Call outside Store locks; the provider owns I/O deadlines and the guard retains the complete protection set.
     * This does not reconstruct registration/RBAC authority or replace later control/Outcome/pin obligations.
     */
    public List<ControlImage> requireControlImages(final List<ControlImage> inputs, final long maximumFrameBytes,
            final BoundedReadBudget budget, final ControlImageAuthority authority, final Runnable protectionGuard) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        final var correlated = correlateControlImages(inputs, maximumFrameBytes);
        final Map<String, TargetResultRecord> pending = new TreeMap<>();
        for (final var first : controlResults) {
            pending.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()), first);
        }
        for (final var input : correlated) {
            protectionGuard.run();
            if (!budget.beforeRead()) { throw budget.incomplete(); }
            final var image = input.image();
            final var first = pending.remove(Bytes.hex(image.systemMutationId()));
            final var result = SystemMutationResult.decode(first.typedPayload());
            final byte[] frame = image.encodeFrame();
            if (!budget.tryCharge(input.source().canonicalBytes().length, frame.length)) { throw budget.incomplete(); }
            final var key = authority.authenticate(input, first);
            if (result.applyStatus() == com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                    && (key == null || !image.verifySignature(key))) {
                throw new IllegalArgumentException("Target Control input signature invalid");
            }
            protectionGuard.run();
            if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
        }
        protectionGuard.run();
        return correlated;
    }

    /** Offline correlation only. Issuer/history authentication must be repeated by restore/publication authority. */
    List<ControlImage> correlateControlImages(final List<ControlImage> inputs, final long maximumFrameBytes) {
        Objects.requireNonNull(inputs, "inputs");
        if (maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE || inputs.size() != controlResults.size()) {
            throw new IllegalArgumentException("Target control closure requires finite bytes and every first result");
        }
        final Map<String, TargetResultRecord> pending = new TreeMap<>();
        for (final var first : controlResults) {
            pending.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()), first);
        }
        final Map<String, ControlImage> complete = new TreeMap<>();
        long bytes = 0;
        for (final var input : inputs) {
            final var image = input.image();
            final var first = pending.remove(Bytes.hex(image.systemMutationId()));
            if (first == null) { throw new IllegalArgumentException("extra or duplicate Target Control input"); }
            new ControlReference(first).requireImage(image, input.source());
            bytes = Math.addExact(bytes, image.encodeFrame().length);
            if (bytes > maximumFrameBytes) {
                throw new IllegalArgumentException("Target Control inputs exceed their byte bound");
            }
            complete.put(Bytes.hex(image.systemMutationId()), input);
        }
        return List.copyOf(complete.values());
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
            final var mapping = builder.kafkaMappings.get(Bytes.hex(body.publishAttemptId()));
            if (body.kafkaReceipt() != null) {
                if (mapping == null) { throw new IllegalArgumentException("Target v6 Admission lacks its retained immutable K2 mapping"); }
                mapping.requireOriginal(image, input.source());
            } else if (mapping != null) {
                throw new IllegalArgumentException("Target K2 mapping cannot supplement a legacy Admission context");
            }
            builder.admissionImage(input);
        }
        return new TargetCheckpointDependencies(builder, admissions, true);
    }

    /** Authenticates original Broker provenance and retains history/credential/pin protection. */
    @FunctionalInterface
    public interface AdmissionHistoryAuthority {
        void requireOriginal(AdmissionImage input, TargetResultRecord first);
    }

    /**
     * Reauthenticates every retained Admission at its first Source, including UNKNOWN/resolved Budgets.
     * Providers run outside Store locks with finite deadlines. Failure retains all original obligations;
     * successful Owner/key/tuple/time/materialization checks grant no SEND or publication/install rights.
     */
    public TargetCheckpointDependencies authenticateAdmissionImages(final List<AdmissionImage> inputs,
            final long maximumFrameBytes, final BoundedReadBudget budget,
            final AdmissionHistoryAuthority history, final TargetPublishAdmissionVerifier.Authority authority,
            final Runnable protectionGuard) {
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(history, "history");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        final var complete = withAdmissionImages(inputs, maximumFrameBytes);
        final var pending = new TreeMap<String, Admission>();
        admissions.forEach(retained -> pending.put(Bytes.hex(retained.attempt().publishAttemptId()), retained));
        for (var input : inputs) {
            final var body = TargetPublishAdmissionBody.decode(input.image().canonicalBody());
            final var retained = pending.remove(Bytes.hex(body.publishAttemptId()));
            beforeAuthentication(budget, protectionGuard);
            if (!budget.tryCharge(input.source().canonicalBytes().length, input.image().encodeFrame().length)) {
                throw budget.incomplete();
            }
            history.requireOriginal(input, retained.first());
            afterAuthentication(budget, protectionGuard);
            final var decision = TargetPublishAdmissionVerifier.decideFirstApplication(root.root().scope(),
                    input.image(), input.source(), (scope, writer, mutation, source) -> {
                        beforeAuthentication(budget, protectionGuard);
                        final var resolved = authority.resolve(scope, writer, mutation, source);
                        afterAuthentication(budget, protectionGuard);
                        return resolved;
                    });
            afterAuthentication(budget, protectionGuard);
            if (decision.rejection() != null) {
                throw new IllegalArgumentException("retained Admission is unauthorized at its original Source: "
                        + decision.rejection());
            }
        }
        afterAuthentication(budget, protectionGuard);
        return complete;
    }

    private static void beforeAuthentication(BoundedReadBudget budget, Runnable protection) {
        protection.run();
        if (!budget.beforeRead()) { throw budget.incomplete(); }
    }

    private static void afterAuthentication(BoundedReadBudget budget, Runnable protection) {
        protection.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
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
            for (final var owner : externalEvidence) { CanonicalProtobuf.bytes(out, 7, owner.canonicalBytes()); }
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
        private final Map<String, TargetResultRecord> controlResults = new TreeMap<>();
        private final Map<String, TargetResultRecord> outcomeResults = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence = new TreeMap<>();
        private final Map<String, ProfileRef> profiles = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings = new TreeMap<>();
        private final Map<String, ProfileUse> profileUses = new TreeMap<>();
        private final Map<String, TargetScheduleBinding> bindings = new TreeMap<>();
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
            prior.controlResults.forEach(this::controlResult);
            prior.outcomeResults.forEach(this::outcomeResult);
            prior.kafkaMappings.forEach(this::kafkaMapping);
            prior.externalEvidence.forEach(this::externalEvidence);
            prior.profiles.forEach(this::profile);
            prior.profileUses.forEach(this::profileUse);
            prior.bindings.forEach(this::binding);
            prior.retryPolicies.forEach(this::retry);
            prior.retryUses.forEach(this::retryUse);
            prior.trustSets.forEach(this::trust);
            prior.objectProfileHashes.forEach(this::objectHash);
            prior.artifactDigests.forEach(this::artifact);
        }

        void externalEvidence(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner owner) {
            final var previous=externalEvidence.putIfAbsent(Bytes.hex(owner.key()),owner);
            if(previous!=null&&!Arrays.equals(previous.canonicalBytes(),owner.canonicalBytes()))throw new IllegalStateException("duplicate external evidence owner");
        }
        void kafkaMapping(com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping) {
            final String attempt = Bytes.hex(mapping.admission().publishAttemptId());
            final var previous = kafkaMappings.putIfAbsent(attempt, mapping);
            if (previous != null && !Arrays.equals(previous.canonicalBytes(), mapping.canonicalBytes())) {
                throw new IllegalStateException("Target checkpoint has divergent mappings for one K2 attempt");
            }
        }

        void control(ColumnFamily family, byte[] key, int type, byte[] payload) {
            controls.put(family.rocksName() + '/' + Bytes.hex(key), new StoredControl(family, key, type, payload));
        }

        void controlResult(TargetResultRecord first) {
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.primaryIdentity().equals(root.root().identity())
                    || result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                    || result.applyStatus() == com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                            && AuthorIdentity.decode(result.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL) {
                throw new IllegalArgumentException("Target Control reference differs from its root/type/author");
            }
            controlResults.put(Bytes.hex(first.key()), first);
        }

        void outcomeResult(TargetResultRecord first) {
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.mutation().source().shardId().equals(root.metadata().shardId())
                    || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME
                            && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION) {
                throw new IllegalArgumentException("Target Outcome reference differs from its Source/type");
            }
            outcomeResults.put(Bytes.hex(first.key()), first);
        }

        void binding(TargetScheduleBinding binding) {
            bindings.put(Bytes.hex(binding.digest()), binding);
            profileUse(new ProfileUse(binding.intent().profile(), binding.bindingSource()));
            retryUse(new RetryUse(binding.intent().retryPolicy(), binding.bindingSource()));
            if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
                final var prepare = PrepareLargeScheduleBody.decode(binding.canonicalBody());
                profileUse(new ProfileUse(prepare.objectStoreProfile(), binding.bindingSource()));
                trust(prepare.trustSet());
            } else if (!binding.intent().hasInlinePayload()) {
                profileUse(new ProfileUse(binding.intent().committedPayload().objectStoreProfile(),
                        binding.bindingSource()));
            }
        }

        void membership(TargetMembershipGrant grant) {
            profileUse(new ProfileUse(grant.memberProfile(), grant.activationSource()));
        }

        void nativeScope(TargetNativePolicyScope scope) { artifact(scope.artifacts().digest()); }

        void admissionImage(AdmissionImage input) {
            final var body = TargetPublishAdmissionBody.decode(input.image().canonicalBody());
            if (!body.materialized()) {
                throw new IllegalArgumentException(
                        "unmaterialized Target Admission requires protected historical closure");
            }
            if (body.nativePublication() != null) {
                final var publication = body.nativePublication();
                profileUse(new ProfileUse(publication.destinationProfile(), input.source()));
                profileUse(new ProfileUse(publication.capabilityProfile(), input.source()));
                profileUse(new ProfileUse(publication.channel().credentialLease().profile(), input.source()));
                artifact(publication.artifactGenerationSetDigest());
                artifact(publication.policyScope().artifacts().digest());
            } else {
                final var publication = body.publication();
                profileUse(new ProfileUse(publication.destinationProfile(), input.source()));
                profileUse(new ProfileUse(publication.capabilityProfile(), input.source()));
                profileUse(new ProfileUse(publication.channel().credentialLease().profile(), input.source()));
                artifact(publication.artifactGenerationSetDigest());
            }
        }

        void profileUse(ProfileUse use) {
            if (!root.metadata().shardId().equals(use.source().shardId())
                    || !root.source().sameSourceIdentity(use.source()) || root.source().kind() != use.source().kind()) {
                throw new IllegalArgumentException("Target Profile use belongs to another Source identity");
            }
            profile(use.reference());
            profileUses.put(Bytes.hex(use.canonicalBytes()), use);
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
