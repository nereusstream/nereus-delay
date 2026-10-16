package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RetryPolicySemantic;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.PayloadProofTrustSetControlCatalog;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.runtime.RetryPolicyCatalog;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Closed Target semantic contents and original Admission frames; external control/pin/authentication is required. */
public final class TargetCheckpointSemanticSnapshot {
    public static final int MAX_SCHEMA_VERSION=6;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-checkpoint-semantic-snapshot\0");

    public record Limits(int maximumRecords, int maximumBytes, long maximumAdmissionFrameBytes) {
        public Limits {
            if (maximumRecords <= 0 || maximumRecords == Integer.MAX_VALUE || maximumBytes <= 0
                    || maximumBytes == Integer.MAX_VALUE || maximumAdmissionFrameBytes <= 0
                    || maximumAdmissionFrameBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target semantic snapshot requires finite positive limits");
            }
        }
    }

    private final int version;
    private final StoreMetadata metadata;
    private final SourcePosition source;
    private final long mutationSequence;
    private final byte[] lineage;
    private final byte[] controlsDigest;
    private final byte[] dependenciesDigest;
    private final List<ProfileSemanticEnvelope> profiles;
    private final List<RetryPolicySemantic> retries;
    private final List<PayloadProofTrustSetSemantic> trusts;
    private final List<TargetCheckpointDependencies.AdmissionImage> admissions;
    private final List<TargetCheckpointDependencies.ControlImage> controls;
    private final List<TargetCheckpointDependencies.OutcomeImage> outcomes;
    private final List<TargetCheckpointDependencies.ExternalEvidenceImage> externalEvidence;
    private final List<TargetCheckpointDependencies.RecoveryEvidenceImage> recoveryEvidence;
    private final TargetCheckpointLifecycleInputs lifecycle;
    private final byte[] digest;

    private TargetCheckpointSemanticSnapshot(int version, StoreMetadata metadata, SourcePosition source,
            long mutationSequence,
            byte[] lineage, byte[] controlsDigest, byte[] dependenciesDigest, List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries, List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,
            List<TargetCheckpointDependencies.ControlImage> controls,
            List<TargetCheckpointDependencies.OutcomeImage> outcomes, Limits limits) {
        this(version,metadata,source,mutationSequence,lineage,controlsDigest,dependenciesDigest,profiles,retries,trusts,
                admissions,controls,outcomes,List.of(),limits);
    }

    private TargetCheckpointSemanticSnapshot(int version, StoreMetadata metadata, SourcePosition source,
            long mutationSequence,
            byte[] lineage, byte[] controlsDigest, byte[] dependenciesDigest, List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries, List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,
            List<TargetCheckpointDependencies.ControlImage> controls,
            List<TargetCheckpointDependencies.OutcomeImage> outcomes,
            List<TargetCheckpointDependencies.ExternalEvidenceImage> externalEvidence, Limits limits) {
        this(version,metadata,source,mutationSequence,lineage,controlsDigest,dependenciesDigest,profiles,retries,trusts,
                admissions,controls,outcomes,externalEvidence,List.of(),limits);
    }
    private TargetCheckpointSemanticSnapshot(int version,StoreMetadata metadata,SourcePosition source,long mutationSequence,
            byte[] lineage,byte[] controlsDigest,byte[] dependenciesDigest,List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries,List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,List<TargetCheckpointDependencies.ControlImage> controls,
            List<TargetCheckpointDependencies.OutcomeImage> outcomes,List<TargetCheckpointDependencies.ExternalEvidenceImage> externalEvidence,
            List<TargetCheckpointDependencies.RecoveryEvidenceImage> recoveryEvidence,Limits limits){
        this(version,metadata,source,mutationSequence,lineage,controlsDigest,dependenciesDigest,profiles,retries,trusts,
                admissions,controls,outcomes,externalEvidence,recoveryEvidence,null,limits);
    }
    private TargetCheckpointSemanticSnapshot(int version,StoreMetadata metadata,SourcePosition source,long mutationSequence,
            byte[] lineage,byte[] controlsDigest,byte[] dependenciesDigest,List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries,List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,List<TargetCheckpointDependencies.ControlImage> controls,
            List<TargetCheckpointDependencies.OutcomeImage> outcomes,List<TargetCheckpointDependencies.ExternalEvidenceImage> externalEvidence,
            List<TargetCheckpointDependencies.RecoveryEvidenceImage> recoveryEvidence,TargetCheckpointLifecycleInputs lifecycle,Limits limits){
        if ((version==6)!=(lifecycle!=null))throw new IllegalArgumentException("Target lifecycle companion requires explicit schema6");
        this.lifecycle=lifecycle;
        if (version < 1 || version > MAX_SCHEMA_VERSION || version == 1 && !controls.isEmpty()
                || version < 3 && !outcomes.isEmpty() || version < 4 && !externalEvidence.isEmpty()||version<5&&!recoveryEvidence.isEmpty()) {
            throw new IllegalArgumentException("invalid Target semantic snapshot version/control inputs");
        }
        this.version = version;
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.source = Objects.requireNonNull(source, "source");
        if (metadata.storeFormatVersion() != 2 || !metadata.shardId().equals(source.shardId())) {
            throw new IllegalArgumentException("Target semantic snapshot has an invalid Store/Source identity");
        }
        this.mutationSequence = mutationSequence;
        Bytes.requireLength(lineage, 16, "lineage");
        Bytes.requireLength(controlsDigest, 32, "controlsDigest");
        Bytes.requireLength(dependenciesDigest, 32, "dependenciesDigest");
        this.lineage = Bytes.copy(lineage);
        this.controlsDigest = Bytes.copy(controlsDigest);
        this.dependenciesDigest = Bytes.copy(dependenciesDigest);
        this.profiles = ordered(profiles, value -> value.ref().canonicalBytes());
        final var immutable = new java.util.HashSet<String>();
        for (final var profile : profiles) {
            if (!immutable.add(Bytes.hex(profile.profileId()) + '/' + Long.toUnsignedString(profile.version()))) {
                throw new IllegalArgumentException("conflicting immutable Profile version in Target semantic snapshot");
            }
        }
        this.retries = ordered(retries, value -> value.ref().canonicalBytes());
        this.trusts = ordered(trusts, value -> value.ref().canonicalBytes());
        this.admissions = ordered(admissions, TargetCheckpointDependencies.AdmissionImage::canonicalBytes);
        this.controls = ordered(controls, value -> value.image().systemMutationId());
        this.outcomes = ordered(outcomes, value -> value.image().systemMutationId());
        this.externalEvidence = ordered(externalEvidence, value -> Bytes.utf8(value.exactKey()));
        this.recoveryEvidence = ordered(recoveryEvidence,value->Bytes.utf8(value.exactKey()));
        if ((long) profiles.size() + retries.size() + trusts.size() + admissions.size()
                + controls.size() + outcomes.size() + externalEvidence.size()+recoveryEvidence.size()+(lifecycle==null?0:lifecycle.recordCount())
                > limits.maximumRecords()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its record bound");
        }
        long frameBytes = 0;
        for (final var admission : admissions) {
            frameBytes = Math.addExact(frameBytes, admission.image().encodeFrame().length);
        }
        if (frameBytes > limits.maximumAdmissionFrameBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its Admission frame bound");
        }
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > limits.maximumBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds its byte bound");
        }
    }

    /** Providers run outside Store locks with finite deadlines; the guard protects the complete input set. */
    public static TargetCheckpointSemanticSnapshot collect(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.AdmissionImage> admissions, ProfileCatalog profiles,
            RetryPolicyCatalog retries, PayloadProofTrustSetControlCatalog trusts, BoundedReadBudget budget,
            Limits limits, Runnable protectionGuard) {
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        final var guard = Objects.requireNonNull(protectionGuard, "protectionGuard");
        guard.run();
        final var profileValues = new TreeMap<String, ProfileSemanticEnvelope>();
        final var pending = new TreeMap<String, ProfileRef>();
        complete.profiles().forEach(ref -> pending.put(Bytes.hex(ref.canonicalBytes()), ref));
        while (!pending.isEmpty()) {
            final var ref = pending.pollFirstEntry().getValue();
            if (profileValues.containsKey(Bytes.hex(ref.canonicalBytes()))) { continue; }
            before(budget, guard);
            final var value = profiles.resolve(ref);
            guard.run();
            if (value == null || !value.ref().equals(ref)) {
                throw new IllegalStateException("Target semantic snapshot lacks an exact Profile value");
            }
            charge(budget, ref.canonicalBytes().length, value.canonicalBytes().length);
            profileValues.put(Bytes.hex(ref.canonicalBytes()), value);
            if (value.body() instanceof DestinationProfileSemantic destination) {
                final var capability = destination.deliveryCapability();
                pending.put(Bytes.hex(capability.canonicalBytes()), capability);
            }
        }
        final var retryValues = complete.resolveRetryPolicies(retries, budget, guard);
        final var trustValues = new ArrayList<PayloadProofTrustSetSemantic>();
        for (final var ref : complete.trustSets()) {
            before(budget, guard);
            final var value = trusts.resolve(ref);
            guard.run();
            if (value == null || !value.ref().equals(ref)) {
                throw new IllegalStateException("Target semantic snapshot lacks an exact Trust Set value");
            }
            charge(budget, ref.canonicalBytes().length, value.canonicalBytes().length);
            trustValues.add(value);
        }
        guard.run();
        return fromCollectedValues(dependencies, admissions, List.copyOf(profileValues.values()),
                retryValues, trustValues, limits, guard);
    }

    /** Complete bounded public semantics collected on the original cut; external authority is still mandatory. */
    public static TargetCheckpointSemanticSnapshot fromCollectedValues(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.AdmissionImage> admissions, List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries, List<PayloadProofTrustSetSemantic> trusts, Limits limits,
            Runnable protectionGuard) {
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        final var root = complete.root();
        final var snapshot = new TargetCheckpointSemanticSnapshot(1, root.metadata(), root.source(),
                root.mutationSequence(), root.root().recoveryLineage(), complete.storedControlsDigest(),
                complete.semanticInputsDigest(), profiles, retries, trusts, admissions, List.of(), List.of(), limits);
        snapshot.validateAgainst(dependencies, limits);
        protectionGuard.run();
        return snapshot;
    }

    /**
     * Creates schema 2 with every original retained Control input on this exact Store cut. External history/issuer
     * authentication runs outside Store locks and must remain protected through candidate placement/publication.
     * Schema 1 remains a readable unpublished component snapshot; neither schema grants publication authority.
     */
    public TargetCheckpointSemanticSnapshot withControlInputs(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> inputs, BoundedReadBudget budget, Limits limits,
            TargetCheckpointDependencies.ControlImageAuthority authority, Runnable protectionGuard) {
        validateAgainst(dependencies, limits);
        final var checked = dependencies.requireControlImages(inputs, limits.maximumBytes(), budget, authority,
                protectionGuard);
        final var result = new TargetCheckpointSemanticSnapshot(Math.max(version, 2), metadata, source,
                mutationSequence, lineage,
                controlsDigest, dependenciesDigest, profiles, retries, trusts, admissions, checked, outcomes, externalEvidence,recoveryEvidence,lifecycle, limits);
        result.validateAgainst(dependencies, limits);
        protectionGuard.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
        return result;
    }

    /** Schema 3 retains every original Outcome input, including durable rejections; requires external authority. */
    public TargetCheckpointSemanticSnapshot withOutcomeInputs(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.OutcomeImage> inputs, BoundedReadBudget budget, Limits limits,
            TargetCheckpointDependencies.OutcomeImageAuthority authority, Runnable protectionGuard) {
        if (version < 2) { throw new IllegalArgumentException("Outcome snapshot requires complete original Controls"); }
        validateAgainst(dependencies, limits);
        final var checked = dependencies.requireOutcomeImages(inputs, limits.maximumBytes(), budget, authority,
                protectionGuard);
        final var result = new TargetCheckpointSemanticSnapshot(Math.max(version, 3), metadata, source, mutationSequence, lineage,
                controlsDigest, dependenciesDigest, profiles, retries, trusts, admissions, controls, checked, externalEvidence,recoveryEvidence,lifecycle, limits);
        result.validateAgainst(dependencies, limits);
        protectionGuard.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
        return result;
    }

    public void authenticateOutcomes(TargetCheckpointDependencies dependencies, Limits limits,
            BoundedReadBudget budget, TargetCheckpointDependencies.OutcomeImageAuthority authority,
            Runnable protectionGuard) {
        if (version < 3) { throw new IllegalArgumentException("Outcome authentication requires schema 3 or later"); }
        validateAgainst(dependencies, limits);
        dependencies.requireOutcomeImages(outcomes, limits.maximumBytes(), budget, authority, protectionGuard);
    }

    /** Schema 4 carries the complete physical objects once, with original CONTROL signatures embedded in their bytes. */
    public TargetCheckpointSemanticSnapshot withExternalEvidence(TargetCheckpointDependencies dependencies,
            TargetCheckpointDependencies.ExternalEvidenceAuthority authority, BoundedReadBudget budget, Limits limits,
            Runnable protectionGuard) {
        if(version<3)throw new IllegalArgumentException("external evidence requires original Admission/Control/Outcome closure");
        final var values=dependencies.resolveExternalEvidence(authority,budget,protectionGuard);
        if(values.isEmpty()){requireExternalEvidenceComplete(dependencies);return this;}
        final var result=new TargetCheckpointSemanticSnapshot(Math.max(version,4),metadata,source,mutationSequence,lineage,controlsDigest,
                dependenciesDigest,profiles,retries,trusts,admissions,controls,outcomes,values,recoveryEvidence,lifecycle,limits);
        result.validateAgainst(dependencies,limits);result.requireExternalEvidenceComplete(dependencies);return result;
    }
    public void requireExternalEvidenceComplete(TargetCheckpointDependencies dependencies) {
        if(!dependencies.externalEvidence().isEmpty()&&version<4)throw new IllegalStateException("checkpoint publication lacks actual external object bytes");
        dependencies.requireExternalEvidenceImages(externalEvidence);
    }
    public void authenticateExternalEvidence(TargetCheckpointDependencies dependencies, Limits limits,
            TargetCheckpointDependencies.ExternalEvidenceAuthority authority, BoundedReadBudget budget, Runnable guard) {
        validateAgainst(dependencies,limits); requireExternalEvidenceComplete(dependencies);
        final var actual=ordered(dependencies.resolveExternalEvidence(authority,budget,guard),value->Bytes.utf8(value.exactKey()));
        if(!actual.stream().map(v->Bytes.hex(v.canonicalBytes())).toList().equals(
                externalEvidence.stream().map(v->Bytes.hex(v.canonicalBytes())).toList()))
            throw new IllegalStateException("checkpoint external evidence is not the authenticated retained catalog object");
    }
    /** Schema5 retains each full purpose-signed recovery object exactly once, without changing schemas1..4. */
    public TargetCheckpointSemanticSnapshot withRecoveryEvidence(TargetCheckpointDependencies dependencies,
            TargetCheckpointDependencies.RecoveryEvidenceAuthority authority,BoundedReadBudget budget,Limits limits,Runnable guard){
        if(version<3)throw new IllegalArgumentException("recovery proof companion requires complete original Outcomes");
        requireExternalEvidenceComplete(dependencies);
        final var actual=dependencies.resolveRecoveryEvidence(outcomes,authority,budget,guard);
        if(actual.isEmpty()){requireRecoveryEvidenceComplete(dependencies);return this;}
        final var result=new TargetCheckpointSemanticSnapshot(Math.max(version,5),metadata,source,mutationSequence,lineage,controlsDigest,dependenciesDigest,
                profiles,retries,trusts,admissions,controls,outcomes,externalEvidence,actual,lifecycle,limits);
        result.validateAgainst(dependencies,limits);result.requireRecoveryEvidenceComplete(dependencies);return result;
    }
    public void requireRecoveryEvidenceComplete(TargetCheckpointDependencies dependencies){
        if(!dependencies.recoveryEvidence().isEmpty()&&version<5)throw new IllegalStateException("checkpoint recovery owners require schema5/full raw proofs");
        dependencies.requireRecoveryEvidenceImages(recoveryEvidence,outcomes);
    }
    public void authenticateRecoveryEvidence(TargetCheckpointDependencies dependencies,Limits limits,
            TargetCheckpointDependencies.RecoveryEvidenceAuthority authority,BoundedReadBudget budget,Runnable guard){
        validateAgainst(dependencies,limits);requireRecoveryEvidenceComplete(dependencies);
        final var actual=ordered(dependencies.resolveRecoveryEvidence(outcomes,authority,budget,guard),value->Bytes.utf8(value.exactKey()));
        if(!actual.stream().map(value->Bytes.hex(value.canonicalBytes())).toList().equals(
                recoveryEvidence.stream().map(value->Bytes.hex(value.canonicalBytes())).toList()))
            throw new IllegalStateException("checkpoint recovery proof differs from authenticated original raw Catalog object/seal");
    }

    /** Schema6 is built complete in one step; old component schemas cannot temporarily stand in for g8 inputs. */
    public static TargetCheckpointSemanticSnapshot fromLifecycleValues(TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.AdmissionImage> admissions,List<ProfileSemanticEnvelope> profiles,
            List<RetryPolicySemantic> retries,List<PayloadProofTrustSetSemantic> trusts,
            List<TargetCheckpointDependencies.ControlImage> controls,List<TargetCheckpointDependencies.OutcomeImage> outcomes,
            TargetCheckpointDependencies.ControlImageAuthority controlAuthority,TargetCheckpointDependencies.OutcomeImageAuthority outcomeAuthority,
            TargetCheckpointDependencies.ExternalEvidenceAuthority externalAuthority,TargetCheckpointDependencies.RecoveryEvidenceAuthority recoveryAuthority,
            TargetCheckpointLifecycleInputs.Authority lifecycleAuthority,BoundedReadBudget budget,Limits limits,Runnable guard){
        guard.run();final var complete=dependencies.withAdmissionImages(admissions,limits.maximumAdmissionFrameBytes());
        final var originalControls=complete.requireControlImages(controls,limits.maximumBytes(),budget,controlAuthority,guard);
        final var originalOutcomes=complete.requireOutcomeImages(outcomes,limits.maximumBytes(),budget,outcomeAuthority,guard);
        final var external=complete.resolveExternalEvidence(externalAuthority,budget,guard);
        final var recovery=complete.resolveRecoveryEvidence(originalOutcomes,recoveryAuthority,budget,guard);
        final var lifecycle=TargetCheckpointLifecycleInputs.collect(complete,lifecycleAuthority,budget,limits,guard);
        final var root=complete.root();
        final var result=new TargetCheckpointSemanticSnapshot(6,root.metadata(),root.source(),root.mutationSequence(),
                root.root().recoveryLineage(),complete.storedControlsDigest(),complete.semanticInputsDigest(),profiles,retries,trusts,
                admissions,originalControls,originalOutcomes,external,recovery,lifecycle,limits);
        result.validateAgainst(complete,limits);guard.run();if(!budget.beforeTimedWork())throw budget.incomplete();return result;
    }
    public void requireLifecycleComplete(TargetCheckpointDependencies dependencies,Limits limits){
        if(dependencies.hasLifecycleInputs()&&(version<6||lifecycle==null))throw new IllegalStateException("checkpoint omits full Target lifecycle inputs");
        if(lifecycle!=null)lifecycle.validateAgainst(dependencies,limits);
    }
    public void authenticateLifecycle(TargetCheckpointDependencies dependencies,Limits limits,
            TargetCheckpointLifecycleInputs.Authority authority,BoundedReadBudget budget,Runnable guard){
        validateAgainst(dependencies,limits);requireLifecycleComplete(dependencies,limits);
        if(lifecycle!=null)lifecycle.authenticate(dependencies,authority,budget,limits,guard);
    }
    public TargetCheckpointLifecycleInputs lifecycle(){return lifecycle;}

    /** Exact image contents and closure still need historical publication, keys, pins and activation authority. */
    public void validateAgainst(TargetCheckpointDependencies dependencies, Limits limits) {
        if(Objects.requireNonNull(dependencies).hasLifecycleInputs()&&version<6)
            throw new IllegalStateException("Target activation/retirement requires complete schema6 companion");
        if(lifecycle!=null)lifecycle.validateAgainst(dependencies,limits);
        if ((long) profiles.size() + retries.size() + trusts.size() + admissions.size()
                + controls.size() + outcomes.size() + externalEvidence.size()+recoveryEvidence.size()+(lifecycle==null?0:lifecycle.recordCount())
                        > limits.maximumRecords()
                || canonicalBytes().length > limits.maximumBytes()) {
            throw new IllegalArgumentException("Target semantic snapshot exceeds validation limits");
        }
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        if(version>=4)requireExternalEvidenceComplete(complete);
        if(version>=5)requireRecoveryEvidenceComplete(complete);
        if (version >= 2) { complete.correlateControlImages(controls, limits.maximumBytes()); }
        if (version >= 3) { complete.correlateOutcomeImages(outcomes, limits.maximumBytes()); }
        final var root = complete.root();
        if (!Arrays.equals(metadata.encode(), root.metadata().encode())
                || !Arrays.equals(source.canonicalBytes(), root.source().canonicalBytes())
                || mutationSequence != root.mutationSequence()
                || !Arrays.equals(lineage, root.root().recoveryLineage())
                || !Arrays.equals(controlsDigest, complete.storedControlsDigest())
                || !Arrays.equals(dependenciesDigest, complete.semanticInputsDigest())) {
            throw new IllegalArgumentException(
                    "Target semantic snapshot differs from the physical Store cut/dependencies");
        }
        final var expected = new TreeMap<String, ProfileRef>();
        complete.profiles().forEach(ref -> expected.put(Bytes.hex(ref.canonicalBytes()), ref));
        final var actual = new TreeMap<String, ProfileSemanticEnvelope>();
        profiles.forEach(value -> actual.put(Bytes.hex(value.ref().canonicalBytes()), value));
        final var seen = new java.util.HashSet<String>();
        final var queue = new ArrayList<>(expected.values());
        for (int index = 0; index < queue.size(); index++) {
            final var ref = queue.get(index);
            final var identity = Bytes.hex(ref.canonicalBytes());
            if (!seen.add(identity)) { continue; }
            final var value = actual.get(identity);
            if (value == null) {
                throw new IllegalArgumentException("Target semantic snapshot omits a required Profile");
            }
            if (value.body() instanceof DestinationProfileSemantic destination) {
                queue.add(destination.deliveryCapability());
            }
        }
        if (!actual.keySet().equals(seen)) {
            throw new IllegalArgumentException("Target semantic snapshot has extra Profile values");
        }
        if (!complete.retryPolicies().stream().map(ref -> Bytes.hex(ref.canonicalBytes())).sorted().toList()
                .equals(retries.stream().map(value -> Bytes.hex(value.ref().canonicalBytes())).sorted().toList())
                || !complete.trustSets().stream().map(ref -> Bytes.hex(ref.canonicalBytes())).sorted().toList()
                .equals(trusts.stream().map(value -> Bytes.hex(value.ref().canonicalBytes())).sorted().toList())) {
            throw new IllegalArgumentException("Target semantic snapshot Retry/Trust version set differs");
        }
        for (final var hash : complete.objectProfileHashes()) {
            if (profiles.stream().noneMatch(value -> value.profileKind() == ProfileKind.OBJECT_STORE
                    && Arrays.equals(hash, value.semanticHash()))) {
                throw new IllegalArgumentException("Target semantic snapshot omits an object owner's Profile");
            }
        }
    }

    /** Must authenticate immutable semantic publication at this original use with held history/key/pin rights. */
    @FunctionalInterface
    public interface ProfileAuthority {
        ProfileSemanticEnvelope resolve(ProfileRef reference, SourcePosition originalUse);
    }

    /** Revalidates every original Profile use and transitive Capability outside Store locks under finite protection. */
    public void authenticateProfiles(TargetCheckpointDependencies dependencies, Limits limits,
            BoundedReadBudget budget, ProfileAuthority authority, Runnable protectionGuard) {
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        validateAgainst(dependencies, limits);
        final var complete = dependencies.withAdmissionImages(admissions, limits.maximumAdmissionFrameBytes());
        final var values = new TreeMap<String, ProfileSemanticEnvelope>();
        profiles.forEach(value -> values.put(Bytes.hex(value.ref().canonicalBytes()), value));
        final var pending = new TreeMap<String, TargetCheckpointDependencies.ProfileUse>();
        complete.profileUses().forEach(use -> pending.put(Bytes.hex(use.canonicalBytes()), use));
        final var seen = new java.util.HashSet<String>();
        final var covered = new java.util.HashSet<String>();
        while (!pending.isEmpty()) {
            final var selected = pending.pollFirstEntry();
            if (!seen.add(selected.getKey())) { continue; }
            final var use = selected.getValue();
            final var expected = values.get(Bytes.hex(use.reference().canonicalBytes()));
            if (expected == null) { throw new IllegalArgumentException("Profile use lacks companion semantics"); }
            before(budget, protectionGuard);
            final var authenticated = authority.resolve(use.reference(), use.source());
            protectionGuard.run();
            if (authenticated == null || !Arrays.equals(expected.canonicalBytes(), authenticated.canonicalBytes())) {
                throw new IllegalStateException("Profile semantics not authenticated at their original Source use");
            }
            charge(budget, use.canonicalBytes().length, authenticated.canonicalBytes().length);
            covered.add(Bytes.hex(use.reference().canonicalBytes()));
            if (expected.body() instanceof DestinationProfileSemantic destination) {
                final var capability = new TargetCheckpointDependencies.ProfileUse(
                        destination.deliveryCapability(), use.source());
                pending.put(Bytes.hex(capability.canonicalBytes()), capability);
            }
        }
        if (!covered.equals(values.keySet())) {
            throw new IllegalArgumentException("Profile semantic closure lacks original Source uses");
        }
        protectionGuard.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
    }

    /** Cheap admission identity check; the complete physical ledger is still required before placement. */
    public void requireCut(StoreMetadata expectedMetadata, SourcePosition expectedSource, long expectedSequence,
            byte[] expectedLineage) {
        if (expectedSource == null || !Arrays.equals(metadata.encode(), expectedMetadata.encode())
                || !Arrays.equals(source.canonicalBytes(), expectedSource.canonicalBytes())
                || mutationSequence != expectedSequence || !Arrays.equals(lineage, expectedLineage)) {
            throw new IllegalArgumentException("Target semantic companion request differs from its Store cut");
        }
    }

    public byte[] snapshotDigest() { return Bytes.copy(digest); }
    /** Exact Target dependency-version inventory; values still require original visibility/authentication. */
    public byte[] semanticInputsDigest() { return Bytes.copy(dependenciesDigest); }
    public int schemaVersion() { return version; }
    public List<ProfileSemanticEnvelope> profiles() { return profiles; }
    public List<RetryPolicySemantic> retryPolicies() { return retries; }
    public List<PayloadProofTrustSetSemantic> trustSets() { return trusts; }
    public List<TargetCheckpointDependencies.AdmissionImage> admissions() { return admissions; }
    public List<TargetCheckpointDependencies.ControlImage> controls() { return controls; }
    public List<TargetCheckpointDependencies.OutcomeImage> outcomes() { return outcomes; }

    public List<TargetCheckpointDependencies.ExternalEvidenceImage> externalEvidence() { return externalEvidence; }
    public List<TargetCheckpointDependencies.RecoveryEvidenceImage> recoveryEvidence(){return recoveryEvidence;}

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out,digestField(version), digest);
        });
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, version);
            CanonicalProtobuf.bytes(out, 2, metadata.encode());
            CanonicalProtobuf.bytes(out, 3, source.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 4, mutationSequence);
            CanonicalProtobuf.bytes(out, 5, lineage);
            CanonicalProtobuf.bytes(out, 6, controlsDigest);
            CanonicalProtobuf.bytes(out, 7, dependenciesDigest);
            for (final var profile : profiles) { CanonicalProtobuf.bytes(out, 8, profile.canonicalBytes()); }
            for (final var retry : retries) { CanonicalProtobuf.bytes(out, 9, retry.canonicalBytes()); }
            for (final var trust : trusts) { CanonicalProtobuf.bytes(out, 10, trust.canonicalBytes()); }
            for (final var admission : admissions) { CanonicalProtobuf.bytes(out, 11, admission.canonicalBytes()); }
            for (final var control : controls) { CanonicalProtobuf.bytes(out, 12, control.canonicalBytes()); }
            for (final var outcome : outcomes) { CanonicalProtobuf.bytes(out, 13, outcome.canonicalBytes()); }
            for (final var image : externalEvidence) { CanonicalProtobuf.bytes(out,14,image.canonicalBytes()); }
            for(final var image:recoveryEvidence){CanonicalProtobuf.bytes(out,15,image.canonicalBytes());}
            if(lifecycle!=null)CanonicalProtobuf.bytes(out,16,lifecycle.canonicalBytes());
        });
    }

    public static TargetCheckpointSemanticSnapshot decode(byte[] encoded, Limits limits) {
        if (encoded == null || encoded.length == 0 || encoded.length > limits.maximumBytes()) {
            throw new IllegalArgumentException("invalid Target semantic snapshot length");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, true);
        final var fields = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            if (fields.size() >= (long) limits.maximumRecords() + 9) {
                throw new IllegalArgumentException("Target semantic snapshot exceeds its field bound");
            }
            fields.add(reader.next());
        }
        if (fields.size() < 8 || fields.size() > (long) limits.maximumRecords() + 9) {
            throw new IllegalArgumentException("invalid Target semantic snapshot field count");
        }
        final int version = QueryCodecSupport.uint32(fields.get(0), 1);
        if (version < 1 || version > MAX_SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported Target semantic snapshot version");
        }
        final var profiles = new ArrayList<ProfileSemanticEnvelope>();
        final var retries = new ArrayList<RetryPolicySemantic>();
        final var trusts = new ArrayList<PayloadProofTrustSetSemantic>();
        final var admissions = new ArrayList<TargetCheckpointDependencies.AdmissionImage>();
        final var controls = new ArrayList<TargetCheckpointDependencies.ControlImage>();
        final var outcomes = new ArrayList<TargetCheckpointDependencies.OutcomeImage>();
        final var external = new ArrayList<TargetCheckpointDependencies.ExternalEvidenceImage>();
        final var recovery = new ArrayList<TargetCheckpointDependencies.RecoveryEvidenceImage>();
        int index = 7;
        while (index < fields.size() && fields.get(index).number() == 8) {
            profiles.add(ProfileSemanticEnvelope.decode(QueryCodecSupport.bytes(fields.get(index++), 8)));
        }
        while (index < fields.size() && fields.get(index).number() == 9) {
            retries.add(RetryPolicySemantic.decode(QueryCodecSupport.bytes(fields.get(index++), 9)));
        }
        while (index < fields.size() && fields.get(index).number() == 10) {
            trusts.add(PayloadProofTrustSetSemantic.decode(QueryCodecSupport.bytes(fields.get(index++), 10)));
        }
        while (index < fields.size() && fields.get(index).number() == 11) {
            final var image = QueryCodecSupport.read(
                    QueryCodecSupport.bytes(fields.get(index++), 11), "Admission image");
            QueryCodecSupport.requireNumbers(image, new int[] {1, 2}, "Admission image");
            admissions.add(new TargetCheckpointDependencies.AdmissionImage(
                    SourcePositionCodec.decode(QueryCodecSupport.bytes(image.get(0), 1)),
                    SystemMutation.decodeFrame(QueryCodecSupport.bytes(image.get(1), 2))));
        }
        while (version >= 2 && index < fields.size() && fields.get(index).number() == 12) {
            final var image = QueryCodecSupport.read(QueryCodecSupport.bytes(fields.get(index++), 12), "Control image");
            QueryCodecSupport.requireNumbers(image, new int[] {1, 2}, "Control image");
            controls.add(new TargetCheckpointDependencies.ControlImage(
                    SourcePositionCodec.decode(QueryCodecSupport.bytes(image.get(0), 1)),
                    SystemMutation.decodeTargetControlFrame(QueryCodecSupport.bytes(image.get(1), 2))));
        }
        while (version >= 3 && index < fields.size() && fields.get(index).number() == 13) {
            final var image = QueryCodecSupport.read(QueryCodecSupport.bytes(fields.get(index++), 13), "Outcome image");
            QueryCodecSupport.requireNumbers(image, new int[] {1, 2}, "Outcome image");
            outcomes.add(new TargetCheckpointDependencies.OutcomeImage(
                    SourcePositionCodec.decode(QueryCodecSupport.bytes(image.get(0), 1)),
                    SystemMutation.decodeFrame(QueryCodecSupport.bytes(image.get(1), 2))));
        }
        while(version>=4&&index<fields.size()&&fields.get(index).number()==14){
            external.add(TargetCheckpointDependencies.ExternalEvidenceImage.decode(QueryCodecSupport.nested(fields.get(index++),14)));
        }
        while(version>=5&&index<fields.size()&&fields.get(index).number()==15){
            recovery.add(TargetCheckpointDependencies.RecoveryEvidenceImage.decode(QueryCodecSupport.nested(fields.get(index++),15)));
        }
        final TargetCheckpointLifecycleInputs lifecycle;
        if(version>=6){
            if(index>=fields.size()||fields.get(index).number()!=16)throw new IllegalArgumentException("schema6 omits mandatory lifecycle image");
            lifecycle=TargetCheckpointLifecycleInputs.decode(QueryCodecSupport.bytes(fields.get(index++),16),limits);
        }else lifecycle=null;
        if (index + 1 != fields.size()) {
            throw new IllegalArgumentException("unknown Target semantic snapshot fields");
        }
        final var result = new TargetCheckpointSemanticSnapshot(
                version, StoreMetadata.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                SourcePositionCodec.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                QueryCodecSupport.uint64Bits(fields.get(3), 4), QueryCodecSupport.fixed(fields.get(4), 5, 16),
                QueryCodecSupport.fixed(fields.get(5), 6, 32), QueryCodecSupport.fixed(fields.get(6), 7, 32),
                profiles, retries, trusts, admissions, controls, outcomes, external,recovery,lifecycle, limits);
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(index),
                digestField(version), 32))) {
            throw new IllegalArgumentException("Target semantic snapshot digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target semantic snapshot");
        return result;
    }
    private static int digestField(int version){return switch(version){case 1->12;case 2->13;case 3->14;case 4->15;case 5->16;case 6->17;
        default->throw new IllegalArgumentException("unknown Target semantic snapshot version");};}

    private static void before(BoundedReadBudget budget, Runnable guard) {
        guard.run();
        if (!budget.beforeRead()) { throw budget.incomplete(); }
    }

    private static void charge(BoundedReadBudget budget, int key, int value) {
        if (!budget.tryCharge(key, value) || !budget.beforeTimedWork()) { throw budget.incomplete(); }
    }

    private static <T> List<T> ordered(List<T> values, java.util.function.Function<T, byte[]> identity) {
        final var ordered = new TreeMap<String, T>();
        for (final var value : values) {
            if (ordered.put(Bytes.hex(identity.apply(value)), value) != null) {
                throw new IllegalArgumentException("duplicate Target semantic snapshot identity");
            }
        }
        return List.copyOf(ordered.values());
    }
}
