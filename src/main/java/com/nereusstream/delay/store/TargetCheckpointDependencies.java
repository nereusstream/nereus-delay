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
import com.nereusstream.delay.protocol.TargetSloWriterContract;
import com.nereusstream.delay.protocol.TargetSloWriterControlBody;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.runtime.RetryPolicyCatalog;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.ArrayList;
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
    /** Derived from the complete fold, retaining no inline payload bytes and changing no companion digest. */
    public record MessageState(TargetMessageLocator locator,long stateVersion,
            com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex runtime,byte[] primaryDigest){
        public MessageState { Objects.requireNonNull(locator);Objects.requireNonNull(runtime);primaryDigest=Bytes.copy(primaryDigest);Bytes.requireLength(primaryDigest,32,"primary digest"); }
        @Override public byte[] primaryDigest(){return Bytes.copy(primaryDigest);}
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

        /** Same original Source lookup, with no fallback to today's terminal policy or another publication. */
        public RetryPolicyCatalog.Publication resolvePublication(RetryPolicyCatalog catalog) {
            final var value = Objects.requireNonNull(catalog, "catalog").resolvePublication(reference, source);
            if (value == null || !reference.matches(value.retry())) {
                throw new IllegalStateException("Target Retry Policy publication is not visible at its original Source use");
            }
            return value;
        }
    }

    /** Static content and its original use only; not a zero-reference, reader-exit or Source-retention receipt. */
    public record TerminalPolicyUse(RetryUse use, RetryPolicyCatalog.Publication publication) {
        public TerminalPolicyUse {
            Objects.requireNonNull(use, "use");
            Objects.requireNonNull(publication, "publication");
            if (!use.reference().matches(publication.retry()) || publication.terminal() == null) {
                throw new IllegalArgumentException("original retry use lacks matching terminal policy content");
            }
            publication.terminal().requireBinding(publication.retry());
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
    private final List<TargetResultRecord> retirementResults;
    private final Map<String,TargetResultRecord> systemPositions;
    private final List<Admission> admissions;
    private final List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings;
    private final List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence;
    private final List<com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> recoveryEvidence;
    private final List<com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> retirements;
    private final Map<String,com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord> payloadObjectIntents;
    private final List<com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord> evidenceConfirmations;
    private final List<com.nereusstream.delay.protocol.TargetProtocolActivationRecord> protocolActivations;
    private final List<com.nereusstream.delay.protocol.TargetSloObservationState> sloStates;
    private final List<com.nereusstream.delay.protocol.TargetSloObservationGeneration> sloGenerations;
    private final List<com.nereusstream.delay.protocol.TargetSloObservationSample> sloSamples;
    private final List<com.nereusstream.delay.protocol.TargetSloObservationEventRecord> sloEvents;
    private final List<com.nereusstream.delay.protocol.TargetSloCustodyRecord> sloCustodies;
    private final Map<String,TargetSloWriterEpoch> sloWriterEpochs;
    private final List<com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> sourceHistories;
    private final List<com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord> historyTips;
    private final List<ProfileRef> profiles;
    private final List<ProfileUse> profileUses;
    private final List<TargetScheduleBinding> bindings;
    private final Map<String,TargetScheduleBinding> bindingIndex;
    private final Map<String,com.nereusstream.delay.runtime.TargetReservationRecord> reservationProjections;
    private final List<RetryPolicyRef> retryPolicies;
    private final List<RetryUse> retryUses;
    private final List<PayloadProofTrustSetRef> trustSets;
    private final List<byte[]> objectProfileHashes;
    private final List<byte[]> artifactDigests;
    private final boolean admissionImagesComplete;
    private final Map<String,MessageState> messageStates;
    private final Map<String,com.nereusstream.delay.protocol.TargetQuotaPayloadOwner> payloadOwners;
    private final List<com.nereusstream.delay.runtime.TargetTerminalGenerationRecord> terminalGenerations;
    private final Map<String,TargetResultRecord> retentionFirsts,retentionPositions;
    private final Map<String,TargetResultRecord> timeFences;
    private final Map<String,TargetResultRecord> resultRecords;
    private final Map<String,com.nereusstream.delay.protocol.TargetQuotaIncarnation> incarnations;
    private final Map<String,com.nereusstream.delay.protocol.TargetQuotaCounter> quotaCounters;
    private final Map<com.nereusstream.delay.protocol.TargetQuotaIdentity,Long> ownerRecordCounts;

    private final IngressFenceState ingressFence;

    private TargetCheckpointDependencies(Builder builder, List<Admission> admissions, boolean complete) {
        root = builder.root;
        controls = List.copyOf(builder.controls.values());
        controlResults = List.copyOf(builder.controlResults.values());
        outcomeResults = List.copyOf(builder.outcomeResults.values());
        retirementResults=List.copyOf(builder.retirementResults.values());
        systemPositions=Map.copyOf(builder.systemPositions);
        this.admissions = List.copyOf(admissions);
        kafkaMappings = List.copyOf(builder.kafkaMappings.values());
        externalEvidence = List.copyOf(builder.externalEvidence.values());
        recoveryEvidence = List.copyOf(builder.recoveryEvidence.values());
        retirements = List.copyOf(builder.retirements.values());
        evidenceConfirmations = List.copyOf(builder.evidenceConfirmations.values());
        protocolActivations=List.copyOf(builder.protocolActivations.values());
        sloStates=List.copyOf(builder.sloStates.values());
        sloGenerations=List.copyOf(builder.sloGenerations.values());
        sloSamples=List.copyOf(builder.sloSamples.values());
        sloEvents=List.copyOf(builder.sloEvents.values());
        sloCustodies=List.copyOf(builder.sloCustodies.values());
        sloWriterEpochs=Map.copyOf(builder.sloWriterEpochs);
        sourceHistories=List.copyOf(builder.sourceHistories.values());
        historyTips=List.copyOf(builder.historyTips.values());
        profiles = List.copyOf(builder.profiles.values());
        profileUses = List.copyOf(builder.profileUses.values());
        bindings = List.copyOf(builder.bindings.values());bindingIndex=Map.copyOf(builder.bindings);reservationProjections=Map.copyOf(builder.reservationProjections);
        retryPolicies = List.copyOf(builder.retries.values());
        retryUses = List.copyOf(builder.retryUses.values());
        trustSets = List.copyOf(builder.trusts.values());
        objectProfileHashes = copies(builder.objectHashes.values().stream().toList());
        artifactDigests = copies(builder.artifacts.values().stream().toList());
        admissionImagesComplete = complete;
        payloadObjectIntents=Map.copyOf(builder.payloadObjectIntents);
        messageStates=Map.copyOf(builder.messageStates);payloadOwners=Map.copyOf(builder.payloadOwners);
        terminalGenerations=List.copyOf(builder.terminalGenerations.values());
        retentionFirsts=Map.copyOf(builder.retentionFirsts);retentionPositions=Map.copyOf(builder.retentionPositions);
        ingressFence=builder.ingressFence;
        timeFences=Map.copyOf(builder.timeFences);
        resultRecords=Map.copyOf(builder.resultRecords);
        incarnations=Map.copyOf(builder.incarnations);quotaCounters=Map.copyOf(builder.quotaCounters);
        ownerRecordCounts=Map.copyOf(builder.ownerRecordCounts);
    }

    public TargetCheckpointRootVerifier.RootProof root() { return root; }
    public com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord payloadObjectIntent(com.nereusstream.delay.protocol.DelayMessageId id){return payloadObjectIntents.get(Bytes.hex(id.bytes()));}
    public List<com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord> payloadObjectIntents(){return List.copyOf(payloadObjectIntents.values());}
    public MessageState messageState(com.nereusstream.delay.protocol.DelayMessageId id){return messageStates.get(Bytes.hex(id.bytes()));}
    public com.nereusstream.delay.protocol.TargetQuotaPayloadOwner payloadOwner(com.nereusstream.delay.protocol.DelayMessageId id){return payloadOwners.get(Bytes.hex(id.bytes()));}
    public List<com.nereusstream.delay.runtime.TargetTerminalGenerationRecord> terminalGenerations(){return terminalGenerations;}
    public IngressFenceState ingressFence(){return ingressFence;}
    public com.nereusstream.delay.protocol.TargetQuotaIncarnation incarnation(byte[] key){return incarnations.get(Bytes.hex(key));}
    public com.nereusstream.delay.protocol.TargetQuotaCounter quotaCounter(com.nereusstream.delay.protocol.TargetQuotaIdentity id){return quotaCounters.get(Bytes.hex(id.key()));}
    /** Counted from every actual business row in the complete independent fold, including zero-valued charges. */
    public long ownerRecordCount(com.nereusstream.delay.protocol.TargetQuotaIdentity id){return ownerRecordCounts.getOrDefault(id,0L);}
    public TargetResultRecord resultRecord(byte[] key){return resultRecords.get(Bytes.hex(key));}
    /** Exact original Source first/POSITION for RETAINED, not a later physical duplicate or a caller timestamp. */
    public TargetResultRecord requireRetentionDecision(com.nereusstream.delay.protocol.TargetQuotaPayloadOwner retained){
        final String token=retentionToken(retained.mutation());
        final var first=Objects.requireNonNull(retentionFirsts.get(token),"retained owner lost original Source first");
        final var position=Objects.requireNonNull(retentionPositions.get(token),"retained owner lost original Source POSITION");
        if(!first.mutation().equals(retained.mutation())||!position.mutation().equals(first.mutation()))
            throw new IllegalStateException("retained owner Source stamp differs from actual original decision");
        position.requireFirst(first);
        if(!first.appliedFirst())throw new IllegalStateException("retained owner references a non-APPLIED Source first");
        return first;
    }
    private static String retentionToken(com.nereusstream.delay.protocol.TargetQuotaMutation stamp){return Bytes.hex(Bytes.sha256(stamp.canonicalBytes()));}
    public List<StoredControl> controls() { return controls; }
    /** Every retained APPLY_SHARD_CONTROL first result, including durable rejections. */
    public List<TargetResultRecord> controlResults() { return controlResults; }
    public List<ControlReference> controlReferences() {
        return controlResults.stream().map(ControlReference::new).toList();
    }
    public List<TargetResultRecord> outcomeResults() { return outcomeResults; }
    public List<TargetResultRecord> retirementResults(){return retirementResults;}
    /** Exact original POSITION from the same complete physical fold; no re-encoding or missing-row substitute. */
    public void requireSystemPosition(TargetResultRecord first){
        if(first.kind()!=TargetResultRecord.Kind.SYSTEM)throw new IllegalArgumentException("original system first required");
        final var key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},first.mutation().source().canonicalBytes());
        final var position=Objects.requireNonNull(systemPositions.get(Bytes.hex(key)),"authenticated image lacks original system POSITION");
        if(!position.mutation().equals(first.mutation()))throw new IllegalStateException("image POSITION changed original first stamp");
        position.requireFirst(first);
    }
    public TargetResultRecord requireTimeFence(com.nereusstream.delay.ownership.SourceReplayMutation original){
        final var input=original.mutation();
        if(input.type()!=SystemMutationType.TIME_FENCE)throw new IllegalArgumentException("original TimeFence required");
        final var first=Objects.requireNonNull(timeFences.get(Bytes.hex(input.systemMutationId())),"original Source TimeFence first");
        if(!Arrays.equals(first.typedPayload(),SystemMutationResult.from(input,com.nereusstream.delay.runtime.ApplyStatus.APPLIED,
                com.nereusstream.delay.protocol.StableCode.OK,original.position().canonicalBytes()).encode())
                ||!Arrays.equals(first.mutation().source().canonicalBytes(),original.position().canonicalBytes())
                ||!Arrays.equals(first.mutation().mutationDigest(),Bytes.sha256(input.canonicalEnvelope())))
            throw new IllegalStateException("TimeFence input differs from actual original Source decision");
        requireSystemPosition(first);return first;
    }
    public List<com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> sourceHistories(){return sourceHistories;}
    public List<com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord> historyTips(){return historyTips;}
    public boolean hasLifecycleInputs(){return !historyTips.isEmpty()||!sourceHistories.isEmpty()||!protocolActivations.isEmpty()||!retirements.isEmpty()||!evidenceConfirmations.isEmpty()||!retirementResults.isEmpty();}
    public List<OutcomeReference> outcomeReferences() {
        return outcomeResults.stream().map(OutcomeReference::new).toList();
    }
    public List<Admission> admissions() { return admissions; }
    public List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings() { return kafkaMappings; }
    public List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence() { return externalEvidence; }
    public List<com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> recoveryEvidence() { return recoveryEvidence; }
    public List<com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> retirements() { return retirements; }
    public List<com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord> evidenceConfirmations() { return evidenceConfirmations; }
    public List<com.nereusstream.delay.protocol.TargetProtocolActivationRecord> protocolActivations(){return protocolActivations;}
    public List<com.nereusstream.delay.protocol.TargetSloObservationState> sloStates(){return sloStates;}
    public List<com.nereusstream.delay.protocol.TargetSloObservationGeneration> sloGenerations(){return sloGenerations;}
    public List<com.nereusstream.delay.protocol.TargetSloObservationSample> sloSamples(){return sloSamples;}
    public List<com.nereusstream.delay.protocol.TargetSloObservationEventRecord> sloEvents(){return sloEvents;}
    public List<com.nereusstream.delay.protocol.TargetSloCustodyRecord> sloCustodies(){return sloCustodies;}
    public List<TargetSloWriterEpoch> sloWriterEpochs(){return sloWriterEpochs.values().stream().sorted((a,b)->Arrays.compareUnsigned(a.key(),b.key())).toList();}
    /** Exact retained applied registration; an absent or substituted epoch never falls back to the generation signer. */
    public TargetSloWriterEpoch sloWriterEpoch(TargetSloWriterContract.Reference reference){
        final var writer=sloWriterEpochs.get(Bytes.hex(Objects.requireNonNull(reference).key(root.root().scope())));
        if(writer!=null&&!writer.reference().equals(reference))throw new IllegalStateException("checkpoint writer reference changed its original applied registration");
        return writer;
    }

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

    /** Full historical authentication plus actual Catalog raw bytes/prepared seal, resolved outside Store I/O. */
    @FunctionalInterface
    public interface RecoveryEvidenceAuthority {
        com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery resolve(
                com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner original,OutcomeImage adoptingOutcome);
    }
    public record RecoveryEvidenceImage(String exactKey,com.nereusstream.delay.protocol.TargetRecoveryProof proof){
        public RecoveryEvidenceImage{Objects.requireNonNull(proof).reference(Objects.requireNonNull(exactKey));}
        public byte[] canonicalBytes(){return CanonicalProtobuf.message(out->{
            CanonicalProtobuf.bytes(out,1,Bytes.utf8(exactKey));CanonicalProtobuf.bytes(out,2,proof.canonicalBytes());
        });}
        public static RecoveryEvidenceImage decode(byte[] encoded){
            if(encoded==null||encoded.length==0||encoded.length>com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_PROOF_BYTES
                    +com.nereusstream.delay.protocol.TargetRecoveryProofRef.MAX_KEY_BYTES+16)throw new IllegalArgumentException("invalid checkpoint recovery proof length");
            final var reader=new CanonicalProtobuf.Reader(encoded,true);final var f=new java.util.ArrayList<CanonicalProtobuf.Reader.Field>(2);
            while(reader.hasRemaining()){if(f.size()>=2)throw new IllegalArgumentException("unknown checkpoint recovery proof fields");f.add(reader.next());}
            com.nereusstream.delay.protocol.QueryCodecSupport.requireNumbers(f,new int[]{1,2},"checkpoint recovery proof");
            final var image=new RecoveryEvidenceImage(new String(com.nereusstream.delay.protocol.QueryCodecSupport.bytes(f.get(0),1),
                    java.nio.charset.StandardCharsets.UTF_8),com.nereusstream.delay.protocol.TargetRecoveryProof.decode(
                    com.nereusstream.delay.protocol.QueryCodecSupport.nested(f.get(1),2)));
            com.nereusstream.delay.protocol.QueryCodecSupport.requireCanonical(encoded,image.canonicalBytes(),"checkpoint recovery proof");return image;
        }
    }
    public List<RecoveryEvidenceImage> resolveRecoveryEvidence(List<OutcomeImage> outcomes,RecoveryEvidenceAuthority authority,
            BoundedReadBudget budget,Runnable guard){
        if(!recoveryEvidence.isEmpty())Objects.requireNonNull(authority,"original recovery proof history/Catalog authority");
        final var byOutcome=new TreeMap<String,OutcomeImage>();for(var input:outcomes)
            if(byOutcome.put(Bytes.hex(input.image().systemMutationId()),input)!=null)throw new IllegalArgumentException("duplicate adopting Outcome image");
        final var values=new java.util.ArrayList<RecoveryEvidenceImage>();
        for(var owner:recoveryEvidence){
            final var input=Objects.requireNonNull(byOutcome.get(Bytes.hex(owner.outcomeId())),"recovery adopting signed Outcome image");
            owner.requireSource(input.image(),input.source());beforeAuthentication(budget,guard);
            // Check the complete declared raw-object bound before initiating external I/O or allocating its image.
            if(!budget.tryCharge(Bytes.utf8(owner.reference().exactKey()).length,Math.toIntExact(owner.reference().canonicalValueBytes())))throw budget.incomplete();
            final var actual=Objects.requireNonNull(authority.resolve(owner,input),"actual authenticated recovery object and seal");
            guard.run();actual.requireExact(input.image());actual.verified().requireExact(owner.reference());owner.requireProof(actual.verified().proof());
            values.add(new RecoveryEvidenceImage(owner.reference().exactKey(),actual.verified().proof()));afterAuthentication(budget,guard);
        }
        requireRecoveryEvidenceImages(values,outcomes);return List.copyOf(values);
    }
    /** Structural complete-cut correlation only; the authority above must still authenticate every actual signed object. */
    public void requireRecoveryEvidenceImages(List<RecoveryEvidenceImage> images,List<OutcomeImage> outcomes){
        final var byKey=new TreeMap<String,RecoveryEvidenceImage>();
        for(var image:images)if(byKey.put(image.exactKey(),image)!=null)throw new IllegalArgumentException("duplicate checkpoint recovery proof key");
        if(byKey.size()!=recoveryEvidence.size())throw new IllegalStateException("checkpoint lacks complete raw recovery proof bytes");
        final var owners=new TreeMap<String,com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner>();
        for(var owner:recoveryEvidence){
            final var image=Objects.requireNonNull(byKey.get(owner.reference().exactKey()),"original recovery proof image");owner.requireProof(image.proof());
            if(owners.put(Bytes.hex(owner.outcomeId()),owner)!=null)throw new IllegalStateException("one Outcome owns multiple recovery proofs");
        }
        final var firstById=new TreeMap<String,TargetResultRecord>();
        for(var first:outcomeResults)firstById.put(Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()),first);
        final var used=new java.util.HashSet<String>();
        final var seen=new java.util.HashSet<String>();
        for(var input:outcomes){
            final String id=Bytes.hex(input.image().systemMutationId());
            if(!seen.add(id))throw new IllegalArgumentException("duplicate recovery companion Outcome image");
            final var first=Objects.requireNonNull(firstById.get(id),"actual retained Outcome first");
            if(SystemMutationResult.decode(first.typedPayload()).applyStatus()!=com.nereusstream.delay.runtime.ApplyStatus.APPLIED)continue;
            final var body=input.image().type()==SystemMutationType.PUBLISH_OUTCOME?
                    com.nereusstream.delay.protocol.PublishOutcomeBody.decode(input.image().canonicalBody()):
                    com.nereusstream.delay.protocol.PublishOutcomeBody.decodeEvidenceResolution(input.image().canonicalBody());
            if(body.recoveryProof()==null)continue;
            final var owner=Objects.requireNonNull(owners.get(id),"APPLIED proof-bearing Outcome has no Native owner");
            owner.requireSource(input.image(),input.source());
            if(!owner.reference().equals(body.recoveryProof()))throw new IllegalStateException("Outcome replaced its exact recovery proof ref");
            final var proof=byKey.get(owner.reference().exactKey()).proof();
            if(!Arrays.equals(body.evidence(),proof.evidence().canonicalBytes())||!first.mutation().equals(owner.adoptedAt()))
                throw new IllegalStateException("recovery image changed adopting evidence/first Source");
            if(body.evidenceCursor()!=null&&!Arrays.equals(body.evidenceCursor().canonicalBytes(),
                    com.nereusstream.delay.protocol.TargetKafkaReceiptWire.cursor(proof.mapping(),proof.evidence()).canonicalBytes()))
                throw new IllegalStateException("recovery image changed resolution cursor");
            used.add(id);
        }
        if(!seen.equals(firstById.keySet()))throw new IllegalStateException("recovery companion lacks complete retained Outcome input inventory");
        if(!used.equals(owners.keySet()))throw new IllegalStateException("recovery proof lacks its exact APPLIED signed Outcome image");
    }

    public TargetScheduleBinding binding(byte[] digest){return bindingIndex.get(Bytes.hex(digest));}
    public com.nereusstream.delay.runtime.TargetReservationRecord reservationProjection(byte[] key){return reservationProjections.get(Bytes.hex(key));}
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
    /** Actual fold inputs, including earlier generations; package-local so audits can join them in one pass. */
    List<TargetScheduleBinding> retainedBindings(){return bindings;}
    /** Bounded selected group; scans the complete retained binding inventory once under the original guard. */
    public Map<com.nereusstream.delay.protocol.DelayMessageId,List<RetryUse>> retryUsesFor(
            java.util.Set<com.nereusstream.delay.protocol.DelayMessageId> ids,Runnable current){
        if(ids.isEmpty()||ids.size()>32)throw new IllegalArgumentException("bounded inline Message group required");
        final var selected=new java.util.HashMap<com.nereusstream.delay.protocol.DelayMessageId,TreeMap<String,RetryUse>>();
        for(var binding:bindings){
            current.run();if(!ids.contains(binding.messageId()))continue;
            final var use=new RetryUse(binding.intent().retryPolicy(),binding.bindingSource());
            selected.computeIfAbsent(binding.messageId(),id->new TreeMap<>()).put(Bytes.hex(use.canonicalBytes()),use);
        }
        final var result=new java.util.HashMap<com.nereusstream.delay.protocol.DelayMessageId,List<RetryUse>>();
        selected.forEach((id,uses)->result.put(id,List.copyOf(uses.values())));current.run();return Map.copyOf(result);
    }
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

    /**
     * Complete static policy closure over one Message's actual retained bindings/generations, outside Native locks.
     * Missing legacy content is unavailable, never an empty successful closure. The provider and held guard
     * must authenticate the original Source/Profile/CONTROL history, as for resolveRetryPolicies.
     */
    public List<TerminalPolicyUse> resolveTerminalPolicies(final com.nereusstream.delay.protocol.DelayMessageId messageId,
            final RetryPolicyCatalog catalog,
            final BoundedReadBudget budget, final Runnable protectionGuard) {
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(protectionGuard, "protectionGuard").run();
        final var values = new ArrayList<TerminalPolicyUse>();
        final var selected = new TreeMap<String, RetryUse>();
        Objects.requireNonNull(messageId, "messageId");
        for (final var binding : bindings) {
            protectionGuard.run();
            if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
            if (messageId.equals(binding.messageId())) {
                final var use = new RetryUse(binding.intent().retryPolicy(), binding.bindingSource());
                selected.put(Bytes.hex(use.canonicalBytes()), use);
            }
        }
        if (!admissionImagesComplete || selected.isEmpty()) {
            throw new IllegalStateException("terminal policy closure requires complete original inputs and retained retry uses");
        }
        for (final var use : selected.values()) {
            protectionGuard.run();
            if (!budget.beforeRead()) { throw budget.incomplete(); }
            final var publication = use.resolvePublication(catalog);
            protectionGuard.run();
            if (publication.terminal() == null) {
                throw new IllegalStateException("original Retry Policy has no terminal content; payload must remain retained");
            }
            final int bytes = Math.addExact(publication.retry().canonicalBytes().length,
                    publication.terminal().canonicalBytes().length);
            if (!budget.tryCharge(use.canonicalBytes().length, bytes) || !budget.beforeTimedWork()) {
                throw budget.incomplete();
            }
            values.add(new TerminalPolicyUse(use, publication));
        }
        protectionGuard.run();
        return List.copyOf(values);
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
            if ((image.type() != SystemMutationType.APPLY_SHARD_CONTROL && image.type() != SystemMutationType.TARGET_SLO_OBSERVATION)
                    || !source.shardId().equals(image.shardId())) {
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
            if ((image.type() != SystemMutationType.APPLY_SHARD_CONTROL && image.type() != SystemMutationType.TARGET_SLO_OBSERVATION)
                    || result.mutationType() != image.type() || !Arrays.equals(actualSource.canonicalBytes(), source().canonicalBytes())
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
        /** Original SERVICE event history, endpoint/time and retained budget; APPLIED uses its frozen sample. */
        default java.security.PublicKey authenticateObservation(ControlImage input,TargetResultRecord first,
                com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
                com.nereusstream.delay.protocol.TargetSloObservationSample sample){
            throw new IllegalStateException("checkpoint authority lacks original SLO event authentication");
        }
        default java.security.PublicKey authenticateObservation(ControlImage input,TargetResultRecord first,
                com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
                com.nereusstream.delay.protocol.TargetSloObservationSample sample,TargetSloWriterEpoch writer){
            if(com.nereusstream.delay.protocol.TargetSloObservationEventBody.decode(input.image().canonicalBody()).writerEpoch()!=null)
                throw new IllegalStateException("checkpoint authority lacks original writer-epoch event authentication");
            return authenticateObservation(input,first,generation,sample);
        }
        /** Original registration/RBAC, complete resource materialization and independent original Fence authority. */
        default java.security.PublicKey authenticateWriterEpoch(ControlImage input,TargetResultRecord first,
                TargetSloWriterEpoch writer,TargetSloWriterEpoch predecessor){
            if(first.appliedFirst())throw new IllegalStateException("checkpoint authority lacks original writer Control/Fence authentication");
            return authenticate(input,first);
        }
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
            final var key=authenticateInput(input,first,authority);
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

    java.security.PublicKey authenticateInput(ControlImage input,TargetResultRecord first,ControlImageAuthority authority){
        final var image=input.image();
        if(image.type()==SystemMutationType.TARGET_SLO_OBSERVATION){
            final var body=com.nereusstream.delay.protocol.TargetSloObservationEventBody.decode(image.canonicalBody());
            final var sample=sloSamples.stream().filter(v->Arrays.equals(v.observation().sampleId(),body.sampleId())).findFirst().orElse(null);
            final var generation=sloGenerations.stream().filter(v->v.generation()==body.generation()).findFirst().orElse(null);
            final var writer=body.writerEpoch()==null||!first.appliedFirst()?null:sloWriterEpoch(body.writerEpoch());
            if(first.appliedFirst()&&(generation==null||body.writerEpoch()!=null&&writer==null
                    ||body.writerEpoch()==null&&generation.policy().requiresWriterEpoch()))
                throw new IllegalStateException("checkpoint APPLIED observation lacks its mandatory actual writer-epoch branch");
            return authority.authenticateObservation(input,first,generation,sample,writer);
        }
        if(TargetSloWriterControlBody.isControl(image)){
            final var request=TargetSloWriterControlBody.decode(image.canonicalBody()).request();
            final byte[] key=request.contract()!=null?TargetSloWriterEpoch.key(request.scope(),request.contract().writerIdentityDigest(),request.epoch()):request.reference().key(request.scope());
            final var writer=sloWriterEpochs.get(Bytes.hex(key));
            final var predecessor=request.contract()!=null&&request.epoch()!=1?sloWriterEpochs.get(Bytes.hex(TargetSloWriterEpoch.key(request.scope(),request.contract().writerIdentityDigest(),request.epoch()-1))):null;
            return authority.authenticateWriterEpoch(input,first,writer,predecessor);
        }
        return authority.authenticate(input,first);
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
        final var correlated = List.copyOf(complete.values());
        com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.requireControlInputs(sloGenerations, correlated, controlResults);
        requireWriterControlInputs(complete);
        return correlated;
    }

    private void requireWriterControlInputs(Map<String,ControlImage> inputs){
        for(var writer:sloWriterEpochs.values()){
            requireWriterControlInput(inputs,writer.register());
            if(writer.closed())requireWriterControlInput(inputs,writer.close());
        }
        final var firsts=new TreeMap<String,TargetResultRecord>();
        for(var first:controlResults)firsts.put(Bytes.hex(first.logicalId()),first);
        for(var input:inputs.values()){
            if(!TargetSloWriterControlBody.isControl(input.image())||!Objects.requireNonNull(firsts.get(Bytes.hex(input.image().systemMutationId()))).appliedFirst())continue;
            final var request=TargetSloWriterControlBody.decode(input.image().canonicalBody()).request();
            final var key=request.contract()!=null?TargetSloWriterEpoch.key(request.scope(),request.contract().writerIdentityDigest(),request.epoch()):request.reference().key(request.scope());
            final var writer=Objects.requireNonNull(sloWriterEpochs.get(Bytes.hex(key)),"APPLIED writer Control lacks its complete retained Native62");
            final var original=request.contract()!=null?writer.register():Objects.requireNonNull(writer.close(),"APPLIED writer CLOSE lacks its retained original Control");
            if(!Arrays.equals(original.input().canonicalEnvelope(),input.image().canonicalEnvelope())
                    ||!original.appliedAt().equals(firsts.get(Bytes.hex(input.image().systemMutationId())).mutation()))
                throw new IllegalStateException("APPLIED writer Control differs from its retained original Native62 input/stamp");
        }
    }
    private void requireWriterControlInput(Map<String,ControlImage> inputs,TargetSloWriterEpoch.Control control){
        final var input=Objects.requireNonNull(inputs.get(Bytes.hex(control.input().systemMutationId())),"Native62 lost its complete original Control input");
        if(!Arrays.equals(input.image().canonicalEnvelope(),control.input().canonicalEnvelope())
                ||!Arrays.equals(input.source().canonicalBytes(),control.appliedAt().source().canonicalBytes()))
            throw new IllegalStateException("Native62 changed its original Control input/Source");
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
        com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.requireAdmissionInputs(sloGenerations,sloSamples,inputs);
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
            for (final var owner : recoveryEvidence) { CanonicalProtobuf.bytes(out, 8, owner.canonicalBytes()); }
            for (final var record : retirements) { CanonicalProtobuf.bytes(out, 9, Bytes.sha256(record.canonicalBytes())); }
            for (final var record : evidenceConfirmations) { CanonicalProtobuf.bytes(out, 10, Bytes.sha256(record.canonicalBytes())); }
            for (final var record : protocolActivations) { CanonicalProtobuf.bytes(out, 11, record.digest()); }
            for(final var first:retirementResults){CanonicalProtobuf.bytes(out,12,Bytes.sha256(first.canonicalBytes()));}
            for(final var retained:sourceHistories){CanonicalProtobuf.bytes(out,13,retained.digest());}
            for(final var tip:historyTips){CanonicalProtobuf.bytes(out,14,tip.digest());}
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
        private final Map<String,TargetResultRecord> retirementResults=new TreeMap<>();
        private final Map<String,TargetResultRecord> systemPositions=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalEvidence = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> recoveryEvidence = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> retirements = new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord> payloadObjectIntents=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord> evidenceConfirmations = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetProtocolActivationRecord> protocolActivations=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetSloObservationState> sloStates=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetSloObservationGeneration> sloGenerations=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetSloObservationSample> sloSamples=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetSloObservationEventRecord> sloEvents=new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetSloCustodyRecord> sloCustodies=new TreeMap<>();
        private final Map<String,TargetSloWriterEpoch> sloWriterEpochs=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> sourceHistories=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord> historyTips=new TreeMap<>();
        private final Map<String, ProfileRef> profiles = new TreeMap<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings = new TreeMap<>();
        private final Map<String, ProfileUse> profileUses = new TreeMap<>();
        private final Map<String, TargetScheduleBinding> bindings = new TreeMap<>();
        private final Map<String,com.nereusstream.delay.runtime.TargetReservationRecord> reservationProjections=new TreeMap<>();
        private final Map<String, RetryPolicyRef> retries = new TreeMap<>();
        private final Map<String, RetryUse> retryUses = new TreeMap<>();
        private final Map<String, PayloadProofTrustSetRef> trusts = new TreeMap<>();
        private final Map<String, byte[]> objectHashes = new TreeMap<>();
        private final Map<String, byte[]> artifacts = new TreeMap<>();
        private final Map<String,MessageState> messageStates=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetQuotaPayloadOwner> payloadOwners=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.runtime.TargetTerminalGenerationRecord> terminalGenerations=new TreeMap<>();
        private final Map<String,TargetResultRecord> retentionFirsts=new TreeMap<>(),retentionPositions=new TreeMap<>();
        private final Map<String,TargetResultRecord> timeFences=new TreeMap<>();
        private final Map<String,TargetResultRecord> resultRecords=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetQuotaIncarnation> incarnations=new TreeMap<>();
        private final Map<String,com.nereusstream.delay.protocol.TargetQuotaCounter> quotaCounters=new TreeMap<>();
        private final Map<com.nereusstream.delay.protocol.TargetQuotaIdentity,Long> ownerRecordCounts=new java.util.HashMap<>();
        private IngressFenceState ingressFence=new IngressFenceState(IngressFenceState.OPEN,null);

        Builder(TargetCheckpointRootVerifier.RootProof root) { this.root = root; }

        Builder(TargetCheckpointDependencies prior) {
            this(prior.root);
            for (final var control : prior.controls) { control(control.family(), control.key(),
                    control.valueType(), control.payload()); }
            prior.controlResults.forEach(this::controlResult);
            prior.outcomeResults.forEach(this::outcomeResult);
            prior.retirementResults.forEach(this::retirementResult);
            prior.systemPositions.values().forEach(this::systemPosition);
            prior.kafkaMappings.forEach(this::kafkaMapping);
            prior.externalEvidence.forEach(this::externalEvidence);
            prior.recoveryEvidence.forEach(this::recoveryEvidence);
            prior.retirements.forEach(this::retirement);
            prior.evidenceConfirmations.forEach(this::evidenceConfirmation);
            prior.protocolActivations.forEach(this::protocolActivation);
            prior.sloStates.forEach(this::sloState);
            prior.sloGenerations.forEach(this::sloGeneration);
            prior.sloSamples.forEach(this::sloSample);
            prior.sloEvents.forEach(this::sloEvent);
            prior.sloCustodies.forEach(this::sloCustody);
            prior.sloWriterEpochs.values().forEach(this::sloWriterEpoch);
            prior.sourceHistories.forEach(this::sourceHistory);
            prior.historyTips.forEach(this::historyTip);
            prior.profiles.forEach(this::profile);
            prior.profileUses.forEach(this::profileUse);
            prior.bindings.forEach(this::binding);reservationProjections.putAll(prior.reservationProjections);
            prior.retryPolicies.forEach(this::retry);
            prior.retryUses.forEach(this::retryUse);
            prior.trustSets.forEach(this::trust);
            prior.objectProfileHashes.forEach(this::objectHash);
            prior.artifactDigests.forEach(this::artifact);
            payloadObjectIntents.putAll(prior.payloadObjectIntents);messageStates.putAll(prior.messageStates);payloadOwners.putAll(prior.payloadOwners);
            prior.terminalGenerations.forEach(this::terminalGeneration);
            retentionFirsts.putAll(prior.retentionFirsts);retentionPositions.putAll(prior.retentionPositions);ingressFence=prior.ingressFence;
            timeFences.putAll(prior.timeFences);
            resultRecords.putAll(prior.resultRecords);incarnations.putAll(prior.incarnations);quotaCounters.putAll(prior.quotaCounters);
            ownerRecordCounts.putAll(prior.ownerRecordCounts);
        }
        void incarnation(com.nereusstream.delay.protocol.TargetQuotaIncarnation value){if(incarnations.putIfAbsent(Bytes.hex(value.key()),value)!=null)throw new IllegalStateException("duplicate actual incarnation");}
        void quotaCounter(com.nereusstream.delay.protocol.TargetQuotaCounter value){if(quotaCounters.putIfAbsent(Bytes.hex(value.identity().key()),value)!=null)throw new IllegalStateException("duplicate actual quota counter");}
        void ownerRecord(com.nereusstream.delay.protocol.TargetQuotaIdentity id){ownerRecordCounts.merge(id,1L,Math::addExact);}
        void messageState(com.nereusstream.delay.runtime.TargetMessageRecord message,byte[] canonical){
            messageStates.put(Bytes.hex(message.locator().messageId().bytes()),new MessageState(message.locator(),message.stateVersion(),message.runtime(),Bytes.sha256(canonical)));
        }
        void payloadOwner(com.nereusstream.delay.protocol.TargetQuotaPayloadOwner owner){payloadOwners.put(Bytes.hex(owner.messageId().bytes()),owner);}
        void terminalGeneration(com.nereusstream.delay.runtime.TargetTerminalGenerationRecord terminal){terminalGenerations.put(Bytes.hex(terminal.key()),terminal);}
        void ingressFence(IngressFenceState state){ingressFence=Objects.requireNonNull(state);}
        void timeFence(TargetResultRecord first){timeFences.put(Bytes.hex(first.logicalId()),first);}
        void captureRetentionDecisions(List<com.nereusstream.delay.runtime.TargetResultLedgerAudit.Stored> rows){
            final var needed=new java.util.HashSet<String>();
            for(var owner:payloadOwners.values())if(owner.phase()==com.nereusstream.delay.protocol.TargetQuotaPayloadOwner.Phase.RETAINED)needed.add(retentionToken(owner.mutation()));
            for(var history:sourceHistories.values())if(history.input().type()==SystemMutationType.TARGET_RESOURCE_RECLAIM){
                final var body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(history.input().canonicalBody());
                if(body.inlinePayloadRelease()||body.objectPayloadReclaim()&&!body.uncommittedObjectReclaim())for(var operation:body.payloadOperations())needed.add(retentionToken(operation.expected().mutation()));
            }
            for(var row:rows){
                final var value=TargetResultRecord.decode(row.payload());final String token=retentionToken(value.mutation());
                resultRecords.put(Bytes.hex(value.key()),value);
                if(!needed.contains(token))continue;
                if(value.kind()==TargetResultRecord.Kind.COMMAND||value.kind()==TargetResultRecord.Kind.SYSTEM)retentionFirsts.put(token,value);
                else if(value.kind()==TargetResultRecord.Kind.POSITION_COMMAND||value.kind()==TargetResultRecord.Kind.POSITION_SYSTEM)retentionPositions.put(token,value);
            }
        }

        void externalEvidence(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner owner) {
            final var previous=externalEvidence.putIfAbsent(Bytes.hex(owner.key()),owner);
            if(previous!=null&&!Arrays.equals(previous.canonicalBytes(),owner.canonicalBytes()))throw new IllegalStateException("duplicate external evidence owner");
        }
        void recoveryEvidence(com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner owner) {
            final var previous=recoveryEvidence.putIfAbsent(Bytes.hex(owner.key()),owner);
            if(previous!=null&&!Arrays.equals(previous.canonicalBytes(),owner.canonicalBytes()))throw new IllegalStateException("duplicate recovery evidence owner");
        }
        void payloadObjectIntent(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord record){payloadObjectIntents.put(Bytes.hex(record.expectedOwner().messageId().bytes()),record);}
        void retirement(com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord record) {
            if(retirements.putIfAbsent(Bytes.hex(record.key()),record)!=null)
                throw new IllegalStateException("duplicate Target retiring identity");
        }
        void evidenceConfirmation(com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord record) {
            if(evidenceConfirmations.putIfAbsent(Bytes.hex(record.key()),record)!=null)
                throw new IllegalStateException("duplicate Target confirmed identity");
        }
        void historyTip(com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord record){
            if(historyTips.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate accepted history tip scope");
        }
        void sourceHistory(com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord record){
            if(sourceHistories.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate retirement Source history");
        }
        void sloState(com.nereusstream.delay.protocol.TargetSloObservationState record) {
            if(sloStates.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate Target SLO current state");
        }
        void sloGeneration(com.nereusstream.delay.protocol.TargetSloObservationGeneration record) {
            if(sloGenerations.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate Target SLO generation");
        }
        void sloCustody(com.nereusstream.delay.protocol.TargetSloCustodyRecord record){
            if(sloCustodies.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate exact Target custody version");
        }
        void sloWriterEpoch(TargetSloWriterEpoch record){
            if(sloWriterEpochs.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate original SLO writer epoch");
        }
        void sloEvent(com.nereusstream.delay.protocol.TargetSloObservationEventRecord record) {
            if(sloEvents.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate Target SLO Source event");
        }
        void sloSample(com.nereusstream.delay.protocol.TargetSloObservationSample record) {
            if(sloSamples.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate Target SLO sample");
        }
        void protocolActivation(com.nereusstream.delay.protocol.TargetProtocolActivationRecord record){
            if(protocolActivations.putIfAbsent(Bytes.hex(record.key()),record)!=null)throw new IllegalStateException("duplicate Target protocol activation");
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
                    || (result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL && result.mutationType() != SystemMutationType.TARGET_SLO_OBSERVATION)
                    || result.applyStatus() == com.nereusstream.delay.runtime.ApplyStatus.APPLIED
                            && AuthorIdentity.decode(result.authorIdentity()).kind() != (result.mutationType()==SystemMutationType.TARGET_SLO_OBSERVATION
                                ?AuthorIdentity.Kind.SERVICE:AuthorIdentity.Kind.CONTROL)) {
                throw new IllegalArgumentException("Target Control reference differs from its root/type/author");
            }
            controlResults.put(Bytes.hex(first.key()), first);
        }

        void retirementResult(TargetResultRecord first){
            final var result=SystemMutationResult.decode(first.typedPayload());
            if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(root.root().identity())
                    ||result.mutationType()!=SystemMutationType.RESOURCE_RETIRE_INTENT&&result.mutationType()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED
                        &&result.mutationType()!=SystemMutationType.TARGET_RESOURCE_RECLAIM)
                throw new IllegalArgumentException("retirement first differs from actual Source root/type");
            retirementResults.put(Bytes.hex(first.key()),first);
        }
        void systemPosition(TargetResultRecord position){
            if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM)throw new IllegalArgumentException("actual system POSITION required");
            if(systemPositions.putIfAbsent(Bytes.hex(position.key()),position)!=null)throw new IllegalStateException("duplicate physical system POSITION");
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

        void reservation(byte[] key,com.nereusstream.delay.runtime.TargetReservationRecord record){
            if(reservationProjections.putIfAbsent(Bytes.hex(key),record)!=null)throw new IllegalStateException("duplicate physical Native reservation projection");
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
