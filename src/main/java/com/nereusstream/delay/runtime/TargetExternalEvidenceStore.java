package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.TargetChannelControlBody;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetExternalEvidenceOwner;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Two unique external owners and exact attempt references in the same native Admission/accounting batch. */
public final class TargetExternalEvidenceStore {
    private TargetExternalEvidenceStore() {}

    /** Non-DTO evidence from actual Source first result and retained owner/ref rows, usable for conservative catalog adoption. */
    public static final class Adoption {
        private final TargetKafkaReceiptMapping mapping;
        private final TargetExternalEvidenceOwner protection;
        private final TargetExternalEvidenceOwner anchor;
        private Adoption(TargetKafkaReceiptMapping mapping, TargetExternalEvidenceOwner protection, TargetExternalEvidenceOwner anchor) {
            this.mapping = mapping; this.protection = protection; this.anchor = anchor;
        }
        public TargetKafkaReceiptMapping mapping() { return mapping; }
        public TargetExternalEvidenceOwner protection() { return protection; }
        public TargetExternalEvidenceOwner anchor() { return anchor; }
    }

    public static Adoption readAdoption(TargetStoreBackend backend, com.nereusstream.delay.store.BoundedReadBudget reads,
            TargetKafkaReceiptMapping mapping, TargetQuotaScope scope, byte[] lineage, TargetStoreBackend.ReadAuthority authority) {
        return backend.guardedRead(reads, reader -> readAdoption(reader, mapping, scope, lineage), authority);
    }

    /** Actual complete Native use/Attempt preflight for one captured immutable Catalog cut; no deletion permission. */
    public static final class ControlClosure {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot;
        private ControlClosure(OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot){this.snapshot=snapshot;}
        public OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot(){return snapshot;}
    }
    public static ControlClosure readControlClosure(TargetStoreBackend backend,com.nereusstream.delay.store.BoundedReadBudget reads,
            TargetQuotaScope scope,byte[] lineage,OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot,
            TargetStoreBackend.ReadAuthority authority){
        if(scope.target()!=null||reads.maxBytes()==Long.MAX_VALUE
                ||snapshot.preparedUses().size()>com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES)
            throw new IllegalArgumentException("CONTROL closure requires actual Shard scope and finite complete use/read bounds");
        return backend.guardedRead(reads,reader->{
            readControlUses(reader,scope,lineage,snapshot.owner(),null,snapshot.preparedUses());
            return new ControlClosure(snapshot);
        },authority);
    }

    private record ControlUseState(com.nereusstream.delay.store.StoreMetadata store,
            com.nereusstream.delay.protocol.TargetQuotaMutation cut,com.nereusstream.delay.store.IngressFenceState fence,
            List<com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Use> uses) {}

    /** Actual closed Catalog/Native input cut. Serialized context data alone cannot construct this receipt. */
    public static final class ClosedUses {
        private final OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure;
        private final com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource;
        private final ControlUseState state;
        private ClosedUses(OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource,ControlUseState state){
            this.closure=closure;this.resource=resource;this.state=state;
        }
        public OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure(){return closure;}
        public com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource(){return resource;}
        public com.nereusstream.delay.protocol.TargetEvidenceRetirementContext retirementContext(byte[] copyDigest,long copyBytes){
            final var owner=resource.provenance() instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Protection p
                    ?p.owner():((com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Anchor)resource.provenance()).owner();
            return new com.nereusstream.delay.protocol.TargetEvidenceRetirementContext(closure.revision(),closure.digest(),
                    closure.canonicalBytes().length,state.store,state.cut,state.fence,state.uses,copyDigest,copyBytes,owner.primaryOwner());
        }
    }
    public static ClosedUses readClosedControlUses(TargetStoreBackend backend,com.nereusstream.delay.store.BoundedReadBudget reads,
            TargetQuotaScope scope,byte[] lineage,OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            Adoption applied,int kind,TargetStoreBackend.ReadAuthority authority){
        if(scope.target()!=null||reads.maxBytes()==Long.MAX_VALUE
                ||closure.preparedUses().size()>com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.MAX_USES)
            throw new IllegalArgumentException("closed CONTROL uses require actual Shard scope and finite complete read bounds");
        final var owner=kind==TargetKafkaReceiptControlArtifact.PROTECTION?Objects.requireNonNull(applied).protection()
                :kind==TargetKafkaReceiptControlArtifact.ANCHOR?Objects.requireNonNull(applied).anchor():null;
        if(owner==null)throw new IllegalArgumentException("closed CONTROL uses require protection or anchor");
        return backend.guardedRead(reads,reader->{
            final com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Provenance provenance;
            final com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Anchor anchor;
            if(kind==TargetKafkaReceiptControlArtifact.PROTECTION){
                anchor=null;provenance=new com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Protection(owner);
            }else{
                final byte[] indexKey=referenceByAttemptKey(owner.creatorAttemptId());
                final var creator=TargetExternalEvidenceReference.decodeForStore(indexKey,
                        payload(reader,indexKey,TargetExternalEvidenceReference.VALUE_TYPE),scope.shard(),lineage);
                if(creator.kind()!=kind||!Arrays.equals(creator.artifactDigest(),owner.artifactDigest())
                        ||!Arrays.equals(creator.attemptId(),owner.creatorAttemptId()))
                    throw new IllegalStateException("closed anchor lost actual creator Attempt alias");
                final byte[] mappingKey=Bytes.concat(new byte[]{TargetKeyCodec.KAFKA_RECEIPT_MAPPING_TAG,TargetKeyCodec.KEY_FORMAT},creator.mappingId());
                final var mapping=TargetKafkaReceiptMapping.decodeForStore(mappingKey,payload(reader,mappingKey,TargetKafkaReceiptMapping.VALUE_TYPE),scope.shard());
                anchor=new com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Anchor(owner,mapping.mappingId(),mapping.mutationHash());
                provenance=anchor;
            }
            final var resource=closure.controlResource(provenance);
            final var state=readControlUses(reader,scope,lineage,owner,anchor,closure.preparedUses());
            reader.requireWithinElapsedBudget();return new ClosedUses(closure,resource,state);
        },authority);
    }

    private static ControlUseState readControlUses(TargetStoreBackend.Reader reader,TargetQuotaScope scope,byte[] lineage,
            TargetExternalEvidenceOwner owner,com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Anchor anchor,
            List<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedPreparedUse> preparedUses){
        final var cut=reader.aggregate().mutation();
        cut.requireAtOrBefore(reader.sourceSequence(),reader.source());
        final var sourceOwner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),reader.aggregate().accountingIncarnation(),null,null);
        final var root=descriptor(reader,sourceOwner,scope,lineage);root.latestMutation().requireAtOrBefore(cut);
        final byte[] rawFence=reader.get(ColumnFamily.META,com.nereusstream.delay.store.KeyCodec.metaFixed(4));
        final var fence=rawFence==null?new com.nereusstream.delay.store.IngressFenceState(com.nereusstream.delay.store.IngressFenceState.OPEN,null)
                :com.nereusstream.delay.store.IngressFenceState.decode(TargetValueEnvelope.decode(rawFence,1).payload());
        final var uses=new ArrayList<com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Use>();
        for(var use:preparedUses){
            final byte[] key=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_SYSTEM_TAG,TargetKeyCodec.KEY_FORMAT},use.id());
            final byte[] raw=reader.get(ColumnFamily.DEDUPE,key);
            if(raw==null){
                if(fence.proofId()==null||fence.closedThroughEpochMs()<use.retryUntil())
                    throw new IllegalStateException("CONTROL use still has a possible future first Admission");
                uses.add(new com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Use(use.id(),use.hash(),use.retryUntil(),
                        com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Conclusion.SOURCE_FENCED,new byte[0],new byte[0]));continue;
            }
            final var first=TargetResultRecord.decode(TargetValueEnvelope.decode(raw,TargetResultRecord.VALUE_TYPE).payload());
            first.requireStored(key,TargetResultRecord.VALUE_TYPE,first.canonicalBytes());final var result=SystemMutationResult.decode(first.typedPayload());
            if(first.kind()!=TargetResultRecord.Kind.SYSTEM||!first.primaryIdentity().equals(sourceOwner)
                    ||result.mutationType()!=com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION
                    ||!Arrays.equals(result.mutationId(),use.id())||!Arrays.equals(result.mutationHash(),use.hash())
                    ||result.retryUntilEpochMs()!=use.retryUntil()||!Arrays.equals(result.appliedSourcePosition(),first.mutation().source().canonicalBytes()))
                throw new IllegalStateException("CONTROL sealed use changed original Admission first");
            first.mutation().requireAtOrBefore(cut);first.requireOwner(root);
            final byte[] positionKey=Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG,TargetKeyCodec.KEY_FORMAT},first.mutation().source().canonicalBytes());
            final byte[] positionRaw=Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE,positionKey),"CONTROL Admission POSITION");
            final var position=TargetResultRecord.decode(TargetValueEnvelope.decode(positionRaw,TargetResultRecord.VALUE_TYPE).payload());
            position.requireStored(positionKey,TargetResultRecord.VALUE_TYPE,position.canonicalBytes());position.requireFirst(first);position.requireOwner(root);
            if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(first.mutation()))
                throw new IllegalStateException("CONTROL use lost exact POSITION Source stamp");
            uses.add(new com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Use(use.id(),use.hash(),use.retryUntil(),
                    result.applyStatus()==ApplyStatus.APPLIED?com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Conclusion.RETAINED_APPLIED
                        :com.nereusstream.delay.protocol.TargetEvidenceRetirementContext.Conclusion.RETAINED_REJECTED,first.digest(),position.digest()));
        }
        TargetEvidenceRetirementStore.requireControlReferences(reader,List.of(),scope,lineage,sourceOwner,owner,anchor,uses,cut,cut);
        reader.requireWithinElapsedBudget();return new ControlUseState(reader.metadata(),cut,fence,List.copyOf(uses));
    }

    static Adoption readAdoption(TargetStoreBackend.Reader reader, TargetKafkaReceiptMapping mapping,
            TargetQuotaScope scope, byte[] lineage) {
        final var sourceOwner=new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,scope.shard(),
                reader.aggregate().accountingIncarnation(),null,null);
        final var result=readAdoption(reader,List.of(),mapping,scope,lineage,reader.aggregate().mutation(),sourceOwner);
        reader.requireWithinElapsedBudget();return result;
    }

    /** Same exact joins over the selected projected view, including immutable recovery accounting. */
    static Adoption readAdoption(TargetRecordAccounting.View reader,List<TargetStoreBackend.Edit> overlay,
            TargetKafkaReceiptMapping mapping,TargetQuotaScope scope,byte[] lineage,
            com.nereusstream.delay.protocol.TargetQuotaMutation frontier,TargetQuotaIdentity sourceOwner) {
        if(sourceOwner.kind()!=TargetQuotaIdentity.Kind.SHARD||!sourceOwner.shard().equals(scope.shard()))
            throw new IllegalArgumentException("external adoption requires its actual Source root");
        final var actual = TargetKafkaReceiptMapping.decodeForStore(mapping.key(), payload(reader, overlay, mapping.key(),
                TargetKafkaReceiptMapping.VALUE_TYPE), scope.shard());
        if (!Arrays.equals(actual.canonicalBytes(), mapping.canonicalBytes())) { throw new IllegalStateException("Source adoption refers to another immutable mapping"); }
        final byte[] bodyHash=com.nereusstream.delay.protocol.SystemMutation.computeMutationHash(scope.shard(),
                com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION,
                actual.admission().retryUntilEpochMs(),actual.admission().canonicalBytes());
        if(!Arrays.equals(bodyHash,actual.mutationHash())||!Arrays.equals(actual.recoveryLineage(),lineage))
            throw new IllegalStateException("external adoption changed original Admission body hash/lineage");
        actual.mutation().requireAtOrBefore(frontier);
        final byte[] firstId = com.nereusstream.delay.protocol.SystemMutation.computeSystemMutationId(scope.shard(),
                com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION,
                actual.admission().publishAttemptId(), actual.mutationHash());
        final byte[] firstKey = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, firstId);
        final byte[] firstRaw = reader.projected(ColumnFamily.DEDUPE, firstKey, overlay);
        if (firstRaw == null) { throw new IllegalStateException("external adoption lacks actual Admission first result"); }
        final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(firstRaw, TargetResultRecord.VALUE_TYPE).payload());
        first.requireStored(firstKey, TargetResultRecord.VALUE_TYPE, first.canonicalBytes());
        final var result = SystemMutationResult.decode(first.typedPayload());
        final var author=com.nereusstream.delay.protocol.AuthorIdentity.decode(result.authorIdentity());
        if (first.kind() != TargetResultRecord.Kind.SYSTEM || !first.mutation().equals(actual.mutation())
                ||author.kind()!=com.nereusstream.delay.protocol.AuthorIdentity.Kind.OWNER||!actual.admission().owner().equals(author.asOwnerIdentity())
                || result.applyStatus() != ApplyStatus.APPLIED || result.stableCode() != com.nereusstream.delay.protocol.StableCode.OK
                || !first.primaryIdentity().equals(sourceOwner) || result.retryUntilEpochMs()!=actual.admission().retryUntilEpochMs()
                || result.mutationType() != com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !Arrays.equals(result.mutationId(), firstId) || !Arrays.equals(result.mutationHash(), actual.mutationHash())
                || !Arrays.equals(result.appliedSourcePosition(), actual.mutation().source().canonicalBytes())) {
            throw new IllegalStateException("external adoption lacks exact Source APPLIED first facts");
        }
        final byte[] positionKey = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                actual.mutation().source().canonicalBytes());
        final byte[] positionRaw = reader.projected(ColumnFamily.DEDUPE, positionKey, overlay);
        if (positionRaw == null) { throw new IllegalStateException("external adoption lost its POSITION companion"); }
        final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(positionRaw, TargetResultRecord.VALUE_TYPE).payload());
        position.requireStored(positionKey, TargetResultRecord.VALUE_TYPE, position.canonicalBytes());
        position.requireFirst(first);
        if(position.kind()!=TargetResultRecord.Kind.POSITION_SYSTEM||!position.mutation().equals(actual.mutation()))
            throw new IllegalStateException("external adoption POSITION changed original Source");
        final var budget = TargetQuotaAttemptBudget.decodeForStore(Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,
                TargetKeyCodec.KEY_FORMAT}, actual.admission().publishAttemptId()), payload(reader, overlay, Bytes.concat(new byte[] {
                TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT}, actual.admission().publishAttemptId()),
                TargetQuotaAttemptBudget.VALUE_TYPE), scope.shard());
        budget.mutation().requireAtOrBefore(frontier);
        final var body = actual.admission();
        if (!Arrays.equals(budget.admissionDigest(),actual.mutationHash()) || !budget.locator().equals(body.locator())
                || budget.executionBytes()!=body.executionBytes() || !budget.commitment().equals(body.commitment())
                || !Arrays.equals(budget.recoveryLineage(),lineage)) {
            throw new IllegalStateException("external adoption lacks its exact original Admission/Attempt commitment");
        }
        final var owners = new ArrayList<TargetExternalEvidenceOwner>();
        for (int kind : List.of(TargetKafkaReceiptControlArtifact.PROTECTION, TargetKafkaReceiptControlArtifact.ANCHOR)) {
            final byte[] refKey = TargetExternalEvidenceReference.key(kind, actual.mappingId());
            final var ref = TargetExternalEvidenceReference.decodeForStore(refKey, payload(reader, overlay, refKey,
                    TargetExternalEvidenceReference.VALUE_TYPE), scope.shard(), lineage);
            if (!ref.mutation().equals(actual.mutation()) || !Arrays.equals(ref.attemptId(), budget.publishAttemptId())
                    || !ref.referenceOwner().equals(budget.primaryIdentity()) || !ref.accounting().equals(budget.accounting())) {
                throw new IllegalStateException("external adoption reference changed exact mapping/Attempt scope");
            }
            final byte[] digest = kind == TargetKafkaReceiptControlArtifact.PROTECTION
                    ? actual.admission().kafkaReceipt().retentionProtectionDigest() : actual.admission().kafkaReceipt().scanObservationDigest();
            if (!Arrays.equals(digest, ref.artifactDigest())) { throw new IllegalStateException("external adoption references replaced artifact"); }
            final byte[] ownerKey = TargetExternalEvidenceOwner.key(kind, digest);
            final var owner = TargetExternalEvidenceOwner.decodeForStore(ownerKey, payload(reader, overlay, ownerKey,
                    TargetExternalEvidenceOwner.VALUE_TYPE), scope.shard(), scope.tenantScope(), lineage);
            owner.createdAt().requireAtOrBefore(actual.mutation());
            final var liveOwner = descriptor(reader, overlay, owner.primaryOwner().identity(), scope, lineage);
            liveOwner.latestMutation().requireAtOrBefore(frontier);
            if (!liveOwner.accounting().equals(owner.primaryOwner().accounting())
                    || !liveOwner.identity().target().equals(budget.primaryIdentity().target())
                    || !owner.originalDomain().equals(actual.admission().channel().context().domain())) {
                throw new IllegalStateException("external adoption changed frozen primary Target/accounting/recovery scope");
            }
            if (kind == TargetKafkaReceiptControlArtifact.ANCHOR) {
                final byte[] indexKey = referenceByAttemptKey(actual.admission().publishAttemptId());
                final var index = TargetExternalEvidenceReference.decodeForStore(indexKey,
                        payload(reader, overlay, indexKey, TargetExternalEvidenceReference.VALUE_TYPE), scope.shard(), lineage);
                if (!Arrays.equals(index.canonicalBytes(), ref.canonicalBytes())) {
                    throw new IllegalStateException("external adoption lost exact attempt-reference alias");
                }
            }
            owners.add(owner);
        }
        first.requireOwner(descriptor(reader, overlay, sourceOwner, scope, lineage));
        position.requireOwner(descriptor(reader, overlay, sourceOwner, scope, lineage));
        return new Adoption(actual, owners.get(0), owners.get(1));
    }

    public static List<TargetStoreBackend.Edit> adopt(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            TargetKafkaReceiptMapping mapping, TargetQuotaAttemptBudget attempt, TargetKafkaReceiptExternalEvidence evidence) {
        evidence.requireAdmission(mapping.admission());
        final var original = evidence.protection();
        final var control = original.control();
        final var request = (TargetChannelControlRequest) control.request().branch();
        final var originalChannel = request.channel();
        // Protect the original recovery domain. Owner/channel renewal does not transfer the old blob's primary charge.
        if (!Arrays.equals(originalChannel.context().producerIdentity(), mapping.admission().channel().context().producerIdentity())) {
            throw new IllegalArgumentException("protection artifact cannot be borrowed across Target/recovery domains");
        }
        final var reference = new ControlRef(control.operationId(), control.requestHash(), 0);
        final byte[] eventKey = TargetKeyCodec.channelControl(reference.logicalOperationIdentity(request.controlKind()));
        final var event = TargetChannelControlRecord.decodeForStore(eventKey,
                payload(reader, eventKey, TargetChannelControlRecord.VALUE_TYPE), scope.shard());
        if (!Arrays.equals(event.body().request().canonicalBytes(), request.canonicalBytes())
                || !event.author().equals(control.author()) || !Arrays.equals(event.recoveryLineage(), lineage)
                || !Arrays.equals(event.mutation().source().canonicalBytes(), original.authorizedAt().canonicalBytes())) {
            throw new IllegalStateException("external protection lacks its original APPLIED Channel Control/Source/lineage");
        }
        event.mutation().requireAtOrBefore(mapping.mutation());
        final var originalContext = originalChannel.context();
        final var protectionIdentity = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.TARGET, scope.shard(),
                originalContext.accountingIncarnation(), originalContext.target(), null);
        final var protectionOwner = descriptor(reader, protectionIdentity, scope, lineage);
        final var anchorOwner = descriptor(reader, attempt.primaryIdentity(), scope, lineage);
        anchorOwner.requireAttemptBudget(attempt);
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        final var protectedBlob = owner(reader, scope, lineage, original, evidence.protectionKey(), protectionOwner,
                originalContext.domain(), original, null, mapping, edits);
        final var anchoredBlob = owner(reader, scope, lineage, evidence.anchor(), evidence.anchorKey(), anchorOwner,
                mapping.admission().channel().context().domain(), original, attempt.publishAttemptId(), mapping, edits);
        if (!attempt.allocated().covers(anchoredBlob.blobCharge().add(
                com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.maximumRecoveryCharge(anchorOwner.accounting())))) {
            throw new IllegalArgumentException("new K2 Admission has not allocated its anchor and finite complete recovery object/native STATE window");
        }
        for (var blob : List.of(protectedBlob, anchoredBlob)) {
            final var ref = new TargetExternalEvidenceReference(blob.kind(), blob.artifactDigest(), mapping.mappingId(),
                    attempt.publishAttemptId(), attempt.primaryIdentity(), attempt.accounting(), mapping.mutation(), lineage);
            if (reader.get(ColumnFamily.META, ref.key()) != null) { throw new IllegalStateException("new mapping already has external evidence reference"); }
            edits.add(reader.replace(ColumnFamily.META, ref.key(), TargetExternalEvidenceReference.VALUE_TYPE, ref.canonicalBytes()));
            if (ref.kind() == TargetKafkaReceiptControlArtifact.ANCHOR) {
                final byte[] index = referenceByAttemptKey(attempt.publishAttemptId());
                if (reader.get(ColumnFamily.META, index) != null) { throw new IllegalStateException("attempt already has an anchor reference"); }
                edits.add(reader.replace(ColumnFamily.META, index, TargetExternalEvidenceReference.VALUE_TYPE, ref.canonicalBytes()));
            }
        }
        return List.copyOf(edits);
    }

    private static TargetExternalEvidenceOwner owner(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            TargetKafkaReceiptControlArtifact artifact, String exactKey, TargetQuotaIncarnation frozenOwner,
            TargetKeyCodec.Domain domain, TargetKafkaReceiptControlArtifact originalControl, byte[] creator,
            TargetKafkaReceiptMapping mapping, List<TargetStoreBackend.Edit> edits) {
        final byte[] key = TargetExternalEvidenceOwner.key(artifact.kind(), artifact.digest());
        final var proposed = new TargetExternalEvidenceOwner(artifact.kind(), artifact.digest(), exactKey,
                Bytes.sha256(artifact.canonicalBytes()), artifact.canonicalBytes().length, frozenOwner, domain,
                Bytes.sha256(originalControl.control().canonicalBytes()), originalControl.authorizedAt(), creator, mapping.mutation());
        final byte[] prior = reader.get(ColumnFamily.META, key);
        if (prior != null) {
            final var existing = TargetExternalEvidenceOwner.decodeForStore(key,
                    TargetValueEnvelope.decode(prior, TargetExternalEvidenceOwner.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope(), lineage);
            existing.requireObject(artifact.kind(), artifact.digest(), exactKey, artifact.canonicalBytes());
            existing.createdAt().requireAtOrBefore(mapping.mutation());
            if (!existing.primaryOwner().identity().equals(frozenOwner.identity())
                    || !existing.primaryOwner().accounting().equals(frozenOwner.accounting())
                    || !existing.originalDomain().equals(domain)
                    || !Arrays.equals(existing.originalControlDigest(), proposed.originalControlDigest())
                    || !Arrays.equals(existing.originalControlSource().canonicalBytes(), proposed.originalControlSource().canonicalBytes())) {
                throw new IllegalStateException("external digest cannot change its frozen primary owner/scope/accounting");
            }
            return existing;
        }
        edits.add(reader.replace(ColumnFamily.META, key, TargetExternalEvidenceOwner.VALUE_TYPE, proposed.canonicalBytes()));
        return proposed;
    }

    /** Reservation consumed by the uniquely owned anchor is excluded from each referencing Attempt's remaining reserve. */
    public static CapacityVector remainingAttemptCharge(TargetRecordAccounting.View reader, List<TargetStoreBackend.Edit> overlay,
            TargetQuotaAttemptBudget budget, TargetQuotaScope scope, byte[] lineage) {
        // Mapping selection is an exact point lookup through the anchor reference indexed by attempt below.
        final byte[] indexKey = referenceByAttemptKey(budget.publishAttemptId());
        final byte[] index = reader.projected(ColumnFamily.META, indexKey, overlay);
        if (index == null) {
            if(reader.projected(ColumnFamily.META,com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.attemptKey(budget.publishAttemptId()),overlay)!=null)
                throw new IllegalStateException("recovery Attempt alias lost original anchor reference");
            return budget.effectiveCharge();
        }
        final var ref = TargetExternalEvidenceReference.decode(TargetValueEnvelope.decode(index,
                TargetExternalEvidenceReference.VALUE_TYPE).payload());
        requireAttemptReference(ref, budget, scope, lineage);
        final byte[] ownerKey = TargetExternalEvidenceOwner.key(ref.kind(), ref.artifactDigest());
        final var owner = TargetExternalEvidenceOwner.decodeForStore(ownerKey,
                TargetValueEnvelope.decode(required(reader, ownerKey, overlay), TargetExternalEvidenceOwner.VALUE_TYPE).payload(),
                scope.shard(), scope.tenantScope(), lineage);
        if (!owner.primaryOwner().identity().equals(budget.primaryIdentity()) || !owner.primaryOwner().accounting().equals(budget.accounting())) {
            throw new IllegalStateException("anchor reservation consumption changes its frozen attempt accounting");
        }
        final var afterAnchor=budget.phase() == TargetQuotaAttemptBudget.Phase.RELEASED ? budget.effectiveCharge()
                : budget.effectiveCharge().subtract(owner.blobCharge());
        return TargetRecoveryEvidenceStore.remainingAttemptCharge(reader,overlay,budget,scope,lineage,afterAnchor);
    }

    public static byte[] referenceByAttemptKey(byte[] attempt) {
        return Bytes.concat(new byte[] {TargetKeyCodec.EXTERNAL_EVIDENCE_REFERENCE_TAG, TargetKeyCodec.KEY_FORMAT, 3}, attempt);
    }
    public static void requireAttemptReference(TargetExternalEvidenceReference ref, TargetQuotaAttemptBudget budget,
            TargetQuotaScope scope, byte[] lineage) {
        if (ref.kind() != TargetKafkaReceiptControlArtifact.ANCHOR || !Arrays.equals(ref.attemptId(), budget.publishAttemptId())
                || !ref.referenceOwner().equals(budget.primaryIdentity()) || !ref.accounting().equals(budget.accounting())
                || !Arrays.equals(ref.lineage(), lineage) || !ref.referenceOwner().shard().equals(scope.shard())) {
            throw new IllegalStateException("anchor reference differs from its exact frozen attempt");
        }
    }
    private static TargetQuotaIncarnation descriptor(TargetStoreBackend.Reader reader, TargetQuotaIdentity identity,
            TargetQuotaScope scope, byte[] lineage) {
        return descriptor(reader,List.of(),identity,scope,lineage);
    }
    private static TargetQuotaIncarnation descriptor(TargetRecordAccounting.View reader,List<TargetStoreBackend.Edit> overlay,
            TargetQuotaIdentity identity,TargetQuotaScope scope,byte[] lineage) {
        final byte[] key = identity.key(); key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var value = TargetQuotaIncarnation.decodeForStore(key, payload(reader, overlay, key, TargetQuotaIncarnation.VALUE_TYPE), scope.shard(), scope.tenantScope());
        if (!value.identity().equals(identity) || !Arrays.equals(value.recoveryLineage(), lineage)) {
            throw new IllegalStateException("external artifact owner differs from its actual frozen Target incarnation");
        }
        return value;
    }
    private static byte[] payload(TargetStoreBackend.Reader reader, byte[] key, int type) {
        return payload(reader,List.of(),key,type);
    }
    private static byte[] payload(TargetRecordAccounting.View reader,List<TargetStoreBackend.Edit> overlay,byte[] key,int type) {
        return TargetValueEnvelope.decode(required(reader,key,overlay),type).payload();
    }
    private static byte[] required(TargetRecordAccounting.View reader, byte[] key, List<TargetStoreBackend.Edit> overlay) {
        final byte[] value = reader.projected(ColumnFamily.META, key, overlay);
        if (value == null) { throw new IllegalStateException("retained external evidence record is missing"); } return value;
    }
}
