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

    static Adoption readAdoption(TargetStoreBackend.Reader reader, TargetKafkaReceiptMapping mapping,
            TargetQuotaScope scope, byte[] lineage) {
            final var actual = TargetKafkaReceiptMapping.decodeForStore(mapping.key(), payload(reader, mapping.key(),
                    TargetKafkaReceiptMapping.VALUE_TYPE), scope.shard());
            if (!Arrays.equals(actual.canonicalBytes(), mapping.canonicalBytes())) { throw new IllegalStateException("Source adoption refers to another immutable mapping"); }
            actual.mutation().requireAtOrBefore(reader.aggregate().mutation());
            final byte[] firstId = com.nereusstream.delay.protocol.SystemMutation.computeSystemMutationId(scope.shard(),
                    com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    actual.admission().publishAttemptId(), actual.mutationHash());
            final byte[] firstKey = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, firstId);
            final byte[] firstRaw = reader.get(ColumnFamily.DEDUPE, firstKey);
            if (firstRaw == null) { throw new IllegalStateException("external adoption lacks actual Admission first result"); }
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(firstRaw, TargetResultRecord.VALUE_TYPE).payload());
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM || !first.mutation().equals(actual.mutation())
                    || result.applyStatus() != ApplyStatus.APPLIED || result.stableCode() != com.nereusstream.delay.protocol.StableCode.OK
                    || result.mutationType() != com.nereusstream.delay.protocol.SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || !Arrays.equals(result.mutationId(), firstId) || !Arrays.equals(result.mutationHash(), actual.mutationHash())
                    || !Arrays.equals(result.appliedSourcePosition(), actual.mutation().source().canonicalBytes())) {
                throw new IllegalStateException("external adoption lacks exact Source APPLIED first facts");
            }
            final byte[] positionKey = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    actual.mutation().source().canonicalBytes());
            final byte[] positionRaw = reader.get(ColumnFamily.DEDUPE, positionKey);
            if (positionRaw == null) { throw new IllegalStateException("external adoption lost its POSITION companion"); }
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(positionRaw, TargetResultRecord.VALUE_TYPE).payload());
            position.requireFirst(first);
            final var budget = TargetQuotaAttemptBudget.decodeForStore(Bytes.concat(new byte[] {TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,
                    TargetKeyCodec.KEY_FORMAT}, actual.admission().publishAttemptId()), payload(reader, Bytes.concat(new byte[] {
                    TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG, TargetKeyCodec.KEY_FORMAT}, actual.admission().publishAttemptId()),
                    TargetQuotaAttemptBudget.VALUE_TYPE), scope.shard());
            final var body = actual.admission();
            if (!Arrays.equals(budget.admissionDigest(),actual.mutationHash()) || !budget.locator().equals(body.locator())
                    || budget.executionBytes()!=body.executionBytes() || !budget.commitment().equals(body.commitment())
                    || !Arrays.equals(budget.recoveryLineage(),lineage)) {
                throw new IllegalStateException("external adoption lacks its exact original Admission/Attempt commitment");
            }
            final var owners = new ArrayList<TargetExternalEvidenceOwner>();
            for (int kind : List.of(TargetKafkaReceiptControlArtifact.PROTECTION, TargetKafkaReceiptControlArtifact.ANCHOR)) {
                final byte[] refKey = TargetExternalEvidenceReference.key(kind, actual.mappingId());
                final var ref = TargetExternalEvidenceReference.decodeForStore(refKey, payload(reader, refKey,
                        TargetExternalEvidenceReference.VALUE_TYPE), scope.shard(), lineage);
                if (!ref.mutation().equals(actual.mutation()) || !Arrays.equals(ref.attemptId(), budget.publishAttemptId())
                        || !ref.referenceOwner().equals(budget.primaryIdentity()) || !ref.accounting().equals(budget.accounting())) {
                    throw new IllegalStateException("external adoption reference changed exact mapping/Attempt scope");
                }
                final byte[] digest = kind == TargetKafkaReceiptControlArtifact.PROTECTION
                        ? actual.admission().kafkaReceipt().retentionProtectionDigest() : actual.admission().kafkaReceipt().scanObservationDigest();
                if (!Arrays.equals(digest, ref.artifactDigest())) { throw new IllegalStateException("external adoption references replaced artifact"); }
                final byte[] ownerKey = TargetExternalEvidenceOwner.key(kind, digest);
                final var owner = TargetExternalEvidenceOwner.decodeForStore(ownerKey, payload(reader, ownerKey,
                        TargetExternalEvidenceOwner.VALUE_TYPE), scope.shard(), scope.tenantScope(), lineage);
                owner.createdAt().requireAtOrBefore(actual.mutation());
                final var liveOwner = descriptor(reader, owner.primaryOwner().identity(), scope, lineage);
                if (!liveOwner.accounting().equals(owner.primaryOwner().accounting())
                        || !liveOwner.identity().target().equals(budget.primaryIdentity().target())
                        || !owner.originalDomain().equals(actual.admission().channel().context().domain())) {
                    throw new IllegalStateException("external adoption changed frozen primary Target/accounting/recovery scope");
                }
                if (kind == TargetKafkaReceiptControlArtifact.ANCHOR) {
                    final byte[] indexKey = referenceByAttemptKey(actual.admission().publishAttemptId());
                    final var index = TargetExternalEvidenceReference.decodeForStore(indexKey,
                            payload(reader, indexKey, TargetExternalEvidenceReference.VALUE_TYPE), scope.shard(), lineage);
                    if (!Arrays.equals(index.canonicalBytes(), ref.canonicalBytes())) {
                        throw new IllegalStateException("external adoption lost exact attempt-reference alias");
                    }
                }
                owners.add(owner);
            }
            first.requireOwner(descriptor(reader, new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                    reader.aggregate().accountingIncarnation(), null, null), scope, lineage));
            position.requireOwner(descriptor(reader, new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                    reader.aggregate().accountingIncarnation(), null, null), scope, lineage));
            reader.requireWithinElapsedBudget();
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
        final byte[] key = identity.key(); key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var value = TargetQuotaIncarnation.decodeForStore(key, payload(reader, key, TargetQuotaIncarnation.VALUE_TYPE), scope.shard(), scope.tenantScope());
        if (!value.identity().equals(identity) || !Arrays.equals(value.recoveryLineage(), lineage)) {
            throw new IllegalStateException("external artifact owner differs from its actual frozen Target incarnation");
        }
        return value;
    }
    private static byte[] payload(TargetStoreBackend.Reader reader, byte[] key, int type) {
        return TargetValueEnvelope.decode(required(reader, key, List.of()), type).payload();
    }
    private static byte[] required(TargetRecordAccounting.View reader, byte[] key, List<TargetStoreBackend.Edit> overlay) {
        final byte[] value = reader.projected(ColumnFamily.META, key, overlay);
        if (value == null) { throw new IllegalStateException("retained external evidence record is missing"); } return value;
    }
}
