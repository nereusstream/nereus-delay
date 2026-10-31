package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.RecoveryCandidateKind;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryInstallState;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetChannelSlot;
import com.nereusstream.delay.protocol.TargetExternalEvidenceOwner;
import com.nereusstream.delay.protocol.TargetExternalEvidenceReference;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetKafkaReceiptMapping;
import com.nereusstream.delay.protocol.TargetKafkaReceiptSlot;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetMigrationArchiveRow;
import com.nereusstream.delay.protocol.TargetMigrationArchiveState;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetExpiryRef;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetResultLedgerAudit;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetTerminalGenerationRecord;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/** Recovery-only fold of actual Target business records against their independently encoded counters. */
final class TargetCheckpointLedgerAudit {
    private TargetCheckpointLedgerAudit() {}

    static TargetCheckpointDependencies audit(
            final RocksDB db,
            final Map<ColumnFamily, ColumnFamilyHandle> handles,
            final ColumnFamilyHandle defaultHandle,
            final TargetCheckpointRootVerifier.RootProof proof,
            final TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
        return audit(new ImageSource(db, handles, defaultHandle), proof, limits, false);
    }

    static TargetCheckpointDependencies auditLive(
            final ShardStore store,
            final BoundedReadBudget readBudget,
            final TargetCheckpointRootVerifier.RootProof proof,
            final TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
        return audit(new StoreSource(store, readBudget, limits.maxRecords()), proof, limits, true);
    }

    private static TargetCheckpointDependencies audit(
            final LedgerSource source,
            final TargetCheckpointRootVerifier.RootProof proof,
            final TargetCheckpointRootVerifier.LedgerAuditLimits limits,
            final boolean auditQuotaRecords) {
        final var dependencies = new TargetCheckpointDependencies.Builder(proof);
        final var budget = new Budget(limits);
        source.requireDefaultEmpty();
        final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
            @Override
            public ShardId shardId() {
                return proof.metadata().shardId();
            }

            @Override
            public byte[] projected(
                    final ColumnFamily family, final byte[] key, final List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty()) {
                    throw new IllegalArgumentException("recovery ledger audit cannot use a write overlay");
                }
                final byte[] raw = source.get(family, key);
                budget.point(key.length, raw == null ? 0 : raw.length);
                return raw;
            }

            @Override
            public ShardStore.KeyValue first(
                    ColumnFamily family, byte[] lower, byte[] upper, List<TargetStoreBackend.Edit> overlay) {
                if (!overlay.isEmpty() || lower == null || upper == null || Arrays.compareUnsigned(lower, upper) >= 0) {
                    throw new IllegalArgumentException(
                            "recovery reference reads require a finite range without overlay");
                }
                final var row = source.first(family, lower, upper);
                budget.point(lower.length + upper.length, row == null ? 0 : row.key().length + row.value().length);
                return row;
            }
        };
        final var accounting = new TargetRecordAccounting(
                view,
                List.of(),
                proof.root().scope(),
                proof.root().recoveryLineage(),
                proof.aggregate().mutation(),
                proof.bookkeeping(),
                TargetQueueState.MAX_DOMAIN_SLOTS);
        final byte[] ingressRaw = view.projected(ColumnFamily.META, KeyCodec.metaFixed(4), List.of());
        dependencies.ingressFence(
                ingressRaw == null
                        ? new IngressFenceState(IngressFenceState.OPEN, null)
                        : IngressFenceState.decode(
                                TargetValueEnvelope.decode(ingressRaw, 1).payload()));
        final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt = new HashMap<>();
        final Map<TargetQuotaIdentity, CapacityVector> resultContributions = new HashMap<>();
        final List<TargetResultLedgerAudit.Stored> resultRows = new ArrayList<>();
        final List<TargetQuotaGrantActivation> grantActivations = new ArrayList<>();
        final List<TargetMembershipClosureRecord> membershipClosures = new ArrayList<>();
        final List<TargetNativePolicyControlRecord> nativeControls = new ArrayList<>();
        final List<TargetChannelControlRecord> channelControls = new ArrayList<>();
        final List<TargetChannelIdentity> channelIdentities = new ArrayList<>();
        final List<TargetChannelSlot> channelSlots = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloObservationState> sloStates = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloObservationGeneration> sloGenerations = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloObservationSample> sloSamples = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloObservationEventRecord> sloEvents = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloCustodyRecord> sloCustodies = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetSloWriterEpoch> sloWriterEpochs = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetParentControlCredit> parentCredits = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetScopeControlObligation> scopeObligations = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetKafkaReceiptSlot> kafkaSlots = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalOwners = new ArrayList<>();
        final Map<String, com.nereusstream.delay.protocol.TargetExternalEvidenceReference> externalReferences =
                new HashMap<>();
        final List<com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> recoveryOwners = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> retirements = new ArrayList<>();
        final Map<String, com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference> recoveryReferences =
                new HashMap<>();
        final List<TargetInitialControlSnapshot> initialControls = new ArrayList<>();
        final List<TargetQuotaAttemptBudget> attemptBudgets = new ArrayList<>();
        final Map<String, TargetQuotaPayloadOwner> payloadOwners = new HashMap<>();
        final var primaryMessages = new java.util.HashSet<String>();
        final var eligibilityMessages = new ArrayList<TargetMessageRecord>();
        final List<com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> sourceHistories =
                new ArrayList<>();
        final List<AttemptProjection> attemptProjections = new ArrayList<>();
        final List<TargetQuotaCounter> counters = new ArrayList<>();
        final List<TargetQuotaTotal> totals = new ArrayList<>();
        final var recovery = new RecoveryMetadata();
        final var rootCharge =
                TargetRecordAccounting.resources(proof.bookkeeping().charge());
        merge(rebuilt, proof.root().identity(), rootCharge);
        merge(rebuilt, proof.root().tenantIdentity(), TargetRecordAccounting.resources(rootCharge.resources()));
        for (ColumnFamily family : ColumnFamily.values()) {
            source.scan(family, budget, (key, raw) -> {
                if (!skipInfrastructure(family, key, raw, proof, counters, totals, rebuilt, recovery)) {
                    final int type = TargetStoreBackend.businessType(family, key);
                    final byte[] payload = TargetValueEnvelope.decode(raw, type).payload();
                    if (family == ColumnFamily.ID && type == TargetScheduleBinding.VALUE_TYPE) {
                        final var binding = TargetScheduleBinding.decode(payload);
                        dependencies.binding(binding);
                    } else if (family == ColumnFamily.ID
                            && type == com.nereusstream.delay.runtime.TargetReservationRecord.VALUE_TYPE) {
                        dependencies.reservation(
                                key, com.nereusstream.delay.runtime.TargetReservationRecord.decode(payload));
                    } else if (family == ColumnFamily.META) {
                        final int tag = Byte.toUnsignedInt(key[0]);
                        if (Arrays.equals(key, KeyCodec.metaFixed(10))) {
                            final var snapshot = TargetInitialControlSnapshot.decode(payload);
                            initialControls.add(snapshot);
                            dependencies.control(family, key, type, payload);
                            snapshot.input()
                                    .profiles()
                                    .forEach(ref -> dependencies.profileUse(new TargetCheckpointDependencies.ProfileUse(
                                            ref, snapshot.input().source())));
                        }
                        if (tag >= TargetKeyCodec.STATE_TAG && tag <= TargetKeyCodec.NATIVE_POLICY_SNAPSHOT_TAG
                                || tag == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG
                                || tag == TargetKeyCodec.CLOSE_TAG
                                || tag == TargetKeyCodec.MEMBERSHIP_CLOSURE_TAG
                                || tag >= TargetKeyCodec.NATIVE_PUBLISHER_TAG) {
                            dependencies.control(family, key, type, payload);
                        }
                        if (type == TargetMembershipGrant.VALUE_TYPE) {
                            dependencies.membership(TargetMembershipGrant.decode(payload));
                        } else if (type == TargetChannelIdentity.VALUE_TYPE) {
                            dependencies.profile(TargetChannelIdentity.decode(payload)
                                    .credentialLease()
                                    .profile());
                        } else if (type == TargetNativePolicyScope.VALUE_TYPE) {
                            dependencies.nativeScope(TargetNativePolicyScope.decode(payload));
                        } else if (type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                            final var control = TargetNativePolicyControlRecord.decode(payload);
                            dependencies.nativeScope(control.body().request().scope());
                            if (control.grant() != null) {
                                dependencies.membership(control.grant());
                            }
                        } else if (type == TargetChannelControlRecord.VALUE_TYPE) {
                            final var event = TargetChannelControlRecord.decode(payload);
                            dependencies.profileUse(new TargetCheckpointDependencies.ProfileUse(
                                    event.body()
                                            .request()
                                            .channel()
                                            .credentialLease()
                                            .profile(),
                                    event.mutation().source()));
                        } else if (type == TargetQuotaPayloadOwner.VALUE_TYPE) {
                            final var owner = TargetQuotaPayloadOwner.decode(payload);
                            if (owner.objectStoreProfileHash() != null) {
                                dependencies.objectHash(owner.objectStoreProfileHash());
                            }
                        }
                    }
                    if (family == ColumnFamily.DEDUPE) {
                        resultRows.add(new TargetResultLedgerAudit.Stored(key, type, payload));
                    } else if (family == ColumnFamily.ID && type == TargetMessageRecord.VALUE_TYPE) {
                        final var message = TargetMessageRecord.decodeForStore(
                                key, payload, proof.metadata().shardId());
                        attemptProjections.add(new AttemptProjection(message.locator(), message.runtime()));
                        dependencies.messageState(message, payload);
                        primaryMessages.add(
                                Bytes.hex(message.locator().messageId().bytes()));
                        eligibilityMessages.add(message);
                        auditMessageDependencies(message, view);
                    } else if (family == ColumnFamily.META && type == TargetOrderState.VALUE_TYPE) {
                        auditOrderStateDependencies(TargetOrderState.decode(payload), view);
                    } else if (family == ColumnFamily.META && type == TargetChannelIdentity.VALUE_TYPE) {
                        channelIdentities.add(TargetChannelIdentity.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetChannelControlRecord.VALUE_TYPE) {
                        channelControls.add(TargetChannelControlRecord.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloObservationState.VALUE_TYPE) {
                        final var state = com.nereusstream.delay.protocol.TargetSloObservationState.decodeForStore(
                                key,
                                payload,
                                new TargetQuotaScope(
                                        proof.metadata().shardId(),
                                        proof.bookkeeping().tenantScope(),
                                        null),
                                proof.root().recoveryLineage());
                        sloStates.add(state);
                        dependencies.sloState(state);
                        dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.head(key, payload.length, state));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloObservationGeneration.VALUE_TYPE) {
                        final var generation =
                                com.nereusstream.delay.protocol.TargetSloObservationGeneration.decodeForStore(
                                        key,
                                        payload,
                                        new TargetQuotaScope(
                                                proof.metadata().shardId(),
                                                proof.bookkeeping().tenantScope(),
                                                null),
                                        proof.root().recoveryLineage());
                        sloGenerations.add(generation);
                        dependencies.sloGeneration(generation);
                        dependencies.scopeNativeRow(
                                TargetScopeNativeUsageAudit.generation(key, payload.length, generation));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloObservationSample.VALUE_TYPE) {
                        final var sample = com.nereusstream.delay.protocol.TargetSloObservationSample.decodeForStore(
                                key,
                                payload,
                                new TargetQuotaScope(
                                        proof.metadata().shardId(),
                                        proof.bookkeeping().tenantScope(),
                                        null),
                                proof.root().recoveryLineage());
                        if (sample.primary(key)) {
                            sloSamples.add(sample);
                            dependencies.sloSample(sample);
                        }
                        dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.sample(key, payload.length, sample));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloObservationEventRecord.VALUE_TYPE) {
                        final var event =
                                com.nereusstream.delay.protocol.TargetSloObservationEventRecord.decodeForStore(
                                        key,
                                        payload,
                                        new TargetQuotaScope(
                                                proof.metadata().shardId(),
                                                proof.bookkeeping().tenantScope(),
                                                null),
                                        proof.root().recoveryLineage());
                        sloEvents.add(event);
                        dependencies.sloEvent(event);
                        dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.event(key, payload.length, event));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloCustodyRecord.VALUE_TYPE) {
                        final var custody = com.nereusstream.delay.protocol.TargetSloCustodyRecord.decodeForStore(
                                key,
                                payload,
                                new TargetQuotaScope(
                                        proof.metadata().shardId(),
                                        proof.bookkeeping().tenantScope(),
                                        null),
                                proof.root().recoveryLineage());
                        sloCustodies.add(custody);
                        dependencies.sloCustody(custody);
                        dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.custody(key, payload.length, custody));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetSloWriterEpoch.VALUE_TYPE) {
                        final var writer = com.nereusstream.delay.protocol.TargetSloWriterEpoch.decodeForStore(
                                key, payload, proof.root().scope(), proof.root().recoveryLineage());
                        writer.mutation().requireAtOrBefore(proof.aggregate().mutation());
                        sloWriterEpochs.add(writer);
                        dependencies.sloWriterEpoch(writer);
                        dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.writer(key, payload.length, writer));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetParentControlCredit.VALUE_TYPE) {
                        final var credit = com.nereusstream.delay.protocol.TargetParentControlCredit.decodeForStore(
                                key, payload, proof.root().scope(), proof.root().recoveryLineage());
                        credit.mutation().requireAtOrBefore(proof.aggregate().mutation());
                        parentCredits.add(credit);
                        dependencies.parentCredit(credit);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetScopeControlObligation.VALUE_TYPE) {
                        final var obligation =
                                com.nereusstream.delay.protocol.TargetScopeControlObligation.decodeForStore(
                                        key,
                                        payload,
                                        proof.root().scope(),
                                        proof.root().recoveryLineage());
                        obligation
                                .mutation()
                                .requireAtOrBefore(proof.aggregate().mutation());
                        scopeObligations.add(obligation);
                        dependencies.scopeObligation(obligation);
                    } else if (family == ColumnFamily.META && type == TargetChannelSlot.VALUE_TYPE) {
                        channelSlots.add(TargetChannelSlot.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE) {
                        final var mapping = com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.decodeForStore(
                                key, payload, proof.metadata().shardId());
                        kafkaMappings.add(mapping);
                        dependencies.kafkaMapping(mapping);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE) {
                        kafkaSlots.add(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.VALUE_TYPE) {
                        final var owner = com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.bookkeeping().tenantScope(),
                                proof.root().recoveryLineage());
                        owner.createdAt().requireAtOrBefore(proof.aggregate().mutation());
                        externalOwners.add(owner);
                        dependencies.externalEvidence(owner);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetExternalEvidenceReference.VALUE_TYPE) {
                        final var ref = com.nereusstream.delay.protocol.TargetExternalEvidenceReference.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.root().recoveryLineage());
                        ref.mutation().requireAtOrBefore(proof.aggregate().mutation());
                        if (externalReferences.put(Bytes.hex(key), ref) != null) {
                            throw new IllegalStateException("duplicate external reference");
                        }
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.VALUE_TYPE) {
                        final var owner = com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.bookkeeping().tenantScope(),
                                proof.root().recoveryLineage());
                        owner.adoptedAt().requireAtOrBefore(proof.aggregate().mutation());
                        recoveryOwners.add(owner);
                        dependencies.recoveryEvidence(owner);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.VALUE_TYPE) {
                        final var reference =
                                com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.decodeForStore(
                                        key,
                                        payload,
                                        proof.metadata().shardId(),
                                        proof.root().recoveryLineage());
                        reference
                                .adoptedAt()
                                .requireAtOrBefore(proof.aggregate().mutation());
                        if (recoveryReferences.put(Bytes.hex(key), reference) != null) {
                            throw new IllegalStateException("duplicate recovery reference");
                        }
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.VALUE_TYPE) {
                        final var record =
                                com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.decodeForStore(
                                        key,
                                        payload,
                                        proof.root().scope(),
                                        proof.root().recoveryLineage());
                        record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                        dependencies.payloadObjectIntent(record);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.VALUE_TYPE) {
                        final var record = com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.bookkeeping().tenantScope(),
                                proof.root().recoveryLineage());
                        record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                        retirements.add(record);
                        dependencies.retirement(record);
                    } else if (family == ColumnFamily.META && type == TargetQuotaGrantActivation.VALUE_TYPE) {
                        grantActivations.add(TargetQuotaGrantActivation.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.bookkeeping().tenantScope()));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.VALUE_TYPE) {
                        final var record = com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.root().tenantScope(),
                                proof.root().recoveryLineage());
                        record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                        dependencies.evidenceConfirmation(record);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.VALUE_TYPE) {
                        final var tip = com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.decodeForStore(
                                key, payload, proof.root().scope(), proof.root().recoveryLineage());
                        tip.decidedAt().requireAtOrBefore(proof.aggregate().mutation());
                        dependencies.historyTip(tip);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.VALUE_TYPE) {
                        final var record =
                                com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.decodeForStore(
                                        key,
                                        payload,
                                        proof.root().scope(),
                                        proof.root().recoveryLineage());
                        record.decidedAt().requireAtOrBefore(proof.aggregate().mutation());
                        dependencies.sourceHistory(record);
                        sourceHistories.add(record);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetProtocolActivationRecord.VALUE_TYPE) {
                        final var record =
                                com.nereusstream.delay.protocol.TargetProtocolActivationRecord.decodeForStore(
                                        key,
                                        payload,
                                        proof.root().scope(),
                                        proof.root().recoveryLineage());
                        record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                        dependencies.protocolActivation(record);
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationRoot.VALUE_TYPE) {
                        dependencies.migrationRoot(com.nereusstream.delay.protocol.TargetMigrationRoot.decode(payload));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationOrigin.VALUE_TYPE) {
                        dependencies.migrationOrigin(
                                com.nereusstream.delay.protocol.TargetMigrationOrigin.decode(payload));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.VALUE_TYPE) {
                        dependencies.migrationGuard(
                                com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.decode(payload));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.VALUE_TYPE) {
                        dependencies.migrationPrefix(
                                com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.decode(payload));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.VALUE_TYPE) {
                        dependencies.migrationAttempt(
                                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.decode(payload));
                    } else if (family == ColumnFamily.META
                            && type == com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.VALUE_TYPE) {
                        if (TargetMigrationArchiveState.isKey(key)) {
                            dependencies.migrationArchiveState(TargetMigrationArchiveState.decode(payload));
                        } else {
                            dependencies.migrationRetirement(
                                    com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.decode(payload));
                        }
                    } else if (family == ColumnFamily.META && type == TargetMigrationArchiveRow.VALUE_TYPE) {
                        dependencies.migrationArchive(TargetMigrationArchiveRow.decode(payload));
                    } else if (family == ColumnFamily.META && type == TargetQuotaAttemptBudget.VALUE_TYPE) {
                        attemptBudgets.add(TargetQuotaAttemptBudget.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetQuotaPayloadOwner.VALUE_TYPE) {
                        final var owner = TargetQuotaPayloadOwner.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.root().tenantScope());
                        payloadOwners.put(Bytes.hex(owner.messageId().bytes()), owner);
                        dependencies.payloadOwner(owner);
                    } else if (family == ColumnFamily.META && type == TargetMembershipClosureRecord.VALUE_TYPE) {
                        membershipClosures.add(TargetMembershipClosureRecord.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.root().recoveryLineage()));
                    } else if (family == ColumnFamily.META && type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                        nativeControls.add(TargetNativePolicyControlRecord.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.root().recoveryLineage()));
                    } else if (family == ColumnFamily.TERMINAL && type == TargetTerminalGenerationRecord.VALUE_TYPE) {
                        final var terminal = TargetTerminalGenerationRecord.decode(payload);
                        if (!Arrays.equals(key, terminal.key())) {
                            throw new IllegalArgumentException("Target checkpoint terminal key mismatch");
                        }
                        attemptProjections.add(new AttemptProjection(terminal.locator(), terminal.runtime()));
                        dependencies.terminalGeneration(terminal);
                    }
                    if (type == com.nereusstream.delay.protocol.TargetQuotaIncarnation.VALUE_TYPE) {
                        dependencies.incarnation(com.nereusstream.delay.protocol.TargetQuotaIncarnation.decodeForStore(
                                key,
                                payload,
                                proof.metadata().shardId(),
                                proof.bookkeeping().tenantScope()));
                    }
                    final var charge = accounting.charge(family, key, raw);
                    if (charge != null) {
                        dependencies.ownerRecord(charge.owner().identity());
                        merge(rebuilt, charge.owner().identity(), charge.primary());
                        merge(rebuilt, charge.owner().tenantIdentity(), charge.mirror());
                        if (family == ColumnFamily.DEDUPE) {
                            resultContributions.merge(
                                    charge.owner().identity(), charge.primary().resources(), CapacityVector::add);
                            resultContributions.merge(
                                    charge.owner().tenantIdentity(),
                                    charge.mirror().resources(),
                                    CapacityVector::add);
                        }
                    }
                }
            });
        }
        counters.forEach(dependencies::quotaCounter);
        if (auditQuotaRecords) {
            TargetCheckpointRootVerifier.auditQuotaRecords(proof, counters, totals, grantActivations);
        }
        auditAttemptBudgetReferences(attemptBudgets, attemptProjections);
        recovery.verify(proof);
        final long resultBytes = limits.maxKeyValueBytes() > Long.MAX_VALUE - limits.maxPointReadBytes()
                ? Long.MAX_VALUE
                : limits.maxKeyValueBytes() + limits.maxPointReadBytes();
        final var resultSummary = TargetResultLedgerAudit.auditRows(
                proof.root().scope(),
                proof.root().recoveryLineage(),
                proof.mutationSequence(),
                proof.source(),
                new TargetResultLedgerAudit.Limits(limits.maxRecords(), limits.maxPointReads(), resultBytes),
                resultRows,
                key -> {
                    final byte[] raw = view.projected(ColumnFamily.META, key, List.of());
                    if (raw == null) {
                        return null;
                    }
                    final var value = TargetValueEnvelope.decodeAny(raw);
                    return new TargetResultLedgerAudit.Stored(key, value.valueType(), value.payload());
                },
                com.nereusstream.delay.runtime.TargetMigrationStore.decodeRoot(view.projected(
                        ColumnFamily.META,
                        com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(
                                proof.metadata().shardId()),
                        List.of())));
        if (resultSummary.resultRecords() != resultRows.size()
                || !resultSummary.contributions().equals(resultContributions)) {
            throw new IllegalStateException("Target checkpoint result ledger differs from actual DEDUPE accounting");
        }
        auditGrantResults(grantActivations, resultRows, proof.root().identity());
        for (final var snapshot : initialControls) {
            if (!snapshot.root().equals(proof.root().identity())
                    || !snapshot.input().scope().equals(proof.root().scope())
                    || !Arrays.equals(snapshot.input().lineage(), proof.root().recoveryLineage())) {
                throw new IllegalStateException("Target initial control snapshot crosses its root/lineage/scope");
            }
            snapshot.mutation().requireAtOrBefore(proof.aggregate().mutation());
            auditGrantResults(
                    List.of(snapshot.initialGrant()), resultRows, proof.root().identity());
            final var first = resultRows.stream()
                    .filter(row -> Arrays.equals(
                            row.key(),
                            Bytes.concat(
                                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1},
                                    snapshot.initialGrant().systemMutationId())))
                    .findFirst()
                    .orElseThrow();
            final var firstAuthor = SystemMutationResult.decode(
                            TargetResultRecord.decode(first.payload()).typedPayload())
                    .authorIdentity();
            if (!Arrays.equals(snapshot.authorIdentity(), firstAuthor)) {
                throw new IllegalStateException("Target initial control snapshot changes its original Control author");
            }
        }
        auditControlResults(
                membershipClosures,
                nativeControls,
                channelControls,
                resultRows,
                proof.root().identity());
        TargetChannelHistoryAudit.requireComplete(channelControls, channelIdentities, channelSlots);
        for (var writer : sloWriterEpochs) {
            com.nereusstream.delay.runtime.TargetSloWriterEpochStore.requireOrigin(
                    view,
                    List.of(),
                    proof.root().scope(),
                    proof.root().recoveryLineage(),
                    proof.root().identity(),
                    writer,
                    proof.aggregate().mutation());
        }
        for (var credit : parentCredits) {
            com.nereusstream.delay.runtime.TargetParentControlCreditStore.requireOrigin(
                    view,
                    List.of(),
                    proof.root().scope(),
                    proof.root().recoveryLineage(),
                    proof.root(),
                    credit,
                    proof.aggregate().mutation());
        }
        for (var obligation : scopeObligations) {
            com.nereusstream.delay.runtime.TargetScopeControlObligationStore.requireOrigin(
                    view,
                    List.of(),
                    proof.root().scope(),
                    proof.root().recoveryLineage(),
                    proof.root(),
                    obligation,
                    proof.aggregate().mutation());
        }
        com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.requireComplete(
                sloStates,
                sloGenerations,
                new TargetQuotaScope(
                        proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                proof.root().recoveryLineage(),
                proof.aggregate().mutation());
        com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.requireSamples(
                sloGenerations,
                sloSamples,
                resultRows.stream()
                        .map(r -> TargetResultRecord.decode(r.payload()))
                        .toList(),
                sloEvents,
                sloCustodies,
                sloWriterEpochs);
        com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.requireEligibilityMessages(
                sloGenerations, sloSamples, eligibilityMessages);
        final var admissions =
                auditAdmissionResults(attemptBudgets, resultRows, proof.root().identity());
        auditKafkaReceiptReferences(
                kafkaMappings,
                kafkaSlots,
                channelIdentities,
                admissions,
                resultRows,
                proof.root().identity());
        auditExternalEvidenceReferences(
                kafkaMappings, externalOwners, externalReferences, attemptBudgets, channelControls);
        auditRecoveryEvidenceReferences(
                kafkaMappings, externalOwners, recoveryOwners, recoveryReferences, attemptBudgets, resultRows);
        auditRetirementResults(retirements, resultRows);
        auditReclaimResults(sourceHistories, attemptBudgets, payloadOwners, primaryMessages, resultRows);
        TargetQuotaDelta.audit(proof.aggregate(), counters, rebuilt);
        for (final var stored : resultRows) {
            if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                final var first = TargetResultRecord.decode(stored.payload());
                if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.APPLY_SHARD_CONTROL
                        || SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.TARGET_SLO_OBSERVATION) {
                    dependencies.controlResult(first);
                } else if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                        == SystemMutationType.TIME_FENCE) {
                    dependencies.timeFence(first);
                } else if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.PUBLISH_OUTCOME
                        || SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.EVIDENCE_RESOLUTION) {
                    dependencies.outcomeResult(first);
                } else if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.RESOURCE_RETIRE_INTENT
                        || SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.RESOURCE_DELETE_CONFIRMED
                        || SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                    dependencies.retirementResult(first);
                }
            } else if (stored.key()[0] == TargetKeyCodec.RESULT_POSITION_TAG) {
                final var position = TargetResultRecord.decode(stored.payload());
                if (position.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    dependencies.systemPosition(position);
                }
            }
        }
        dependencies.captureRetentionDecisions(resultRows);
        final var complete = dependencies.finish(admissions);
        com.nereusstream.delay.runtime.TargetPayloadObligationSummaryAudit.requireComplete(complete);
        com.nereusstream.delay.runtime.TargetMigrationArchiveStore.requireCheckpoint(complete);
        for (var history : complete.sourceHistories()) {
            if (history.input().type() == SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                final var body = com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(
                        history.input().canonicalBody());
                if (body.selfContainedInlinePayloadRelease()) {
                    final var first = complete.retirementResults().stream()
                            .filter(r ->
                                    Arrays.equals(r.logicalId(), history.input().systemMutationId()))
                            .findFirst()
                            .orElseThrow();
                    if (first.appliedFirst()) {
                        com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.requireRecordedSourceFacts(
                                complete, body);
                    }
                } else if (body.uncommittedObjectReclaim()) {
                    final var first = complete.resultRecord(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            history.input().systemMutationId()));
                    if (first.appliedFirst()) {
                        com.nereusstream.delay.runtime.TargetUncommittedPayloadReclaim.requireClosed(
                                complete, history.input(), true, () -> {});
                    }
                } else if (body.nativeRetainedReclaim()) {
                    final var first = complete.resultRecord(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            history.input().systemMutationId()));
                    if (first.appliedFirst()) {
                        com.nereusstream.delay.runtime.TargetNativeRetainedReclaim.requireClosed(
                                complete, history.input(), true, () -> {});
                    }
                }
            }
        }
        return complete;
    }
    /** A reclaim event cannot survive without the actual phase result for every operation in its bounded group. */
    private static void auditReclaimResults(
            List<com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> histories,
            List<TargetQuotaAttemptBudget> budgets,
            Map<String, TargetQuotaPayloadOwner> payloadOwners,
            java.util.Set<String> primaryMessages,
            List<TargetResultLedgerAudit.Stored> results) {
        if (histories.stream().noneMatch(h -> h.input().type() == SystemMutationType.TARGET_RESOURCE_RECLAIM)) {
            return;
        }
        final var byAttempt = new HashMap<String, TargetQuotaAttemptBudget>();
        for (var budget : budgets) {
            byAttempt.put(Bytes.hex(budget.publishAttemptId()), budget);
        }
        final var firsts = new HashMap<String, TargetResultRecord>();
        for (var row : results) {
            final var record = TargetResultRecord.decode(row.payload());
            if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                firsts.put(Bytes.hex(record.logicalId()), record);
            }
        }
        final var projected = com.nereusstream.delay.runtime.TargetNativeRetainedReclaim.projectedBudgets(
                histories, id -> firsts.get(Bytes.hex(id)));
        for (var entry : projected.entrySet()) {
            if (!Arrays.equals(
                    Objects.requireNonNull(byAttempt.get(entry.getKey()), "reclaimed retained Budget")
                            .canonicalBytes(),
                    entry.getValue().canonicalBytes())) {
                throw new IllegalStateException("APPLIED reclaim chain lost latest exact retained allocation state");
            }
        }
        for (var history : histories) {
            if (history.input().type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                continue;
            }
            final var first = Objects.requireNonNull(
                    firsts.get(Bytes.hex(history.input().systemMutationId())), "reclaim actual first");
            if (SystemMutationResult.decode(first.typedPayload()).applyStatus()
                    != com.nereusstream.delay.runtime.ApplyStatus.APPLIED) {
                continue;
            }
            final var body = com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(
                    history.input().canonicalBody());
            for (var operation : body.operations()) {
                switch (operation) {
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.UnusedAttemptReserve unused -> {
                        final var actual = Objects.requireNonNull(
                                byAttempt.get(Bytes.hex(unused.expected().publishAttemptId())),
                                "reclaim actual retained budget");
                        if (!projected.containsKey(Bytes.hex(actual.publishAttemptId()))) {
                            throw new IllegalStateException(
                                    "APPLIED unused-reserve reclaim lost exact actual phase/Source result");
                        }
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.InlinePayloadRelease inline -> {
                        final String id =
                                Bytes.hex(inline.expected().messageId().bytes());
                        final var owner = Objects.requireNonNull(
                                payloadOwners.get(id), "APPLIED inline release lost permanent owner STATE");
                        if (!owner.mutation().equals(history.decidedAt()) || primaryMessages.contains(id)) {
                            throw new IllegalStateException("APPLIED inline release lost actual Source/absent primary");
                        }
                        owner.requireReleasedFrom(inline.expected(), body.floor());
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.UncommittedObjectRetire ignored -> {
                        // The complete original reservation/causal join is checked by its Native history contribution.
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.UncommittedObjectConfirm
                            confirmation -> {
                        final String id = Bytes.hex(confirmation
                                .intent()
                                .expectedOwner()
                                .messageId()
                                .bytes());
                        final var actual = Objects.requireNonNull(payloadOwners.get(id), "uncommitted released owner");
                        if (primaryMessages.contains(id) || !actual.mutation().equals(history.decidedAt())) {
                            throw new IllegalStateException("uncommitted confirmation lost exact final state");
                        }
                        actual.requireReleasedFrom(confirmation.intent().expectedOwner(), body.floor());
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.ObjectPayloadRetire ignored -> {
                        // Full pending/successor verification follows after the dependency fold.
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.ObjectPayloadConfirm
                            confirmation -> {
                        final String id = Bytes.hex(confirmation
                                .intent()
                                .operation()
                                .expected()
                                .messageId()
                                .bytes());
                        final var actual = Objects.requireNonNull(payloadOwners.get(id), "OBJECT released owner");
                        if (primaryMessages.contains(id) || !actual.mutation().equals(history.decidedAt())) {
                            throw new IllegalStateException("OBJECT confirmation lost exact final state");
                        }
                        actual.requireReleasedFrom(
                                confirmation.intent().operation().expected(), body.floor());
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.RetainedRecordDelete leaf -> {
                        for (var row : results) {
                            if (Arrays.equals(row.key(), leaf.expected().key())) {
                                throw new IllegalStateException("APPLIED Native record deletion retains its row");
                            }
                        }
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.IncarnationDrain ignored -> {
                        // Native54 accounting checks exact drain or its later actual retirement successor.
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.IncarnationRetire ignored -> {
                        // Descriptor/grant absence and retained zero counters are checked by the actual Source history.
                    }
                    case com.nereusstream.delay.protocol.TargetResourceReclaimBody.RetainedAllocationRelease
                            allocation -> {
                        if (!projected.containsKey(
                                Bytes.hex(allocation.expected().publishAttemptId()))) {
                            throw new IllegalStateException("APPLIED retained settlement lost its Budget");
                        }
                    }
                }
            }
        }
    }

    @FunctionalInterface
    private interface EntryVisitor {
        void visit(byte[] key, byte[] raw);
    }

    private interface LedgerSource {
        byte[] get(ColumnFamily family, byte[] key);

        ShardStore.KeyValue first(ColumnFamily family, byte[] lower, byte[] upper);

        void requireDefaultEmpty();

        void scan(ColumnFamily family, Budget budget, EntryVisitor visitor);
    }

    static record AttemptProjection(TargetMessageLocator locator, TargetGenerationRuntimeIndex runtime) {
        AttemptProjection {
            if (locator == null || runtime == null) {
                throw new IllegalArgumentException("Target attempt projection is incomplete");
            }
            runtime.requireMessageProjection(locator);
        }
    }

    /** Checks that every unresolved durable attempt reserve has exactly its matching open Message obligation. */
    static void auditAttemptBudgetReferences(
            final List<TargetQuotaAttemptBudget> budgets, final List<AttemptProjection> projections) {
        final Map<String, TargetQuotaAttemptBudget> byAttempt = new HashMap<>();
        for (TargetQuotaAttemptBudget budget : budgets) {
            final String id = HexFormat.of().formatHex(budget.publishAttemptId());
            if (byAttempt.putIfAbsent(id, budget) != null) {
                throw new IllegalStateException("Target checkpoint repeats an attempt budget identity");
            }
        }
        final Map<String, AttemptProjectionRef> referenced = new HashMap<>();
        final Map<TargetMessageLocator, byte[]> generationRuntimes = new HashMap<>();
        for (AttemptProjection projection : projections) {
            final byte[] runtimeDigest = projection.runtime().runtimeDigest();
            final byte[] priorRuntimeDigest = generationRuntimes.putIfAbsent(projection.locator(), runtimeDigest);
            if (priorRuntimeDigest != null && !Arrays.equals(priorRuntimeDigest, runtimeDigest)) {
                throw new IllegalStateException("Target checkpoint Message and Terminal runtime disagree");
            }
            for (AttemptObligationRef obligation : projection.runtime().attemptObligations()) {
                final String id = HexFormat.of().formatHex(obligation.publishAttemptId());
                final var next = new AttemptProjectionRef(projection.locator(), obligation);
                final AttemptProjectionRef prior = referenced.putIfAbsent(id, next);
                if (prior != null
                        && (!prior.locator().equals(next.locator())
                                || !Arrays.equals(
                                        prior.obligation().canonicalBytes(),
                                        next.obligation().canonicalBytes()))) {
                    throw new IllegalStateException("Target checkpoint attempt reference identity conflicts");
                }
            }
        }
        for (Map.Entry<String, AttemptProjectionRef> entry : referenced.entrySet()) {
            final TargetQuotaAttemptBudget budget = byAttempt.get(entry.getKey());
            if (budget == null) {
                throw new IllegalStateException("Target checkpoint open attempt lacks its quota budget");
            }
            final AttemptProjectionRef reference = entry.getValue();
            final boolean matches = budget.locator().equals(reference.locator())
                    && switch (budget.phase()) {
                        case ADMITTED -> reference.obligation().ledgerState() == AttemptLedgerState.PUBLISHING;
                        case UNKNOWN -> reference.obligation().ledgerState() == AttemptLedgerState.UNCERTAIN;
                        case RESOLVED_AWAITING_FLOOR, RETAINED, RELEASED -> false;
                    };
            if (!matches) {
                throw new IllegalStateException("Target checkpoint attempt reference contradicts its quota budget");
            }
        }
        for (Map.Entry<String, TargetQuotaAttemptBudget> entry : byAttempt.entrySet()) {
            final var phase = entry.getValue().phase();
            final boolean open =
                    phase == TargetQuotaAttemptBudget.Phase.ADMITTED || phase == TargetQuotaAttemptBudget.Phase.UNKNOWN;
            if (open != referenced.containsKey(entry.getKey())) {
                throw new IllegalStateException("Target checkpoint open quota budget lacks its attempt reference");
            }
        }
    }

    private record AttemptProjectionRef(TargetMessageLocator locator, AttemptObligationRef obligation) {}

    private static final class ImageSource implements LedgerSource {
        private final RocksDB db;
        private final Map<ColumnFamily, ColumnFamilyHandle> handles;
        private final ColumnFamilyHandle defaultHandle;

        private ImageSource(
                final RocksDB db,
                final Map<ColumnFamily, ColumnFamilyHandle> handles,
                final ColumnFamilyHandle defaultHandle) {
            this.db = db;
            this.handles = handles;
            this.defaultHandle = defaultHandle;
        }

        @Override
        public byte[] get(final ColumnFamily family, final byte[] key) {
            try {
                return db.get(handles.get(family), key);
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot read Target checkpoint accounting dependency", failure);
            }
        }

        @Override
        public ShardStore.KeyValue first(ColumnFamily family, byte[] lower, byte[] upper) {
            try (RocksIterator iterator = db.newIterator(handles.get(family))) {
                iterator.seek(lower);
                final var result = iterator.isValid() && Arrays.compareUnsigned(iterator.key(), upper) < 0
                        ? new ShardStore.KeyValue(iterator.key(), iterator.value())
                        : null;
                iterator.status();
                return result;
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot read Target checkpoint reference range", failure);
            }
        }

        @Override
        public void requireDefaultEmpty() {
            try (RocksIterator empty = db.newIterator(defaultHandle)) {
                empty.seekToFirst();
                if (empty.isValid()) {
                    throw new IllegalArgumentException("Target checkpoint default column family is not empty");
                }
                empty.status();
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot scan Target checkpoint default column family", failure);
            }
        }

        @Override
        public void scan(final ColumnFamily family, final Budget budget, final EntryVisitor visitor) {
            try (RocksIterator iterator = db.newIterator(handles.get(family))) {
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    final byte[] key = iterator.key();
                    final byte[] raw = iterator.value();
                    budget.scan(key.length, raw.length);
                    visitor.visit(key, raw);
                    iterator.next();
                }
                iterator.status();
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot scan Target checkpoint business ledger", failure);
            }
        }
    }

    private static final class StoreSource implements LedgerSource {
        private final ShardStore store;
        private final BoundedReadBudget readBudget;
        private final int scanLimit;

        private StoreSource(final ShardStore store, final BoundedReadBudget readBudget, final int maxRecords) {
            this.store = store;
            this.readBudget = readBudget;
            this.scanLimit = Math.addExact(maxRecords, 1);
        }

        @Override
        public byte[] get(final ColumnFamily family, final byte[] key) {
            return store.get(family, key);
        }

        @Override
        public ShardStore.KeyValue first(ColumnFamily family, byte[] lower, byte[] upper) {
            final ShardStore.KeyValue[] row = {null};
            final var result = store.visitResult(family, lower, upper, 1, readBudget, (entry, shared) -> {
                row[0] = entry;
                return false;
            });
            if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                throw readBudget.incomplete();
            }
            if (result.stop() != ShardStore.VisitStop.RANGE_END
                    && result.stop() != ShardStore.VisitStop.VISITOR_STOPPED) {
                throw new IllegalArgumentException("Target reference range minimum was not established");
            }
            return row[0];
        }

        @Override
        public void requireDefaultEmpty() {
            // ShardStore validates that the default column family is empty when it opens.
        }

        @Override
        public void scan(final ColumnFamily family, final Budget budget, final EntryVisitor visitor) {
            final var result = store.visitResult(family, null, null, scanLimit, readBudget, (entry, shared) -> {
                budget.scan(entry.key().length, entry.value().length);
                visitor.visit(entry.key(), entry.value());
                return true;
            });
            if (result.stop() == ShardStore.VisitStop.INCOMPLETE) {
                throw readBudget.incomplete();
            }
            if (result.stop() != ShardStore.VisitStop.RANGE_END) {
                throw new IllegalArgumentException("Target live ledger scan exceeded its per-family safety bound");
            }
        }
    }

    private static void auditMessageDependencies(
            final TargetMessageRecord message, final TargetRecordAccounting.View view) {
        if (!message.runtime().terminal()) {
            final var expiry = new TargetExpiryRef(message.locator(), message.expireAtEpochMs());
            requireMessageIndex(view, expiry.encodedKey(), TargetExpiryRef.VALUE_TYPE, expiry.canonicalBytes());
        }
        final TargetTimelineWorkRef work = message.runtime().timeline();
        if (work != null) {
            requireMessageIndex(view, work.ordinaryKey(), TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes());
            if (work.nativeCandidate()) {
                requireMessageIndex(view, work.nativeKey(), TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes());
            }
        }
        final byte[] bindingKey =
                TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
        final byte[] bindingRaw = view.projected(ColumnFamily.ID, bindingKey, List.of());
        if (bindingRaw == null) {
            throw new IllegalStateException("Target checkpoint Message lacks its original Schedule binding");
        }
        final TargetScheduleBinding binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(bindingRaw, TargetScheduleBinding.VALUE_TYPE)
                        .payload(),
                view.shardId());
        binding.requireLocator(message.locator());
        binding.requireMessageSource(message.scheduleSource());
    }

    private static void requireMessageIndex(
            final TargetRecordAccounting.View view, final byte[] key, final int type, final byte[] expected) {
        final byte[] raw = view.projected(ColumnFamily.TIMELINE, key, List.of());
        if (raw == null
                || !Bytes.constantTimeEquals(
                        TargetValueEnvelope.decode(raw, type).payload(), expected)) {
            throw new IllegalStateException("Target checkpoint Message lacks its exact current timeline index");
        }
    }

    static void auditOrderStateDependencies(final TargetOrderState state, final TargetRecordAccounting.View view) {
        final var head = state.serviceableHead();
        if (head != null) {
            final byte[] raw = view.projected(ColumnFamily.TIMELINE, head.key(), List.of());
            if (raw == null) {
                throw new IllegalStateException("Target checkpoint strict domain lacks its serviceable head");
            }
            final byte[] work = TargetValueEnvelope.decode(raw, TargetTimelineWorkRef.VALUE_TYPE)
                    .payload();
            state.requireServiceableProjection(head.key(), work, requireOrderMessage(head.messageId(), view));
        }
        final var barrier = state.barrier();
        if (barrier != null) {
            final var current = requireOrderMessage(barrier.locator().messageId(), view);
            if (current.locator().equals(barrier.locator())) {
                state.requireBarrierProjection(current);
            } else {
                if (Integer.compareUnsigned(
                                current.locator().generation(),
                                barrier.locator().generation())
                        <= 0) {
                    throw new IllegalStateException(
                            "Target checkpoint barrier references another or future generation");
                }
                final byte[] key = TargetTerminalGenerationRecord.key(barrier.locator());
                final byte[] raw = view.projected(ColumnFamily.TERMINAL, key, List.of());
                if (raw == null) {
                    throw new IllegalStateException("Target checkpoint historical barrier lacks its terminal summary");
                }
                final var terminal = TargetTerminalGenerationRecord.decode(
                        TargetValueEnvelope.decode(raw, TargetTerminalGenerationRecord.VALUE_TYPE)
                                .payload());
                state.requireTerminalBarrierProjection(terminal);
            }
        }
    }

    private static TargetMessageRecord requireOrderMessage(
            final DelayMessageId messageId, final TargetRecordAccounting.View view) {
        final byte[] key = TargetKeyCodec.message(messageId);
        final byte[] raw = view.projected(ColumnFamily.ID, key, List.of());
        if (raw == null) {
            throw new IllegalStateException("Target checkpoint strict domain lacks its referenced Message");
        }
        return TargetMessageRecord.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(),
                view.shardId());
    }

    private static void auditGrantResults(
            final List<TargetQuotaGrantActivation> grants,
            final List<TargetResultLedgerAudit.Stored> resultRows,
            final TargetQuotaIdentity rootIdentity) {
        final Map<String, TargetResultRecord> systems = new HashMap<>();
        for (TargetResultLedgerAudit.Stored stored : resultRows) {
            if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                systems.put(HexFormat.of().formatHex(stored.key()), TargetResultRecord.decode(stored.payload()));
            }
        }
        for (TargetQuotaGrantActivation grant : grants) {
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, grant.systemMutationId());
            final TargetResultRecord record = systems.get(HexFormat.of().formatHex(key));
            if (record == null
                    || record.kind() != TargetResultRecord.Kind.SYSTEM
                    || !record.primaryIdentity().equals(rootIdentity)
                    || !record.mutation().equals(grant.mutation())) {
                throw new IllegalStateException("Target grant activation lacks its exact first System result");
            }
            final SystemMutationResult outcome = SystemMutationResult.decode(record.typedPayload());
            if (!Arrays.equals(outcome.mutationId(), grant.systemMutationId())
                    || !Arrays.equals(outcome.mutationHash(), grant.systemMutationHash())
                    || outcome.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                    || outcome.applyStatus() != ApplyStatus.APPLIED
                    || outcome.stableCode() != StableCode.OK
                    || !Arrays.equals(
                            outcome.appliedSourcePosition(),
                            grant.mutation().source().canonicalBytes())
                    || (grant.allocation() != null
                                    && grant.allocation().allocation().equals(grant.mutation()))
                            != (record.allocation() != null)) {
                throw new IllegalStateException("Target grant activation contradicts its first System result");
            }
            if (record.allocation() != null
                    && !Arrays.equals(
                            record.allocation().canonicalBytes(),
                            grant.allocation().canonicalBytes())) {
                throw new IllegalStateException("Target grant activation contradicts its first allocation result");
            }
        }
    }

    private static void auditControlResults(
            final List<TargetMembershipClosureRecord> closures,
            final List<TargetNativePolicyControlRecord> controls,
            final List<TargetChannelControlRecord> channels,
            final List<TargetResultLedgerAudit.Stored> resultRows,
            final TargetQuotaIdentity root) {
        final Map<String, TargetResultRecord> systems = new HashMap<>();
        for (final var stored : resultRows) {
            if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                systems.put(HexFormat.of().formatHex(stored.key()), TargetResultRecord.decode(stored.payload()));
            }
        }
        for (final var closure : closures) {
            final var body = closure.body();
            requireControlResult(
                    systems,
                    root,
                    closure.mutation(),
                    body.retryUntil(),
                    body.logicalIdentity(),
                    body.canonicalBytes(),
                    null);
        }
        for (final var control : controls) {
            final var body = control.body();
            final var author = control.author();
            requireControlResult(
                    systems,
                    root,
                    control.mutation(),
                    body.retryUntil(),
                    body.logicalIdentity(),
                    body.canonicalBytes(),
                    AuthorIdentity.control(
                                    author.operationActorIdHash(),
                                    author.authenticatedRoleSetHash(),
                                    author.tenantResourceScopeHash())
                            .canonicalBytes());
        }
        for (final var control : channels) {
            final var body = control.body();
            final var author = control.author();
            requireControlResult(
                    systems,
                    root,
                    control.mutation(),
                    body.retryUntil(),
                    body.logicalIdentity(),
                    body.canonicalBytes(),
                    AuthorIdentity.control(
                                    author.operationActorIdHash(),
                                    author.authenticatedRoleSetHash(),
                                    author.tenantResourceScopeHash())
                            .canonicalBytes());
        }
    }

    /**
     * Independent exact inventory/first-result correlation; complete blob cryptographic authentication is outside Store
     * I/O.
     */
    private static void auditRetirementResults(
            List<com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> records,
            List<TargetResultLedgerAudit.Stored> results) {
        if (records.isEmpty()) {
            return;
        }
        final var byKey = new HashMap<String, TargetResultRecord>();
        for (var stored : results) {
            byKey.put(Bytes.hex(stored.key()), TargetResultRecord.decode(stored.payload()));
        }
        for (var record : records) {
            final var intent = record.intent();
            final byte[] firstKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                    intent.systemMutationId());
            final var first = Objects.requireNonNull(byKey.get(Bytes.hex(firstKey)), "retiring intent actual first");
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.mutation().equals(record.appliedAt())
                    || result.mutationType() != SystemMutationType.RESOURCE_RETIRE_INTENT
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.stableCode() != StableCode.OK
                    || !Arrays.equals(result.mutationId(), intent.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), intent.mutationHash())
                    || result.retryUntilEpochMs() != intent.retryUntilEpochMs()
                    || !Arrays.equals(result.authorIdentity(), intent.authorIdentity())) {
                throw new IllegalStateException("retiring row lacks exact APPLIED intent first");
            }
            final byte[] positionKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    record.appliedAt().source().canonicalBytes());
            final var position = Objects.requireNonNull(byKey.get(Bytes.hex(positionKey)), "retiring intent POSITION");
            position.requireFirst(first);
            if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                    || !position.mutation().equals(record.appliedAt())) {
                throw new IllegalStateException("retiring row changed Source POSITION companion");
            }
        }
    }

    private static void auditRecoveryEvidenceReferences(
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> mappings,
            List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> originalOwners,
            List<com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> owners,
            Map<String, com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference> references,
            List<TargetQuotaAttemptBudget> budgets,
            List<TargetResultLedgerAudit.Stored> results) {
        if (owners.isEmpty() && references.isEmpty()) {
            return;
        }
        final var byMapping = new HashMap<String, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping>();
        final var byBudget = new HashMap<String, TargetQuotaAttemptBudget>();
        final var byOriginal = new HashMap<String, com.nereusstream.delay.protocol.TargetExternalEvidenceOwner>();
        final var byResult = new HashMap<String, TargetResultRecord>();
        for (var mapping : mappings) {
            byMapping.put(Bytes.hex(mapping.mappingId()), mapping);
        }
        for (var budget : budgets) {
            byBudget.put(Bytes.hex(budget.publishAttemptId()), budget);
        }
        for (var original : originalOwners) {
            byOriginal.put(Bytes.hex(original.key()), original);
        }
        for (var row : results) {
            final var result = TargetResultRecord.decode(row.payload());
            result.requireStored(row.key(), row.type(), row.payload());
            byResult.put(Bytes.hex(row.key()), result);
        }
        final var expected = new java.util.HashSet<String>();
        final var objectKeys = new java.util.HashSet<String>();
        final var selectedAttempts = new java.util.HashSet<String>();
        for (var owner : owners) {
            final var ref = owner.reference();
            if (!objectKeys.add(Bytes.hex(owner.key())) || !selectedAttempts.add(Bytes.hex(ref.attemptId()))) {
                throw new IllegalStateException("recovery evidence has duplicate unique object/Attempt adoption");
            }
            final var mapping =
                    Objects.requireNonNull(byMapping.get(Bytes.hex(ref.mappingId())), "recovery original mapping");
            final var budget =
                    Objects.requireNonNull(byBudget.get(Bytes.hex(ref.attemptId())), "recovery original Attempt");
            final var original = Objects.requireNonNull(
                    byOriginal.get(Bytes.hex(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.key(
                            2, mapping.admission().kafkaReceipt().scanObservationDigest()))),
                    "recovery original anchor owner");
            if (!Arrays.equals(
                            owner.primaryOwner().canonicalBytes(),
                            original.primaryOwner().canonicalBytes())
                    || !owner.admissionMutation().equals(mapping.mutation())
                    || !Arrays.equals(owner.admissionHash(), mapping.mutationHash())
                    || !Arrays.equals(ref.attemptId(), mapping.admission().publishAttemptId())
                    || !Arrays.equals(budget.admissionDigest(), owner.admissionHash())
                    || budget.resolvedAt() == null
                    || !budget.resolvedAt().equals(owner.adoptedAt())) {
                throw new IllegalStateException(
                        "recovery owner changed original mapping/Admission/frozen owner/resolved Source");
            }
            for (byte[] key : List.of(
                    owner.referenceRecord().key(), owner.referenceRecord().attemptKey())) {
                final var actual = Objects.requireNonNull(
                        references.get(Bytes.hex(key)), "recovery exact reference/Attempt alias");
                actual.requireOwner(owner);
                actual.requireAttempt(budget);
                expected.add(Bytes.hex(key));
            }
            final byte[] firstKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, owner.outcomeId());
            final var first =
                    Objects.requireNonNull(byResult.get(Bytes.hex(firstKey)), "recovery adopting first result");
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.mutation().equals(owner.adoptedAt())
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.mutationType() != owner.outcomeType()
                    || !Arrays.equals(result.mutationId(), owner.outcomeId())
                    || !Arrays.equals(result.mutationHash(), owner.outcomeHash())
                    || !Arrays.equals(
                            result.appliedSourcePosition(),
                            owner.adoptedAt().source().canonicalBytes())) {
                throw new IllegalStateException("recovery evidence lacks actual adopting APPLIED Source first");
            }
            final byte[] positionKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    owner.adoptedAt().source().canonicalBytes());
            final var position = Objects.requireNonNull(
                    byResult.get(Bytes.hex(positionKey)), "recovery adopting POSITION companion");
            position.requireFirst(first);
            if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                    || !position.mutation().equals(owner.adoptedAt())) {
                throw new IllegalStateException("recovery evidence changed actual Source POSITION");
            }
        }
        if (!expected.equals(references.keySet())) {
            throw new IllegalStateException("recovery reference inventory has orphan/missing rows");
        }
    }

    /**
     * Independent inventory correlation. Complete actual external byte authentication follows outside the Store view.
     */
    private static void auditExternalEvidenceReferences(
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> mappings,
            List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> owners,
            Map<String, com.nereusstream.delay.protocol.TargetExternalEvidenceReference> references,
            List<TargetQuotaAttemptBudget> budgets,
            List<TargetChannelControlRecord> controls) {
        final var byOwner = new HashMap<String, com.nereusstream.delay.protocol.TargetExternalEvidenceOwner>();
        final var byAttempt = new HashMap<String, TargetQuotaAttemptBudget>();
        final var expected = new java.util.HashSet<String>();
        final var used = new java.util.HashSet<String>();
        final var creation = new java.util.HashSet<String>();
        for (var owner : owners) {
            if (byOwner.put(Bytes.hex(owner.key()), owner) != null) {
                throw new IllegalStateException("duplicate external primary owner");
            }
        }
        for (var budget : budgets) {
            byAttempt.put(Bytes.hex(budget.publishAttemptId()), budget);
        }
        for (var mapping : mappings) {
            final var attempt = Objects.requireNonNull(
                    byAttempt.get(Bytes.hex(mapping.admission().publishAttemptId())),
                    "external mapping Attempt budget");
            for (int kind : List.of(1, 2)) {
                final byte[] digest = kind == 1
                        ? mapping.admission().kafkaReceipt().retentionProtectionDigest()
                        : mapping.admission().kafkaReceipt().scanObservationDigest();
                final String key = Bytes.hex(
                        com.nereusstream.delay.protocol.TargetExternalEvidenceReference.key(kind, mapping.mappingId()));
                final var ref =
                        Objects.requireNonNull(references.get(key), "mapping has no original external reference");
                expected.add(key);
                final String ownerKey =
                        Bytes.hex(com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.key(kind, digest));
                final var owner = Objects.requireNonNull(
                        byOwner.get(ownerKey), "external reference has no unique retained owner");
                used.add(ownerKey);
                if (!Arrays.equals(ref.artifactDigest(), digest)
                        || !Arrays.equals(ref.attemptId(), attempt.publishAttemptId())
                        || !ref.referenceOwner().equals(attempt.primaryIdentity())
                        || !ref.accounting().equals(attempt.accounting())
                        || !ref.mutation().equals(mapping.mutation())
                        || !owner.originalDomain()
                                .equals(mapping.admission().channel().context().domain())
                        || !owner.primaryOwner()
                                .identity()
                                .target()
                                .equals(attempt.primaryIdentity().target())) {
                    throw new IllegalStateException(
                            "external reference changed mapping/Attempt/frozen domain responsibility");
                }
                owner.createdAt().requireAtOrBefore(ref.mutation());
                if (owner.createdAt().equals(mapping.mutation())) {
                    if (kind == 2 && !Arrays.equals(owner.creatorAttemptId(), attempt.publishAttemptId())) {
                        throw new IllegalStateException("anchor creator is not its first Source Attempt");
                    }
                    creation.add(ownerKey);
                }
                if (kind == 2) {
                    final String alias =
                            Bytes.hex(com.nereusstream.delay.runtime.TargetExternalEvidenceStore.referenceByAttemptKey(
                                    attempt.publishAttemptId()));
                    expected.add(alias);
                    final var actual =
                            Objects.requireNonNull(references.get(alias), "missing exact attempt-reference alias");
                    if (!Arrays.equals(actual.canonicalBytes(), ref.canonicalBytes())) {
                        throw new IllegalStateException("anchor alias differs from its original reference");
                    }
                }
                final boolean control = controls.stream()
                        .anyMatch(c -> Arrays.equals(
                                        c.mutation().source().canonicalBytes(),
                                        owner.originalControlSource().canonicalBytes())
                                && c.body()
                                        .request()
                                        .channel()
                                        .context()
                                        .target()
                                        .equals(owner.primaryOwner().identity().target())
                                && c.body()
                                        .request()
                                        .channel()
                                        .context()
                                        .domain()
                                        .equals(owner.originalDomain()));
                if (!control) {
                    throw new IllegalStateException(
                            "external evidence lacks its original APPLIED Channel Control domain");
                }
            }
        }
        if (!expected.equals(references.keySet())
                || !used.equals(byOwner.keySet())
                || !creation.equals(byOwner.keySet())) {
            throw new IllegalStateException(
                    "external owner/ref inventory lacks complete original mapping/adoption closure");
        }
    }

    private static void auditKafkaReceiptReferences(
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> mappings,
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptSlot> slots,
            List<TargetChannelIdentity> identities,
            List<TargetCheckpointDependencies.Admission> admissions,
            List<TargetResultLedgerAudit.Stored> results,
            TargetQuotaIdentity root) {
        if (mappings.isEmpty() && slots.isEmpty()) {
            return;
        }
        final var byAttempt = new HashMap<String, TargetCheckpointDependencies.Admission>();
        for (var admission : admissions) {
            byAttempt.put(Bytes.hex(admission.attempt().publishAttemptId()), admission);
        }
        final var bySlot = new HashMap<String, com.nereusstream.delay.protocol.TargetKafkaReceiptSlot>();
        final var byMapping = new HashMap<String, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping>();
        final var latest = new HashMap<String, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping>();
        final var sequenceIds = new java.util.HashSet<String>();
        final var channelIds = new HashMap<String, TargetChannelIdentity>();
        final var resultsByStamp = new HashMap<String, TargetResultRecord>();
        for (var row : results) {
            if (row.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                final var result = TargetResultRecord.decode(row.payload());
                if (resultsByStamp.put(Bytes.hex(result.mutation().canonicalBytes()), result) != null) {
                    throw new IllegalStateException("multiple Source first results share a K2 settlement stamp");
                }
            }
        }
        for (var channel : identities) {
            channelIds.put(Bytes.hex(channel.digest()), channel);
        }
        for (var slot : slots) {
            if (bySlot.put(Bytes.hex(slot.key()), slot) != null) {
                throw new IllegalStateException("duplicate Target K2 slot");
            }
            final var channel = channelIds.get(Bytes.hex(slot.channel().digest()));
            if (channel == null
                    || !Arrays.equals(channel.canonicalBytes(), slot.channel().canonicalBytes())) {
                throw new IllegalStateException("Target K2 slot lacks exact retained channel identity");
            }
        }
        for (var mapping : mappings) {
            final var body = mapping.admission();
            final var retained = byAttempt.get(Bytes.hex(body.publishAttemptId()));
            final var first = retained == null ? null : retained.first();
            final var hash = SystemMutation.computeMutationHash(
                    body.shard(),
                    SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    body.retryUntilEpochMs(),
                    body.canonicalBytes());
            if (first == null
                    || !first.primaryIdentity().equals(root)
                    || !mapping.mutation().equals(first.mutation())
                    || !Arrays.equals(hash, mapping.mutationHash())
                    || !Arrays.equals(retained.attempt().admissionDigest(), hash)
                    || !retained.attempt().locator().equals(body.locator())
                    || retained.attempt().executionBytes() != body.executionBytes()
                    || !retained.attempt().commitment().equals(body.commitment())) {
                throw new IllegalStateException("Target K2 mapping lacks exact retained Admission/budget/first Source");
            }
            final var original = SystemMutationResult.decode(first.typedPayload());
            if (!body.owner()
                            .equals(AuthorIdentity.decode(original.authorIdentity())
                                    .asOwnerIdentity())
                    || original.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                throw new IllegalStateException("Target K2 mapping changes original Owner/deadline");
            }
            final String slotKey =
                    Bytes.hex(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.key(body.channel()));
            final var slot = bySlot.get(slotKey);
            if (slot == null
                    || Long.compareUnsigned(slot.lastAllocatedSequence(), mapping.logicalSequence()) < 0
                    || !Arrays.equals(slot.recoveryLineage(), mapping.recoveryLineage())
                    || !sequenceIds.add(slotKey + "/" + Long.toUnsignedString(mapping.logicalSequence()))
                    || byMapping.put(Bytes.hex(mapping.mappingId()), mapping) != null) {
                throw new IllegalStateException("Target K2 mapping lacks its stable sequence/slot history");
            }
            final var channel = channelIds.get(Bytes.hex(body.channel().digest()));
            if (channel == null
                    || !Arrays.equals(channel.canonicalBytes(), body.channel().canonicalBytes())) {
                throw new IllegalStateException("Target K2 mapping lacks original retained channel generation");
            }
            final var previous = latest.get(slotKey);
            if (previous == null || Long.compareUnsigned(mapping.logicalSequence(), previous.logicalSequence()) > 0) {
                latest.put(slotKey, mapping);
            }
            final boolean unresolved = retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
            if (unresolved) {
                slot.requirePending(mapping);
            } else if (Arrays.equals(slot.pendingMappingId(), mapping.mappingId())) {
                throw new IllegalStateException("resolved Target K2 budget still occupies its logical slot");
            }
        }
        for (var slot : slots) {
            final String key = Bytes.hex(slot.key());
            final var mapping = latest.get(key);
            if (mapping == null || mapping.logicalSequence() != slot.lastAllocatedSequence()) {
                throw new IllegalStateException("Target K2 slot sequence lacks its immutable latest mapping");
            }
            if (slot.pendingMappingId() != null) {
                slot.requirePending(mapping);
                if (!slot.mutation().equals(mapping.mutation())) {
                    throw new IllegalStateException("pending K2 slot changes its Admission Source stamp");
                }
            } else {
                slot.mutation().requireAfter(mapping.mutation());
                final var outcome = resultsByStamp.get(Bytes.hex(slot.mutation().canonicalBytes()));
                if (outcome == null) {
                    throw new IllegalStateException("free Target K2 slot lacks its settling Source result");
                }
                final var result = SystemMutationResult.decode(outcome.typedPayload());
                if (result.applyStatus() != ApplyStatus.APPLIED
                        || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME
                                && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION
                        || !Arrays.equals(
                                result.mutationId(),
                                SystemMutation.computeSystemMutationId(
                                        root.shard(),
                                        result.mutationType(),
                                        mapping.admission().publishAttemptId(),
                                        result.mutationHash()))) {
                    throw new IllegalStateException("free Target K2 slot does not settle its exact latest attempt");
                }
            }
        }
    }

    private static List<TargetCheckpointDependencies.Admission> auditAdmissionResults(
            final List<TargetQuotaAttemptBudget> attempts,
            final List<TargetResultLedgerAudit.Stored> resultRows,
            final TargetQuotaIdentity root) {
        final Map<String, TargetResultRecord> systems = new HashMap<>();
        for (final var stored : resultRows) {
            if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                systems.put(HexFormat.of().formatHex(stored.key()), TargetResultRecord.decode(stored.payload()));
            }
        }
        final var admissions = new ArrayList<TargetCheckpointDependencies.Admission>();
        for (final var attempt : attempts) {
            final var type = SystemMutationType.TARGET_PUBLISH_ADMISSION;
            final byte[] id = SystemMutation.computeSystemMutationId(
                    root.shard(), type, attempt.publishAttemptId(), attempt.admissionDigest());
            final byte[] key =
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
            final var first = systems.get(HexFormat.of().formatHex(key));
            if (first == null
                    || first.kind() != TargetResultRecord.Kind.SYSTEM
                    || first.allocation() != null
                    || !first.primaryIdentity().equals(root)) {
                throw new IllegalStateException("Target retained Budget lacks its exact Admission first result");
            }
            first.mutation().requireAtOrBefore(attempt.mutation());
            if (attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
                if (!first.mutation().equals(attempt.mutation())) {
                    throw new IllegalStateException("Target ADMITTED Budget changes its Admission source stamp");
                }
            } else {
                attempt.mutation().requireAfter(first.mutation());
            }
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (result.mutationType() != type
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.stableCode() != StableCode.OK
                    || AuthorIdentity.decode(result.authorIdentity()).kind() != AuthorIdentity.Kind.OWNER
                    || !Arrays.equals(result.mutationId(), id)
                    || !Arrays.equals(result.mutationHash(), attempt.admissionDigest())
                    || !Arrays.equals(
                            result.appliedSourcePosition(),
                            first.mutation().source().canonicalBytes())) {
                throw new IllegalStateException("Target retained Budget contradicts its Admission first result");
            }
            admissions.add(new TargetCheckpointDependencies.Admission(attempt, first));
        }
        return List.copyOf(admissions);
    }

    private static void requireControlResult(
            final Map<String, TargetResultRecord> systems,
            final TargetQuotaIdentity root,
            final com.nereusstream.delay.protocol.TargetQuotaMutation mutation,
            final long retryUntil,
            final byte[] logicalIdentity,
            final byte[] body,
            final byte[] author) {
        final var type = SystemMutationType.APPLY_SHARD_CONTROL;
        final byte[] hash = SystemMutation.computeMutationHash(root.shard(), type, retryUntil, body);
        final byte[] id = SystemMutation.computeSystemMutationId(root.shard(), type, logicalIdentity, hash);
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
        final var record = systems.get(HexFormat.of().formatHex(key));
        if (record == null
                || record.kind() != TargetResultRecord.Kind.SYSTEM
                || record.allocation() != null
                || !record.primaryIdentity().equals(root)
                || !record.mutation().equals(mutation)) {
            throw new IllegalStateException("Target retained control lacks its exact first System result");
        }
        final var result = SystemMutationResult.decode(record.typedPayload());
        if (result.mutationType() != type
                || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || result.retryUntilEpochMs() != retryUntil
                || !Arrays.equals(result.mutationId(), id)
                || !Arrays.equals(result.mutationHash(), hash)
                || !Arrays.equals(
                        result.appliedSourcePosition(), mutation.source().canonicalBytes())
                || AuthorIdentity.decode(result.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL
                || author != null && !Arrays.equals(author, result.authorIdentity())) {
            throw new IllegalStateException("Target retained control contradicts its first System result");
        }
    }

    private static boolean skipInfrastructure(
            final ColumnFamily family,
            final byte[] key,
            final byte[] raw,
            final TargetCheckpointRootVerifier.RootProof proof,
            final List<TargetQuotaCounter> counters,
            final List<TargetQuotaTotal> totals,
            final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt,
            final RecoveryMetadata recovery) {
        if (family != ColumnFamily.META || key.length < 2 || key[1] != TargetKeyCodec.KEY_FORMAT) {
            return false;
        }
        final int tag = Byte.toUnsignedInt(key[0]);
        if (tag == 1) {
            if (key.length != 3 || Byte.toUnsignedInt(key[2]) == 0 || Byte.toUnsignedInt(key[2]) > 10) {
                throw new IllegalArgumentException("unknown Target checkpoint fixed metadata key");
            }
            final byte[] payload = TargetValueEnvelope.decode(raw, 1).payload();
            switch (Byte.toUnsignedInt(key[2])) {
                case 1 -> requireFixedBytes(payload, Bytes.u32be(2), "Store format");
                case 2 -> requireFixedBytes(payload, proof.metadata().encode(), "Store identity");
                case 3 -> requireFixedBytes(payload, proof.source().canonicalBytes(), "source position");
                case 4 -> {
                    IngressFenceState.decode(payload);
                    final var charge = TargetRecordAccounting.resources(proof.root()
                            .accounting()
                            .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length));
                    merge(rebuilt, proof.root().identity(), charge);
                    merge(rebuilt, proof.root().tenantIdentity(), charge);
                }
                case 5 -> requireFixedBytes(payload, Bytes.u64beBits(proof.mutationSequence()), "mutation sequence");
                case 6 -> {
                    final var cursors = StoreRuntimeMetadata.decodeEvidenceCursors(payload);
                    StoreRuntimeMetadata.requireEvidenceCursorScope(
                            cursors, 2, proof.metadata().shardId());
                    if (!cursors.isEmpty()) {
                        final var charge = TargetRecordAccounting.resources(proof.root()
                                .accounting()
                                .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length));
                        merge(rebuilt, proof.root().identity(), charge);
                        merge(rebuilt, proof.root().tenantIdentity(), charge);
                    }
                }
                case 7 -> requireNonZeroIdentity(payload, 16, "checkpoint identity");
                case 8 -> Bytes.requireLength(payload, Long.BYTES, "opened Owner epoch");
                case 9 -> {
                    if (payload.length != 1 || Byte.toUnsignedInt(payload[0]) > 1) {
                        throw new IllegalArgumentException("Target checkpoint has an invalid clean-close marker");
                    }
                }
                case 10 -> {
                    return false;
                }
                default -> throw new IllegalArgumentException("unknown Target checkpoint fixed metadata key");
            }
            return true;
        }
        if (tag == 7) {
            if (key.length != 3 || Byte.toUnsignedInt(key[2]) == 0 || Byte.toUnsignedInt(key[2]) > 4) {
                throw new IllegalArgumentException("unknown Target checkpoint recovery metadata key");
            }
            recovery.accept(
                    Byte.toUnsignedInt(key[2]),
                    TargetValueEnvelope.decode(raw, 1).payload());
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_COUNTER_TAG) {
            counters.add(TargetQuotaCounter.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetQuotaCounter.VALUE_TYPE)
                            .payload(),
                    proof.metadata().shardId()));
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_TOTAL_TAG) {
            totals.add(TargetQuotaTotal.decodeForStore(
                    key,
                    TargetValueEnvelope.decode(raw, TargetQuotaTotal.VALUE_TYPE).payload(),
                    proof.metadata().shardId(),
                    proof.bookkeeping().tenantScope()));
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_AGGREGATE_TAG) {
            if (!Arrays.equals(key, proof.aggregate().key())
                    || !Bytes.constantTimeEquals(
                            TargetValueEnvelope.decode(raw, TargetQuotaAggregate.VALUE_TYPE)
                                    .payload(),
                            proof.aggregate().canonicalBytes())) {
                throw new IllegalStateException("Target checkpoint has an extra or changed aggregate");
            }
            return true;
        }
        if (tag == TargetKeyCodec.QUOTA_BOOKKEEPING_TAG) {
            if (!Arrays.equals(key, proof.bookkeeping().key())
                    || !Bytes.constantTimeEquals(
                            TargetValueEnvelope.decode(raw, TargetQuotaBookkeeping.VALUE_TYPE)
                                    .payload(),
                            proof.bookkeeping().canonicalBytes())) {
                throw new IllegalStateException("Target checkpoint has an extra or changed bookkeeping root");
            }
            return true;
        }
        return false;
    }

    /** Decode the entire local recovery projection before accepting a copied image as a candidate. */
    private static final class RecoveryMetadata {
        private RecoveryCandidateRef lineageBase;
        private RecoveryFloorRef floor;
        private long catalogGeneration;
        private RecoveryInstallState installState;

        void accept(final int kind, final byte[] payload) {
            switch (kind) {
                case 1 -> lineageBase = RecoveryCandidateRef.decode(payload);
                case 2 -> floor = RecoveryFloorRef.decode(payload);
                case 3 -> {
                    Bytes.requireLength(payload, Long.BYTES, "Target recovery catalog generation");
                    catalogGeneration = Bytes.readU64be(payload, 0);
                    if (catalogGeneration == 0) {
                        throw new IllegalArgumentException("Target recovery catalog generation must be nonzero");
                    }
                }
                case 4 -> installState = RecoveryInstallState.decode(payload);
                default -> throw new IllegalArgumentException("unknown Target checkpoint recovery metadata key");
            }
        }

        void verify(final TargetCheckpointRootVerifier.RootProof proof) {
            final var metadata = proof.metadata();
            final byte[] lineage = proof.root().recoveryLineage();
            if (lineageBase != null) {
                requireFixedBytes(lineageBase.recoveryLineageId(), lineage, "recovery lineage");
                if (lineageBase.kind() == RecoveryCandidateKind.LOCAL_STORE) {
                    requireFixedBytes(
                            lineageBase.storeIncarnation(), metadata.storeIncarnation(), "local recovery incarnation");
                }
            }
            if (floor != null) {
                if (!metadata.shardId().equals(floor.appliedSourcePosition().shardId())) {
                    throw new IllegalArgumentException("Target recovery Floor belongs to another Shard");
                }
                requireFixedBytes(floor.recoveryLineageId(), lineage, "recovery Floor lineage");
            }
            if (installState != null) {
                requireFixedBytes(
                        installState.storeIncarnation(), metadata.storeIncarnation(), "recovery install incarnation");
                if (!Arrays.equals(
                        installState.checkpointId(), lineageBase == null ? null : lineageBase.checkpointId())) {
                    throw new IllegalArgumentException("Target recovery install checkpoint differs from lineage base");
                }
            }
            new StoreRecoveryMetadata(lineageBase, floor, catalogGeneration, installState);
        }
    }

    /** One original full Native fold; its progress is owned by TargetRetirementLedgerCursor. */
    static final class RetirementCursor {
        private final TargetCheckpointRootVerifier.RootProof proof;
        private final TargetCheckpointDependencies.Builder dependencies;
        private final TargetCheckpointRootVerifier.LedgerAuditLimits auditLimits;
        private final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt = new HashMap<>();
        private final Map<TargetQuotaIdentity, CapacityVector> resultContributions = new HashMap<>();
        private final List<TargetResultLedgerAudit.Stored> resultRows = new ArrayList<>();
        private final List<TargetQuotaGrantActivation> grantActivations = new ArrayList<>();
        private final List<TargetMembershipClosureRecord> membershipClosures = new ArrayList<>();
        private final List<TargetNativePolicyControlRecord> nativeControls = new ArrayList<>();
        private final List<TargetChannelControlRecord> channelControls = new ArrayList<>();
        private final List<TargetChannelIdentity> channelIdentities = new ArrayList<>();
        private final List<TargetChannelSlot> channelSlots = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloObservationState> sloStates = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloObservationGeneration> sloGenerations =
                new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloObservationSample> sloSamples = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloObservationEventRecord> sloEvents =
                new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloCustodyRecord> sloCustodies = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetSloWriterEpoch> sloWriterEpochs = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetParentControlCredit> parentCredits = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetScopeControlObligation> scopeObligations =
                new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetKafkaReceiptSlot> kafkaSlots = new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetExternalEvidenceOwner> externalOwners =
                new ArrayList<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetExternalEvidenceReference> externalReferences =
                new HashMap<>();
        private final List<com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner> recoveryOwners =
                new ArrayList<>();
        private final List<com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord> retirements =
                new ArrayList<>();
        private final Map<String, com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference> recoveryReferences =
                new HashMap<>();
        private final List<TargetInitialControlSnapshot> initialControls = new ArrayList<>();
        private final List<TargetQuotaAttemptBudget> attemptBudgets = new ArrayList<>();
        private final Map<String, TargetQuotaPayloadOwner> payloadOwners = new HashMap<>();
        private final java.util.Set<String> primaryMessages = new java.util.HashSet<String>();
        private final List<TargetMessageRecord> eligibilityMessages = new ArrayList<TargetMessageRecord>();
        private final List<com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord> sourceHistories =
                new ArrayList<>();
        private final List<AttemptProjection> attemptProjections = new ArrayList<>();
        private final List<TargetQuotaCounter> counters = new ArrayList<>();
        private final List<TargetQuotaTotal> totals = new ArrayList<>();
        private final RecoveryMetadata recovery = new RecoveryMetadata();
        private final List<TargetCheckpointDependencies.Admission> admissions = new ArrayList<>();
        private final Map<DelayMessageId, List<TargetCheckpointDependencies.Admission>> messageAdmissions =
                new HashMap<>();
        private final Map<DelayMessageId, java.util.NavigableMap<String, TargetCheckpointDependencies.RetryUse>>
                messageRetryUses = new HashMap<>();
        private final Map<DelayMessageId, List<TargetTerminalGenerationRecord>> messageTerminals = new HashMap<>();
        private final Map<String, TargetCheckpointDependencies.Admission> admissionIndex = new HashMap<>();
        private final Map<String, TargetKafkaReceiptMapping> mappingIndex = new HashMap<>(),
                latestMapping = new HashMap<>();
        private final Map<String, TargetKafkaReceiptSlot> kafkaSlotIndex = new HashMap<>();
        private final Map<String, TargetChannelIdentity> channelIndex = new HashMap<>();
        private final Map<String, TargetExternalEvidenceOwner> externalIndex = new HashMap<>();
        private final Map<String, TargetResultRecord> resultByStamp = new HashMap<>();
        private final java.util.Set<String> mappingSequences = new java.util.HashSet<>(),
                controlDomains = new java.util.HashSet<>();
        private final java.util.Set<String> expectedExternal = new java.util.HashSet<>(),
                usedExternal = new java.util.HashSet<>(),
                createdExternal = new java.util.HashSet<>();
        private final java.util.Set<String> expectedRecovery = new java.util.HashSet<>(),
                recoveryObjects = new java.util.HashSet<>(),
                recoveryAttempts = new java.util.HashSet<>();
        private final java.util.NavigableMap<Long, TargetRetirementSourceHistoryRecord> orderedHistory =
                new java.util.TreeMap<>(Long::compareUnsigned);
        private final Map<String, TargetQuotaAttemptBudget> projectedBudgets = new HashMap<>();
        private final java.util.Set<String> settledShares = new java.util.HashSet<>();
        private final Map<String, TargetQuotaMutation> settledAt = new HashMap<>();
        private final Map<TargetQuotaIdentity, Long> grantsByOwner = new HashMap<>();
        private final Map<TargetQuotaIdentity, CapacityVector> mirrorSums = new HashMap<>();
        private java.util.Iterator<TargetRetirementSourceHistoryRecord> projectedHistory;
        private java.util.Iterator<Map.Entry<String, TargetQuotaAttemptBudget>> projectedRows;
        private final Map<String, TargetResultRecord> resultIndex = new HashMap<>();
        private final List<TargetResultRecord> firstRows = new ArrayList<>();
        private final Map<String, TargetQuotaAttemptBudget> budgetIndex = new HashMap<>();
        private final Map<TargetQuotaIdentity, TargetQuotaCounter> counterIndex = new HashMap<>();
        private final Map<com.nereusstream.delay.protocol.TargetPartitionId, TargetQuotaUsage> targetUsage =
                new HashMap<>();
        private final Map<com.nereusstream.delay.protocol.TargetPartitionId, TargetQuotaTotal> totalIndex =
                new HashMap<>();
        private final java.util.Set<String> inventoryKeys = new java.util.HashSet<>();
        private final Map<String, AttemptProjectionRef> referenced = new HashMap<>();
        private final Map<TargetMessageLocator, byte[]> runtimes = new HashMap<>();
        private TargetQuotaUsage shardUsage = TargetQuotaUsage.empty();
        private int joinPhase, joinIndex;
        private java.util.Iterator<Map.Entry<String, AttemptProjectionRef>> referenceRows;
        private TargetResultLedgerAudit.RetirementCursor results;
        private TargetChannelHistoryAudit.RetirementCursor channels;
        private com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit.RetirementCursor sloHistory;
        private TargetCheckpointDependencies.Builder.RetirementFreeze freezing;
        private com.nereusstream.delay.runtime.TargetPayloadObligationSummaryAudit.RetirementCursor payloadSummary;
        private com.nereusstream.delay.runtime.TargetMigrationArchiveStore.RetirementCursor archiveHistory;

        private int family;
        private byte[] lower;
        private boolean collected;
        private TargetCheckpointDependencies complete;

        RetirementCursor(
                TargetCheckpointRootVerifier.RootProof proof, TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
            this.proof = Objects.requireNonNull(proof);
            auditLimits = Objects.requireNonNull(limits);
            dependencies = new TargetCheckpointDependencies.Builder(proof);
            final var rootCharge =
                    TargetRecordAccounting.resources(proof.bookkeeping().charge());
            merge(rebuilt, proof.root().identity(), rootCharge);
            merge(rebuilt, proof.root().tenantIdentity(), TargetRecordAccounting.resources(rootCharge.resources()));
        }

        boolean advance(
                TargetRetirementLedgerCursor.InventoryView view,
                int maximumRows,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            int used = 0;
            while (!collected && used < maximumRows) {
                final var current = ColumnFamily.values()[family];
                final var row = view.fullNext(current, lower);
                if (row == null) {
                    family++;
                    lower = null;
                    if (family == ColumnFamily.values().length) {
                        collected = true;
                    }
                    continue;
                }
                final byte[] key = row.key(), raw = row.value();
                copied.accept((long) key.length + raw.length);
                accept(view, current, key, raw);
                retained.accept((long) key.length + raw.length);
                lower = Bytes.concat(key, new byte[] {0});
                copied.accept(lower.length);
                used++;
            }
            if (!collected) {
                return false;
            }
            return advanceJoins(view, maximumRows - used, retained, copied);
        }

        private void accept(
                TargetRetirementLedgerCursor.InventoryView view, ColumnFamily family, byte[] key, byte[] raw) {
            if (skipInfrastructure(family, key, raw, proof, counters, totals, rebuilt, recovery)) {
                return;
            }
            final int selectedType = TargetStoreBackend.businessType(family, key);
            final byte[] selectedPayload =
                    TargetValueEnvelope.decode(raw, selectedType).payload();
            if (family == ColumnFamily.ID && selectedType == TargetMessageRecord.VALUE_TYPE) {
                auditMessageDependencies(
                        TargetMessageRecord.decodeForStore(
                                key, selectedPayload, proof.metadata().shardId()),
                        view);
            } else if (family == ColumnFamily.META && selectedType == TargetOrderState.VALUE_TYPE) {
                auditOrderStateDependencies(TargetOrderState.decode(selectedPayload), view);
            }
            final var accounting = new TargetRecordAccounting(
                    view,
                    List.of(),
                    proof.root().scope(),
                    proof.root().recoveryLineage(),
                    proof.aggregate().mutation(),
                    proof.bookkeeping(),
                    TargetQueueState.MAX_DOMAIN_SLOTS);
            final var selectedCharge = accounting.charge(family, key, raw);
            final int type = TargetStoreBackend.businessType(family, key);
            final byte[] payload = TargetValueEnvelope.decode(raw, type).payload();
            if (family == ColumnFamily.ID && type == TargetScheduleBinding.VALUE_TYPE) {
                final var binding = TargetScheduleBinding.decode(payload);
                dependencies.binding(binding);
                final var use = new TargetCheckpointDependencies.RetryUse(
                        binding.intent().retryPolicy(), binding.bindingSource());
                messageRetryUses
                        .computeIfAbsent(binding.messageId(), ignored -> new java.util.TreeMap<>())
                        .put(Bytes.hex(use.canonicalBytes()), use);
            } else if (family == ColumnFamily.ID
                    && type == com.nereusstream.delay.runtime.TargetReservationRecord.VALUE_TYPE) {
                dependencies.reservation(key, com.nereusstream.delay.runtime.TargetReservationRecord.decode(payload));
            } else if (family == ColumnFamily.META) {
                final int tag = Byte.toUnsignedInt(key[0]);
                if (Arrays.equals(key, KeyCodec.metaFixed(10))) {
                    final var snapshot = TargetInitialControlSnapshot.decode(payload);
                    initialControls.add(snapshot);
                    dependencies.control(family, key, type, payload);
                    snapshot.input()
                            .profiles()
                            .forEach(ref -> dependencies.profileUse(new TargetCheckpointDependencies.ProfileUse(
                                    ref, snapshot.input().source())));
                }
                if (tag >= TargetKeyCodec.STATE_TAG && tag <= TargetKeyCodec.NATIVE_POLICY_SNAPSHOT_TAG
                        || tag == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG
                        || tag == TargetKeyCodec.CLOSE_TAG
                        || tag == TargetKeyCodec.MEMBERSHIP_CLOSURE_TAG
                        || tag >= TargetKeyCodec.NATIVE_PUBLISHER_TAG) {
                    dependencies.control(family, key, type, payload);
                }
                if (type == TargetMembershipGrant.VALUE_TYPE) {
                    dependencies.membership(TargetMembershipGrant.decode(payload));
                } else if (type == TargetChannelIdentity.VALUE_TYPE) {
                    dependencies.profile(TargetChannelIdentity.decode(payload)
                            .credentialLease()
                            .profile());
                } else if (type == TargetNativePolicyScope.VALUE_TYPE) {
                    dependencies.nativeScope(TargetNativePolicyScope.decode(payload));
                } else if (type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                    final var control = TargetNativePolicyControlRecord.decode(payload);
                    dependencies.nativeScope(control.body().request().scope());
                    if (control.grant() != null) {
                        dependencies.membership(control.grant());
                    }
                } else if (type == TargetChannelControlRecord.VALUE_TYPE) {
                    final var event = TargetChannelControlRecord.decode(payload);
                    dependencies.profileUse(new TargetCheckpointDependencies.ProfileUse(
                            event.body().request().channel().credentialLease().profile(),
                            event.mutation().source()));
                } else if (type == TargetQuotaPayloadOwner.VALUE_TYPE) {
                    final var owner = TargetQuotaPayloadOwner.decode(payload);
                    if (owner.objectStoreProfileHash() != null) {
                        dependencies.objectHash(owner.objectStoreProfileHash());
                    }
                }
            }
            if (family == ColumnFamily.DEDUPE) {
                resultRows.add(new TargetResultLedgerAudit.Stored(key, type, payload));
                resultIndex.put(Bytes.hex(key), TargetResultRecord.decode(payload));
                firstRows.add(resultIndex.get(Bytes.hex(key)));
            } else if (family == ColumnFamily.ID && type == TargetMessageRecord.VALUE_TYPE) {
                final var message = TargetMessageRecord.decodeForStore(
                        key, payload, proof.metadata().shardId());
                attemptProjections.add(new AttemptProjection(message.locator(), message.runtime()));
                dependencies.messageState(message, payload);
                primaryMessages.add(Bytes.hex(message.locator().messageId().bytes()));
                eligibilityMessages.add(message);

            } else if (family == ColumnFamily.META && type == TargetOrderState.VALUE_TYPE) {

            } else if (family == ColumnFamily.META && type == TargetChannelIdentity.VALUE_TYPE) {
                channelIdentities.add(TargetChannelIdentity.decodeForStore(
                        key, payload, proof.metadata().shardId()));
            } else if (family == ColumnFamily.META && type == TargetChannelControlRecord.VALUE_TYPE) {
                channelControls.add(TargetChannelControlRecord.decodeForStore(
                        key, payload, proof.metadata().shardId()));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloObservationState.VALUE_TYPE) {
                final var state = com.nereusstream.delay.protocol.TargetSloObservationState.decodeForStore(
                        key,
                        payload,
                        new TargetQuotaScope(
                                proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                        proof.root().recoveryLineage());
                sloStates.add(state);
                dependencies.sloState(state);
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.head(key, payload.length, state));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloObservationGeneration.VALUE_TYPE) {
                final var generation = com.nereusstream.delay.protocol.TargetSloObservationGeneration.decodeForStore(
                        key,
                        payload,
                        new TargetQuotaScope(
                                proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                        proof.root().recoveryLineage());
                sloGenerations.add(generation);
                dependencies.sloGeneration(generation);
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.generation(key, payload.length, generation));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloObservationSample.VALUE_TYPE) {
                final var sample = com.nereusstream.delay.protocol.TargetSloObservationSample.decodeForStore(
                        key,
                        payload,
                        new TargetQuotaScope(
                                proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                        proof.root().recoveryLineage());
                if (sample.primary(key)) {
                    sloSamples.add(sample);
                    dependencies.sloSample(sample);
                }
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.sample(key, payload.length, sample));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloObservationEventRecord.VALUE_TYPE) {
                final var event = com.nereusstream.delay.protocol.TargetSloObservationEventRecord.decodeForStore(
                        key,
                        payload,
                        new TargetQuotaScope(
                                proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                        proof.root().recoveryLineage());
                sloEvents.add(event);
                dependencies.sloEvent(event);
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.event(key, payload.length, event));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloCustodyRecord.VALUE_TYPE) {
                final var custody = com.nereusstream.delay.protocol.TargetSloCustodyRecord.decodeForStore(
                        key,
                        payload,
                        new TargetQuotaScope(
                                proof.metadata().shardId(), proof.bookkeeping().tenantScope(), null),
                        proof.root().recoveryLineage());
                sloCustodies.add(custody);
                dependencies.sloCustody(custody);
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.custody(key, payload.length, custody));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetSloWriterEpoch.VALUE_TYPE) {
                final var writer = com.nereusstream.delay.protocol.TargetSloWriterEpoch.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                writer.mutation().requireAtOrBefore(proof.aggregate().mutation());
                sloWriterEpochs.add(writer);
                dependencies.sloWriterEpoch(writer);
                dependencies.scopeNativeRow(TargetScopeNativeUsageAudit.writer(key, payload.length, writer));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetParentControlCredit.VALUE_TYPE) {
                final var credit = com.nereusstream.delay.protocol.TargetParentControlCredit.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                credit.mutation().requireAtOrBefore(proof.aggregate().mutation());
                parentCredits.add(credit);
                dependencies.parentCredit(credit);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetScopeControlObligation.VALUE_TYPE) {
                final var obligation = com.nereusstream.delay.protocol.TargetScopeControlObligation.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                obligation.mutation().requireAtOrBefore(proof.aggregate().mutation());
                scopeObligations.add(obligation);
                dependencies.scopeObligation(obligation);
            } else if (family == ColumnFamily.META && type == TargetChannelSlot.VALUE_TYPE) {
                channelSlots.add(TargetChannelSlot.decodeForStore(
                        key, payload, proof.metadata().shardId()));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE) {
                final var mapping = com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.decodeForStore(
                        key, payload, proof.metadata().shardId());
                kafkaMappings.add(mapping);
                dependencies.kafkaMapping(mapping);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE) {
                kafkaSlots.add(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(
                        key, payload, proof.metadata().shardId()));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.VALUE_TYPE) {
                final var owner = com.nereusstream.delay.protocol.TargetExternalEvidenceOwner.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.bookkeeping().tenantScope(),
                        proof.root().recoveryLineage());
                owner.createdAt().requireAtOrBefore(proof.aggregate().mutation());
                externalOwners.add(owner);
                dependencies.externalEvidence(owner);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetExternalEvidenceReference.VALUE_TYPE) {
                final var ref = com.nereusstream.delay.protocol.TargetExternalEvidenceReference.decodeForStore(
                        key, payload, proof.metadata().shardId(), proof.root().recoveryLineage());
                ref.mutation().requireAtOrBefore(proof.aggregate().mutation());
                if (externalReferences.put(Bytes.hex(key), ref) != null) {
                    throw new IllegalStateException("duplicate external reference");
                }
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.VALUE_TYPE) {
                final var owner = com.nereusstream.delay.protocol.TargetRecoveryEvidenceOwner.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.bookkeeping().tenantScope(),
                        proof.root().recoveryLineage());
                owner.adoptedAt().requireAtOrBefore(proof.aggregate().mutation());
                recoveryOwners.add(owner);
                dependencies.recoveryEvidence(owner);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.VALUE_TYPE) {
                final var reference = com.nereusstream.delay.protocol.TargetRecoveryEvidenceReference.decodeForStore(
                        key, payload, proof.metadata().shardId(), proof.root().recoveryLineage());
                reference.adoptedAt().requireAtOrBefore(proof.aggregate().mutation());
                if (recoveryReferences.put(Bytes.hex(key), reference) != null) {
                    throw new IllegalStateException("duplicate recovery reference");
                }
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.VALUE_TYPE) {
                final var record = com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                dependencies.payloadObjectIntent(record);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.VALUE_TYPE) {
                final var record = com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.bookkeeping().tenantScope(),
                        proof.root().recoveryLineage());
                record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                retirements.add(record);
                dependencies.retirement(record);
            } else if (family == ColumnFamily.META && type == TargetQuotaGrantActivation.VALUE_TYPE) {
                grantActivations.add(TargetQuotaGrantActivation.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.bookkeeping().tenantScope()));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.VALUE_TYPE) {
                final var record = com.nereusstream.delay.protocol.TargetEvidenceConfirmedRecord.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.root().tenantScope(),
                        proof.root().recoveryLineage());
                record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                dependencies.evidenceConfirmation(record);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.VALUE_TYPE) {
                final var tip = com.nereusstream.delay.protocol.TargetRetirementHistoryTipRecord.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                tip.decidedAt().requireAtOrBefore(proof.aggregate().mutation());
                dependencies.historyTip(tip);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.VALUE_TYPE) {
                final var record = com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                record.decidedAt().requireAtOrBefore(proof.aggregate().mutation());
                dependencies.sourceHistory(record);
                sourceHistories.add(record);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetProtocolActivationRecord.VALUE_TYPE) {
                final var record = com.nereusstream.delay.protocol.TargetProtocolActivationRecord.decodeForStore(
                        key, payload, proof.root().scope(), proof.root().recoveryLineage());
                record.appliedAt().requireAtOrBefore(proof.aggregate().mutation());
                dependencies.protocolActivation(record);
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationRoot.VALUE_TYPE) {
                dependencies.migrationRoot(com.nereusstream.delay.protocol.TargetMigrationRoot.decode(payload));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationOrigin.VALUE_TYPE) {
                dependencies.migrationOrigin(com.nereusstream.delay.protocol.TargetMigrationOrigin.decode(payload));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.VALUE_TYPE) {
                dependencies.migrationGuard(
                        com.nereusstream.delay.protocol.TargetMigrationCustodyGuard.decode(payload));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.VALUE_TYPE) {
                dependencies.migrationPrefix(
                        com.nereusstream.delay.protocol.TargetMigrationPrefixRecord.decode(payload));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.VALUE_TYPE) {
                dependencies.migrationAttempt(
                        com.nereusstream.delay.protocol.TargetMigrationActivationAttempt.decode(payload));
            } else if (family == ColumnFamily.META
                    && type == com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.VALUE_TYPE) {
                if (TargetMigrationArchiveState.isKey(key)) {
                    dependencies.migrationArchiveState(TargetMigrationArchiveState.decode(payload));
                } else {
                    dependencies.migrationRetirement(
                            com.nereusstream.delay.protocol.TargetMigrationRetirementRecord.decode(payload));
                }
            } else if (family == ColumnFamily.META && type == TargetMigrationArchiveRow.VALUE_TYPE) {
                dependencies.migrationArchive(TargetMigrationArchiveRow.decode(payload));
            } else if (family == ColumnFamily.META && type == TargetQuotaAttemptBudget.VALUE_TYPE) {
                attemptBudgets.add(TargetQuotaAttemptBudget.decodeForStore(
                        key, payload, proof.metadata().shardId()));
            } else if (family == ColumnFamily.META && type == TargetQuotaPayloadOwner.VALUE_TYPE) {
                final var owner = TargetQuotaPayloadOwner.decodeForStore(
                        key, payload, proof.metadata().shardId(), proof.root().tenantScope());
                payloadOwners.put(Bytes.hex(owner.messageId().bytes()), owner);
                dependencies.payloadOwner(owner);
            } else if (family == ColumnFamily.META && type == TargetMembershipClosureRecord.VALUE_TYPE) {
                membershipClosures.add(TargetMembershipClosureRecord.decodeForStore(
                        key, payload, proof.metadata().shardId(), proof.root().recoveryLineage()));
            } else if (family == ColumnFamily.META && type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                nativeControls.add(TargetNativePolicyControlRecord.decodeForStore(
                        key, payload, proof.metadata().shardId(), proof.root().recoveryLineage()));
            } else if (family == ColumnFamily.TERMINAL && type == TargetTerminalGenerationRecord.VALUE_TYPE) {
                final var terminal = TargetTerminalGenerationRecord.decode(payload);
                if (!Arrays.equals(key, terminal.key())) {
                    throw new IllegalArgumentException("Target checkpoint terminal key mismatch");
                }
                attemptProjections.add(new AttemptProjection(terminal.locator(), terminal.runtime()));
                dependencies.terminalGeneration(terminal);
                messageTerminals
                        .computeIfAbsent(terminal.locator().messageId(), ignored -> new ArrayList<>())
                        .add(terminal);
            }
            if (type == com.nereusstream.delay.protocol.TargetQuotaIncarnation.VALUE_TYPE) {
                dependencies.incarnation(com.nereusstream.delay.protocol.TargetQuotaIncarnation.decodeForStore(
                        key,
                        payload,
                        proof.metadata().shardId(),
                        proof.bookkeeping().tenantScope()));
            }
            final var charge = selectedCharge;
            if (charge != null) {
                dependencies.ownerRecord(charge.owner().identity());
                merge(rebuilt, charge.owner().identity(), charge.primary());
                merge(rebuilt, charge.owner().tenantIdentity(), charge.mirror());
                if (family == ColumnFamily.DEDUPE) {
                    resultContributions.merge(
                            charge.owner().identity(), charge.primary().resources(), CapacityVector::add);
                    resultContributions.merge(
                            charge.owner().tenantIdentity(), charge.mirror().resources(), CapacityVector::add);
                }
            }
        }

        private boolean advanceJoins(
                TargetRetirementLedgerCursor.InventoryView view,
                int maximumRows,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            int used = 0;
            while (used < maximumRows) {
                switch (joinPhase) {
                    case 0 -> {
                        if (joinIndex == 0) {
                            proof.bookkeeping().requireRoot(proof.aggregate());
                            recovery.verify(proof);
                        }
                        if (joinIndex >= counters.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var counter = counters.get(joinIndex);
                        requireInventorySource(counter.mutation());
                        requireInventoryKey(counter.identity().key());
                        proof.aggregate().requireCounter(counter);
                        if (counter.identity().kind().isMirror()
                                && !Arrays.equals(
                                        counter.identity().tenantScope(),
                                        proof.bookkeeping().tenantScope())) {
                            throw new IllegalStateException("bookkeeping inventory has a foreign tenant mirror");
                        }
                        if (counterIndex.putIfAbsent(counter.identity(), counter) != null) {
                            throw new IllegalStateException("duplicate Target quota counter identity");
                        }
                        if (!counter.identity().kind().isMirror()) {
                            shardUsage = shardUsage.add(counter.usage());
                            if (counter.identity().kind().hasTarget()) {
                                targetUsage.merge(counter.identity().target(), counter.usage(), TargetQuotaUsage::add);
                            }
                        }
                        dependencies.quotaCounter(counter);
                    }
                    case 1 -> {
                        if (joinIndex >= totals.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var total = totals.get(joinIndex);
                        requireInventorySource(total.mutation());
                        requireInventoryKey(total.key());
                        total.scope()
                                .requireRoute(
                                        proof.metadata().shardId(),
                                        proof.bookkeeping().tenantScope());
                        total.requireAggregate(proof.aggregate());
                        if (totalIndex.putIfAbsent(total.scope().target(), total) != null
                                || !total.usage()
                                        .equals(targetUsage.getOrDefault(
                                                total.scope().target(), TargetQuotaUsage.empty()))) {
                            throw new IllegalStateException("Target quota total differs from primary counters");
                        }
                    }
                    case 2 -> {
                        if (joinIndex >= grantActivations.size()) {
                            final var book = proof.bookkeeping();
                            final var primary = counterIndex.get(book.owner());
                            final var mirror = counterIndex.get(book.tenantOwner());
                            if (primary == null
                                    || mirror == null
                                    || !primary.usage().resources().covers(book.charge())
                                    || !mirror.usage().resources().covers(book.charge())
                                    || !book.inventory()
                                            .equals(
                                                    new com.nereusstream.delay.protocol.TargetQuotaBookkeeping
                                                            .Inventory(
                                                            counters.size(), totals.size(), grantActivations.size()))
                                    || !shardUsage.equals(proof.aggregate().usage())) {
                                throw new IllegalStateException(
                                        "bookkeeping inventory or root storage commitment mismatch");
                            }
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var grant = grantActivations.get(joinIndex);
                        requireInventorySource(grant.mutation());
                        requireInventoryKey(grant.key());
                        grant.grant()
                                .scope()
                                .requireRoute(
                                        proof.metadata().shardId(),
                                        proof.bookkeeping().tenantScope());
                        grant.mutation().requireAtOrBefore(proof.aggregate().mutation());
                        if (grant.allocation() != null) {
                            grantsByOwner.merge(grant.allocation().identity(), 1L, Math::addExact);
                        }
                    }
                    case 3 -> {
                        if (joinIndex >= counters.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var counter = counters.get(joinIndex);
                        final var id = counter.identity();
                        if (id.kind().isMirror()) {
                            if (!counterIndex.containsKey(id.primary())) {
                                throw new IllegalStateException("Target quota tenant mirror has no primary counter");
                            }
                        } else {
                            final var mirrorId = new TargetQuotaIdentity(
                                    id.kind().hasTarget()
                                            ? TargetQuotaIdentity.Kind.TENANT_TARGET
                                            : TargetQuotaIdentity.Kind.TENANT_SHARD,
                                    proof.metadata().shardId(),
                                    id.accountingIncarnation(),
                                    id.target(),
                                    proof.bookkeeping().tenantScope());
                            final var mirror = counterIndex.get(mirrorId);
                            if (mirror == null
                                    || !counter.usage()
                                            .resources()
                                            .equals(mirror.usage().resources())) {
                                throw new IllegalStateException("Target quota primary and tenant mirror disagree");
                            }
                            if (id.kind() == TargetQuotaIdentity.Kind.TARGET) {
                                final var total = Objects.requireNonNull(
                                        totalIndex.get(id.target()), "Target quota primary counter total");
                                total.requireCounter(counter);
                            }
                        }
                    }
                    case 4 -> {
                        if (joinIndex >= attemptBudgets.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var budget = attemptBudgets.get(joinIndex);
                        if (budgetIndex.putIfAbsent(Bytes.hex(budget.publishAttemptId()), budget) != null) {
                            throw new IllegalStateException("Target checkpoint repeats an attempt budget identity");
                        }
                    }
                    case 5 -> {
                        if (joinIndex >= attemptProjections.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var projection = attemptProjections.get(joinIndex);
                        final byte[] digest = projection.runtime().runtimeDigest();
                        final byte[] prior = runtimes.putIfAbsent(projection.locator(), digest);
                        if (prior != null && !Arrays.equals(prior, digest)) {
                            throw new IllegalStateException("Target checkpoint Message and Terminal runtime disagree");
                        }
                        for (var obligation : projection.runtime().attemptObligations()) {
                            final String id = Bytes.hex(obligation.publishAttemptId());
                            final var ref = new AttemptProjectionRef(projection.locator(), obligation);
                            final var previous = referenced.putIfAbsent(id, ref);
                            if (previous != null
                                    && (!previous.locator().equals(ref.locator())
                                            || !Arrays.equals(
                                                    previous.obligation().canonicalBytes(),
                                                    obligation.canonicalBytes()))) {
                                throw new IllegalStateException(
                                        "Target checkpoint attempt reference identity conflicts");
                            }
                        }
                    }
                    default -> {
                        return advanceRemainingJoins(view, maximumRows - used, retained, copied);
                    }
                }
                joinIndex++;
                used++;
                view.requireWithinElapsedBudget();
            }
            return false;
        }

        private void requireInventorySource(com.nereusstream.delay.protocol.TargetQuotaMutation mutation) {
            if (!mutation.source()
                            .sameSourceIdentity(proof.bookkeeping().mutation().source())
                    || com.nereusstream.delay.protocol.TargetQuotaBookkeeping.maximumSourceBytes(mutation.source())
                            != com.nereusstream.delay.protocol.TargetQuotaBookkeeping.maximumSourceBytes(
                                    proof.bookkeeping().mutation().source())) {
                throw new IllegalStateException("bookkeeping inventory changed original Source");
            }
        }

        private void requireInventoryKey(byte[] key) {
            if (!inventoryKeys.add(Bytes.hex(key))) {
                throw new IllegalStateException("bookkeeping inventory repeats Native projection key");
            }
        }

        private boolean advanceRemainingJoins(
                TargetRetirementLedgerCursor.InventoryView view,
                int maximumRows,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            int used = 0;
            while (used < maximumRows) {
                switch (joinPhase) {
                    case 6 -> {
                        if (referenceRows == null) {
                            referenceRows = referenced.entrySet().iterator();
                        }
                        if (!referenceRows.hasNext()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var entry = referenceRows.next();
                        final var budget = Objects.requireNonNull(
                                budgetIndex.get(entry.getKey()), "Target checkpoint open attempt quota budget");
                        final var ref = entry.getValue();
                        final boolean matches = budget.locator().equals(ref.locator())
                                && switch (budget.phase()) {
                                    case ADMITTED -> ref.obligation().ledgerState() == AttemptLedgerState.PUBLISHING;
                                    case UNKNOWN -> ref.obligation().ledgerState() == AttemptLedgerState.UNCERTAIN;
                                    case RESOLVED_AWAITING_FLOOR, RETAINED, RELEASED -> false;
                                };
                        if (!matches) {
                            throw new IllegalStateException(
                                    "Target checkpoint attempt reference contradicts its quota budget");
                        }
                    }
                    case 7 -> {
                        if (joinIndex >= attemptBudgets.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var budget = attemptBudgets.get(joinIndex++);
                        final var phase = budget.phase();
                        if ((phase == TargetQuotaAttemptBudget.Phase.ADMITTED
                                        || phase == TargetQuotaAttemptBudget.Phase.UNKNOWN)
                                != referenced.containsKey(Bytes.hex(budget.publishAttemptId()))) {
                            throw new IllegalStateException(
                                    "Target checkpoint open quota budget lacks its attempt reference");
                        }
                    }
                    case 8 -> {
                        if (results == null) {
                            final var migration =
                                    com.nereusstream.delay.runtime.TargetMigrationStore.decodeRoot(view.projected(
                                            ColumnFamily.META,
                                            com.nereusstream.delay.protocol.TargetMigrationRoot.keyFor(
                                                    proof.metadata().shardId()),
                                            List.of()));
                            results = new TargetResultLedgerAudit.RetirementCursor(
                                    proof.root().scope(),
                                    proof.root().recoveryLineage(),
                                    proof.mutationSequence(),
                                    proof.source(),
                                    new TargetResultLedgerAudit.Limits(
                                            auditLimits.maxRecords(),
                                            auditLimits.maxPointReads(),
                                            Math.addExact(
                                                    auditLimits.maxKeyValueBytes(), auditLimits.maxPointReadBytes())),
                                    resultRows,
                                    migration);
                        }
                        final boolean done = results.advance(
                                key -> {
                                    final byte[] raw = view.projected(ColumnFamily.META, key, List.of());
                                    if (raw == null) {
                                        return null;
                                    }
                                    final var value = TargetValueEnvelope.decodeAny(raw);
                                    return new TargetResultLedgerAudit.Stored(key, value.valueType(), value.payload());
                                },
                                maximumRows - used,
                                view::requireWithinElapsedBudget);
                        if (!done) {
                            return false;
                        }
                        if (results.resultRecords() != resultRows.size()
                                || results.contributions().size() != resultContributions.size()) {
                            throw new IllegalStateException(
                                    "Target checkpoint result ledger differs from actual DEDUPE accounting");
                        }
                        contributionRows = results.contributions().entrySet().iterator();
                        joinPhase++;
                        joinIndex = 0;
                        return false;
                    }
                    case 9 -> {
                        if (!contributionRows.hasNext()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var entry = contributionRows.next();
                        if (!entry.getValue().equals(resultContributions.get(entry.getKey()))) {
                            throw new IllegalStateException(
                                    "Target checkpoint result ledger differs from actual DEDUPE accounting");
                        }
                    }
                    case 10 -> {
                        if (joinIndex >= grantActivations.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        requireGrant(grantActivations.get(joinIndex++));
                    }
                    case 11 -> {
                        if (joinIndex >= initialControls.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var snapshot = initialControls.get(joinIndex++);
                        if (!snapshot.root().equals(proof.root().identity())
                                || !snapshot.input().scope().equals(proof.root().scope())
                                || !Arrays.equals(
                                        snapshot.input().lineage(), proof.root().recoveryLineage())) {
                            throw new IllegalStateException(
                                    "Target initial control snapshot crosses its root/lineage/scope");
                        }
                        snapshot.mutation().requireAtOrBefore(proof.aggregate().mutation());
                        requireGrant(snapshot.initialGrant());
                        final var first = resultIndex.get(Bytes.hex(Bytes.concat(
                                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                                snapshot.initialGrant().systemMutationId())));
                        if (!Arrays.equals(
                                snapshot.authorIdentity(),
                                SystemMutationResult.decode(first.typedPayload())
                                        .authorIdentity())) {
                            throw new IllegalStateException(
                                    "Target initial control snapshot changes its original Control author");
                        }
                    }
                    case 12 -> {
                        if (joinIndex >= membershipClosures.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var control = membershipClosures.get(joinIndex++);
                        final var body = control.body();
                        requireControlResult(
                                resultIndex,
                                proof.root().identity(),
                                control.mutation(),
                                body.retryUntil(),
                                body.logicalIdentity(),
                                body.canonicalBytes(),
                                null);
                    }
                    case 13 -> {
                        if (joinIndex >= nativeControls.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var control = nativeControls.get(joinIndex++);
                        final var body = control.body();
                        final var author = control.author();
                        requireControlResult(
                                resultIndex,
                                proof.root().identity(),
                                control.mutation(),
                                body.retryUntil(),
                                body.logicalIdentity(),
                                body.canonicalBytes(),
                                AuthorIdentity.control(
                                                author.operationActorIdHash(),
                                                author.authenticatedRoleSetHash(),
                                                author.tenantResourceScopeHash())
                                        .canonicalBytes());
                    }
                    case 14 -> {
                        if (joinIndex >= channelControls.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var control = channelControls.get(joinIndex++);
                        final var body = control.body();
                        final var author = control.author();
                        requireControlResult(
                                resultIndex,
                                proof.root().identity(),
                                control.mutation(),
                                body.retryUntil(),
                                body.logicalIdentity(),
                                body.canonicalBytes(),
                                AuthorIdentity.control(
                                                author.operationActorIdHash(),
                                                author.authenticatedRoleSetHash(),
                                                author.tenantResourceScopeHash())
                                        .canonicalBytes());
                    }
                    case 15 -> {
                        if (channels == null) {
                            channels = new TargetChannelHistoryAudit.RetirementCursor(
                                    channelControls, channelIdentities, channelSlots);
                        }
                        if (!channels.advance(maximumRows - used, view::requireWithinElapsedBudget)) {
                            return false;
                        }
                        joinPhase++;
                        joinIndex = 0;
                        return false;
                    }
                    case 16 -> {
                        if (joinIndex >= sloWriterEpochs.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        com.nereusstream.delay.runtime.TargetSloWriterEpochStore.requireOrigin(
                                view,
                                List.of(),
                                proof.root().scope(),
                                proof.root().recoveryLineage(),
                                proof.root().identity(),
                                sloWriterEpochs.get(joinIndex),
                                proof.aggregate().mutation());
                        joinIndex++;
                    }
                    case 17 -> {
                        if (joinIndex >= parentCredits.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        com.nereusstream.delay.runtime.TargetParentControlCreditStore.requireOrigin(
                                view,
                                List.of(),
                                proof.root().scope(),
                                proof.root().recoveryLineage(),
                                proof.root(),
                                parentCredits.get(joinIndex),
                                proof.aggregate().mutation());
                        joinIndex++;
                    }
                    case 18 -> {
                        if (joinIndex >= scopeObligations.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        com.nereusstream.delay.runtime.TargetScopeControlObligationStore.requireOrigin(
                                view,
                                List.of(),
                                proof.root().scope(),
                                proof.root().recoveryLineage(),
                                proof.root(),
                                scopeObligations.get(joinIndex),
                                proof.aggregate().mutation());
                        joinIndex++;
                    }
                    default -> {
                        return advanceBusinessJoins(view, maximumRows - used, retained, copied);
                    }
                }
                used++;
                view.requireWithinElapsedBudget();
            }
            return false;
        }

        private boolean advanceBusinessJoins(
                TargetRetirementLedgerCursor.InventoryView view,
                int maximumRows,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            int used = 0;
            while (used < maximumRows) {
                switch (joinPhase) {
                    case 19 -> {
                        if (sloHistory == null) {
                            sloHistory =
                                    new com.nereusstream.delay.runtime.TargetSloObservationHistoryAudit
                                            .RetirementCursor(
                                            sloStates,
                                            sloGenerations,
                                            proof.root().scope(),
                                            proof.root().recoveryLineage(),
                                            proof.aggregate().mutation(),
                                            sloSamples,
                                            firstRows,
                                            sloEvents,
                                            sloCustodies,
                                            sloWriterEpochs,
                                            eligibilityMessages);
                        }
                        if (!sloHistory.advance(
                                maximumRows - used, view::requireWithinElapsedBudget, retained, copied)) {
                            return false;
                        }
                        joinPhase++;
                        joinIndex = 0;
                        return false;
                    }
                    case 20 -> {
                        if (joinIndex >= attemptBudgets.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var attempt = attemptBudgets.get(joinIndex);
                        final var first = requireAdmission(attempt);
                        final var admission = new TargetCheckpointDependencies.Admission(attempt, first);
                        admissions.add(admission);
                        admissionIndex.put(Bytes.hex(attempt.publishAttemptId()), admission);
                        messageAdmissions
                                .computeIfAbsent(attempt.locator().messageId(), ignored -> new ArrayList<>())
                                .add(admission);
                        joinIndex++;
                    }
                    case 21 -> {
                        if (joinIndex >= channelIdentities.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var channel = channelIdentities.get(joinIndex++);
                        channelIndex.put(Bytes.hex(channel.digest()), channel);
                    }
                    case 22 -> {
                        if (joinIndex >= kafkaSlots.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var slot = kafkaSlots.get(joinIndex++);
                        if (kafkaSlotIndex.putIfAbsent(Bytes.hex(slot.key()), slot) != null) {
                            throw new IllegalStateException("duplicate Target K2 slot");
                        }
                        final var channel =
                                channelIndex.get(Bytes.hex(slot.channel().digest()));
                        if (channel == null
                                || !Arrays.equals(
                                        channel.canonicalBytes(), slot.channel().canonicalBytes())) {
                            throw new IllegalStateException("Target K2 slot lacks exact retained channel identity");
                        }
                    }
                    case 23 -> {
                        if (joinIndex >= resultRows.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var stored = resultRows.get(joinIndex++);
                        if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                            final var first = resultIndex.get(Bytes.hex(stored.key()));
                            if (resultByStamp.putIfAbsent(
                                            Bytes.hex(first.mutation().canonicalBytes()), first)
                                    != null) {
                                throw new IllegalStateException(
                                        "multiple Source first results share a K2 settlement stamp");
                            }
                        }
                    }
                    case 24 -> {
                        if (joinIndex >= kafkaMappings.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        requireKafkaMapping(kafkaMappings.get(joinIndex++));
                    }
                    case 25 -> {
                        if (joinIndex >= kafkaSlots.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var slot = kafkaSlots.get(joinIndex++);
                        final var mapping = latestMapping.get(Bytes.hex(slot.key()));
                        if (mapping == null || mapping.logicalSequence() != slot.lastAllocatedSequence()) {
                            throw new IllegalStateException(
                                    "Target K2 slot sequence lacks its immutable latest mapping");
                        }
                        if (slot.pendingMappingId() != null) {
                            slot.requirePending(mapping);
                            if (!slot.mutation().equals(mapping.mutation())) {
                                throw new IllegalStateException("pending K2 slot changes its Admission Source stamp");
                            }
                        } else {
                            slot.mutation().requireAfter(mapping.mutation());
                            final var outcome = Objects.requireNonNull(
                                    resultByStamp.get(Bytes.hex(slot.mutation().canonicalBytes())),
                                    "free Target K2 slot settling Source result");
                            final var result = SystemMutationResult.decode(outcome.typedPayload());
                            if (result.applyStatus() != ApplyStatus.APPLIED
                                    || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME
                                            && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION
                                    || !Arrays.equals(
                                            result.mutationId(),
                                            SystemMutation.computeSystemMutationId(
                                                    proof.root().identity().shard(),
                                                    result.mutationType(),
                                                    mapping.admission().publishAttemptId(),
                                                    result.mutationHash()))) {
                                throw new IllegalStateException(
                                        "free Target K2 slot does not settle its exact latest attempt");
                            }
                        }
                    }
                    case 26 -> {
                        if (joinIndex >= channelControls.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var control = channelControls.get(joinIndex++);
                        final var channel = control.body().request().channel();
                        controlDomains.add(controlDomain(
                                control.mutation().source(),
                                channel.context().target(),
                                channel.context().domain()));
                    }
                    case 27 -> {
                        if (joinIndex >= externalOwners.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var owner = externalOwners.get(joinIndex++);
                        if (externalIndex.putIfAbsent(Bytes.hex(owner.key()), owner) != null) {
                            throw new IllegalStateException("duplicate external primary owner");
                        }
                    }
                    case 28 -> {
                        if (joinIndex >= kafkaMappings.size()) {
                            if (expectedExternal.size() != externalReferences.size()
                                    || usedExternal.size() != externalIndex.size()
                                    || createdExternal.size() != externalIndex.size()) {
                                throw new IllegalStateException(("external owner/ref inventory lacks complete original "
                                        + "mapping/adoption closure"));
                            }
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        requireExternalMapping(kafkaMappings.get(joinIndex++));
                    }
                    case 29 -> {
                        if (joinIndex >= recoveryOwners.size()) {
                            if (expectedRecovery.size() != recoveryReferences.size()) {
                                throw new IllegalStateException("recovery reference inventory has orphan/missing rows");
                            }
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        requireRecoveryOwner(recoveryOwners.get(joinIndex++));
                    }
                    case 30 -> {
                        if (joinIndex >= retirements.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var record = retirements.get(joinIndex++);
                        final var intent = record.intent();
                        final var first = Objects.requireNonNull(
                                resultIndex.get(Bytes.hex(Bytes.concat(
                                        new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                                        intent.systemMutationId()))),
                                "retiring intent actual first");
                        final var result = SystemMutationResult.decode(first.typedPayload());
                        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                                || !first.mutation().equals(record.appliedAt())
                                || result.mutationType() != SystemMutationType.RESOURCE_RETIRE_INTENT
                                || result.applyStatus() != ApplyStatus.APPLIED
                                || result.stableCode() != StableCode.OK
                                || !Arrays.equals(result.mutationId(), intent.systemMutationId())
                                || !Arrays.equals(result.mutationHash(), intent.mutationHash())
                                || result.retryUntilEpochMs() != intent.retryUntilEpochMs()
                                || !Arrays.equals(result.authorIdentity(), intent.authorIdentity())) {
                            throw new IllegalStateException("retiring row lacks exact APPLIED intent first");
                        }
                        final var position = requirePosition(record.appliedAt(), first);
                        if (position.kind() != TargetResultRecord.Kind.POSITION_SYSTEM) {
                            throw new IllegalStateException("retiring row changed Source POSITION companion");
                        }
                    }
                    case 31 -> {
                        if (joinIndex >= sourceHistories.size()) {
                            projectedHistory = orderedHistory.values().iterator();
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var history = sourceHistories.get(joinIndex++);
                        if (orderedHistory.putIfAbsent(history.decidedAt().sequence(), history) != null) {
                            throw new IllegalStateException("retirement histories share one actual Source decision");
                        }
                    }
                    case 32 -> {
                        if (!projectedHistory.hasNext()) {
                            projectedRows = projectedBudgets.entrySet().iterator();
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        projectHistory(projectedHistory.next());
                    }
                    case 33 -> {
                        if (!projectedRows.hasNext()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var entry = projectedRows.next();
                        if (!Arrays.equals(
                                Objects.requireNonNull(budgetIndex.get(entry.getKey()), "reclaimed retained Budget")
                                        .canonicalBytes(),
                                entry.getValue().canonicalBytes())) {
                            throw new IllegalStateException(
                                    "APPLIED reclaim chain lost latest exact retained allocation state");
                        }
                    }
                    case 34 -> {
                        if (joinIndex >= sourceHistories.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        requireReclaimResult(sourceHistories.get(joinIndex++));
                    }
                    case 35 -> {
                        if (joinIndex >= counters.size()) {
                            if (counterIndex.size() != rebuilt.size()
                                    || !shardUsage.equals(proof.aggregate().usage())) {
                                throw new IllegalStateException("quota aggregate/ledger identity set mismatch");
                            }
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var counter = counters.get(joinIndex++);
                        proof.aggregate().requireCounter(counter);
                        if (!counter.usage().equals(rebuilt.get(counter.identity()))) {
                            throw new IllegalStateException(
                                    "quota counter differs from independently rebuilt ledger usage");
                        }
                        if (counter.identity().kind().isMirror()) {
                            final var primary =
                                    counterIndex.get(counter.identity().primary());
                            final var mirrors = mirrorSums.merge(
                                    counter.identity().primary(),
                                    counter.usage().resources(),
                                    CapacityVector::add);
                            if (primary == null || !primary.usage().resources().covers(mirrors)) {
                                throw new IllegalStateException("tenant quota mirror has no covering primary counter");
                            }
                        }
                    }
                    case 36 -> {
                        if (joinIndex >= resultRows.size()) {
                            joinPhase++;
                            joinIndex = 0;
                            continue;
                        }
                        final var stored = resultRows.get(joinIndex++);
                        final var first = resultIndex.get(Bytes.hex(stored.key()));
                        if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                            switch (SystemMutationResult.decode(first.typedPayload())
                                    .mutationType()) {
                                case APPLY_SHARD_CONTROL, TARGET_SLO_OBSERVATION -> dependencies.controlResult(first);
                                case TIME_FENCE -> dependencies.timeFence(first);
                                case PUBLISH_OUTCOME, EVIDENCE_RESOLUTION -> dependencies.outcomeResult(first);
                                case RESOURCE_RETIRE_INTENT, RESOURCE_DELETE_CONFIRMED, TARGET_RESOURCE_RECLAIM ->
                                    dependencies.retirementResult(first);
                                default -> {}
                            }
                        } else if (stored.key()[0] == TargetKeyCodec.RESULT_POSITION_TAG
                                && first.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                            dependencies.systemPosition(first);
                        }
                    }
                    default -> {
                        return advanceFrozenJoins(view, maximumRows - used, retained, copied);
                    }
                }
                used++;
                view.requireWithinElapsedBudget();
            }
            return false;
        }

        private boolean advanceFrozenJoins(
                TargetRetirementLedgerCursor.InventoryView view,
                int maximumRows,
                java.util.function.LongConsumer retained,
                java.util.function.LongConsumer copied) {
            if (maximumRows < 1) {
                return false;
            }
            if (joinPhase == 37) {
                if (freezing == null) {
                    freezing = dependencies.beginRetirementFreeze(admissions, resultRows);
                }
                if (!freezing.advance(maximumRows, view::requireWithinElapsedBudget, retained, copied)) {
                    return false;
                }
                complete = freezing.result();
                joinPhase = 38;
                joinIndex = 0;
                return false;
            }
            if (joinPhase == 38) {
                if (payloadSummary == null) {
                    payloadSummary =
                            new com.nereusstream.delay.runtime.TargetPayloadObligationSummaryAudit.RetirementCursor(
                                    complete);
                }
                if (!payloadSummary.advance(maximumRows, view::requireWithinElapsedBudget, retained, copied)) {
                    return false;
                }
                joinPhase = 39;
                joinIndex = 0;
                return false;
            }
            if (joinPhase == 39) {
                if (archiveHistory == null) {
                    archiveHistory =
                            new com.nereusstream.delay.runtime.TargetMigrationArchiveStore.RetirementCursor(complete);
                }
                if (!archiveHistory.advance(maximumRows, view::requireWithinElapsedBudget, retained, copied)) {
                    return false;
                }
                joinPhase = 40;
                joinIndex = 0;
                return false;
            }
            if (joinPhase == 40) {
                int used = 0;
                while (joinIndex < sourceHistories.size() && used < maximumRows) {
                    final var history = sourceHistories.get(joinIndex);
                    if (history.input().type() == SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                        final var body =
                                TargetResourceReclaimBody.decode(history.input().canonicalBody());
                        final var first = complete.resultRecord(Bytes.concat(
                                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                                history.input().systemMutationId()));
                        if (first != null && first.appliedFirst()) {
                            if (body.selfContainedInlinePayloadRelease()) {
                                com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.requireRecordedSourceFacts(
                                        complete, body);
                            } else if (body.uncommittedObjectReclaim()) {
                                com.nereusstream.delay.runtime.TargetUncommittedPayloadReclaim.requirePreparedClosed(
                                        view.ledger(), history.input(), true, view::requireWithinElapsedBudget);
                            } else if (body.nativeRetainedReclaim()) {
                                com.nereusstream.delay.runtime.TargetNativeRetainedReclaim.requirePreparedClosed(
                                        view.ledger(), body, history.input(), true, view::requireWithinElapsedBudget);
                            }
                        }
                    }
                    joinIndex++;
                    used++;
                    view.requireWithinElapsedBudget();
                }
                if (joinIndex < sourceHistories.size()) {
                    return false;
                }
                joinPhase = 41;
            }
            return joinPhase == 41;
        }

        private TargetResultRecord requireAdmission(TargetQuotaAttemptBudget attempt) {
            final byte[] id = SystemMutation.computeSystemMutationId(
                    proof.root().identity().shard(),
                    SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    attempt.publishAttemptId(),
                    attempt.admissionDigest());
            final var first = resultIndex.get(Bytes.hex(
                    Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id)));
            if (first == null
                    || first.kind() != TargetResultRecord.Kind.SYSTEM
                    || first.allocation() != null
                    || !first.primaryIdentity().equals(proof.root().identity())) {
                throw new IllegalStateException("Target retained Budget lacks its exact Admission first result");
            }
            first.mutation().requireAtOrBefore(attempt.mutation());
            if (attempt.phase() == TargetQuotaAttemptBudget.Phase.ADMITTED) {
                if (!first.mutation().equals(attempt.mutation())) {
                    throw new IllegalStateException("Target ADMITTED Budget changes its Admission source stamp");
                }
            } else {
                attempt.mutation().requireAfter(first.mutation());
            }
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (result.mutationType() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.stableCode() != StableCode.OK
                    || AuthorIdentity.decode(result.authorIdentity()).kind() != AuthorIdentity.Kind.OWNER
                    || !Arrays.equals(result.mutationId(), id)
                    || !Arrays.equals(result.mutationHash(), attempt.admissionDigest())
                    || !Arrays.equals(
                            result.appliedSourcePosition(),
                            first.mutation().source().canonicalBytes())) {
                throw new IllegalStateException("Target retained Budget contradicts its Admission first result");
            }
            return first;
        }

        private void requireKafkaMapping(TargetKafkaReceiptMapping mapping) {
            final var body = mapping.admission();
            final var retained = admissionIndex.get(Bytes.hex(body.publishAttemptId()));
            final var first = retained == null ? null : retained.first();
            final byte[] hash = SystemMutation.computeMutationHash(
                    body.shard(),
                    SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    body.retryUntilEpochMs(),
                    body.canonicalBytes());
            if (first == null
                    || !first.primaryIdentity().equals(proof.root().identity())
                    || !mapping.mutation().equals(first.mutation())
                    || !Arrays.equals(hash, mapping.mutationHash())
                    || !Arrays.equals(retained.attempt().admissionDigest(), hash)
                    || !retained.attempt().locator().equals(body.locator())
                    || retained.attempt().executionBytes() != body.executionBytes()
                    || !retained.attempt().commitment().equals(body.commitment())) {
                throw new IllegalStateException("Target K2 mapping lacks exact retained Admission/budget/first Source");
            }
            final var original = SystemMutationResult.decode(first.typedPayload());
            if (!body.owner()
                            .equals(AuthorIdentity.decode(original.authorIdentity())
                                    .asOwnerIdentity())
                    || original.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                throw new IllegalStateException("Target K2 mapping changes original Owner/deadline");
            }
            final String slotKey = Bytes.hex(TargetKafkaReceiptSlot.key(body.channel()));
            final var slot = kafkaSlotIndex.get(slotKey);
            if (slot == null
                    || Long.compareUnsigned(slot.lastAllocatedSequence(), mapping.logicalSequence()) < 0
                    || !Arrays.equals(slot.recoveryLineage(), mapping.recoveryLineage())
                    || !mappingSequences.add(slotKey + "/" + Long.toUnsignedString(mapping.logicalSequence()))
                    || mappingIndex.putIfAbsent(Bytes.hex(mapping.mappingId()), mapping) != null) {
                throw new IllegalStateException("Target K2 mapping lacks its stable sequence/slot history");
            }
            final var channel = channelIndex.get(Bytes.hex(body.channel().digest()));
            if (channel == null
                    || !Arrays.equals(channel.canonicalBytes(), body.channel().canonicalBytes())) {
                throw new IllegalStateException("Target K2 mapping lacks original retained channel generation");
            }
            final var previous = latestMapping.get(slotKey);
            if (previous == null || Long.compareUnsigned(mapping.logicalSequence(), previous.logicalSequence()) > 0) {
                latestMapping.put(slotKey, mapping);
            }
            final boolean open = retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
            if (open) {
                slot.requirePending(mapping);
            } else if (Arrays.equals(slot.pendingMappingId(), mapping.mappingId())) {
                throw new IllegalStateException("resolved Target K2 budget still occupies its logical slot");
            }
        }

        private static String controlDomain(
                SourcePosition source, TargetPartitionId target, TargetKeyCodec.Domain domain) {
            return Bytes.hex(source.canonicalBytes()) + ":" + Bytes.hex(target.bytes()) + ":" + domain.slot() + ":"
                    + Long.toUnsignedString(domain.generation());
        }

        private void requireExternalMapping(TargetKafkaReceiptMapping mapping) {
            final var attempt = Objects.requireNonNull(
                    budgetIndex.get(Bytes.hex(mapping.admission().publishAttemptId())),
                    "external mapping Attempt budget");
            for (int kind : List.of(1, 2)) {
                final byte[] digest = kind == 1
                        ? mapping.admission().kafkaReceipt().retentionProtectionDigest()
                        : mapping.admission().kafkaReceipt().scanObservationDigest();
                final String key = Bytes.hex(TargetExternalEvidenceReference.key(kind, mapping.mappingId()));
                final var ref =
                        Objects.requireNonNull(externalReferences.get(key), "mapping original external reference");
                expectedExternal.add(key);
                final String ownerKey = Bytes.hex(TargetExternalEvidenceOwner.key(kind, digest));
                final var owner =
                        Objects.requireNonNull(externalIndex.get(ownerKey), "external reference unique retained owner");
                usedExternal.add(ownerKey);
                if (!Arrays.equals(ref.artifactDigest(), digest)
                        || !Arrays.equals(ref.attemptId(), attempt.publishAttemptId())
                        || !ref.referenceOwner().equals(attempt.primaryIdentity())
                        || !ref.accounting().equals(attempt.accounting())
                        || !ref.mutation().equals(mapping.mutation())
                        || !owner.originalDomain()
                                .equals(mapping.admission().channel().context().domain())
                        || !owner.primaryOwner()
                                .identity()
                                .target()
                                .equals(attempt.primaryIdentity().target())) {
                    throw new IllegalStateException(
                            "external reference changed mapping/Attempt/frozen domain responsibility");
                }
                owner.createdAt().requireAtOrBefore(ref.mutation());
                if (owner.createdAt().equals(mapping.mutation())) {
                    if (kind == 2 && !Arrays.equals(owner.creatorAttemptId(), attempt.publishAttemptId())) {
                        throw new IllegalStateException("anchor creator is not its first Source Attempt");
                    }
                    createdExternal.add(ownerKey);
                }
                if (kind == 2) {
                    final String alias =
                            Bytes.hex(com.nereusstream.delay.runtime.TargetExternalEvidenceStore.referenceByAttemptKey(
                                    attempt.publishAttemptId()));
                    expectedExternal.add(alias);
                    if (!Arrays.equals(
                            Objects.requireNonNull(externalReferences.get(alias), "exact attempt-reference alias")
                                    .canonicalBytes(),
                            ref.canonicalBytes())) {
                        throw new IllegalStateException("anchor alias differs from its original reference");
                    }
                }
                if (!controlDomains.contains(controlDomain(
                        owner.originalControlSource(),
                        owner.primaryOwner().identity().target(),
                        owner.originalDomain()))) {
                    throw new IllegalStateException(
                            "external evidence lacks its original APPLIED Channel Control domain");
                }
            }
        }

        private void requireRecoveryOwner(TargetRecoveryEvidenceOwner owner) {
            final var ref = owner.reference();
            if (!recoveryObjects.add(Bytes.hex(owner.key())) || !recoveryAttempts.add(Bytes.hex(ref.attemptId()))) {
                throw new IllegalStateException("recovery evidence has duplicate unique object/Attempt adoption");
            }
            final var mapping =
                    Objects.requireNonNull(mappingIndex.get(Bytes.hex(ref.mappingId())), "recovery original mapping");
            final var budget =
                    Objects.requireNonNull(budgetIndex.get(Bytes.hex(ref.attemptId())), "recovery original Attempt");
            final var original = Objects.requireNonNull(
                    externalIndex.get(Bytes.hex(TargetExternalEvidenceOwner.key(
                            2, mapping.admission().kafkaReceipt().scanObservationDigest()))),
                    "recovery original anchor owner");
            if (!Arrays.equals(
                            owner.primaryOwner().canonicalBytes(),
                            original.primaryOwner().canonicalBytes())
                    || !owner.admissionMutation().equals(mapping.mutation())
                    || !Arrays.equals(owner.admissionHash(), mapping.mutationHash())
                    || !Arrays.equals(ref.attemptId(), mapping.admission().publishAttemptId())
                    || !Arrays.equals(budget.admissionDigest(), owner.admissionHash())
                    || budget.resolvedAt() == null
                    || !budget.resolvedAt().equals(owner.adoptedAt())) {
                throw new IllegalStateException(
                        "recovery owner changed original mapping/Admission/frozen owner/resolved Source");
            }
            for (byte[] key : List.of(
                    owner.referenceRecord().key(), owner.referenceRecord().attemptKey())) {
                final var actual = Objects.requireNonNull(
                        recoveryReferences.get(Bytes.hex(key)), "recovery exact reference/Attempt alias");
                actual.requireOwner(owner);
                actual.requireAttempt(budget);
                expectedRecovery.add(Bytes.hex(key));
            }
            final var first = Objects.requireNonNull(
                    resultIndex.get(Bytes.hex(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            owner.outcomeId()))),
                    "recovery adopting first result");
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.mutation().equals(owner.adoptedAt())
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.mutationType() != owner.outcomeType()
                    || !Arrays.equals(result.mutationId(), owner.outcomeId())
                    || !Arrays.equals(result.mutationHash(), owner.outcomeHash())
                    || !Arrays.equals(
                            result.appliedSourcePosition(),
                            owner.adoptedAt().source().canonicalBytes())) {
                throw new IllegalStateException("recovery evidence lacks actual adopting APPLIED Source first");
            }
            if (requirePosition(owner.adoptedAt(), first).kind() != TargetResultRecord.Kind.POSITION_SYSTEM) {
                throw new IllegalStateException("recovery evidence changed actual Source POSITION");
            }
        }

        private TargetResultRecord requirePosition(TargetQuotaMutation at, TargetResultRecord first) {
            final var position = Objects.requireNonNull(
                    resultIndex.get(Bytes.hex(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                            at.source().canonicalBytes()))),
                    "actual Source POSITION companion");
            position.requireFirst(first);
            if (!position.mutation().equals(at)) {
                throw new IllegalStateException("actual Source POSITION changed its first stamp");
            }
            return position;
        }

        private void projectHistory(TargetRetirementSourceHistoryRecord history) {
            if (history.input().type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                return;
            }
            final var first = Objects.requireNonNull(
                    resultIndex.get(Bytes.hex(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            history.input().systemMutationId()))),
                    "actual reclaim first");
            if (!first.appliedFirst()) {
                return;
            }
            final var body = TargetResourceReclaimBody.decode(history.input().canonicalBody());
            for (var operation : body.operations()) {
                final TargetQuotaAttemptBudget expected, next;
                if (operation instanceof TargetResourceReclaimBody.UnusedAttemptReserve unused) {
                    expected = unused.expected();
                    next = expected.unusedReserveProjection(body.floor(), history.decidedAt());
                } else if (operation instanceof TargetResourceReclaimBody.RetainedAllocationRelease allocation) {
                    expected = allocation.expected();
                    next = expected.retainedReleaseProjection(
                            allocation.remaining(), body.floor(), history.decidedAt());
                    for (var receipt : allocation.confirmations()) {
                        final String key = Bytes.hex(expected.publishAttemptId()) + ":"
                                + Bytes.hex(TargetResourceReclaimBody.RetainedAllocationRelease.shareIdentity(receipt));
                        if (!settledShares.add(key)) {
                            throw new IllegalStateException(
                                    "one owned-object share has multiple APPLIED allocation settlements");
                        }
                        settledAt.put(key, history.decidedAt());
                    }
                } else {
                    continue;
                }
                final var prior = projectedBudgets.put(Bytes.hex(expected.publishAttemptId()), next);
                if (prior != null && !Arrays.equals(prior.canonicalBytes(), expected.canonicalBytes())) {
                    throw new IllegalStateException("retained allocation history changed exact previous Budget");
                }
            }
        }

        private void requireReclaimResult(TargetRetirementSourceHistoryRecord history) {
            if (history.input().type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
                return;
            }
            final var first = Objects.requireNonNull(
                    resultIndex.get(Bytes.hex(Bytes.concat(
                            new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                            history.input().systemMutationId()))),
                    "reclaim actual first");
            if (!first.appliedFirst()) {
                return;
            }
            final var body = TargetResourceReclaimBody.decode(history.input().canonicalBody());
            for (var operation : body.operations()) {
                switch (operation) {
                    case TargetResourceReclaimBody.UnusedAttemptReserve unused -> {
                        if (!projectedBudgets.containsKey(Bytes.hex(Objects.requireNonNull(
                                        budgetIndex.get(
                                                Bytes.hex(unused.expected().publishAttemptId())),
                                        "reclaim actual retained budget")
                                .publishAttemptId()))) {
                            throw new IllegalStateException(
                                    "APPLIED unused-reserve reclaim lost exact actual phase/Source result");
                        }
                    }
                    case TargetResourceReclaimBody.InlinePayloadRelease inline -> {
                        requireReleasedOwner(inline.expected(), body, history);
                    }
                    case TargetResourceReclaimBody.UncommittedObjectConfirm confirm -> {
                        requireReleasedOwner(confirm.intent().expectedOwner(), body, history);
                    }
                    case TargetResourceReclaimBody.ObjectPayloadConfirm confirm -> {
                        requireReleasedOwner(confirm.intent().operation().expected(), body, history);
                    }
                    case TargetResourceReclaimBody.RetainedRecordDelete leaf -> {
                        if (resultIndex.containsKey(Bytes.hex(leaf.expected().key()))) {
                            throw new IllegalStateException("APPLIED Native record deletion retains its row");
                        }
                    }
                    case TargetResourceReclaimBody.RetainedAllocationRelease allocation -> {
                        if (!projectedBudgets.containsKey(
                                Bytes.hex(allocation.expected().publishAttemptId()))) {
                            throw new IllegalStateException("APPLIED retained settlement lost its Budget");
                        }
                    }
                    default -> {}
                }
            }
        }

        private void requireReleasedOwner(
                TargetQuotaPayloadOwner expected,
                TargetResourceReclaimBody body,
                TargetRetirementSourceHistoryRecord history) {
            final String id = Bytes.hex(expected.messageId().bytes());
            final var owner = Objects.requireNonNull(payloadOwners.get(id), "APPLIED payload release permanent owner");
            if (!owner.mutation().equals(history.decidedAt()) || primaryMessages.contains(id)) {
                throw new IllegalStateException("APPLIED payload release lost actual Source/absent primary");
            }
            owner.requireReleasedFrom(expected, body.floor());
        }

        private java.util.Iterator<Map.Entry<TargetQuotaIdentity, CapacityVector>> contributionRows;

        private void requireGrant(TargetQuotaGrantActivation grant) {
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, grant.systemMutationId());
            final var first = resultIndex.get(Bytes.hex(key));
            if (first == null
                    || first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.primaryIdentity().equals(proof.root().identity())
                    || !first.mutation().equals(grant.mutation())) {
                throw new IllegalStateException("Target grant activation lacks its exact first System result");
            }
            final var result = SystemMutationResult.decode(first.typedPayload());
            if (!Arrays.equals(result.mutationId(), grant.systemMutationId())
                    || !Arrays.equals(result.mutationHash(), grant.systemMutationHash())
                    || result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                    || result.applyStatus() != ApplyStatus.APPLIED
                    || result.stableCode() != StableCode.OK
                    || !Arrays.equals(
                            result.appliedSourcePosition(),
                            grant.mutation().source().canonicalBytes())
                    || (grant.allocation() != null
                                    && grant.allocation().allocation().equals(grant.mutation()))
                            != (first.allocation() != null)
                    || first.allocation() != null
                            && !Arrays.equals(
                                    first.allocation().canonicalBytes(),
                                    grant.allocation().canonicalBytes())) {
                throw new IllegalStateException("Target grant activation contradicts its first System result");
            }
        }

        TargetCheckpointDependencies result() {
            if (complete == null || joinPhase != 41) {
                throw new IllegalStateException("retirement ledger joins are incomplete");
            }
            return complete;
        }

        TargetCheckpointDependencies dependenciesForClosure() {
            if (complete == null) {
                throw new IllegalStateException("retirement ledger has not sealed its actual dependency rows");
            }
            return complete;
        }

        TargetKafkaReceiptMapping mapping(byte[] id) {
            dependenciesForClosure();
            return Objects.requireNonNull(mappingIndex.get(Bytes.hex(id)), "original actual Kafka mapping");
        }

        TargetQuotaAttemptBudget projectedBudget(byte[] id) {
            dependenciesForClosure();
            return projectedBudgets.get(Bytes.hex(id));
        }

        TargetQuotaMutation settlement(byte[] attempt, byte[] share) {
            dependenciesForClosure();
            return settledAt.get(Bytes.hex(attempt) + ":" + Bytes.hex(share));
        }

        TargetResultRecord resultAt(TargetQuotaMutation at) {
            dependenciesForClosure();
            return resultByStamp.get(Bytes.hex(at.canonicalBytes()));
        }

        long grantCount(TargetQuotaIdentity owner) {
            dependenciesForClosure();
            return grantsByOwner.getOrDefault(owner, 0L);
        }

        java.util.Collection<TargetCheckpointDependencies.RetryUse> retryUses(DelayMessageId id) {
            dependenciesForClosure();
            final var values = messageRetryUses.get(id);
            return values == null ? List.of() : java.util.Collections.unmodifiableCollection(values.values());
        }

        List<TargetTerminalGenerationRecord> terminals(DelayMessageId id) {
            dependenciesForClosure();
            return java.util.Collections.unmodifiableList(messageTerminals.getOrDefault(id, List.of()));
        }

        List<TargetCheckpointDependencies.Admission> messageAdmissions(DelayMessageId id) {
            dependenciesForClosure();
            return java.util.Collections.unmodifiableList(messageAdmissions.getOrDefault(id, List.of()));
        }

        void clear() {}
    }

    private static void requireFixedBytes(final byte[] actual, final byte[] expected, final String description) {
        if (!Bytes.constantTimeEquals(actual, expected)) {
            throw new IllegalArgumentException("Target checkpoint fixed " + description + " differs from root");
        }
    }

    private static void requireNonZeroIdentity(final byte[] value, final int length, final String description) {
        Bytes.requireLength(value, length, description);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException("Target checkpoint " + description + " is zero");
    }

    private static void merge(
            final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt,
            final TargetQuotaIdentity identity,
            final TargetQuotaUsage contribution) {
        rebuilt.merge(identity, contribution, TargetQuotaUsage::add);
    }

    private static final class Budget {
        private final TargetCheckpointRootVerifier.LedgerAuditLimits limits;
        private int records;
        private long bytes;
        private int pointReads;
        private long pointBytes;

        private Budget(final TargetCheckpointRootVerifier.LedgerAuditLimits limits) {
            this.limits = limits;
        }

        private void scan(final int keyBytes, final int valueBytes) {
            final long size = (long) keyBytes + valueBytes;
            if (records >= limits.maxRecords() || size > limits.maxKeyValueBytes() - bytes) {
                throw new IllegalArgumentException("Target checkpoint ledger scan budget exceeded");
            }
            records++;
            bytes += size;
        }

        private void point(final int keyBytes, final int valueBytes) {
            final long size = (long) keyBytes + valueBytes;
            if (pointReads >= limits.maxPointReads() || size > limits.maxPointReadBytes() - pointBytes) {
                throw new IllegalArgumentException("Target checkpoint ledger point-read budget exceeded");
            }
            pointReads++;
            pointBytes += size;
        }
    }
}
