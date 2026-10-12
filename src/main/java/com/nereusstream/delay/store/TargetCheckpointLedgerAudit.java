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
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelSlot;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetNativePolicyControlRecord;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
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
        };
        final var accounting = new TargetRecordAccounting(
                view,
                List.of(),
                proof.root().scope(),
                proof.root().recoveryLineage(),
                proof.aggregate().mutation(),
                proof.bookkeeping(),
                TargetQueueState.MAX_DOMAIN_SLOTS);
        final Map<TargetQuotaIdentity, TargetQuotaUsage> rebuilt = new HashMap<>();
        final Map<TargetQuotaIdentity, CapacityVector> resultContributions = new HashMap<>();
        final List<TargetResultLedgerAudit.Stored> resultRows = new ArrayList<>();
        final List<TargetQuotaGrantActivation> grantActivations = new ArrayList<>();
        final List<TargetMembershipClosureRecord> membershipClosures = new ArrayList<>();
        final List<TargetNativePolicyControlRecord> nativeControls = new ArrayList<>();
        final List<TargetChannelControlRecord> channelControls = new ArrayList<>();
        final List<TargetChannelIdentity> channelIdentities = new ArrayList<>();
        final List<TargetChannelSlot> channelSlots = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> kafkaMappings = new ArrayList<>();
        final List<com.nereusstream.delay.protocol.TargetKafkaReceiptSlot> kafkaSlots = new ArrayList<>();
        final List<TargetInitialControlSnapshot> initialControls = new ArrayList<>();
        final List<TargetQuotaAttemptBudget> attemptBudgets = new ArrayList<>();
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
                    } else if (family == ColumnFamily.META) {
                        final int tag = Byte.toUnsignedInt(key[0]);
                        if (Arrays.equals(key, KeyCodec.metaFixed(10))) {
                            final var snapshot = TargetInitialControlSnapshot.decode(payload);
                            initialControls.add(snapshot);
                            dependencies.control(family, key, type, payload);
                            snapshot.input().profiles().forEach(ref -> dependencies.profileUse(
                                    new TargetCheckpointDependencies.ProfileUse(ref, snapshot.input().source())));
                        }
                        if (tag >= TargetKeyCodec.STATE_TAG && tag <= TargetKeyCodec.NATIVE_POLICY_SNAPSHOT_TAG
                                || tag == TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG
                                || tag == TargetKeyCodec.CLOSE_TAG || tag == TargetKeyCodec.MEMBERSHIP_CLOSURE_TAG
                                || tag >= TargetKeyCodec.NATIVE_PUBLISHER_TAG) {
                            dependencies.control(family, key, type, payload);
                        }
                        if (type == TargetMembershipGrant.VALUE_TYPE) {
                            dependencies.membership(TargetMembershipGrant.decode(payload));
                        } else if (type == TargetChannelIdentity.VALUE_TYPE) {
                            dependencies.profile(TargetChannelIdentity.decode(payload).credentialLease().profile());
                        } else if (type == TargetNativePolicyScope.VALUE_TYPE) {
                            dependencies.nativeScope(TargetNativePolicyScope.decode(payload));
                        } else if (type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                            final var control = TargetNativePolicyControlRecord.decode(payload);
                            dependencies.nativeScope(control.body().request().scope());
                            if (control.grant() != null) { dependencies.membership(control.grant()); }
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
                    } else if (family == ColumnFamily.ID && type == TargetMessageRecord.VALUE_TYPE) {
                        final var message =
                                TargetMessageRecord.decodeForStore(key, payload, proof.metadata().shardId());
                        attemptProjections.add(new AttemptProjection(message.locator(), message.runtime()));
                        auditMessageDependencies(message, view);
                    } else if (family == ColumnFamily.META && type == TargetOrderState.VALUE_TYPE) {
                        auditOrderStateDependencies(TargetOrderState.decode(payload), view);
                    } else if (family == ColumnFamily.META && type == TargetChannelIdentity.VALUE_TYPE) {
                        channelIdentities.add(TargetChannelIdentity.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetChannelControlRecord.VALUE_TYPE) {
                        channelControls.add(TargetChannelControlRecord.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetChannelSlot.VALUE_TYPE) {
                        channelSlots.add(TargetChannelSlot.decodeForStore(key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.VALUE_TYPE) {
                        final var mapping = com.nereusstream.delay.protocol.TargetKafkaReceiptMapping.decodeForStore(key, payload, proof.metadata().shardId());
                        kafkaMappings.add(mapping);
                        dependencies.kafkaMapping(mapping);
                    } else if (family == ColumnFamily.META && type == com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.VALUE_TYPE) {
                        kafkaSlots.add(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.decodeForStore(key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetQuotaGrantActivation.VALUE_TYPE) {
                        grantActivations.add(TargetQuotaGrantActivation.decodeForStore(
                                key, payload, proof.metadata().shardId(), proof.bookkeeping().tenantScope()));
                    } else if (family == ColumnFamily.META && type == TargetQuotaAttemptBudget.VALUE_TYPE) {
                        attemptBudgets.add(TargetQuotaAttemptBudget.decodeForStore(
                                key, payload, proof.metadata().shardId()));
                    } else if (family == ColumnFamily.META && type == TargetMembershipClosureRecord.VALUE_TYPE) {
                        membershipClosures.add(TargetMembershipClosureRecord.decodeForStore(
                                key, payload, proof.metadata().shardId(), proof.root().recoveryLineage()));
                    } else if (family == ColumnFamily.META && type == TargetNativePolicyControlRecord.VALUE_TYPE) {
                        nativeControls.add(TargetNativePolicyControlRecord.decodeForStore(
                                key, payload, proof.metadata().shardId(), proof.root().recoveryLineage()));
                    } else if (family == ColumnFamily.TERMINAL
                            && type == TargetTerminalGenerationRecord.VALUE_TYPE) {
                        final var terminal = TargetTerminalGenerationRecord.decode(payload);
                        if (!Arrays.equals(key, terminal.key())) {
                            throw new IllegalArgumentException("Target checkpoint terminal key mismatch");
                        }
                        attemptProjections.add(new AttemptProjection(terminal.locator(), terminal.runtime()));
                    }
                    final var charge = accounting.charge(family, key, raw);
                    if (charge != null) {
                        merge(rebuilt, charge.owner().identity(), charge.primary());
                        merge(rebuilt, charge.owner().tenantIdentity(), charge.mirror());
                        if (family == ColumnFamily.DEDUPE) {
                            resultContributions.merge(
                                    charge.owner().identity(),
                                    charge.primary().resources(),
                                    CapacityVector::add);
                            resultContributions.merge(
                                    charge.owner().tenantIdentity(),
                                    charge.mirror().resources(),
                                    CapacityVector::add);
                        }
                    }
                }
            });
        }
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
                });
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
            auditGrantResults(List.of(snapshot.initialGrant()), resultRows, proof.root().identity());
            final var first = resultRows.stream().filter(row -> Arrays.equals(row.key(), Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, snapshot.initialGrant().systemMutationId())))
                    .findFirst().orElseThrow();
            final var firstAuthor = SystemMutationResult.decode(
                    TargetResultRecord.decode(first.payload()).typedPayload()).authorIdentity();
            if (!Arrays.equals(snapshot.authorIdentity(), firstAuthor)) {
                throw new IllegalStateException("Target initial control snapshot changes its original Control author");
            }
        }
        auditControlResults(membershipClosures, nativeControls, channelControls, resultRows, proof.root().identity());
        auditChannelHistory(channelControls, channelIdentities, channelSlots);
        final var admissions = auditAdmissionResults(attemptBudgets, resultRows, proof.root().identity());
        auditKafkaReceiptReferences(kafkaMappings, kafkaSlots, channelIdentities, admissions, resultRows, proof.root().identity());
        TargetQuotaDelta.audit(proof.aggregate(), counters, rebuilt);
        for (final var stored : resultRows) {
            if (stored.key()[0] == TargetKeyCodec.RESULT_SYSTEM_TAG) {
                final var first = TargetResultRecord.decode(stored.payload());
                if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                        == SystemMutationType.APPLY_SHARD_CONTROL) {
                    dependencies.controlResult(first);
                } else if (SystemMutationResult.decode(first.typedPayload()).mutationType()
                        == SystemMutationType.PUBLISH_OUTCOME
                        || SystemMutationResult.decode(first.typedPayload()).mutationType()
                                == SystemMutationType.EVIDENCE_RESOLUTION) {
                    dependencies.outcomeResult(first);
                }
            }
        }
        return dependencies.finish(admissions);
    }

    @FunctionalInterface
    private interface EntryVisitor {
        void visit(byte[] key, byte[] raw);
    }

    private interface LedgerSource {
        byte[] get(ColumnFamily family, byte[] key);

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
                                        prior.obligation().canonicalBytes(), next.obligation().canonicalBytes()))) {
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
                        case ADMITTED -> reference.obligation().ledgerState()
                                == AttemptLedgerState.PUBLISHING;
                        case UNKNOWN -> reference.obligation().ledgerState()
                                == AttemptLedgerState.UNCERTAIN;
                        case RESOLVED_AWAITING_FLOOR, RETAINED, RELEASED -> false;
                    };
            if (!matches) {
                throw new IllegalStateException("Target checkpoint attempt reference contradicts its quota budget");
            }
        }
        for (Map.Entry<String, TargetQuotaAttemptBudget> entry : byAttempt.entrySet()) {
            final var phase = entry.getValue().phase();
            final boolean open = phase == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || phase == TargetQuotaAttemptBudget.Phase.UNKNOWN;
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
            requireMessageIndex(
                    view, expiry.encodedKey(), TargetExpiryRef.VALUE_TYPE, expiry.canonicalBytes());
        }
        final TargetTimelineWorkRef work = message.runtime().timeline();
        if (work != null) {
            requireMessageIndex(view, work.ordinaryKey(), TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes());
            if (work.nativeCandidate()) {
                requireMessageIndex(view, work.nativeKey(), TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes());
            }
        }
        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(message.locator().scheduleBindingDigest());
        final byte[] bindingRaw = view.projected(ColumnFamily.ID, bindingKey, List.of());
        if (bindingRaw == null) {
            throw new IllegalStateException("Target checkpoint Message lacks its original Schedule binding");
        }
        final TargetScheduleBinding binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(bindingRaw, TargetScheduleBinding.VALUE_TYPE).payload(),
                view.shardId());
        binding.requireLocator(message.locator());
        binding.requireMessageSource(message.scheduleSource());
    }

    private static void requireMessageIndex(
            final TargetRecordAccounting.View view, final byte[] key, final int type, final byte[] expected) {
        final byte[] raw = view.projected(ColumnFamily.TIMELINE, key, List.of());
        if (raw == null
                || !Bytes.constantTimeEquals(TargetValueEnvelope.decode(raw, type).payload(), expected)) {
            throw new IllegalStateException("Target checkpoint Message lacks its exact current timeline index");
        }
    }

    static void auditOrderStateDependencies(
            final TargetOrderState state, final TargetRecordAccounting.View view) {
        final var head = state.serviceableHead();
        if (head != null) {
            final byte[] raw = view.projected(ColumnFamily.TIMELINE, head.key(), List.of());
            if (raw == null) {
                throw new IllegalStateException("Target checkpoint strict domain lacks its serviceable head");
            }
            final byte[] work = TargetValueEnvelope.decode(raw, TargetTimelineWorkRef.VALUE_TYPE).payload();
            state.requireServiceableProjection(
                    head.key(), work, requireOrderMessage(head.messageId(), view));
        }
        final var barrier = state.barrier();
        if (barrier != null) {
            final var current = requireOrderMessage(barrier.locator().messageId(), view);
            if (current.locator().equals(barrier.locator())) {
                state.requireBarrierProjection(current);
            } else {
                if (Integer.compareUnsigned(current.locator().generation(), barrier.locator().generation()) <= 0) {
                    throw new IllegalStateException(
                            "Target checkpoint barrier references another or future generation");
                }
                final byte[] key = TargetTerminalGenerationRecord.key(barrier.locator());
                final byte[] raw = view.projected(ColumnFamily.TERMINAL, key, List.of());
                if (raw == null) {
                    throw new IllegalStateException("Target checkpoint historical barrier lacks its terminal summary");
                }
                final var terminal = TargetTerminalGenerationRecord.decode(
                        TargetValueEnvelope.decode(raw, TargetTerminalGenerationRecord.VALUE_TYPE).payload());
                state.requireTerminalBarrierProjection(terminal);
            }
        }
    }

    private static TargetMessageRecord requireOrderMessage(
            final DelayMessageId messageId,
            final TargetRecordAccounting.View view) {
        final byte[] key = TargetKeyCodec.message(messageId);
        final byte[] raw = view.projected(ColumnFamily.ID, key, List.of());
        if (raw == null) {
            throw new IllegalStateException("Target checkpoint strict domain lacks its referenced Message");
        }
        return TargetMessageRecord.decodeForStore(
                key, TargetValueEnvelope.decode(raw, TargetMessageRecord.VALUE_TYPE).payload(), view.shardId());
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
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                    grant.systemMutationId());
            final TargetResultRecord record = systems.get(HexFormat.of().formatHex(key));
            if (record == null || record.kind() != TargetResultRecord.Kind.SYSTEM
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
                    || !Arrays.equals(outcome.appliedSourcePosition(), grant.mutation().source().canonicalBytes())
                    || (grant.allocation() != null
                            && grant.allocation().allocation().equals(grant.mutation()))
                            != (record.allocation() != null)) {
                throw new IllegalStateException("Target grant activation contradicts its first System result");
            }
            if (record.allocation() != null
                    && !Arrays.equals(record.allocation().canonicalBytes(), grant.allocation().canonicalBytes())) {
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
            requireControlResult(systems, root, closure.mutation(), body.retryUntil(), body.logicalIdentity(),
                    body.canonicalBytes(), null);
        }
        for (final var control : controls) {
            final var body = control.body();
            final var author = control.author();
            requireControlResult(systems, root, control.mutation(), body.retryUntil(), body.logicalIdentity(),
                    body.canonicalBytes(), AuthorIdentity.control(author.operationActorIdHash(),
                            author.authenticatedRoleSetHash(), author.tenantResourceScopeHash()).canonicalBytes());
        }
        for (final var control : channels) {
            final var body = control.body();
            final var author = control.author();
            requireControlResult(systems, root, control.mutation(), body.retryUntil(), body.logicalIdentity(),
                    body.canonicalBytes(), AuthorIdentity.control(author.operationActorIdHash(),
                            author.authenticatedRoleSetHash(), author.tenantResourceScopeHash()).canonicalBytes());
        }
    }

    private static void auditKafkaReceiptReferences(
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptMapping> mappings,
            List<com.nereusstream.delay.protocol.TargetKafkaReceiptSlot> slots, List<TargetChannelIdentity> identities,
            List<TargetCheckpointDependencies.Admission> admissions, List<TargetResultLedgerAudit.Stored> results,
            TargetQuotaIdentity root) {
        if (mappings.isEmpty() && slots.isEmpty()) { return; }
        final var byAttempt = new HashMap<String, TargetCheckpointDependencies.Admission>();
        for (var admission : admissions) { byAttempt.put(Bytes.hex(admission.attempt().publishAttemptId()), admission); }
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
        for (var channel : identities) { channelIds.put(Bytes.hex(channel.digest()), channel); }
        for (var slot : slots) {
            if (bySlot.put(Bytes.hex(slot.key()), slot) != null) { throw new IllegalStateException("duplicate Target K2 slot"); }
            final var channel = channelIds.get(Bytes.hex(slot.channel().digest()));
            if (channel == null || !Arrays.equals(channel.canonicalBytes(), slot.channel().canonicalBytes())) {
                throw new IllegalStateException("Target K2 slot lacks exact retained channel identity");
            }
        }
        for (var mapping : mappings) {
            final var body = mapping.admission();
            final var retained = byAttempt.get(Bytes.hex(body.publishAttemptId()));
            final var first = retained == null ? null : retained.first();
            final var hash = SystemMutation.computeMutationHash(body.shard(), SystemMutationType.TARGET_PUBLISH_ADMISSION,
                    body.retryUntilEpochMs(), body.canonicalBytes());
            if (first == null || !first.primaryIdentity().equals(root) || !mapping.mutation().equals(first.mutation())
                    || !Arrays.equals(hash, mapping.mutationHash())
                    || !Arrays.equals(retained.attempt().admissionDigest(), hash)
                    || !retained.attempt().locator().equals(body.locator())
                    || retained.attempt().executionBytes() != body.executionBytes()
                    || !retained.attempt().commitment().equals(body.commitment())) {
                throw new IllegalStateException("Target K2 mapping lacks exact retained Admission/budget/first Source");
            }
            final var original = SystemMutationResult.decode(first.typedPayload());
            if (!body.owner().equals(AuthorIdentity.decode(original.authorIdentity()).asOwnerIdentity())
                    || original.retryUntilEpochMs() != body.retryUntilEpochMs()) {
                throw new IllegalStateException("Target K2 mapping changes original Owner/deadline");
            }
            final String slotKey = Bytes.hex(com.nereusstream.delay.protocol.TargetKafkaReceiptSlot.key(body.channel()));
            final var slot = bySlot.get(slotKey);
            if (slot == null || Long.compareUnsigned(slot.lastAllocatedSequence(), mapping.logicalSequence()) < 0
                    || !Arrays.equals(slot.recoveryLineage(), mapping.recoveryLineage())
                    || !sequenceIds.add(slotKey + "/" + Long.toUnsignedString(mapping.logicalSequence()))
                    || byMapping.put(Bytes.hex(mapping.mappingId()), mapping) != null) {
                throw new IllegalStateException("Target K2 mapping lacks its stable sequence/slot history");
            }
            final var channel = channelIds.get(Bytes.hex(body.channel().digest()));
            if (channel == null || !Arrays.equals(channel.canonicalBytes(), body.channel().canonicalBytes())) {
                throw new IllegalStateException("Target K2 mapping lacks original retained channel generation");
            }
            final var previous = latest.get(slotKey);
            if (previous == null || Long.compareUnsigned(mapping.logicalSequence(), previous.logicalSequence()) > 0) {
                latest.put(slotKey, mapping);
            }
            final boolean unresolved = retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.ADMITTED
                    || retained.attempt().phase() == TargetQuotaAttemptBudget.Phase.UNKNOWN;
            if (unresolved) { slot.requirePending(mapping); }
            else if (Arrays.equals(slot.pendingMappingId(), mapping.mappingId())) {
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
                if (!slot.mutation().equals(mapping.mutation())) { throw new IllegalStateException("pending K2 slot changes its Admission Source stamp"); }
            } else {
                slot.mutation().requireAfter(mapping.mutation());
                final var outcome = resultsByStamp.get(Bytes.hex(slot.mutation().canonicalBytes()));
                if (outcome == null) { throw new IllegalStateException("free Target K2 slot lacks its settling Source result"); }
                final var result = SystemMutationResult.decode(outcome.typedPayload());
                if (result.applyStatus() != ApplyStatus.APPLIED
                        || result.mutationType() != SystemMutationType.PUBLISH_OUTCOME
                                && result.mutationType() != SystemMutationType.EVIDENCE_RESOLUTION
                        || !Arrays.equals(result.mutationId(), SystemMutation.computeSystemMutationId(root.shard(),
                                result.mutationType(), mapping.admission().publishAttemptId(), result.mutationHash()))) {
                    throw new IllegalStateException("free Target K2 slot does not settle its exact latest attempt");
                }
            }
        }
    }

    private static void auditChannelHistory(final List<TargetChannelControlRecord> controls,
            final List<TargetChannelIdentity> identities, final List<TargetChannelSlot> slots) {
        final Map<String, TargetChannelIdentity> byIdentity = new HashMap<>();
        final Map<String, TargetChannelSlot> bySlot = new HashMap<>();
        final Map<String, List<TargetChannelControlRecord>> history = new HashMap<>();
        final java.util.Set<String> created = new java.util.HashSet<>();
        for (final var identity : identities) {
            if (byIdentity.putIfAbsent(Bytes.hex(identity.digest()), identity) != null) {
                throw new IllegalStateException("checkpoint repeats a channel immutable identity");
            }
        }
        for (final var slot : slots) {
            if (bySlot.putIfAbsent(Bytes.hex(slot.key()), slot) != null) {
                throw new IllegalStateException("checkpoint repeats a current channel slot");
            }
        }
        for (final var event : controls) {
            final var requested = event.body().request().channel();
            final var identity = byIdentity.get(Bytes.hex(requested.digest()));
            if (identity == null || !Arrays.equals(identity.canonicalBytes(), requested.canonicalBytes())) {
                throw new IllegalStateException("channel lifecycle event lacks its exact retained NV20 identity");
            }
            final String key = Bytes.hex(TargetKeyCodec.channelSlot(identity.context().producerIdentity()));
            history.computeIfAbsent(key, ignored -> new ArrayList<>()).add(event);
        }
        if (history.size() != bySlot.size()) {
            throw new IllegalStateException("channel history and current slot inventory differ");
        }
        for (final var entry : history.entrySet()) {
            final var events = entry.getValue();
            events.sort((left, right) -> left.mutation().source().compareTo(right.mutation().source()));
            TargetChannelSlot prior = null;
            for (final var event : events) {
                final var request = event.body().request();
                final var channel = request.channel();
                if (prior != null) { event.mutation().requireAfter(prior.mutation()); }
                switch (request.operationKind()) {
                    case CREATE_TARGET_CHANNEL -> {
                        if (prior != null) {
                            throw new IllegalStateException("channel creation overwrote an existing stable slot");
                        }
                    }
                    case RENEW_TARGET_CHANNEL -> {
                        if (prior == null || prior.phase() != TargetChannelSlot.Phase.OPEN
                                || !Arrays.equals(prior.channel().digest(), request.expectedPriorDigest())) {
                            throw new IllegalStateException("channel renewal lacks its exact open predecessor");
                        }
                        channel.requireSuccessorOf(prior.channel());
                    }
                    case STOP_TARGET_CHANNEL_NEW_WORK -> {
                        if (prior == null) {
                            throw new IllegalStateException("channel stop lacks its earlier Source creation");
                        }
                        prior.channel().requireExactFrozenIdentity(channel);
                    }
                    default -> throw new IllegalStateException("unregistered channel lifecycle operation");
                }
                if (request.operationKind() != ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK
                        && !created.add(Bytes.hex(channel.digest()))) {
                    throw new IllegalStateException("channel immutable generation has multiple creation events");
                }
                prior = new TargetChannelSlot(event);
            }
            final var current = bySlot.get(entry.getKey());
            if (current == null || prior == null
                    || !Arrays.equals(current.canonicalBytes(), prior.canonicalBytes())) {
                throw new IllegalStateException("current channel slot differs from its complete Source history fold");
            }
        }
        if (created.size() != byIdentity.size()) {
            throw new IllegalStateException("bare channel identity has no authenticated Source creation history");
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
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
            final var first = systems.get(HexFormat.of().formatHex(key));
            if (first == null || first.kind() != TargetResultRecord.Kind.SYSTEM || first.allocation() != null
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
            if (result.mutationType() != type || result.applyStatus() != ApplyStatus.APPLIED
                    || result.stableCode() != StableCode.OK
                    || AuthorIdentity.decode(result.authorIdentity()).kind() != AuthorIdentity.Kind.OWNER
                    || !Arrays.equals(result.mutationId(), id)
                    || !Arrays.equals(result.mutationHash(), attempt.admissionDigest())
                    || !Arrays.equals(result.appliedSourcePosition(), first.mutation().source().canonicalBytes())) {
                throw new IllegalStateException("Target retained Budget contradicts its Admission first result");
            }
            admissions.add(new TargetCheckpointDependencies.Admission(attempt, first));
        }
        return List.copyOf(admissions);
    }

    private static void requireControlResult(
            final Map<String, TargetResultRecord> systems, final TargetQuotaIdentity root,
            final com.nereusstream.delay.protocol.TargetQuotaMutation mutation,
            final long retryUntil, final byte[] logicalIdentity, final byte[] body, final byte[] author) {
        final var type = SystemMutationType.APPLY_SHARD_CONTROL;
        final byte[] hash = SystemMutation.computeMutationHash(root.shard(), type, retryUntil, body);
        final byte[] id = SystemMutation.computeSystemMutationId(root.shard(), type, logicalIdentity, hash);
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
        final var record = systems.get(HexFormat.of().formatHex(key));
        if (record == null || record.kind() != TargetResultRecord.Kind.SYSTEM || record.allocation() != null
                || !record.primaryIdentity().equals(root) || !record.mutation().equals(mutation)) {
            throw new IllegalStateException("Target retained control lacks its exact first System result");
        }
        final var result = SystemMutationResult.decode(record.typedPayload());
        if (result.mutationType() != type || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK || result.retryUntilEpochMs() != retryUntil
                || !Arrays.equals(result.mutationId(), id) || !Arrays.equals(result.mutationHash(), hash)
                || !Arrays.equals(result.appliedSourcePosition(), mutation.source().canonicalBytes())
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
                    StoreRuntimeMetadata.requireEvidenceCursorScope(cursors, 2, proof.metadata().shardId());
                    if (!cursors.isEmpty()) {
                        final var charge = TargetRecordAccounting.resources(proof.root().accounting().recordCharge(
                                TargetQuotaAccounting.RecordClass.STATE, key.length, payload.length));
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
                case 10 -> { return false; }
                default -> throw new IllegalArgumentException("unknown Target checkpoint fixed metadata key");
            }
            return true;
        }
        if (tag == 7) {
            if (key.length != 3 || Byte.toUnsignedInt(key[2]) == 0 || Byte.toUnsignedInt(key[2]) > 4) {
                throw new IllegalArgumentException("unknown Target checkpoint recovery metadata key");
            }
            recovery.accept(Byte.toUnsignedInt(key[2]), TargetValueEnvelope.decode(raw, 1).payload());
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
                    TargetValueEnvelope.decode(raw, TargetQuotaTotal.VALUE_TYPE)
                            .payload(),
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
