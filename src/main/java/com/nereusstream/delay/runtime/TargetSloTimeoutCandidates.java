package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.SloObjectiveName;
import com.nereusstream.delay.protocol.SloPath;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Actual current-cut lifecycle classification, not a historical absence proof or a Final/time authority. */
public final class TargetSloTimeoutCandidates {
    private TargetSloTimeoutCandidates() {}

    public enum Result {
        CURRENT_FINAL,
        KNOWN_COMMAND_SYNC,
        KNOWN_ADMISSION_SYNC,
        NO_KNOWN_ADMISSION_AT_CUT,
        UNKNOWN_REQUIRED_HISTORY
    }

    /**
     * Inspects only the exact current Native view. A caller must separately qualify deadline/time and retained
     * Source history before authorizing a timeout. A known Admission is a positive hint, not complete Final evidence.
     */
    public static Result inspect(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaMutation cut,
            TargetSloObservationSample sample) {
        Objects.requireNonNull(reader);
        Objects.requireNonNull(scope);
        Objects.requireNonNull(cut);
        Objects.requireNonNull(sample);
        Bytes.requireLength(lineage, 16, "actual SLO timeout lineage");
        final var aggregate = reader.aggregate();
        final var root = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        if (!cut.equals(aggregate.mutation())
                || scope.target() != null
                || !scope.shard().equals(reader.shardId())
                || !scope.equals(sample.scope())
                || !Arrays.equals(lineage, sample.lineage())
                || sample.first().kind().position()
                        && (sample.first().kind() != TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED
                                || !root.equals(sample.first().primaryIdentity()))
                || !sameScope(sample.first(), scope, lineage)) {
            throw new IllegalArgumentException("timeout inspection requires the exact current cut/Root/sample scope");
        }
        final var source = Objects.requireNonNull(reader.source(), "actual Source predecessor");
        if (!Arrays.equals(source.canonicalBytes(), cut.source().canonicalBytes())
                || position(reader, scope, lineage, root, source, cut) == null) {
            throw new IllegalStateException("timeout inspection lost its actual current Source POSITION");
        }
        final var sampleRaw = reader.get(ColumnFamily.META, sample.key());
        if (sampleRaw == null
                || !Arrays.equals(
                        TargetValueEnvelope.decode(sampleRaw, TargetSloObservationSample.VALUE_TYPE)
                                .payload(),
                        sample.canonicalBytes())) {
            throw new IllegalStateException("timeout inspection cannot substitute an older sample projection");
        }
        sample.mutation().requireAtOrBefore(cut);
        if (sample.observation().finalObservation() != null) {
            return checked(reader, Result.CURRENT_FINAL);
        }
        TargetSloObservationSamples.requireOrigin(reader, List.of(), scope, lineage, root, sample, cut);
        if (sample.declaration().objective().name() == SloObjectiveName.COMMAND_APPLIED_LATENCY) {
            // Start/first/POS already shared the Command sync, including a stable business rejection.
            return checked(reader, Result.KNOWN_COMMAND_SYNC);
        }
        if (sample.declaration().objective().name() != SloObjectiveName.DUE_ADMISSION_LAG) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        if (sample.admission() != null) {
            // The retained schema2 Start already carries its exact actual first Admission sync.
            return checked(reader, Result.KNOWN_ADMISSION_SYNC);
        }
        final var birth = sample.eligibility();
        if (birth == null
                || sample.declaration().path() != SloPath.ORDINARY_MANAGED
                || birth.nativePolicy() != NativeDeliveryPolicy.FORBID) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }

        final var messageKey = TargetKeyCodec.message(birth.locator().messageId());
        final var messageRaw = reader.get(ColumnFamily.ID, messageKey);
        if (messageRaw == null) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        final var message = TargetMessageRecord.decodeForStore(
                messageKey,
                TargetValueEnvelope.decode(messageRaw, TargetMessageRecord.VALUE_TYPE)
                        .payload(),
                scope.shard());
        if (!message.locator().equals(birth.locator())
                || message.nativeDeliveryPolicy() != birth.nativePolicy()
                || !Arrays.equals(
                        message.scheduleSource().canonicalBytes(),
                        birth.source().canonicalBytes())
                || message.deliverAtEpochMs() != birth.deliverAt()
                || message.expireAtEpochMs() != birth.expireAt()
                || Long.compareUnsigned(message.stateVersion(), birth.stateVersion()) < 0
                || message.stateVersion() == birth.stateVersion()
                        && !Arrays.equals(message.digest(), birth.messageDigest())) {
            // A later Message generation is not the old lifecycle; its zero counter cannot clear the old duty.
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        final var ownerKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_PAYLOAD_OWNER_TAG, TargetKeyCodec.KEY_FORMAT},
                birth.locator().messageId().bytes());
        final var ownerRaw = reader.get(ColumnFamily.META, ownerKey);
        final var bindingKey = TargetKeyCodec.scheduleBinding(birth.locator().scheduleBindingDigest());
        final var bindingRaw = reader.get(ColumnFamily.ID, bindingKey);
        if (ownerRaw == null || bindingRaw == null) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        final var owner = TargetQuotaPayloadOwner.decodeForStore(
                ownerKey,
                TargetValueEnvelope.decode(ownerRaw, TargetQuotaPayloadOwner.VALUE_TYPE)
                        .payload(),
                scope.shard(),
                scope.tenantScope());
        if (!Arrays.equals(lineage, owner.recoveryLineage())
                || owner.phase() == TargetQuotaPayloadOwner.Phase.RESERVED
                || owner.phase() == TargetQuotaPayloadOwner.Phase.RELEASED) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        owner.mutation().requireAtOrBefore(cut);
        owner.requireMessagePayload(message);
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                TargetValueEnvelope.decode(bindingRaw, TargetScheduleBinding.VALUE_TYPE)
                        .payload(),
                scope.shard());
        owner.requireInitialBinding(binding);
        binding.requireLocator(message.locator());
        binding.requireMessageSource(message.scheduleSource());
        final var bindingFirst = creator(reader, scope, lineage, root, binding.bindingSource(), cut);
        if (bindingFirst == null
                || bindingFirst.kind() != TargetResultRecord.Kind.COMMAND
                || !bindingFirst.appliedFirst()) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }

        final var runtime = message.runtime();
        if (runtime.terminal()) {
            final var terminalKey = TargetTerminalGenerationRecord.key(message.locator());
            final var terminalRaw = reader.get(ColumnFamily.TERMINAL, terminalKey);
            if (terminalRaw == null) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
            final var terminal = TargetTerminalGenerationRecord.decode(
                    TargetValueEnvelope.decode(terminalRaw, TargetTerminalGenerationRecord.VALUE_TYPE)
                            .payload());
            if (!Arrays.equals(terminalKey, terminal.key())
                    || !terminal.locator().equals(message.locator())
                    || !Arrays.equals(lineage, terminal.recoveryLineage())
                    || terminal.stateVersion() != message.stateVersion()
                    || !Arrays.equals(terminal.runtime().canonicalBytes(), runtime.canonicalBytes())) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
            terminal.requireOwner(owner);
            terminal.mutation().requireAtOrBefore(cut);
            final var terminalFirst =
                    creator(reader, scope, lineage, root, terminal.mutation().source(), cut);
            if (terminalFirst == null
                    || !terminalFirst.mutation().equals(terminal.mutation())
                    || !terminalFirst.appliedFirst()) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
            if (runtime.admissionsUsed() > 0) {
                return checked(reader, Result.KNOWN_ADMISSION_SYNC);
            }
            if (runtime.aggregateState() == GenerationAggregateState.PUBLISHED
                    || runtime.aggregateState() == GenerationAggregateState.HANDED_OFF) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
            return checked(reader, Result.NO_KNOWN_ADMISSION_AT_CUT);
        }
        if (runtime.admissionsUsed() > 0) {
            return checked(reader, Result.KNOWN_ADMISSION_SYNC);
        }
        if (owner.phase() != TargetQuotaPayloadOwner.Phase.ACTIVE) {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        if (runtime.currentWorkKind() == CurrentSendWorkKind.TIMELINE) {
            final var work = runtime.timeline();
            work.requireScheduleProjection(message.scheduleSource());
            final var workRaw = reader.get(ColumnFamily.TIMELINE, work.ordinaryKey());
            if (work.candidateAttemptNo() != 1
                    || work.workKind() != TimelineWorkKind.INITIAL_SCHEDULE
                    || workRaw == null
                    || !Arrays.equals(
                            TargetValueEnvelope.decode(workRaw, TargetTimelineWorkRef.VALUE_TYPE)
                                    .payload(),
                            work.canonicalBytes())) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
        } else if (runtime.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final var claimKey = TargetClaimRecord.key(runtime.claimId());
            final var claimRaw = reader.get(ColumnFamily.INFLIGHT, claimKey);
            if (claimRaw == null) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
            final var claim =
                    TargetClaimRecord.decode(TargetValueEnvelope.decode(claimRaw, TargetClaimRecord.VALUE_TYPE)
                            .payload());
            claim.requireStored(claimKey);
            claim.requireCurrent(message);
            claim.creation().requireAtOrBefore(cut);
            claim.work().requireScheduleProjection(message.scheduleSource());
            if (claim.work().candidateAttemptNo() != 1
                    || claim.work().workKind() != TimelineWorkKind.INITIAL_SCHEDULE) {
                return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
            }
        } else {
            return checked(reader, Result.UNKNOWN_REQUIRED_HISTORY);
        }
        return checked(reader, Result.NO_KNOWN_ADMISSION_AT_CUT);
    }

    private static Result checked(TargetStoreBackend.Reader reader, Result result) {
        reader.requireWithinElapsedBudget();
        return result;
    }

    private static TargetResultRecord creator(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            SourcePosition source,
            TargetQuotaMutation cut) {
        final var position = position(reader, scope, lineage, root, source, cut);
        if (position == null || !position.kind().referencesFirst()) {
            return null;
        }
        final int tag = position.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                ? TargetKeyCodec.RESULT_SYSTEM_TAG
                : TargetKeyCodec.RESULT_COMMAND_TAG;
        final var first =
                result(reader, Bytes.concat(new byte[] {(byte) tag, TargetKeyCodec.KEY_FORMAT}, position.logicalId()));
        if (first == null
                || first.kind().position()
                || !sameScope(first, scope, lineage)
                || first.kind() == TargetResultRecord.Kind.SYSTEM && !root.equals(first.primaryIdentity())) {
            return null;
        }
        first.mutation().requireSourceApplied();
        first.mutation().requireAtOrBefore(cut);
        position.requireFirst(first);
        if (!first.mutation().equals(position.mutation())) {
            return null;
        }
        return first;
    }

    private static TargetResultRecord position(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaIdentity root,
            SourcePosition source,
            TargetQuotaMutation cut) {
        final var position = result(reader, TargetSloObservationStore.positionKey(source));
        if (position == null
                || !position.kind().position()
                || !owned(position, scope, lineage, root)
                || !Arrays.equals(position.mutation().source().canonicalBytes(), source.canonicalBytes())) {
            return null;
        }
        position.mutation().requireSourceApplied();
        position.mutation().requireAtOrBefore(cut);
        return position;
    }

    private static boolean owned(
            TargetResultRecord record, TargetQuotaScope scope, byte[] lineage, TargetQuotaIdentity root) {
        return root.equals(record.primaryIdentity()) && sameScope(record, scope, lineage);
    }

    private static boolean sameScope(TargetResultRecord record, TargetQuotaScope scope, byte[] lineage) {
        return scope.shard().equals(record.primaryIdentity().shard())
                && Arrays.equals(scope.tenantScope(), record.tenantScope())
                && Arrays.equals(lineage, record.recoveryLineage());
    }

    private static TargetResultRecord result(TargetStoreBackend.Reader reader, byte[] key) {
        final var raw = reader.get(ColumnFamily.DEDUPE, key);
        if (raw == null) {
            return null;
        }
        final var payload =
                TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload();
        final var record = TargetResultRecord.decode(payload);
        record.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        return record;
    }
}
