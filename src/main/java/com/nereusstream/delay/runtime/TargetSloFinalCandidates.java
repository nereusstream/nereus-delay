package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.store.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

/** Bounded reads of existing Starts after an original business sync; this grants no time or writer authority. */
public final class TargetSloFinalCandidates {
    private TargetSloFinalCandidates() {}

    /** The original generation is retained even after STOP or a later generation's ENABLE. */
    public record Candidate(TargetSloObservationGeneration generation, TargetSloObservationSample sample,
            TargetResultRecord syncedFirst) {
        public Candidate {
            Objects.requireNonNull(generation); Objects.requireNonNull(sample); Objects.requireNonNull(syncedFirst);
        }
    }

    /** Stable Command rejections have a completed sync too; the due Start in that batch is not completed here. */
    public static List<Candidate> forCommand(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, TargetResultRecord originalFirst) {
        requireFirst(reader, scope, lineage, originalFirst);
        if (originalFirst.kind() != TargetResultRecord.Kind.COMMAND
                && originalFirst.kind() != TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
            throw new IllegalArgumentException("Command Final candidates require an original logical first or physical expiry");
        }
        final var result = new ArrayList<Candidate>();
        for (var sample : references(reader, scope, lineage, originalFirst.key(), originalFirst.primaryIdentity())) {
            if (sample.declaration().objective().name() != SloObjectiveName.COMMAND_APPLIED_LATENCY) continue;
            if (!Arrays.equals(sample.first().canonicalBytes(), originalFirst.canonicalBytes())
                    || sample.declaration().path() != SloPath.NOT_APPLICABLE
                    || sample.eligibility() != null || sample.admission() != null) {
                throw new IllegalStateException("Command completion changed its original Start or event kind");
            }
            addOpen(reader, scope, lineage, result, sample, originalFirst);
        }
        return List.copyOf(result);
    }

    /**
     * Finds only already-bound first managed Admission Starts. Ordinary schema3 uses the exact Message birth
     * POSITION; retained schema2 uses the Admission first itself. Neither path creates a missing denominator.
     * The ordinary lookup requires the retained same-generation Message; historical absence needs protected
     * original material rather than a scan or a current-generation rebind.
     */
    public static List<Candidate> forAdmission(TargetStoreBackend.Reader reader, TargetQuotaScope scope,
            byte[] lineage, SystemMutation originalAdmission, TargetResultRecord originalFirst) {
        requireFirst(reader, scope, lineage, originalFirst);
        final var input = Objects.requireNonNull(originalAdmission, "original Admission");
        if (input.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || originalFirst.kind() != TargetResultRecord.Kind.SYSTEM || !input.shardId().equals(scope.shard())
                || !Arrays.equals(originalFirst.mutation().mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))) {
            throw new IllegalArgumentException("Admission Final candidates require the exact original Source input");
        }
        final var outcome = SystemMutationResult.decode(originalFirst.typedPayload());
        if (!Arrays.equals(originalFirst.typedPayload(), SystemMutationResult.from(input, outcome.applyStatus(),
                outcome.stableCode(), originalFirst.mutation().source().canonicalBytes()).encode())) {
            throw new IllegalStateException("Admission completion changed its original first result");
        }
        if (outcome.applyStatus() != ApplyStatus.APPLIED || outcome.stableCode() != StableCode.OK) return List.of();
        final var body = TargetPublishAdmissionBody.decode(input.canonicalBody());
        if (body.attemptNo() != 1 || !body.materialized() || body.claimProof().work().candidateAttemptNo() != 1) {
            return List.of();
        }
        final var path = body.nativePublication() == null ? SloPath.ORDINARY_MANAGED : SloPath.MANAGED_PULSAR_HANDOFF;
        final var selected = new LinkedHashMap<String, TargetSloObservationSample>();
        for (var sample : references(reader, scope, lineage, originalFirst.key(), originalFirst.primaryIdentity())) {
            if (sample.declaration().objective().name() != SloObjectiveName.DUE_ADMISSION_LAG) continue;
            if (sample.admission() == null || !Arrays.equals(sample.admission().canonicalEnvelope(), input.canonicalEnvelope())
                    || !Arrays.equals(sample.first().canonicalBytes(), originalFirst.canonicalBytes())
                    || sample.declaration().path() != path) {
                throw new IllegalStateException("retained Admission Start changed its original completion input/path");
            }
            selected.put(Bytes.hex(sample.key()), sample);
        }

        final var messageKey = TargetKeyCodec.message(body.locator().messageId());
        final var messageRaw = reader.get(ColumnFamily.ID, messageKey);
        if (messageRaw == null) {
            throw new IllegalStateException("Admission completion needs the retained exact Message birth Source");
        }
        final var message = TargetMessageRecord.decodeForStore(messageKey,
                TargetValueEnvelope.decode(messageRaw, TargetMessageRecord.VALUE_TYPE).payload(), scope.shard());
        if (!message.locator().equals(body.locator())) {
            throw new IllegalStateException("Admission completion cannot use another Message generation");
        }
        final var birthPosition = positionKey(message.scheduleSource());
        for (var sample : references(reader, scope, lineage, birthPosition, originalFirst.primaryIdentity())) {
            if (sample.declaration().objective().name() != SloObjectiveName.DUE_ADMISSION_LAG) continue;
            final var eligibility = sample.eligibility();
            if (eligibility == null || !eligibility.locator().equals(body.locator())
                    || !Arrays.equals(eligibility.source().canonicalBytes(), message.scheduleSource().canonicalBytes())
                    || eligibility.deliverAt() != message.deliverAtEpochMs()
                    || eligibility.expireAt() != message.expireAtEpochMs()
                    || message.nativeDeliveryPolicy() != NativeDeliveryPolicy.FORBID
                    || body.nativePublication() != null || sample.declaration().path() != path
                    || eligibility.deliverAt() != body.publication().deliverAtEpochMs()) {
                throw new IllegalStateException("Admission completion lacks its original qualified eligibility/path");
            }
            originalFirst.mutation().requireAfter(sample.first().mutation());
            final var prior = selected.putIfAbsent(Bytes.hex(sample.key()), sample);
            if (prior != null && !Arrays.equals(prior.canonicalBytes(), sample.canonicalBytes())) {
                throw new IllegalStateException("Admission completion has inconsistent original Start references");
            }
        }
        final var result = new ArrayList<Candidate>();
        for (var sample : selected.values()) addOpen(reader, scope, lineage, result, sample, originalFirst);
        return List.copyOf(result);
    }

    private static void addOpen(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            List<Candidate> result, TargetSloObservationSample sample, TargetResultRecord first) {
        if (sample.observation().finalObservation() != null) return;
        final var generation = Objects.requireNonNull(TargetSloObservationVerifier.readGeneration(
                reader, scope, lineage, sample.generation()), "original Final candidate generation");
        result.add(new Candidate(generation, sample, first));
    }

    private static List<TargetSloObservationSample> references(TargetStoreBackend.Reader reader,
            TargetQuotaScope scope, byte[] lineage, byte[] originalKey, TargetQuotaIdentity root) {
        final var prefix = TargetSloObservationSample.referencePrefix(originalKey);
        final var upper = prefixUpper(prefix);
        byte[] lower = prefix;
        final var result = new ArrayList<TargetSloObservationSample>();
        for (int count = 0; ; count++) {
            reader.requireWithinElapsedBudget();
            final var row = reader.first(ColumnFamily.META, lower, upper, List.of());
            if (row == null) return List.copyOf(result);
            if (count == TargetSloObservationPolicy.MAX_DECLARATIONS) {
                throw new IllegalStateException("original SLO Source reference exceeds its declared bounded producer set");
            }
            final var sample = TargetSloObservationSample.decodeForStore(row.key(),
                    TargetValueEnvelope.decode(row.value(), TargetSloObservationSample.VALUE_TYPE).payload(), scope, lineage);
            if (!sample.references(originalKey)) throw new IllegalStateException("SLO reference changed original Source key");
            TargetSloObservationSamples.requireOrigin(reader, List.of(), scope, lineage, root, sample,
                    reader.aggregate().mutation());
            result.add(sample);
            lower = Bytes.concat(row.key(), new byte[]{0});
        }
    }

    private static void requireFirst(TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage,
            TargetResultRecord first) {
        Objects.requireNonNull(reader); Objects.requireNonNull(scope); Objects.requireNonNull(first);
        Bytes.requireLength(lineage, 16, "original Final candidate lineage");
        final var aggregate = reader.aggregate();
        final var root = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD, scope.shard(),
                aggregate.accountingIncarnation(), null, null);
        if (scope.target() != null || !scope.shard().equals(reader.shardId()) || !root.equals(first.primaryIdentity())
                || !Arrays.equals(scope.tenantScope(), first.tenantScope()) || !Arrays.equals(lineage, first.recoveryLineage())
                || first.kind() != TargetResultRecord.Kind.COMMAND && first.kind() != TargetResultRecord.Kind.SYSTEM
                        && first.kind() != TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
            throw new IllegalArgumentException("Final completion first changed original Root/scope/lineage");
        }
        first.mutation().requireSourceApplied(); first.mutation().requireAtOrBefore(aggregate.mutation());
        final var actual = result(reader, first.key());
        if (!Arrays.equals(actual.canonicalBytes(), first.canonicalBytes())) {
            throw new IllegalStateException("Final completion is not the actual retained first");
        }
        final var position = result(reader, positionKey(first.mutation().source()));
        if(first.kind()==TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED){
            if(!Arrays.equals(position.canonicalBytes(),first.canonicalBytes()))throw new IllegalStateException("Final completion changed its standalone physical expiry");
        }else position.requireFirst(first);
        if (!position.mutation().equals(first.mutation())) {
            throw new IllegalStateException("Final completion lost its exact first Source POSITION");
        }
        reader.requireWithinElapsedBudget();
    }

    private static TargetResultRecord result(TargetStoreBackend.Reader reader, byte[] key) {
        final var raw = Objects.requireNonNull(reader.get(ColumnFamily.DEDUPE, key), "original synced first/POS");
        final var payload = TargetValueEnvelope.decode(raw, TargetResultRecord.VALUE_TYPE).payload();
        final var value = TargetResultRecord.decode(payload);
        value.requireStored(key, TargetResultRecord.VALUE_TYPE, payload);
        return value;
    }

    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(new byte[]{TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private static byte[] prefixUpper(byte[] prefix) {
        final var upper = Bytes.copy(prefix);
        for (int index = upper.length - 1; index >= 0; index--) if (++upper[index] != 0) return upper;
        throw new IllegalArgumentException("SLO reference prefix has no finite upper bound");
    }
}
