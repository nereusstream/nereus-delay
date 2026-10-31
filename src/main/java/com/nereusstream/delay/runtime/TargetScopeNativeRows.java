package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloControlScopeHeader;
import com.nereusstream.delay.protocol.TargetSloCustodyRecord;
import com.nereusstream.delay.protocol.TargetSloObservationControlBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventBody;
import com.nereusstream.delay.protocol.TargetSloObservationEventRecord;
import com.nereusstream.delay.protocol.TargetSloObservationGeneration;
import com.nereusstream.delay.protocol.TargetSloObservationSample;
import com.nereusstream.delay.protocol.TargetSloObservationState;
import com.nereusstream.delay.protocol.TargetSloWriterControlBody;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable original payer joins shared by Source settlement and cold Native reconstruction. */
public final class TargetScopeNativeRows {
    private TargetScopeNativeRows() {}

    /** A missing modern writer still requires positive original occurrence authority before shared payment. */
    public record SourceCandidate(TargetSloControlScopeHeader.Reference reference, boolean occurrenceRequired) {}

    public static boolean nativeType(int type) {
        return type >= TargetSloObservationState.VALUE_TYPE && type <= TargetSloWriterEpoch.VALUE_TYPE;
    }

    /** Null means an actual old unscoped origin, never a missing modern dependency. */
    public static TargetSloControlScopeHeader.Reference origin(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family,
            byte[] key,
            int type,
            byte[] typedPayload,
            TargetQuotaScope scope,
            byte[] lineage) {
        if (!nativeType(type)) {
            return null;
        }
        if (family != ColumnFamily.META) {
            throw new IllegalStateException("Scope Native row is outside META");
        }
        if (!Arrays.equals(typedPayload, payload(view, overlay, family, key, type))) {
            throw new IllegalStateException("Scope payer row differs from the selected Native view");
        }
        return switch (type) {
            case TargetSloObservationState.VALUE_TYPE -> {
                final var state = TargetSloObservationState.decodeForStore(key, typedPayload, scope, lineage);
                final var generation = generation(view, overlay, scope, lineage, state.generation());
                state.requireGeneration(generation);
                yield generation.enable().body().admissionScope();
            }
            case TargetSloObservationGeneration.VALUE_TYPE ->
                TargetSloObservationGeneration.decodeForStore(key, typedPayload, scope, lineage)
                        .enable()
                        .body()
                        .admissionScope();
            case TargetSloObservationSample.VALUE_TYPE -> {
                final var sample = TargetSloObservationSample.decodeForStore(key, typedPayload, scope, lineage);
                final var generation = generation(view, overlay, scope, lineage, sample.generation());
                if (!Arrays.equals(
                                sample.enableId(), generation.enable().input().systemMutationId())
                        || !sample.enabledAt().equals(generation.enable().appliedAt())
                        || !Arrays.equals(
                                sample.policyDigest(), generation.policy().digest())) {
                    throw new IllegalStateException("Scope sample changed its original ENABLE generation");
                }
                yield generation.enable().body().admissionScope();
            }
            case TargetSloObservationEventRecord.VALUE_TYPE ->
                sourceScope(
                        view,
                        overlay,
                        scope,
                        lineage,
                        TargetSloObservationEventRecord.decodeForStore(key, typedPayload, scope, lineage)
                                .input());
            case TargetSloCustodyRecord.VALUE_TYPE ->
                sourceScope(
                        view,
                        overlay,
                        scope,
                        lineage,
                        TargetSloCustodyRecord.decodeForStore(key, typedPayload, scope, lineage)
                                .input());
            case TargetSloWriterEpoch.VALUE_TYPE ->
                TargetSloWriterEpoch.decodeForStore(key, typedPayload, scope, lineage)
                        .register()
                        .body()
                        .admissionScope();
            default -> throw new IllegalStateException("unknown Scope Native row type");
        };
    }

    /** Exact Control admission or exact applied writer REGISTER; aliases cannot select a payer. */
    public static TargetSloControlScopeHeader.Reference sourceScope(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            SystemMutation input) {
        final var candidate = sourceCandidate(view, overlay, scope, lineage, input);
        if (candidate.occurrenceRequired() && candidate.reference() == null) {
            throw unavailable("modern SLO Source lacks its original writer REGISTER payment proof");
        }
        return candidate.reference();
    }

    /** Only an occurrence authority may resolve a modern absent writer as positively unbound/shared. */
    public static SourceCandidate sourceCandidate(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            SystemMutation input) {
        Objects.requireNonNull(input);
        if (!scope.shard().equals(input.shardId())) {
            throw new IllegalArgumentException("Scope Source payer crosses Shard");
        }
        if (TargetSloObservationControlBody.isControl(input)) {
            final var body = TargetSloObservationControlBody.decode(input.canonicalBody());
            requireScope(scope, lineage, body.request().scope(), body.request().lineage());
            return new SourceCandidate(body.admissionScope(), body.admissionScope() != null);
        }
        if (TargetSloWriterControlBody.isControl(input)) {
            final var body = TargetSloWriterControlBody.decode(input.canonicalBody());
            requireScope(scope, lineage, body.request().scope(), body.request().lineage());
            return new SourceCandidate(body.admissionScope(), body.admissionScope() != null);
        }
        if (input.type() != SystemMutationType.TARGET_SLO_OBSERVATION) {
            return new SourceCandidate(null, false);
        }
        final var body = TargetSloObservationEventBody.decode(input.canonicalBody());
        requireScope(scope, lineage, body.scope(), body.lineage());
        if (body.writerEpoch() == null) {
            return new SourceCandidate(null, false);
        }
        final var reference = body.writerEpoch();
        final var key = reference.key(scope);
        final var raw = view.projected(ColumnFamily.META, key, overlay);
        if (raw == null) {
            return new SourceCandidate(null, true);
        }
        final var writer = TargetSloWriterEpoch.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetSloWriterEpoch.VALUE_TYPE).payload(),
                scope,
                lineage);
        if (!reference.equals(writer.reference())) {
            throw new IllegalStateException("SLO Source payer changed its exact applied writer REGISTER");
        }
        final var payer = writer.register().body().admissionScope();
        return new SourceCandidate(payer, payer != null);
    }

    public static TargetSloObservationGeneration generation(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            long generation) {
        final var key = TargetSloObservationGeneration.key(scope, generation);
        return TargetSloObservationGeneration.decodeForStore(
                key,
                payload(view, overlay, ColumnFamily.META, key, TargetSloObservationGeneration.VALUE_TYPE),
                scope,
                lineage);
    }

    private static void requireScope(
            TargetQuotaScope expected, byte[] lineage, TargetQuotaScope actual, byte[] actualLineage) {
        if (!expected.equals(actual) || !Arrays.equals(lineage, actualLineage)) {
            throw new IllegalStateException("Scope Source payer changed original scope/lineage");
        }
    }

    private static byte[] payload(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            ColumnFamily family,
            byte[] key,
            int type) {
        final var raw = view.projected(family, key, overlay);
        if (raw == null) {
            throw unavailable("Scope Native payer lacks an original retained row");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static TargetScopeNativeSettlement.Unavailable unavailable(String message) {
        return new TargetScopeNativeSettlement.Unavailable(message);
    }
}
