package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Original post-sync observation, or a qualified deadline observation; external time/history trust is mandatory. */
public record TargetSloFinalEvidence(
        Kind kind,
        TrustedUtcIntervalEvidence observedAt,
        TargetQuotaMutation inspectedAt,
        SystemMutation admission,
        TargetQuotaMutation admissionAt,
        RecoveryGap gap) {
    public TargetSloFinalEvidence(
            Kind kind,
            TrustedUtcIntervalEvidence observedAt,
            TargetQuotaMutation inspectedAt,
            SystemMutation admission,
            TargetQuotaMutation admissionAt) {
        this(kind, observedAt, inspectedAt, admission, admissionAt, null);
    }

    public enum Kind {
        COMMAND_SYNC(1),
        ADMISSION_SYNC(2),
        TIMEOUT(3),
        RECOVERY_GAP(4);
        private final int wire;

        Kind(int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        static Kind fromWire(int wire) {
            for (var kind : values()) {
                if (kind.wire == wire) {
                    return kind;
                }
            }
            throw new IllegalArgumentException("unknown SLO endpoint kind");
        }
    }

    public static final int MAX_V1_CANONICAL_BYTES = TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES
            + 128;
    public static final int MAX_CANONICAL_BYTES = MAX_V1_CANONICAL_BYTES + RecoveryGap.MAX_CANONICAL_BYTES + 8;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-final-evidence\0");
    private static final byte[] RECOVERY_DOMAIN = Bytes.utf8("nereus-delay-target-slo-recovery-gap-evidence\0");

    /** A digest names original protected recovery confirmation, not an ordinary failed lookup or new trust. */
    public record RecoveryGap(
            byte[] sampleId,
            byte[] startDigest,
            byte[] bindingDigest,
            long generation,
            Reason reason,
            TargetResultRecord knownFirst,
            TargetResultRecord knownPosition,
            byte[] confirmationDigest) {
        public enum Reason {
            SYNC_TIME_UNRECOVERABLE(1, SloFinalOutcome.BAD_UNQUALIFIED_TIME),
            ENDPOINT_EVIDENCE_UNRECOVERABLE(2, SloFinalOutcome.BAD_EVIDENCE_GAP);
            private final int wire;
            private final SloFinalOutcome outcome;

            Reason(int wire, SloFinalOutcome outcome) {
                this.wire = wire;
                this.outcome = outcome;
            }

            public int wireValue() {
                return wire;
            }

            public SloFinalOutcome outcome() {
                return outcome;
            }

            static Reason fromWire(int wire) {
                for (var reason : values()) {
                    if (reason.wire == wire) {
                        return reason;
                    }
                }
                throw new IllegalArgumentException("unknown SLO recovery gap reason");
            }
        }

        public static final int MAX_CANONICAL_BYTES =
                2 + 3 * 34 + 11 + 2 + 2 * (6 + TargetResultRecord.MAX_CANONICAL_BYTES) + 34;

        public RecoveryGap {
            sampleId = TargetCompatibilityCodec.assigned(sampleId, 32, "recovery gap sample ID");
            startDigest = TargetCompatibilityCodec.assigned(startDigest, 32, "recovery gap original Start digest");
            bindingDigest = TargetCompatibilityCodec.assigned(bindingDigest, 32, "recovery gap immutable binding");
            confirmationDigest = TargetCompatibilityCodec.assigned(
                    confirmationDigest, 32, "original protected recovery confirmation");
            Objects.requireNonNull(reason);
            Objects.requireNonNull(knownFirst);
            Objects.requireNonNull(knownPosition);
            knownFirst.mutation().requireSourceApplied();
            knownPosition.mutation().requireSourceApplied();
            if (generation == 0
                    || knownPosition.primaryIdentity().kind() != TargetQuotaIdentity.Kind.SHARD
                    || !knownFirst
                            .primaryIdentity()
                            .shard()
                            .equals(knownPosition.primaryIdentity().shard())
                    || !Arrays.equals(knownFirst.tenantScope(), knownPosition.tenantScope())
                    || !Arrays.equals(knownFirst.recoveryLineage(), knownPosition.recoveryLineage())
                    || !knownFirst.mutation().equals(knownPosition.mutation())
                    || knownFirst.canonicalBytes().length > TargetResultRecord.MAX_CANONICAL_BYTES
                    || knownPosition.canonicalBytes().length > TargetResultRecord.MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("recovery gap lost its original first/POS scope or sync stamp");
            }
            if (knownFirst.kind() == TargetResultRecord.Kind.POSITION_COMMAND_EXPIRED) {
                if (!Arrays.equals(knownFirst.key(), knownPosition.key())
                        || !Arrays.equals(knownFirst.canonicalBytes(), knownPosition.canonicalBytes())) {
                    throw new IllegalArgumentException(
                            "standalone expired Command gap changed its sole original POSITION");
                }
            } else if (knownFirst.kind() == TargetResultRecord.Kind.COMMAND
                    || knownFirst.kind() == TargetResultRecord.Kind.SYSTEM) {
                if (!knownPosition.kind().position()) {
                    throw new IllegalArgumentException("recovery gap requires the original physical POSITION");
                }
                knownPosition.requireFirst(knownFirst);
            } else {
                throw new IllegalArgumentException("recovery gap requires an original Command or Admission first");
            }
        }

        @Override
        public byte[] sampleId() {
            return Bytes.copy(sampleId);
        }

        @Override
        public byte[] startDigest() {
            return Bytes.copy(startDigest);
        }

        @Override
        public byte[] bindingDigest() {
            return Bytes.copy(bindingDigest);
        }

        @Override
        public byte[] confirmationDigest() {
            return Bytes.copy(confirmationDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, sampleId);
                CanonicalProtobuf.bytes(out, 3, startDigest);
                CanonicalProtobuf.bytes(out, 4, bindingDigest);
                CanonicalProtobuf.uint64Bits(out, 5, generation);
                CanonicalProtobuf.uint32(out, 6, reason.wireValue());
                CanonicalProtobuf.bytes(out, 7, knownFirst.canonicalBytes());
                CanonicalProtobuf.bytes(out, 8, knownPosition.canonicalBytes());
                CanonicalProtobuf.bytes(out, 9, confirmationDigest);
            });
        }

        public static RecoveryGap decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 9, false, "Target SLO protected recovery gap");
            QueryCodecSupport.requireNumbers(
                    f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "Target SLO protected recovery gap");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
                throw new IllegalArgumentException("unknown SLO recovery gap schema");
            }
            final var value = new RecoveryGap(
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    QueryCodecSupport.fixed(f.get(3), 4, 32),
                    QueryCodecSupport.uint64Bits(f.get(4), 5),
                    Reason.fromWire(QueryCodecSupport.uint32(f.get(5), 6)),
                    TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                    TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                    QueryCodecSupport.fixed(f.get(8), 9, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO protected recovery gap");
            return value;
        }
    }

    public TargetSloFinalEvidence {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(observedAt);
        Objects.requireNonNull(inspectedAt).requireSourceApplied();
        TargetNativePolicySnapshot.requireBoundedTime(observedAt);
        if ((kind == Kind.RECOVERY_GAP) != (gap != null)) {
            throw new IllegalArgumentException("only explicit recovery-gap schema carries gap confirmation");
        }
        if ((kind == Kind.ADMISSION_SYNC && admission == null)
                || (kind != Kind.ADMISSION_SYNC && kind != Kind.RECOVERY_GAP && admission != null)
                || (admission == null) != (admissionAt == null)) {
            throw new IllegalArgumentException("SLO Admission endpoint requires its exact input and sync stamp");
        }
        if (admission != null) {
            admissionAt.requireSourceApplied();
            admissionAt.requireAtOrBefore(inspectedAt);
            if (admission.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                    || admission.canonicalEnvelope().length > TargetSloObservationSample.MAX_ADMISSION_INPUT_BYTES
                    || !Arrays.equals(Bytes.sha256(admission.canonicalEnvelope()), admissionAt.mutationDigest())) {
                throw new IllegalArgumentException("SLO endpoint changed original Admission Source input");
            }
        }
        if (gap != null) {
            gap.knownFirst().mutation().requireAtOrBefore(inspectedAt);
            gap.knownPosition().mutation().requireAtOrBefore(inspectedAt);
            if ((gap.knownFirst().kind() == TargetResultRecord.Kind.SYSTEM) != (admission != null)) {
                throw new IllegalArgumentException("recovery gap cannot substitute Command and Admission evidence");
            }
            if (!gap.knownPosition().mutation().source().sameSourceIdentity(inspectedAt.source())) {
                throw new IllegalArgumentException("recovery gap changed original physical Source");
            }
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, isRecoveryGap() ? 2 : 1);
            CanonicalProtobuf.uint32(out, 2, kind.wireValue());
            CanonicalProtobuf.bytes(out, 3, observedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, inspectedAt.canonicalBytes());
            if (admission != null) {
                CanonicalProtobuf.bytes(out, 5, admission.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 6, admissionAt.canonicalBytes());
            }
            if (gap != null) {
                CanonicalProtobuf.bytes(out, 7, gap.canonicalBytes());
            }
        });
    }

    public boolean isRecoveryGap() {
        return kind == Kind.RECOVERY_GAP;
    }
    /** TIMEOUT and recovery-gap durations are observation age, never certified original sync latency. */
    public boolean hasCertifiedSyncLatency() {
        return kind == Kind.COMMAND_SYNC || kind == Kind.ADMISSION_SYNC;
    }

    public byte[] digest() {
        return Bytes.sha256(isRecoveryGap() ? RECOVERY_DOMAIN : DOMAIN, canonicalBytes());
    }
    /** Reconstructs exact v1 timing/outcome fields; never trusts supplied durations or a decision-time proxy. */
    public SloSampleFinal observation(TargetSloObservationSample sample, long revision) {
        requireSample(sample);
        final var start = sample.observation().start();
        final var objective = sample.declaration().objective();
        final long lower = Math.max(
                0,
                Math.subtractExact(observedAt.earliestEpochMs(), start.start().latestEpochMs()));
        final long upper = Math.max(
                0, Math.subtractExact(observedAt.latestEpochMs(), start.start().earliestEpochMs()));
        final var outcome = isRecoveryGap()
                ? gap.reason().outcome()
                : kind == Kind.TIMEOUT
                        ? SloFinalOutcome.BAD_TIMEOUT
                        : upper <= objective.threshold()
                                ? SloFinalOutcome.SUCCESS
                                : lower > objective.threshold()
                                        ? SloFinalOutcome.BAD_DEFINITIVE
                                        : SloFinalOutcome.BAD_UNCERTAIN;
        return new SloSampleFinal(
                start.sampleId(),
                start.startDigest(),
                outcome,
                SloThresholdUnit.MILLISECONDS,
                lower,
                upper,
                null,
                new SloTimeEndpoint(
                        SloTimeEndpointKind.TRUSTED_OBSERVATION,
                        observedAt.earliestEpochMs(),
                        observedAt.latestEpochMs(),
                        Bytes.sha256(observedAt.canonicalBytes())),
                digest(),
                revision);
    }

    public void requireObservation(TargetSloObservationSample sample, SloSampleFinal finalObservation) {
        if (!Arrays.equals(
                observation(sample, finalObservation.observationRevision()).canonicalBytes(),
                finalObservation.canonicalBytes())) {
            throw new IllegalArgumentException("SLO Final differs from its original qualified endpoint/time evidence");
        }
    }

    public void requireSample(TargetSloObservationSample sample) {
        Objects.requireNonNull(sample);
        sample.first().mutation().requireAtOrBefore(inspectedAt);
        if (!sample.scope().shard().equals(inspectedAt.source().shardId())) {
            throw new IllegalArgumentException("SLO observed cut changed Shard");
        }
        if (sample.declaration().objective().direction() != SloThresholdDirection.AT_MOST
                || sample.declaration().objective().unit() != SloThresholdUnit.MILLISECONDS) {
            throw new IllegalArgumentException("unsupported Target SLO Final objective units");
        }
        if (kind == Kind.COMMAND_SYNC) {
            requireCommandEndpoint(sample);
        } else if (kind == Kind.ADMISSION_SYNC) {
            requireAdmissionEndpoint(sample);
        } else if (isRecoveryGap()) {
            requireGapSample(sample);
        } else {
            final var deadline = sample.observation().start().timeoutAtEpochMs();
            if (deadline == null || observedAt.earliestEpochMs() < deadline) {
                throw new IllegalArgumentException("SLO timeout lacks qualified original deadline observation");
            }
        }
    }

    private static void requireCommandEndpoint(TargetSloObservationSample sample) {
        if (sample.declaration().objective().name() != SloObjectiveName.COMMAND_APPLIED_LATENCY
                || sample.admission() != null
                || sample.eligibility() != null) {
            throw new IllegalArgumentException("Command sync cannot complete a due eligibility");
        }
    }

    private void requireAdmissionEndpoint(TargetSloObservationSample sample) {
        final var body = TargetPublishAdmissionBody.decode(admission.canonicalBody());
        if (!body.materialized()) {
            throw new IllegalArgumentException("SLO Admission sync requires original materialized business event");
        }
        final var eligibility = sample.eligibility();
        final var path = body.nativePublication() == null ? SloPath.ORDINARY_MANAGED : SloPath.MANAGED_PULSAR_HANDOFF;
        final long actionAt = body.nativePublication() == null
                ? body.publication().deliverAtEpochMs()
                : body.nativePublication().actionAtEpochMs();
        if (!body.materialized()
                || body.attemptNo() != 1
                || sample.declaration().objective().name() != SloObjectiveName.DUE_ADMISSION_LAG
                || sample.declaration().path() != path
                || sample.observation().start().start().earliestEpochMs() != actionAt
                || eligibility != null && !eligibility.locator().equals(body.locator())
                || eligibility == null
                        && (sample.admission() == null
                                || !Arrays.equals(
                                        sample.admission().canonicalEnvelope(), admission.canonicalEnvelope()))) {
            throw new IllegalArgumentException("Admission sync changed original due event/path/generation");
        }
        sample.first().mutation().requireAtOrBefore(admissionAt);
    }

    private void requireGapSample(TargetSloObservationSample sample) {
        if (gap.generation() != sample.generation()
                || !Arrays.equals(gap.sampleId(), sample.observation().sampleId())
                || !Arrays.equals(
                        gap.startDigest(), sample.observation().start().startDigest())
                || !Arrays.equals(gap.bindingDigest(), sample.bindingDigest())) {
            throw new IllegalArgumentException("recovery gap changed original sample/Start/binding/generation");
        }
        for (var known : new TargetResultRecord[] {gap.knownFirst(), gap.knownPosition()}) {
            if (!sample.scope().shard().equals(known.primaryIdentity().shard())
                    || !Arrays.equals(sample.scope().tenantScope(), known.tenantScope())
                    || !Arrays.equals(sample.lineage(), known.recoveryLineage())
                    || !sample.first()
                            .mutation()
                            .source()
                            .sameSourceIdentity(known.mutation().source())) {
                throw new IllegalArgumentException("recovery gap changed original scope/lineage/physical Source");
            }
            known.mutation().requireAtOrBefore(inspectedAt);
        }
        if (admission == null) {
            requireCommandEndpoint(sample);
            if (!Arrays.equals(gap.knownFirst().canonicalBytes(), sample.first().canonicalBytes())) {
                throw new IllegalArgumentException("Command recovery gap changed exact original first");
            }
            if (sample.first().primaryIdentity().kind() == TargetQuotaIdentity.Kind.SHARD
                    && !sample.first()
                            .primaryIdentity()
                            .equals(gap.knownPosition().primaryIdentity())) {
                throw new IllegalArgumentException("Command recovery gap changed its original Root");
            }
        } else {
            requireAdmissionEndpoint(sample);
            final var first = gap.knownFirst();
            if (first.kind() != TargetResultRecord.Kind.SYSTEM
                    || !first.appliedFirst()
                    || !first.mutation().equals(admissionAt)
                    || first.primaryIdentity().kind() != TargetQuotaIdentity.Kind.SHARD
                    || !first.primaryIdentity().equals(gap.knownPosition().primaryIdentity())
                    || !first.accounting().equals(gap.knownPosition().accounting())
                    || !Arrays.equals(
                            first.typedPayload(),
                            SystemMutationResult.from(
                                            admission,
                                            ApplyStatus.APPLIED,
                                            StableCode.OK,
                                            admissionAt.source().canonicalBytes())
                                    .encode())) {
                throw new IllegalArgumentException(
                        "Admission recovery gap lost its original APPLIED/OK first/POS/Root");
            }
        }
    }

    public static TargetSloFinalEvidence decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Target SLO Final evidence");
        if (f.isEmpty()) {
            throw new IllegalArgumentException("missing SLO Final evidence schema");
        }
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (version < 1 || version > 2) {
            throw new IllegalArgumentException("unknown SLO Final evidence schema");
        }
        final boolean admission = version == 1 ? f.size() == 6 : f.size() == 7;
        QueryCodecSupport.requireNumbers(
                f,
                version == 1
                        ? (admission ? new int[] {1, 2, 3, 4, 5, 6} : new int[] {1, 2, 3, 4})
                        : (admission ? new int[] {1, 2, 3, 4, 5, 6, 7} : new int[] {1, 2, 3, 4, 7}),
                "Target SLO Final evidence");
        if (version == 1 && encoded.length > MAX_V1_CANONICAL_BYTES) {
            throw new IllegalArgumentException("SLO Final evidence1 exceeds its original bound");
        }
        final var kind = Kind.fromWire(QueryCodecSupport.uint32(f.get(1), 2));
        if ((version == 2) != (kind == Kind.RECOVERY_GAP)) {
            throw new IllegalArgumentException("SLO recovery gap and evidence schema differ");
        }
        final var value = new TargetSloFinalEvidence(
                kind,
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                admission
                        ? SystemMutation.decodeTargetEnvelope(
                                QueryCodecSupport.bytes(f.get(4), 5), SystemMutationType.TARGET_PUBLISH_ADMISSION)
                        : null,
                admission ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)) : null,
                version == 1 ? null : RecoveryGap.decode(QueryCodecSupport.bytes(f.get(admission ? 6 : 4), 7)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO Final evidence");
        return value;
    }
}
