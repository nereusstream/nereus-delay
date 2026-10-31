package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Complete original Target sample custody material; this does not certify a Source interval's population. */
public record TargetSloObservationExport(
        TargetSloObservationGeneration generation,
        TargetSloObservationSample sample,
        TargetQuotaMutation cut,
        List<TargetSloObservationEventRecord> events) {
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-export\0");

    public TargetSloObservationExport {
        Objects.requireNonNull(generation);
        Objects.requireNonNull(sample);
        Objects.requireNonNull(cut).requireSourceApplied();
        events = List.copyOf(events);
        final var policy = generation.policy();
        if (!policy.scope().equals(sample.scope())
                || !Arrays.equals(policy.lineage(), sample.lineage())
                || generation.generation() != sample.generation()
                || !Arrays.equals(policy.digest(), sample.policyDigest())
                || !generation.enable().appliedAt().equals(sample.enabledAt())
                || !Arrays.equals(generation.enable().input().systemMutationId(), sample.enableId())
                || policy.declarations().stream()
                        .noneMatch(d -> Arrays.equals(
                                d.canonicalBytes(), sample.declaration().canonicalBytes()))
                || generation.stop() != null
                        && sample.first()
                                        .mutation()
                                        .source()
                                        .compareTo(generation.stop().appliedAt().source())
                                >= 0) {
            throw new IllegalArgumentException(
                    "Target export changed original scope/generation/Control/sample binding");
        }
        policy.requireSource(cut.source());
        sample.mutation().requireAtOrBefore(generation.mutation());
        generation.mutation().requireAtOrBefore(cut);
        if (events.size() > policy.limits().maximumOutboxRecords()
                || events.size() > generation.counts().eventRecords()) {
            throw new IllegalArgumentException("Target export event history exceeds its original finite generation");
        }
        var folded = SloObservationOutbox.open(sample.observation().start());
        var latest = sample.first().mutation();
        long eventBytes = 0;
        var version = new TargetSloObservationSample(
                sample.scope(),
                sample.lineage(),
                sample.generation(),
                sample.enableId(),
                sample.enabledAt(),
                sample.policyDigest(),
                sample.declaration(),
                sample.first(),
                folded,
                latest,
                sample.admission(),
                sample.eligibility());
        for (var event : events) {
            final var body = event.body();
            if (!body.scope().equals(sample.scope())
                    || !Arrays.equals(body.lineage(), sample.lineage())
                    || body.generation() != sample.generation()
                    || !Arrays.equals(body.sampleId(), sample.observation().sampleId())
                    || !Arrays.equals(body.bindingDigest(), sample.bindingDigest())
                    || !Arrays.equals(event.beforeDigest(), folded.recordDigest())) {
                throw new IllegalArgumentException("Target export lost an original immutable Source merge edge");
            }
            if (body.writerEpoch() == null) {
                if (policy.requiresWriterEpoch()
                        || !Arrays.equals(
                                event.input().authorIdentity(), policy.service().canonicalBytes())
                        || event.input().signingKeyVersion() != policy.keyVersion()) {
                    throw new IllegalArgumentException("legacy Target export event changed its Policy1 writer");
                }
            } else if (AuthorIdentity.decode(event.input().authorIdentity()).kind() != AuthorIdentity.Kind.SERVICE) {
                throw new IllegalArgumentException("Target export event lost its registered writer author form");
            }
            policy.requireSource(event.appliedAt().source());
            event.appliedAt().requireAfter(latest);
            event.appliedAt().requireAtOrBefore(sample.mutation());
            if (body.evidence() != null) {
                body.evidence().requireObservation(sample, event.incoming());
            }
            event.requireIncomingFor(version);
            folded = folded.mergeFinal(
                    event.incoming(), sample.declaration().objective().direction());
            if (!Arrays.equals(folded.canonicalBytes(), event.after().canonicalBytes())) {
                throw new IllegalArgumentException("Target export changed the original conservative v1 merge");
            }
            latest = event.appliedAt();
            eventBytes = Math.addExact(eventBytes, event.storedBytes());
            version = new TargetSloObservationSample(
                    sample.scope(),
                    sample.lineage(),
                    sample.generation(),
                    sample.enableId(),
                    sample.enabledAt(),
                    sample.policyDigest(),
                    sample.declaration(),
                    sample.first(),
                    folded,
                    latest,
                    sample.admission(),
                    sample.eligibility());
        }
        if (!latest.equals(sample.mutation())
                || !Arrays.equals(folded.canonicalBytes(), sample.observation().canonicalBytes())
                || eventBytes > generation.counts().eventBytes()) {
            throw new IllegalArgumentException("Target export omits the original complete sample history");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, generation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, sample.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, cut.canonicalBytes());
            for (var event : events) {
                CanonicalProtobuf.bytes(out, 5, event.canonicalBytes());
            }
        });
    }
    /** Gap measurements describe discovery time; they never certify the known historical sync's latency. */
    public List<TargetSloFinalEvidence> recoveryGaps() {
        return events.stream()
                .map(event -> event.body().evidence())
                .filter(value -> value != null && value.isRecoveryGap())
                .toList();
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 6, digest())));
    }

    public static TargetSloObservationExport decode(byte[] encoded, int maximumEvents, int maximumCanonicalBytes) {
        if (maximumEvents < 0
                || maximumEvents > Integer.MAX_VALUE - 6
                || maximumCanonicalBytes <= 0
                || maximumCanonicalBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Target export decode requires explicit finite record/byte limits");
        }
        final var f = TargetCompatibilityCodec.read(
                encoded, maximumCanonicalBytes, maximumEvents + 6, true, "Target SLO export");
        if (f.size() < 5) {
            throw new IllegalArgumentException("Target export is missing original binding fields");
        }
        final int[] numbers = new int[f.size()];
        numbers[0] = 1;
        numbers[1] = 2;
        numbers[2] = 3;
        numbers[3] = 4;
        Arrays.fill(numbers, 4, numbers.length - 1, 5);
        numbers[numbers.length - 1] = 6;
        QueryCodecSupport.requireNumbers(f, numbers, "Target SLO export");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1 || f.size() - 5 > maximumEvents) {
            throw new IllegalArgumentException("unknown Target export schema or excessive event count");
        }
        final var events = new java.util.ArrayList<TargetSloObservationEventRecord>();
        for (int i = 4; i < f.size() - 1; i++) {
            events.add(TargetSloObservationEventRecord.decode(QueryCodecSupport.bytes(f.get(i), 5)));
        }
        final var value = new TargetSloObservationExport(
                TargetSloObservationGeneration.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                TargetSloObservationSample.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                events);
        if (!Arrays.equals(QueryCodecSupport.fixed(f.getLast(), 6, 32), value.digest())) {
            throw new IllegalArgumentException("Target export digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO export");
        return value;
    }
}
