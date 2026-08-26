package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Durable canonical SLO Start projection. */
public final class SloSampleStart {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;
    private static final byte[] SAMPLE_ID_DOMAIN = Bytes.utf8("nereus-delay-slo-sample\0");
    private static final byte[] START_DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-start\0");

    private final byte[] objectiveDigest;
    private final SloObjectiveName objective;
    private final SloPopulation population;
    private final SloPath path;
    private final SloSampleEventIdentity eventIdentity;
    private final byte[] sampleId;
    private final SloTimeEndpoint start;
    private final Long timeoutAtEpochMs;
    private final byte[] startDigest;

    public SloSampleStart(
            final SloObjective objective,
            final SloPath path,
            final SloSampleEventIdentity eventIdentity,
            final SloTimeEndpoint start,
            final Long timeoutAtEpochMs) {
        this(
                Objects.requireNonNull(objective, "objective").objectiveDigest(),
                objective.name(),
                objective.population(),
                path,
                eventIdentity,
                start,
                timeoutAtEpochMs);
        objective.validateStart(this);
    }

    public SloSampleStart(
            final byte[] objectiveDigest,
            final SloObjectiveName objective,
            final SloPopulation population,
            final SloPath path,
            final SloSampleEventIdentity eventIdentity,
            final SloTimeEndpoint start,
            final Long timeoutAtEpochMs) {
        this(
                objectiveDigest,
                objective,
                population,
                path,
                eventIdentity,
                Bytes.sha256(SAMPLE_ID_DOMAIN, objectiveDigest, Bytes.lp32(eventIdentity.canonicalBytes())),
                start,
                timeoutAtEpochMs,
                null);
    }

    private SloSampleStart(
            final byte[] objectiveDigest,
            final SloObjectiveName objective,
            final SloPopulation population,
            final SloPath path,
            final SloSampleEventIdentity eventIdentity,
            final byte[] sampleId,
            final SloTimeEndpoint start,
            final Long timeoutAtEpochMs,
            final byte[] suppliedStartDigest) {
        Bytes.requireLength(objectiveDigest, HASH_LENGTH, "objectiveDigest");
        this.objectiveDigest = Bytes.copy(objectiveDigest);
        this.objective = Objects.requireNonNull(objective, "objective");
        this.population = Objects.requireNonNull(population, "population");
        this.path = Objects.requireNonNull(path, "path");
        this.eventIdentity = Objects.requireNonNull(eventIdentity, "eventIdentity");
        if (eventIdentity.objective() != objective) {
            throw new IllegalArgumentException("SLO event identity objective mismatch");
        }
        validatePath(objective, path);
        this.start = Objects.requireNonNull(start, "start");
        validateDueAdmissionStart(objective, path, eventIdentity, this.start);
        this.sampleId = fixed(sampleId, "sampleId");
        final byte[] expectedSampleId =
                Bytes.sha256(SAMPLE_ID_DOMAIN, this.objectiveDigest, Bytes.lp32(eventIdentity.canonicalBytes()));
        if (!Arrays.equals(this.sampleId, expectedSampleId)) {
            throw new IllegalArgumentException("SLO sample ID mismatch");
        }
        if (timeoutAtEpochMs != null && (timeoutAtEpochMs < 0 || timeoutAtEpochMs < start.earliestEpochMs())) {
            throw new IllegalArgumentException("SLO timeout must be after the start interval");
        }
        this.timeoutAtEpochMs = timeoutAtEpochMs;
        final byte[] expectedStartDigest = Bytes.sha256(START_DIGEST_DOMAIN, fieldsOneToNine());
        if (suppliedStartDigest != null && !Arrays.equals(suppliedStartDigest, expectedStartDigest)) {
            throw new IllegalArgumentException("SLO start digest mismatch");
        }
        this.startDigest = expectedStartDigest;
    }

    public byte[] objectiveDigest() {
        return Bytes.copy(objectiveDigest);
    }

    public SloObjectiveName objective() {
        return objective;
    }

    public SloPopulation population() {
        return population;
    }

    public SloPath path() {
        return path;
    }

    public SloSampleEventIdentity eventIdentity() {
        return eventIdentity;
    }

    public byte[] sampleId() {
        return Bytes.copy(sampleId);
    }

    public SloTimeEndpoint start() {
        return start;
    }

    public Long timeoutAtEpochMs() {
        return timeoutAtEpochMs;
    }

    public byte[] startDigest() {
        return Bytes.copy(startDigest);
    }

    /** Validates this Start against the immutable objective catalog entry. */
    public void validateAgainst(final SloObjective objective) {
        Objects.requireNonNull(objective, "objective");
        if (!Arrays.equals(objectiveDigest, objective.objectiveDigest())
                || this.objective != objective.name()
                || population != objective.population()) {
            throw new IllegalArgumentException("SLO Start does not match its objective digest/catalog entry");
        }
        if (objective.direction() == SloThresholdDirection.AT_MOST) {
            if (timeoutAtEpochMs == null) {
                throw new IllegalArgumentException("AT_MOST SLO Start requires a timeout");
            }
            final long expectedTimeout;
            if (objective.threshold() < 0) {
                throw new IllegalArgumentException("SLO timeout overflows epoch range");
            }
            try {
                expectedTimeout = Math.addExact(start.earliestEpochMs(), objective.threshold());
            } catch (ArithmeticException exception) {
                throw new IllegalArgumentException("SLO timeout overflows epoch range", exception);
            }
            if (timeoutAtEpochMs != expectedTimeout) {
                throw new IllegalArgumentException("SLO timeout does not match objective threshold");
            }
        } else if (timeoutAtEpochMs != null) {
            throw new IllegalArgumentException("AT_LEAST SLO Start cannot carry a timeout");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToNine());
            CanonicalProtobuf.bytes(output, 10, startDigest);
        });
    }

    public static SloSampleStart decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloSampleStart");
        final int[] expected = fields.size() == 9
                ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 10}
                : fields.size() == 10 ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} : new int[0];
        QueryCodecSupport.requireNumbers(fields, expected, "SloSampleStart");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported SloSampleStart version");
        }
        final SloObjectiveName objective = SloObjectiveName.fromWire(QueryCodecSupport.uint(fields.get(2), 3));
        final Long timeout = fields.size() == 10 ? QueryCodecSupport.uint(fields.get(8), 9) : null;
        final SloSampleStart result = new SloSampleStart(
                QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                objective,
                SloPopulation.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                SloPath.fromWire(QueryCodecSupport.uint(fields.get(4), 5)),
                SloSampleEventIdentity.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH),
                SloTimeEndpoint.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                timeout,
                QueryCodecSupport.fixed(fields.get(fields.size() - 1), 10, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloSampleStart");
        return result;
    }

    private byte[] fieldsOneToNine() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, objectiveDigest);
            CanonicalProtobuf.uint32(output, 3, objective.wireValue());
            CanonicalProtobuf.uint32(output, 4, population.wireValue());
            CanonicalProtobuf.uint32(output, 5, path.wireValue());
            CanonicalProtobuf.bytes(output, 6, eventIdentity.canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, sampleId);
            CanonicalProtobuf.bytes(output, 8, start.canonicalBytes());
            if (timeoutAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 9, timeoutAtEpochMs);
            }
        });
    }

    private static void validatePath(final SloObjectiveName objective, final SloPath path) {
        if (objective == SloObjectiveName.DUE_ADMISSION_LAG
                && (path == SloPath.NOT_APPLICABLE || path == SloPath.AUTO_FAST_NATIVE)) {
            throw new IllegalArgumentException("due-admission SLO requires a managed path");
        }
        if (objective == SloObjectiveName.NATIVE_HANDOFF_ACK_LAG && path != SloPath.AUTO_FAST_NATIVE) {
            throw new IllegalArgumentException("native handoff SLO requires AUTO_FAST_NATIVE");
        }
        if (objective != SloObjectiveName.DUE_ADMISSION_LAG
                && objective != SloObjectiveName.NATIVE_HANDOFF_ACK_LAG
                && path != SloPath.NOT_APPLICABLE) {
            throw new IllegalArgumentException("this SLO objective cannot carry a path");
        }
    }

    private static void validateDueAdmissionStart(
            final SloObjectiveName objective,
            final SloPath path,
            final SloSampleEventIdentity eventIdentity,
            final SloTimeEndpoint start) {
        if (objective != SloObjectiveName.DUE_ADMISSION_LAG) {
            return;
        }
        if (eventIdentity.dueAdmissionPath() != path) {
            throw new IllegalArgumentException("due-admission identity path does not match SLO Start path");
        }
        if (start.kind() != SloTimeEndpointKind.SEMANTIC_FIXED_EPOCH
                || start.latestEpochMs() != start.earliestEpochMs()
                || start.earliestEpochMs() != eventIdentity.dueAdmissionPathStartEpochMs()) {
            throw new IllegalArgumentException("due-admission SLO Start does not match identity path start");
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloSampleStart that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
