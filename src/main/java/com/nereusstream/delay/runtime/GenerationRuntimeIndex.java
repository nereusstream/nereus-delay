package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Exact registry-shaped current-generation runtime projection.
 *
 * <p>This object is deliberately independent from the public aggregate status kept by
 * older embedded records. The projection separates one current send work item from
 * the bounded set of admitted attempt obligations and gives every persisted form a
 * canonical digest.</p>
 */
public final class GenerationRuntimeIndex {
    public static final int VERSION = 1;
    public static final int HASH_LENGTH = 32;

    private final GenerationAggregateState aggregateState;
    private final CurrentSendWorkKind currentWorkKind;
    private final TimelineWorkRef timeline;
    private final byte[] claimId;
    private final byte[] publishAttemptId;
    private final List<AttemptObligationRef> attemptObligations;
    private final int admissionsUsed;
    private final int uncertainRetryAdmissionsUsed;
    private final boolean possibleDestinationDuplicate;
    private final long runtimeRevision;
    private final byte[] runtimeDigest;
    private final boolean legacyCompatibility;

    public GenerationRuntimeIndex(
            final GenerationAggregateState aggregateState,
            final CurrentSendWorkKind currentWorkKind,
            final TimelineWorkRef timeline,
            final byte[] claimId,
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean possibleDestinationDuplicate,
            final long runtimeRevision) {
        this(
                aggregateState,
                currentWorkKind,
                timeline,
                claimId,
                publishAttemptId,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                possibleDestinationDuplicate,
                runtimeRevision,
                false);
    }

    private GenerationRuntimeIndex(
            final GenerationAggregateState aggregateState,
            final CurrentSendWorkKind currentWorkKind,
            final TimelineWorkRef timeline,
            final byte[] claimId,
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean possibleDestinationDuplicate,
            final long runtimeRevision,
            final boolean legacyCompatibility) {
        this.aggregateState = Objects.requireNonNull(aggregateState, "aggregateState");
        this.currentWorkKind = Objects.requireNonNull(currentWorkKind, "currentWorkKind");
        this.timeline = timeline;
        this.claimId = optionalFixed(claimId, "claimId");
        this.publishAttemptId = optionalFixed(publishAttemptId, "publishAttemptId");
        this.attemptObligations = copyAndValidateObligations(obligations);
        if (admissionsUsed < 0
                || uncertainRetryAdmissionsUsed < 0
                || uncertainRetryAdmissionsUsed > admissionsUsed
                || runtimeRevision <= 0) {
            throw new IllegalArgumentException("invalid generation runtime counters/revision");
        }
        this.admissionsUsed = admissionsUsed;
        this.uncertainRetryAdmissionsUsed = uncertainRetryAdmissionsUsed;
        this.possibleDestinationDuplicate = possibleDestinationDuplicate;
        this.runtimeRevision = runtimeRevision;
        this.legacyCompatibility = legacyCompatibility;
        validateCurrentWork();
        this.runtimeDigest = computeRuntimeDigest();
    }

    private GenerationRuntimeIndex(
            final GenerationAggregateState aggregateState,
            final CurrentSendWorkKind currentWorkKind,
            final TimelineWorkRef timeline,
            final byte[] claimId,
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean duplicate,
            final long runtimeRevision,
            final byte[] runtimeDigest) {
        this(
                aggregateState,
                currentWorkKind,
                timeline,
                claimId,
                publishAttemptId,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                duplicate,
                runtimeRevision,
                runtimeDigest,
                false);
    }

    private GenerationRuntimeIndex(
            final GenerationAggregateState aggregateState,
            final CurrentSendWorkKind currentWorkKind,
            final TimelineWorkRef timeline,
            final byte[] claimId,
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean duplicate,
            final long runtimeRevision,
            final byte[] runtimeDigest,
            final boolean legacyCompatibility) {
        this(
                aggregateState,
                currentWorkKind,
                timeline,
                claimId,
                publishAttemptId,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                duplicate,
                runtimeRevision,
                legacyCompatibility);
        Bytes.requireLength(runtimeDigest, HASH_LENGTH, "runtimeDigest");
        if (!Bytes.constantTimeEquals(this.runtimeDigest, runtimeDigest)) {
            throw new IllegalArgumentException("generation runtime digest mismatch");
        }
    }

    /**
     * Compatibility-only scalar MessageRecord placeholder. It is never a
     * valid typed runtime value and is replaced before a new MessageRecord
     * is persisted; canonical decode intentionally does not enable this flag.
     */
    static GenerationRuntimeIndex legacyNone(
            final GenerationAggregateState aggregateState, final long runtimeRevision) {
        return new GenerationRuntimeIndex(
                aggregateState,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                List.of(),
                0,
                0,
                false,
                runtimeRevision,
                true);
    }

    public static GenerationRuntimeIndex timeline(
            final GenerationAggregateState aggregateState, final TimelineWorkRef timeline, final long runtimeRevision) {
        return timeline(aggregateState, timeline, List.of(), 0, 0, false, runtimeRevision);
    }

    /**
     * Creates a timeline projection while retaining already admitted attempt
     * history. A definitive retry closes its prior ledger, but it does not
     * reset the generation's admission counters; those counters are part of
     * the replay precondition for the next admission.
     */
    public static GenerationRuntimeIndex timeline(
            final GenerationAggregateState aggregateState,
            final TimelineWorkRef timeline,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean possibleDestinationDuplicate,
            final long runtimeRevision) {
        return new GenerationRuntimeIndex(
                effectiveAggregate(aggregateState, obligations),
                CurrentSendWorkKind.TIMELINE,
                timeline,
                null,
                null,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                possibleDestinationDuplicate,
                runtimeRevision);
    }

    public static GenerationRuntimeIndex claimed(
            final byte[] claimId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean duplicate,
            final long runtimeRevision) {
        final GenerationAggregateState aggregate = effectiveAggregate(GenerationAggregateState.CLAIMED, obligations);
        return new GenerationRuntimeIndex(
                aggregate,
                CurrentSendWorkKind.CLAIMED,
                null,
                claimId,
                null,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                duplicate,
                runtimeRevision);
    }

    public static GenerationRuntimeIndex publishing(
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean duplicate,
            final long runtimeRevision) {
        final GenerationAggregateState aggregate = effectiveAggregate(GenerationAggregateState.PUBLISHING, obligations);
        return new GenerationRuntimeIndex(
                aggregate,
                CurrentSendWorkKind.PUBLISHING,
                null,
                null,
                publishAttemptId,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                duplicate,
                runtimeRevision);
    }

    public static GenerationRuntimeIndex none(
            final GenerationAggregateState aggregateState,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean duplicate,
            final long runtimeRevision) {
        return new GenerationRuntimeIndex(
                aggregateState,
                CurrentSendWorkKind.NONE,
                null,
                null,
                null,
                obligations,
                admissionsUsed,
                uncertainRetryAdmissionsUsed,
                duplicate,
                runtimeRevision);
    }

    public GenerationAggregateState aggregateState() {
        return aggregateState;
    }

    public CurrentSendWorkKind currentWorkKind() {
        return currentWorkKind;
    }

    public TimelineWorkRef timeline() {
        return timeline;
    }

    public byte[] claimId() {
        return Bytes.copy(claimId);
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public List<AttemptObligationRef> attemptObligations() {
        return attemptObligations;
    }

    public int admissionsUsed() {
        return admissionsUsed;
    }

    public int uncertainRetryAdmissionsUsed() {
        return uncertainRetryAdmissionsUsed;
    }

    public boolean possibleDestinationDuplicate() {
        return possibleDestinationDuplicate;
    }

    public long runtimeRevision() {
        return runtimeRevision;
    }

    public byte[] runtimeDigest() {
        return Bytes.copy(runtimeDigest);
    }

    boolean isLegacyCompatibility() {
        return legacyCompatibility;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsOneToSeventeen(output);
            CanonicalProtobuf.bytes(output, 18, runtimeDigest);
        });
    }

    public static GenerationRuntimeIndex decode(final byte[] encoded) {
        return decodeInternal(encoded, false);
    }

    /**
     * Decodes the v5 scalar-constructor placeholder used only while older
     * Java call sites are being migrated to a typed runtime projection.
     * Store readers must use {@link #decode(byte[])}; MessageRecord is the
     * only caller allowed to use this transitional path.
     */
    static GenerationRuntimeIndex decodeLegacyCompatibility(final byte[] encoded) {
        return decodeInternal(encoded, true);
    }

    private static GenerationRuntimeIndex decodeInternal(final byte[] encoded, final boolean allowLegacyCompatibility) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded, true));
        if (fields.size() < 8) {
            throw new IllegalArgumentException("generation runtime index is incomplete");
        }
        int index = 0;
        if (unsigned(fields.get(index++), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported generation runtime index version");
        }
        final GenerationAggregateState aggregateState =
                GenerationAggregateState.fromWire(unsigned(fields.get(index++), 2));
        final CurrentSendWorkKind currentWorkKind = CurrentSendWorkKind.fromWire(unsigned(fields.get(index++), 3));
        TimelineWorkRef timeline = null;
        byte[] claimId = null;
        byte[] publishAttemptId = null;
        if (index < fields.size() && fields.get(index).number() == 10) {
            timeline = TimelineWorkRef.decode(bytes(fields.get(index++), 10));
        } else if (index < fields.size() && fields.get(index).number() == 11) {
            claimId = fixed(fields.get(index++), 11);
        } else if (index < fields.size() && fields.get(index).number() == 12) {
            publishAttemptId = fixed(fields.get(index++), 12);
        }
        final List<AttemptObligationRef> obligations = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 13) {
            obligations.add(AttemptObligationRef.decode(bytes(fields.get(index++), 13)));
        }
        final int admissions = intValue(fields.get(index++), 14);
        final int uncertainAdmissions = intValue(fields.get(index++), 15);
        final boolean duplicate = bool(fields.get(index++), 16);
        final long runtimeRevision = unsigned(fields.get(index++), 17);
        final byte[] runtimeDigest = fixed(fields.get(index++), 18);
        if (index != fields.size()) {
            throw new IllegalArgumentException("unknown or out-of-order generation runtime field");
        }
        final GenerationRuntimeIndex result = new GenerationRuntimeIndex(
                aggregateState,
                currentWorkKind,
                timeline,
                claimId,
                publishAttemptId,
                obligations,
                admissions,
                uncertainAdmissions,
                duplicate,
                runtimeRevision,
                runtimeDigest,
                allowLegacyCompatibility);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical generation runtime index");
        }
        return result;
    }

    public static byte[] obligationSetDigest(final List<AttemptObligationRef> obligations) {
        final List<AttemptObligationRef> checked = copyAndValidateObligations(obligations);
        final byte[] concatenated = CanonicalProtobuf.message(output -> {
            for (AttemptObligationRef obligation : checked) {
                output.writeBytes(obligation.canonicalBytes());
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-attempt-obligation-set\0"), concatenated);
    }

    private void validateCurrentWork() {
        final boolean terminal =
                switch (aggregateState) {
                    case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
                    default -> false;
                };
        if (terminal && currentWorkKind != CurrentSendWorkKind.NONE) {
            throw new IllegalArgumentException("terminal generation cannot have current send work");
        }
        switch (currentWorkKind) {
            case NONE -> {
                if (timeline != null || claimId.length != 0 || publishAttemptId.length != 0) {
                    throw new IllegalArgumentException("NONE runtime index has a current-work branch");
                }
            }
            case TIMELINE -> {
                if (timeline == null || claimId.length != 0 || publishAttemptId.length != 0) {
                    throw new IllegalArgumentException("TIMELINE runtime index branch mismatch");
                }
            }
            case CLAIMED -> {
                if (timeline != null || claimId.length == 0 || publishAttemptId.length != 0) {
                    throw new IllegalArgumentException("CLAIMED runtime index branch mismatch");
                }
            }
            case PUBLISHING -> {
                if (timeline != null || claimId.length != 0 || publishAttemptId.length == 0) {
                    throw new IllegalArgumentException("PUBLISHING runtime index branch mismatch");
                }
                final long matches = attemptObligations.stream()
                        .filter(ref -> ref.ledgerState() == AttemptLedgerState.PUBLISHING
                                && Arrays.equals(ref.publishAttemptId(), publishAttemptId))
                        .count();
                if (matches != 1) {
                    throw new IllegalArgumentException("PUBLISHING current work lacks one matching obligation");
                }
            }
        }
        if (terminal
                || (legacyCompatibility
                        && currentWorkKind == CurrentSendWorkKind.NONE
                        && attemptObligations.isEmpty())) {
            return;
        }
        final boolean hasUncertain =
                attemptObligations.stream().anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN);
        if (hasUncertain && aggregateState != GenerationAggregateState.UNCERTAIN) {
            throw new IllegalArgumentException("non-terminal uncertain obligation requires UNCERTAIN aggregate");
        }
        switch (currentWorkKind) {
            case NONE -> {
                if (!hasUncertain
                        || attemptObligations.stream()
                                .anyMatch(ref -> ref.ledgerState() != AttemptLedgerState.UNCERTAIN)) {
                    throw new IllegalArgumentException("non-terminal NONE work requires only UNCERTAIN obligations");
                }
            }
            case TIMELINE -> {
                final TimelineWorkKind workKind = timeline.workKind();
                if (workKind == TimelineWorkKind.UNCERTAIN_RETRY && !hasUncertain) {
                    throw new IllegalArgumentException("UNCERTAIN_RETRY timeline lacks an UNCERTAIN obligation");
                }
                if (hasUncertain && workKind != TimelineWorkKind.UNCERTAIN_RETRY) {
                    throw new IllegalArgumentException("UNCERTAIN obligation requires UNCERTAIN_RETRY timeline");
                }
                if (!hasUncertain) {
                    final GenerationAggregateState expected = workKind == TimelineWorkKind.INITIAL_SCHEDULE
                            ? GenerationAggregateState.SCHEDULED
                            : GenerationAggregateState.RETRY_WAIT;
                    if (aggregateState != expected) {
                        throw new IllegalArgumentException("timeline work and aggregate state disagree");
                    }
                }
            }
            case CLAIMED -> {
                if (!hasUncertain && aggregateState != GenerationAggregateState.CLAIMED) {
                    throw new IllegalArgumentException("CLAIMED work and aggregate state disagree");
                }
            }
            case PUBLISHING -> {
                if (!hasUncertain && aggregateState != GenerationAggregateState.PUBLISHING) {
                    throw new IllegalArgumentException("PUBLISHING work and aggregate state disagree");
                }
            }
        }
    }

    private void writeFieldsOneToSeventeen(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, VERSION);
        CanonicalProtobuf.uint32(output, 2, aggregateState.wireValue());
        CanonicalProtobuf.uint32(output, 3, currentWorkKind.wireValue());
        if (timeline != null) {
            CanonicalProtobuf.bytes(output, 10, timeline.canonicalBytes());
        } else if (claimId.length != 0) {
            CanonicalProtobuf.bytes(output, 11, claimId);
        } else if (publishAttemptId.length != 0) {
            CanonicalProtobuf.bytes(output, 12, publishAttemptId);
        }
        for (AttemptObligationRef obligation : attemptObligations) {
            CanonicalProtobuf.bytes(output, 13, obligation.canonicalBytes());
        }
        CanonicalProtobuf.uint32(output, 14, admissionsUsed);
        CanonicalProtobuf.uint32(output, 15, uncertainRetryAdmissionsUsed);
        CanonicalProtobuf.uint32(output, 16, possibleDestinationDuplicate ? 1 : 0);
        CanonicalProtobuf.int64(output, 17, runtimeRevision);
    }

    private byte[] computeRuntimeDigest() {
        final byte[] fields = CanonicalProtobuf.message(this::writeFieldsOneToSeventeen);
        return Bytes.sha256(Bytes.utf8("nereus-delay-generation-runtime-index\0"), fields);
    }

    private static List<AttemptObligationRef> copyAndValidateObligations(final List<AttemptObligationRef> obligations) {
        Objects.requireNonNull(obligations, "obligations");
        final List<AttemptObligationRef> copy = List.copyOf(obligations);
        byte[] previousKey = null;
        final List<byte[]> ids = new ArrayList<>();
        for (AttemptObligationRef obligation : copy) {
            Objects.requireNonNull(obligation, "null attempt obligation");
            final byte[] sortKey = Bytes.concat(obligation.publishAttemptId(), obligation.encodedInflightKey());
            if (previousKey != null && compareUnsigned(previousKey, sortKey) >= 0) {
                throw new IllegalArgumentException("attempt obligations are not byte-sorted and unique");
            }
            previousKey = sortKey;
            for (byte[] priorId : ids) {
                if (Arrays.equals(priorId, obligation.publishAttemptId())) {
                    throw new IllegalArgumentException("attempt obligation ID is duplicated");
                }
            }
            ids.add(obligation.publishAttemptId());
        }
        return copy;
    }

    private static GenerationAggregateState effectiveAggregate(
            final GenerationAggregateState requested, final List<AttemptObligationRef> obligations) {
        Objects.requireNonNull(requested, "aggregateState");
        Objects.requireNonNull(obligations, "obligations");
        final boolean hasUncertain =
                obligations.stream().anyMatch(obligation -> obligation.ledgerState() == AttemptLedgerState.UNCERTAIN);
        return hasUncertain
                        && (requested == GenerationAggregateState.CLAIMED
                                || requested == GenerationAggregateState.PUBLISHING
                                || requested == GenerationAggregateState.RETRY_WAIT
                                || requested == GenerationAggregateState.SCHEDULED)
                ? GenerationAggregateState.UNCERTAIN
                : requested;
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    private static byte[] optionalFixed(final byte[] value, final String name) {
        if (value == null) {
            return new byte[0];
        }
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid runtime index varint field " + number);
        }
        return field.unsignedValue();
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("runtime index field exceeds Java int range: " + number);
        }
        return (int) value;
    }

    private static boolean bool(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > 1) {
            throw new IllegalArgumentException("invalid runtime index bool field " + number);
        }
        return value == 1;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid runtime index bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, HASH_LENGTH, "runtime index field " + number);
        return value;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GenerationRuntimeIndex that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
