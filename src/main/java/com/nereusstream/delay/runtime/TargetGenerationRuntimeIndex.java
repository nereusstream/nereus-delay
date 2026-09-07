package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Bounded current work and retained attempt obligations for one Target Message generation. */
public final class TargetGenerationRuntimeIndex {
    public static final int VERSION = 1;
    public static final int MAX_OBLIGATIONS = 1024;
    public static final int MAX_OBLIGATION_BYTES = 34 + 6 + 2 + 48 + 34 + 34;
    public static final int MAX_FIELDS = 10 + MAX_OBLIGATIONS;
    public static final int MAX_CANONICAL_BYTES = 2
            + 6
            + 2
            + 2
            + 4
            + TargetTimelineWorkRef.MAX_CANONICAL_BYTES
            + MAX_OBLIGATIONS * (3 + MAX_OBLIGATION_BYTES)
            + 6
            + 6
            + 2
            + 11
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-generation-runtime\0");
    private final int generation;
    private final GenerationAggregateState aggregateState;
    private final CurrentSendWorkKind currentWorkKind;
    private final TargetTimelineWorkRef timeline;
    private final byte[] claimId;
    private final byte[] publishAttemptId;
    private final List<AttemptObligationRef> obligations;
    private final int admissionsUsed;
    private final int uncertainRetryAdmissionsUsed;
    private final boolean possibleDestinationDuplicate;
    private final long runtimeRevision;
    private final byte[] digest;

    public TargetGenerationRuntimeIndex(
            final int generation,
            final GenerationAggregateState aggregateState,
            final CurrentSendWorkKind currentWorkKind,
            final TargetTimelineWorkRef timeline,
            final byte[] claimId,
            final byte[] publishAttemptId,
            final List<AttemptObligationRef> obligations,
            final int admissionsUsed,
            final int uncertainRetryAdmissionsUsed,
            final boolean possibleDestinationDuplicate,
            final long runtimeRevision) {
        this.generation = generation;
        this.aggregateState = Objects.requireNonNull(aggregateState, "aggregateState");
        this.currentWorkKind = Objects.requireNonNull(currentWorkKind, "currentWorkKind");
        this.timeline = timeline;
        this.claimId = optionalIdentity(claimId, "claimId");
        this.publishAttemptId = optionalIdentity(publishAttemptId, "publishAttemptId");
        Objects.requireNonNull(obligations, "obligations");
        if (obligations.size() > MAX_OBLIGATIONS) {
            throw new IllegalArgumentException("Target obligation count exceeds schema bound");
        }
        this.obligations = List.copyOf(obligations);
        if (admissionsUsed < 0
                || uncertainRetryAdmissionsUsed < 0
                || uncertainRetryAdmissionsUsed > admissionsUsed
                || admissionsUsed < obligations.size()
                || runtimeRevision == 0) {
            throw new IllegalArgumentException("invalid Target runtime counters/revision");
        }
        this.admissionsUsed = admissionsUsed;
        this.uncertainRetryAdmissionsUsed = uncertainRetryAdmissionsUsed;
        this.possibleDestinationDuplicate = possibleDestinationDuplicate;
        this.runtimeRevision = runtimeRevision;
        validateObligations();
        validateCurrentWork();
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private void validateObligations() {
        byte[] priorId = null;
        for (AttemptObligationRef ref : obligations) {
            final byte[] id = ref.publishAttemptId();
            final byte[] key = ref.encodedInflightKey();
            if (ref.generation() != generation
                    || (priorId != null && Arrays.compareUnsigned(priorId, id) >= 0)
                    || ByteBuffer.wrap(key).getInt(10) != 32
                    || !Arrays.equals(id, Arrays.copyOfRange(key, 14, 46))) {
                throw new IllegalArgumentException("Target attempt obligation generation/order/key identity mismatch");
            }
            priorId = id;
        }
    }

    private void validateCurrentWork() {
        switch (currentWorkKind) {
            case NONE -> {
                if (timeline != null || claimId != null || publishAttemptId != null) {
                    throw branchMismatch();
                }
            }
            case TIMELINE -> {
                if (timeline == null || claimId != null || publishAttemptId != null) {
                    throw branchMismatch();
                }
                if (timeline.locator().generation() != generation
                        || timeline.runtimeRevision() != runtimeRevision
                        || admissionsUsed == Integer.MAX_VALUE
                        || timeline.candidateAttemptNo() != admissionsUsed + 1) {
                    throw new IllegalArgumentException("Target timeline generation/revision/next attempt mismatch");
                }
            }
            case CLAIMED -> {
                if (timeline != null || claimId == null || publishAttemptId != null) {
                    throw branchMismatch();
                }
            }
            case PUBLISHING -> {
                if (timeline != null || claimId != null || publishAttemptId == null) {
                    throw branchMismatch();
                }
            }
        }
        if (terminal()) {
            if (currentWorkKind != CurrentSendWorkKind.NONE) {
                throw new IllegalArgumentException("terminal Target generation retains current send work");
            }
            return;
        }
        boolean uncertain = false;
        int publishing = 0;
        for (AttemptObligationRef ref : obligations) {
            if (ref.ledgerState() == AttemptLedgerState.UNCERTAIN) {
                uncertain = true;
            } else {
                publishing++;
                if (currentWorkKind != CurrentSendWorkKind.PUBLISHING
                        || !Arrays.equals(ref.publishAttemptId(), publishAttemptId)) {
                    throw new IllegalArgumentException(
                            "Target nonterminal publishing obligation lacks matching current work");
                }
            }
        }
        if ((currentWorkKind == CurrentSendWorkKind.PUBLISHING ? 1 : 0) != publishing) {
            throw new IllegalArgumentException("Target current publisher lacks exactly one publishing obligation");
        }
        if (uncertain && aggregateState != GenerationAggregateState.UNCERTAIN) {
            throw new IllegalArgumentException("Target aggregate hides an uncertain obligation");
        }
        final GenerationAggregateState expected;
        switch (currentWorkKind) {
            case NONE -> {
                if (!uncertain) {
                    throw new IllegalArgumentException("nonterminal Target NONE requires uncertain obligations");
                }
                expected = GenerationAggregateState.UNCERTAIN;
            }
            case TIMELINE -> {
                if ((timeline.workKind() == TimelineWorkKind.UNCERTAIN_RETRY) != uncertain) {
                    throw new IllegalArgumentException(
                            "Target timeline retry kind disagrees with uncertain obligations");
                }
                if (timeline.workKind() == TimelineWorkKind.INITIAL_SCHEDULE && admissionsUsed != 0) {
                    throw new IllegalArgumentException("Target initial work has already consumed an Admission");
                }
                expected = uncertain
                        ? GenerationAggregateState.UNCERTAIN
                        : timeline.workKind() == TimelineWorkKind.INITIAL_SCHEDULE
                                ? GenerationAggregateState.SCHEDULED
                                : GenerationAggregateState.RETRY_WAIT;
            }
            case CLAIMED ->
                expected = uncertain ? GenerationAggregateState.UNCERTAIN : GenerationAggregateState.CLAIMED;
            case PUBLISHING ->
                expected = uncertain ? GenerationAggregateState.UNCERTAIN : GenerationAggregateState.PUBLISHING;
            default -> throw branchMismatch();
        }
        if (aggregateState != expected) {
            throw new IllegalArgumentException("Target aggregate/current work mismatch");
        }
    }

    public boolean terminal() {
        return switch (aggregateState) {
            case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
            default -> false;
        };
    }

    public int generation() {
        return generation;
    }

    public GenerationAggregateState aggregateState() {
        return aggregateState;
    }

    public CurrentSendWorkKind currentWorkKind() {
        return currentWorkKind;
    }

    public TargetTimelineWorkRef timeline() {
        return timeline;
    }

    public byte[] claimId() {
        return claimId == null ? null : Bytes.copy(claimId);
    }

    public byte[] publishAttemptId() {
        return publishAttemptId == null ? null : Bytes.copy(publishAttemptId);
    }

    public List<AttemptObligationRef> attemptObligations() {
        return obligations;
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
        return Bytes.copy(digest);
    }

    public void requireMessageProjection(final TargetMessageLocator locator) {
        if (locator.generation() != generation
                || (timeline != null && !timeline.locator().equals(locator))) {
            throw new IllegalArgumentException("Target runtime belongs to another Message locator/generation");
        }
    }

    /** Preserves the existing attempt-set digest domain for exact retained-obligation comparisons. */
    public byte[] obligationSetDigest() {
        final byte[] refs = CanonicalProtobuf.message(out -> {
            for (AttemptObligationRef ref : obligations) {
                out.writeBytes(ref.canonicalBytes());
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-attempt-obligation-set\0"), refs);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.uint32Bits(out, 2, generation);
            CanonicalProtobuf.uint32(out, 3, aggregateState.wireValue());
            CanonicalProtobuf.uint32(out, 4, currentWorkKind.wireValue());
            if (timeline != null) {
                CanonicalProtobuf.bytes(out, 5, timeline.canonicalBytes());
            }
            if (claimId != null) {
                CanonicalProtobuf.bytes(out, 6, claimId);
            }
            if (publishAttemptId != null) {
                CanonicalProtobuf.bytes(out, 7, publishAttemptId);
            }
            for (AttemptObligationRef ref : obligations) {
                CanonicalProtobuf.bytes(out, 8, ref.canonicalBytes());
            }
            CanonicalProtobuf.uint32(out, 9, admissionsUsed);
            CanonicalProtobuf.uint32(out, 10, uncertainRetryAdmissionsUsed);
            CanonicalProtobuf.uint32(out, 11, possibleDestinationDuplicate ? 1 : 0);
            CanonicalProtobuf.uint64Bits(out, 12, runtimeRevision);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 13, digest);
        });
    }

    public static TargetGenerationRuntimeIndex decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target runtime exceeds byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            if (fields.size() == MAX_FIELDS) {
                throw new IllegalArgumentException("Target runtime exceeds field count bound");
            }
            fields.add(reader.next());
        }
        if (fields.size() < 9 || QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported or incomplete Target runtime schema");
        }
        int index = 4;
        TargetTimelineWorkRef work = null;
        byte[] claim = null;
        byte[] attempt = null;
        if (fields.get(index).number() == 5) {
            work = TargetTimelineWorkRef.decode(QueryCodecSupport.bytes(fields.get(index++), 5));
        } else if (fields.get(index).number() == 6) {
            claim = QueryCodecSupport.fixed(fields.get(index++), 6, 32);
        } else if (fields.get(index).number() == 7) {
            attempt = QueryCodecSupport.fixed(fields.get(index++), 7, 32);
        }
        final List<AttemptObligationRef> refs = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 8) {
            if (refs.size() == MAX_OBLIGATIONS) {
                throw new IllegalArgumentException("Target obligation count exceeds bound");
            }
            final byte[] bytes = QueryCodecSupport.bytes(fields.get(index++), 8);
            if (bytes.length > MAX_OBLIGATION_BYTES) {
                throw new IllegalArgumentException("Target obligation exceeds byte bound");
            }
            refs.add(AttemptObligationRef.decode(bytes));
        }
        if (fields.size() - index != 5) {
            throw new IllegalArgumentException("unknown or missing Target runtime fields");
        }
        final TargetGenerationRuntimeIndex result = new TargetGenerationRuntimeIndex(
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                GenerationAggregateState.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                CurrentSendWorkKind.fromWire(QueryCodecSupport.uint(fields.get(3), 4)),
                work,
                claim,
                attempt,
                refs,
                QueryCodecSupport.uint32(fields.get(index++), 9),
                QueryCodecSupport.uint32(fields.get(index++), 10),
                QueryCodecSupport.bool(fields.get(index++), 11),
                QueryCodecSupport.uint64Bits(fields.get(index++), 12));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(index), 13, 32))) {
            throw new IllegalArgumentException("Target runtime digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetGenerationRuntimeIndex");
        return result;
    }

    private static byte[] optionalIdentity(final byte[] id, final String name) {
        if (id == null) {
            return null;
        }
        Bytes.requireLength(id, 32, name);
        if (Arrays.equals(id, new byte[32])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(id);
    }

    private static IllegalArgumentException branchMismatch() {
        return new IllegalArgumentException("Target current work branch mismatch");
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetGenerationRuntimeIndex that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
