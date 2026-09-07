package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One reversible Target work item, shared by its ordinary and optional Native candidate indexes. */
public final class TargetTimelineWorkRef {
    public static final int VERSION = 1;
    /** Reserved for the Target Store format; the Lane ValueEnvelope reader rejects this type. */
    public static final int VALUE_TYPE = 14;

    public static final int MAX_CONTROL_REF_BYTES = 34 + 34 + 6;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetMessageLocator.MAX_CANONICAL_BYTES
            + 2
            + 11
            + 11
            + 23
            + 6
            + 11
            + 2
            + 2
            + MAX_CONTROL_REF_BYTES
            + 4
            + TargetSourcePosition.MAX_CANONICAL_BYTES
            + 2
            + 34
            + 34;
    private static final byte[] SEMANTIC_DOMAIN = Bytes.utf8("nereus-delay-target-work-semantic\0");
    private static final byte[] INSTANCE_DOMAIN = Bytes.utf8("nereus-delay-target-work-instance\0");
    private final TargetMessageLocator locator;
    private final TimelineWorkKind workKind;
    private final long deliverAtEpochMs;
    private final long retryEligibilityAtEpochMs;
    private final byte[] sourceOrderToken;
    private final int candidateAttemptNo;
    private final long runtimeRevision;
    private final UncertainRetryAuthority uncertainRetryAuthority;
    private final ControlRef uncertainRetryControl;
    private final SourcePosition uncertainRetryControlPosition;
    private final boolean nativeCandidate;
    private final byte[] semanticWorkDigest;
    private final byte[] workInstanceDigest;

    public TargetTimelineWorkRef(
            final TargetMessageLocator locator,
            final TimelineWorkKind workKind,
            final long deliverAtEpochMs,
            final long retryEligibilityAtEpochMs,
            final byte[] sourceOrderToken,
            final int candidateAttemptNo,
            final long runtimeRevision,
            final UncertainRetryAuthority uncertainRetryAuthority,
            final ControlRef uncertainRetryControl,
            final SourcePosition uncertainRetryControlPosition,
            final boolean nativeCandidate) {
        this.locator = Objects.requireNonNull(locator, "locator");
        this.workKind = Objects.requireNonNull(workKind, "workKind");
        if (deliverAtEpochMs < 0 || retryEligibilityAtEpochMs < 0 || candidateAttemptNo <= 0 || runtimeRevision == 0) {
            throw new IllegalArgumentException("invalid Target work time/attempt/revision");
        }
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.retryEligibilityAtEpochMs = retryEligibilityAtEpochMs;
        Objects.requireNonNull(sourceOrderToken, "sourceOrderToken");
        if (!((sourceOrderToken.length == 9 && sourceOrderToken[0] == 1)
                || (sourceOrderToken.length == 21 && sourceOrderToken[0] == 2))) {
            throw new IllegalArgumentException("Target source-order token is not a closed variant");
        }
        this.sourceOrderToken = Bytes.copy(sourceOrderToken);
        this.candidateAttemptNo = candidateAttemptNo;
        this.runtimeRevision = runtimeRevision;
        this.uncertainRetryAuthority = Objects.requireNonNull(uncertainRetryAuthority, "uncertainRetryAuthority");
        this.uncertainRetryControl = uncertainRetryControl;
        this.uncertainRetryControlPosition = uncertainRetryControlPosition == null
                ? null
                : TargetSourcePosition.requireBounded(uncertainRetryControlPosition);
        this.nativeCandidate = nativeCandidate;
        if (workKind == TimelineWorkKind.INITIAL_SCHEDULE
                && (candidateAttemptNo != 1 || retryEligibilityAtEpochMs != deliverAtEpochMs)) {
            throw new IllegalArgumentException("Target initial work has retry state");
        }
        if (nativeCandidate
                && (workKind != TimelineWorkKind.INITIAL_SCHEDULE
                        || locator.orderingMode() != OrderingMode.BEST_EFFORT)) {
            throw new IllegalArgumentException("Native Target index requires initial best-effort work");
        }
        validateRetryAuthority();
        // The key codec closes source-token variants and validates every locator component.
        ordinaryKey();
        this.semanticWorkDigest = Bytes.sha256(SEMANTIC_DOMAIN, fields(false));
        this.workInstanceDigest = Bytes.sha256(INSTANCE_DOMAIN, instanceFields());
    }

    private void validateRetryAuthority() {
        if ((workKind == TimelineWorkKind.UNCERTAIN_RETRY) != (uncertainRetryAuthority != UncertainRetryAuthority.NONE)
                || (workKind == TimelineWorkKind.UNCERTAIN_RETRY
                        && locator.orderingMode() != OrderingMode.BEST_EFFORT)) {
            throw new IllegalArgumentException("Target work kind/order disagrees with uncertain retry authority");
        }
        final boolean override = uncertainRetryAuthority == UncertainRetryAuthority.CONTROL_OVERRIDE;
        if (override != (uncertainRetryControl != null) || override != (uncertainRetryControlPosition != null)) {
            throw new IllegalArgumentException(
                    "Target control retry requires exactly one complete control/source pair");
        }
        if (override
                && !uncertainRetryControlPosition
                        .shardId()
                        .equals(locator.messageId().routingId().shardId())) {
            throw new IllegalArgumentException("Target control retry source belongs to another Shard");
        }
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public TimelineWorkKind workKind() {
        return workKind;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long retryEligibilityAtEpochMs() {
        return retryEligibilityAtEpochMs;
    }

    public long ordinaryEligibilityAtEpochMs() {
        return Math.max(deliverAtEpochMs, retryEligibilityAtEpochMs);
    }

    public byte[] sourceOrderToken() {
        return Bytes.copy(sourceOrderToken);
    }

    public int candidateAttemptNo() {
        return candidateAttemptNo;
    }

    public long runtimeRevision() {
        return runtimeRevision;
    }

    public UncertainRetryAuthority uncertainRetryAuthority() {
        return uncertainRetryAuthority;
    }

    public ControlRef uncertainRetryControl() {
        return uncertainRetryControl;
    }

    public SourcePosition uncertainRetryControlPosition() {
        return uncertainRetryControlPosition;
    }

    public boolean nativeCandidate() {
        return nativeCandidate;
    }

    public byte[] semanticWorkDigest() {
        return Bytes.copy(semanticWorkDigest);
    }

    public byte[] workInstanceDigest() {
        return Bytes.copy(workInstanceDigest);
    }

    public byte[] ordinaryKey() {
        return locator.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                ? TargetKeyCodec.ordered(
                        locator.target(),
                        locator.orderingDomain(),
                        deliverAtEpochMs,
                        sourceOrderToken,
                        locator.messageId(),
                        locator.generation())
                : TargetKeyCodec.candidate(
                        TargetKeyCodec.CandidateKind.DUE,
                        locator.target(),
                        locator.domain(),
                        ordinaryEligibilityAtEpochMs(),
                        sourceOrderToken,
                        locator.messageId(),
                        locator.generation());
    }

    public byte[] nativeKey() {
        if (!nativeCandidate) {
            throw new IllegalStateException("Target work has no Native candidate");
        }
        return TargetKeyCodec.candidate(
                TargetKeyCodec.CandidateKind.NATIVE,
                locator.target(),
                locator.domain(),
                deliverAtEpochMs,
                sourceOrderToken,
                locator.messageId(),
                locator.generation());
    }

    /** Only validates the summary projection; an ORDER_STATE barrier must independently permit selection. */
    public void requireHeadProjection(final TargetHeadRef head) {
        locator.requireMessageProjection(head.messageId(), head.generation());
        final byte[] expected = head.nativeCandidate()
                ? nativeKey()
                : locator.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                        ? TargetKeyCodec.orderedHead(
                                locator.target(),
                                locator.domain(),
                                ordinaryEligibilityAtEpochMs(),
                                locator.orderingDomain())
                        : ordinaryKey();
        if (!Arrays.equals(expected, head.key())) {
            throw new IllegalArgumentException("Target work head key/eligibility mismatch");
        }
    }

    /**
     * Validates live index routing. Owner, controls, binding and attempt obligations
     * remain mandatory external gates.
     */
    public void requireQueueProjection(final TargetQueueState queue, final CanonicalTargetPartition identity) {
        locator.requireQueueProjection(queue);
        if (!locator.target().equals(identity.id())
                || (nativeCandidate
                        && (identity.resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                                || queue.domains().get(locator.domain().slot()).nativePolicyScopeRef() == null))) {
            throw new IllegalArgumentException("Target work physical identity or Native policy scope mismatch");
        }
    }

    public void requireScheduleProjection(final SourcePosition scheduleSource) {
        TargetSourcePosition.requireBounded(scheduleSource);
        if (!locator.messageId().routingId().shardId().equals(scheduleSource.shardId())
                || !Arrays.equals(sourceOrderToken, scheduleSource.sourceOrderToken())) {
            throw new IllegalArgumentException("Target work schedule source order/Shard mismatch");
        }
        if (uncertainRetryControlPosition != null && uncertainRetryControlPosition.compareTo(scheduleSource) <= 0) {
            throw new IllegalArgumentException("Target retry control must follow Schedule in the same physical source");
        }
    }

    public TargetTimelineWorkRef withRuntimeRevision(final long revision) {
        return new TargetTimelineWorkRef(
                locator,
                workKind,
                deliverAtEpochMs,
                retryEligibilityAtEpochMs,
                sourceOrderToken,
                candidateAttemptNo,
                revision,
                uncertainRetryAuthority,
                uncertainRetryControl,
                uncertainRetryControlPosition,
                nativeCandidate);
    }

    private byte[] fields(final boolean includeRevision) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.uint32(out, 3, workKind.wireValue());
            CanonicalProtobuf.uint64(out, 4, deliverAtEpochMs);
            CanonicalProtobuf.uint64(out, 5, retryEligibilityAtEpochMs);
            CanonicalProtobuf.bytes(out, 6, sourceOrderToken);
            CanonicalProtobuf.uint32(out, 7, candidateAttemptNo);
            if (includeRevision) {
                CanonicalProtobuf.uint64Bits(out, 8, runtimeRevision);
            }
            CanonicalProtobuf.uint32(out, 9, uncertainRetryAuthority.wireValue());
            if (uncertainRetryControl != null) {
                CanonicalProtobuf.bytes(out, 10, uncertainRetryControl.canonicalBytes());
                CanonicalProtobuf.bytes(out, 11, uncertainRetryControlPosition.canonicalBytes());
            }
            CanonicalProtobuf.uint32(out, 12, nativeCandidate ? 1 : 0);
        });
    }

    private byte[] instanceFields() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields(true));
            CanonicalProtobuf.bytes(out, 13, semanticWorkDigest);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(instanceFields());
            CanonicalProtobuf.bytes(out, 14, workInstanceDigest);
        });
    }

    public static TargetTimelineWorkRef decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target work exceeds its byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            if (fields.size() == 14) {
                throw new IllegalArgumentException("Target work exceeds its field count bound");
            }
            fields.add(reader.next());
        }
        final boolean control = fields.size() == 14;
        QueryCodecSupport.requireNumbers(
                fields,
                control
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14},
                "TargetTimelineWorkRef");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target work schema");
        }
        final byte[] controlBytes = control ? QueryCodecSupport.bytes(fields.get(9), 10) : null;
        if (control && controlBytes.length > MAX_CONTROL_REF_BYTES) {
            throw new IllegalArgumentException("Target control reference exceeds its encoding bound");
        }
        final int nativeIndex = control ? 11 : 9;
        final TargetTimelineWorkRef result = new TargetTimelineWorkRef(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TimelineWorkKind.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.bytes(fields.get(5), 6),
                QueryCodecSupport.uint32(fields.get(6), 7),
                QueryCodecSupport.uint64Bits(fields.get(7), 8),
                UncertainRetryAuthority.fromWire(QueryCodecSupport.uint(fields.get(8), 9)),
                control ? ControlRef.decode(controlBytes) : null,
                control ? TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(10), 11)) : null,
                QueryCodecSupport.bool(fields.get(nativeIndex), 12));
        if (!Bytes.constantTimeEquals(
                        result.semanticWorkDigest, QueryCodecSupport.fixed(fields.get(nativeIndex + 1), 13, 32))
                || !Bytes.constantTimeEquals(
                        result.workInstanceDigest, QueryCodecSupport.fixed(fields.get(nativeIndex + 2), 14, 32))) {
            throw new IllegalArgumentException("Target work semantic/instance digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetTimelineWorkRef");
        return result;
    }

    public static TargetTimelineWorkRef decodeForIndex(
            final byte[] key, final byte[] encoded, final SourcePosition scheduleSource) {
        final TargetTimelineWorkRef result = decode(encoded);
        result.requireScheduleProjection(scheduleSource);
        if (!Arrays.equals(key, result.ordinaryKey())
                && !(result.nativeCandidate && Arrays.equals(key, result.nativeKey()))) {
            throw new IllegalArgumentException("Target timeline key differs from its exact work projection");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetTimelineWorkRef that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(workInstanceDigest);
    }
}
