package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPayloadReference;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Complete Message projection for the reserved Target format, with one authoritative runtime aggregate. */
public final class TargetMessageRecord {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 15;
    public static final int MAX_INLINE_BYTES = 1 << 24;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetMessageLocator.MAX_CANONICAL_BYTES
            + 4 * 11
            + 2
            + 4
            + TargetSourcePosition.MAX_CANONICAL_BYTES
            + 5
            + Math.max(MAX_INLINE_BYTES, TargetPayloadReference.MAX_CANONICAL_BYTES)
            + 4
            + TargetGenerationRuntimeIndex.MAX_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-message\0");
    private final TargetMessageLocator locator;
    private final long stateVersion;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final long retryEligibilityAtEpochMs;
    private final NativeDeliveryPolicy nativeDeliveryPolicy;
    private final SourcePosition scheduleSource;
    private final byte[] inlinePayload;
    private final PayloadReference payloadReference;
    private final TargetGenerationRuntimeIndex runtime;
    private final byte[] digest;

    public TargetMessageRecord(
            final TargetMessageLocator locator,
            final long stateVersion,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final long retryEligibilityAtEpochMs,
            final NativeDeliveryPolicy nativeDeliveryPolicy,
            final SourcePosition scheduleSource,
            final byte[] inlinePayload,
            final PayloadReference payloadReference,
            final TargetGenerationRuntimeIndex runtime) {
        this.locator = Objects.requireNonNull(locator, "locator");
        if (stateVersion == 0
                || deliverAtEpochMs < 0
                || expireAtEpochMs < deliverAtEpochMs
                || retryEligibilityAtEpochMs < 0
                || retryEligibilityAtEpochMs > expireAtEpochMs) {
            throw new IllegalArgumentException("invalid Target Message version/time range");
        }
        this.stateVersion = stateVersion;
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.retryEligibilityAtEpochMs = retryEligibilityAtEpochMs;
        this.nativeDeliveryPolicy = Objects.requireNonNull(nativeDeliveryPolicy, "nativeDeliveryPolicy");
        if (locator.orderingMode() == OrderingMode.DELIVERY_TIME_FIFO
                && nativeDeliveryPolicy != NativeDeliveryPolicy.FORBID) {
            throw new IllegalArgumentException("strict Target Message cannot opt into Native delivery");
        }
        this.scheduleSource = TargetSourcePosition.requireBounded(scheduleSource);
        if (!scheduleSource.shardId().equals(locator.messageId().routingId().shardId())) {
            throw new IllegalArgumentException("Target Message Schedule source belongs to another Shard");
        }
        if ((inlinePayload != null) == (payloadReference != null)) {
            throw new IllegalArgumentException("Target Message must have exactly one payload branch");
        }
        if (inlinePayload != null && inlinePayload.length > MAX_INLINE_BYTES) {
            throw new IllegalArgumentException("Target inline payload exceeds its schema bound");
        }
        this.inlinePayload = inlinePayload == null ? null : Bytes.copy(inlinePayload);
        this.payloadReference =
                payloadReference == null ? null : TargetPayloadReference.requireBounded(payloadReference);
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        runtime.requireMessageProjection(locator);
        if (runtime.timeline() != null) {
            final TargetTimelineWorkRef work = runtime.timeline();
            if (work.deliverAtEpochMs() != deliverAtEpochMs
                    || work.retryEligibilityAtEpochMs() != retryEligibilityAtEpochMs
                    || (work.nativeCandidate() && nativeDeliveryPolicy == NativeDeliveryPolicy.FORBID)) {
                throw new IllegalArgumentException(
                        "Target Message timing/Native permission disagrees with current work");
            }
            work.requireScheduleProjection(scheduleSource);
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetMessageLocator locator() {
        return locator;
    }

    public long stateVersion() {
        return stateVersion;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long expireAtEpochMs() {
        return expireAtEpochMs;
    }

    public long retryEligibilityAtEpochMs() {
        return retryEligibilityAtEpochMs;
    }

    public NativeDeliveryPolicy nativeDeliveryPolicy() {
        return nativeDeliveryPolicy;
    }

    public SourcePosition scheduleSource() {
        return scheduleSource;
    }

    public byte[] inlinePayload() {
        return inlinePayload == null ? null : Bytes.copy(inlinePayload);
    }

    public PayloadReference payloadReference() {
        return payloadReference;
    }

    public long payloadLength() {
        return inlinePayload == null ? payloadReference.length() : inlinePayload.length;
    }

    public TargetGenerationRuntimeIndex runtime() {
        return runtime;
    }

    public GenerationAggregateState aggregateState() {
        return runtime.aggregateState();
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.message(locator.messageId());
    }

    /** Validates complete reversible work before any Claim; the caller must also check all live authority gates. */
    public TargetTimelineWorkRef requireTimelineProjection(final byte[] indexKey, final byte[] indexValue) {
        final TargetTimelineWorkRef decoded =
                TargetTimelineWorkRef.decodeForIndex(indexKey, indexValue, scheduleSource);
        if (!decoded.equals(runtime.timeline())) {
            throw new IllegalArgumentException("Target index disagrees with complete Message work");
        }
        return decoded;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, stateVersion);
            CanonicalProtobuf.uint64(out, 4, deliverAtEpochMs);
            CanonicalProtobuf.uint64(out, 5, expireAtEpochMs);
            CanonicalProtobuf.uint64(out, 6, retryEligibilityAtEpochMs);
            CanonicalProtobuf.uint32(out, 7, nativeDeliveryPolicy.wireValue());
            CanonicalProtobuf.bytes(out, 8, scheduleSource.canonicalBytes());
            if (inlinePayload != null) {
                CanonicalProtobuf.bytes(out, 9, inlinePayload);
            } else {
                CanonicalProtobuf.bytes(out, 10, payloadReference.encode());
            }
            CanonicalProtobuf.bytes(out, 11, runtime.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 12, digest);
        });
    }

    public static TargetMessageRecord decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target Message exceeds its byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            if (fields.size() == 11) {
                throw new IllegalArgumentException("Target Message exceeds field count bound");
            }
            fields.add(reader.next());
        }
        final boolean inline = fields.size() > 8 && fields.get(8).number() == 9;
        QueryCodecSupport.requireNumbers(
                fields,
                inline ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12} : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12},
                "TargetMessageRecord");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target Message schema");
        }
        final TargetMessageRecord result = new TargetMessageRecord(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 4),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.uint(fields.get(5), 6),
                NativeDeliveryPolicy.fromWire(QueryCodecSupport.uint(fields.get(6), 7)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(7), 8)),
                inline ? QueryCodecSupport.bytes(fields.get(8), 9) : null,
                inline ? null : TargetPayloadReference.decode(QueryCodecSupport.bytes(fields.get(8), 10)),
                TargetGenerationRuntimeIndex.decode(QueryCodecSupport.bytes(fields.get(9), 11)));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.get(10), 12, 32))) {
            throw new IllegalArgumentException("Target Message digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetMessageRecord");
        return result;
    }

    public static TargetMessageRecord decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId sourceShard) {
        final TargetMessageRecord record = decode(encoded);
        if (!Arrays.equals(key, record.encodedKey())
                || !record.scheduleSource.shardId().equals(sourceShard)) {
            throw new IllegalArgumentException("Target Message key/Shard mismatch");
        }
        return record;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetMessageRecord that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
