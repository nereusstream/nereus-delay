package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Component-local durable SLO outbox projection.
 *
 * <p>The Start is immutable.  A Final is optional until the semantic endpoint
 * is observed, and repeated Finals are merged with the Registry's conservative
 * direction before the containing WriteBatch is synced.</p>
 */
public final class SloObservationOutboxV1 {
    public static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-slo-outbox-v1\0");

    private final SloSampleStartV1 start;
    private final SloSampleFinalV1 finalObservation;
    private final byte[] recordDigest;

    private SloObservationOutboxV1(final SloSampleStartV1 start, final SloSampleFinalV1 finalObservation,
                                   final byte[] suppliedDigest) {
        this.start = Objects.requireNonNull(start, "start");
        this.finalObservation = finalObservation;
        if (finalObservation != null) {
            finalObservation.validateAgainst(start);
        }
        final byte[] expectedDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToThree());
        if (suppliedDigest != null && !Arrays.equals(suppliedDigest, expectedDigest)) {
            throw new IllegalArgumentException("SLO outbox digest mismatch");
        }
        this.recordDigest = expectedDigest;
    }

    public static SloObservationOutboxV1 open(final SloSampleStartV1 start) {
        return new SloObservationOutboxV1(start, null, null);
    }

    public SloSampleStartV1 start() {
        return start;
    }

    public SloSampleFinalV1 finalObservation() {
        return finalObservation;
    }

    public byte[] sampleId() {
        return start.sampleId();
    }

    public byte[] recordDigest() {
        return Bytes.copy(recordDigest);
    }

    public SloObservationOutboxV1 mergeFinal(final SloSampleFinalV1 incoming,
                                             final SloThresholdDirectionV1 direction) {
        Objects.requireNonNull(incoming, "incoming").validateAgainst(start);
        final SloSampleFinalV1 merged = finalObservation == null
                ? incoming : SloSampleFinalV1.merge(finalObservation, incoming, direction);
        return new SloObservationOutboxV1(start, merged, null);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToThree());
            CanonicalProtobuf.bytes(output, 4, recordDigest);
        });
    }

    public static SloObservationOutboxV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "SloObservationOutboxV1");
        if (fields.size() != 3 && fields.size() != 4) {
            throw new IllegalArgumentException("SloObservationOutboxV1 has an unexpected field count");
        }
        final int[] expected = fields.size() == 3 ? new int[]{1, 2, 4} : new int[]{1, 2, 3, 4};
        QueryCodecSupport.requireNumbers(fields, expected, "SloObservationOutboxV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported SloObservationOutboxV1 version");
        }
        final SloSampleStartV1 start = SloSampleStartV1.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final SloSampleFinalV1 finalObservation = fields.size() == 4
                ? SloSampleFinalV1.decode(QueryCodecSupport.nested(fields.get(2), 3)) : null;
        final SloObservationOutboxV1 result = new SloObservationOutboxV1(start, finalObservation,
                QueryCodecSupport.fixed(fields.get(fields.size() - 1), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "SloObservationOutboxV1");
        return result;
    }

    private byte[] fieldsOneToThree() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, start.canonicalBytes());
            if (finalObservation != null) {
                CanonicalProtobuf.bytes(output, 3, finalObservation.canonicalBytes());
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SloObservationOutboxV1 that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
