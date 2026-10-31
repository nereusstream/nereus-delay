package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Exact provider observation data for a retained marker. Only the original Catalog may authenticate the actual operation. */
public record TargetEvidenceRetiredMarkerEvidence(
        TargetEvidenceRetiredMarker marker,
        byte[] observedMarkerDigest,
        long observedMarkerBytes,
        byte[] observedMarkerVersion,
        RequestKind requestKind,
        byte[] requestVersion,
        byte[] providerRequestIdHash,
        byte[] responseHash,
        TrustedUtcIntervalEvidence observedAt) {
    public static final int MAX_BYTES =
            TargetEvidenceRetiredMarker.MAX_BYTES + TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES + 512;

    public enum RequestKind {
        CAS_PAYLOAD_VERSION(1),
        CLAIM_ABSENT_KEY(2);
        private final int wire;

        RequestKind(int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static RequestKind decode(int wire) {
            for (var value : values()) {
                if (value.wire == wire) {
                    return value;
                }
            }
            throw new IllegalArgumentException("unknown marker conditional request kind");
        }
    }

    public TargetEvidenceRetiredMarkerEvidence {
        Objects.requireNonNull(marker);
        Objects.requireNonNull(requestKind);
        Objects.requireNonNull(observedAt);
        observedMarkerDigest = hash(observedMarkerDigest);
        observedMarkerVersion = Bytes.copy(Objects.requireNonNull(observedMarkerVersion));
        requestVersion = Bytes.copy(Objects.requireNonNull(requestVersion));
        providerRequestIdHash = hash(providerRequestIdHash);
        responseHash = hash(responseHash);
        final byte[] actual = marker.canonicalBytes();
        if (observedMarkerBytes != actual.length
                || !Arrays.equals(observedMarkerDigest, Bytes.sha256(actual))
                || observedMarkerVersion.length != 8
                || observedAt.canonicalBytes().length > TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES
                || Arrays.equals(observedMarkerVersion, marker.payloadVersion())
                || (requestKind == RequestKind.CAS_PAYLOAD_VERSION
                        ? !Arrays.equals(requestVersion, marker.payloadVersion())
                        : requestVersion.length != 0)) {
            throw new IllegalArgumentException("marker observation changed actual raw/version/conditional operation");
        }
    }

    @Override
    public byte[] observedMarkerDigest() {
        return Bytes.copy(observedMarkerDigest);
    }

    @Override
    public byte[] observedMarkerVersion() {
        return Bytes.copy(observedMarkerVersion);
    }

    @Override
    public byte[] requestVersion() {
        return Bytes.copy(requestVersion);
    }

    @Override
    public byte[] providerRequestIdHash() {
        return Bytes.copy(providerRequestIdHash);
    }

    @Override
    public byte[] responseHash() {
        return Bytes.copy(responseHash);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, marker.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, observedMarkerDigest);
            CanonicalProtobuf.uint64(out, 4, observedMarkerBytes);
            CanonicalProtobuf.bytes(out, 5, observedMarkerVersion);
            CanonicalProtobuf.uint32(out, 6, requestKind.wire);
            CanonicalProtobuf.bytes(out, 7, requestVersion);
            CanonicalProtobuf.bytes(out, 8, providerRequestIdHash);
            CanonicalProtobuf.bytes(out, 9, responseHash);
            CanonicalProtobuf.bytes(out, 10, observedAt.canonicalBytes());
        });
        if (encoded.length > MAX_BYTES) {
            throw new IllegalArgumentException("marker evidence exceeds finite bound");
        }
        return encoded;
    }

    public static TargetEvidenceRetiredMarkerEvidence decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_BYTES, 10, false, "retained marker evidence");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "retained marker evidence");
        if (QueryCodecSupport.uint(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown retained marker evidence schema");
        }
        final var evidence = new TargetEvidenceRetiredMarkerEvidence(
                TargetEvidenceRetiredMarker.decode(QueryCodecSupport.nested(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.uint(f.get(3), 4),
                QueryCodecSupport.fixed(f.get(4), 5, 8),
                RequestKind.decode(QueryCodecSupport.uint32(f.get(5), 6)),
                QueryCodecSupport.bytes(f.get(6), 7),
                QueryCodecSupport.fixed(f.get(7), 8, 32),
                QueryCodecSupport.fixed(f.get(8), 9, 32),
                TargetEvidenceDeleteConfirmedBody.decodeTime(QueryCodecSupport.nested(f.get(9), 10)));
        QueryCodecSupport.requireCanonical(encoded, evidence.canonicalBytes(), "retained marker evidence");
        return evidence;
    }

    private static byte[] hash(byte[] value) {
        return TargetCompatibilityCodec.assigned(value, 32, "retained marker observation");
    }
}
