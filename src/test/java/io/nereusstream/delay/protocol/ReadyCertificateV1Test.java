package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReadyCertificateV1Test {
    @Test
    void exposesTheAdmissionCertificateThroughOneCanonicalValidator() {
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.create(
                new ShardId(RouteIncarnation.random(), 0));
        final byte[] certificate = PublishAdmissionBody.decode(fixture.body()).readyCertificate().canonicalBytes();
        final ReadyCertificateV1 decoded = ReadyCertificateV1.decode(certificate);
        assertEquals(8_000, decoded.validUntilEpochMs());
        assertEquals(decoded, ReadyCertificateV1.decode(decoded.canonicalBytes()));
    }

    @Test
    void rejectsCertificateLaneDriftAndDigestTampering() {
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.create(
                new ShardId(RouteIncarnation.random(), 0));
        final byte[] certificate = PublishAdmissionBody.decode(fixture.body()).readyCertificate().canonicalBytes();
        final byte[] tampered = certificate.clone();
        tampered[tampered.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ReadyCertificateV1.decode(tampered));
    }

    @Test
    void rejectsCertificateThatOutlivesTheChannelLease() {
        final PublishAdmissionBodyTest.Fixture fixture = PublishAdmissionBodyTest.Fixture.create(
                new ShardId(RouteIncarnation.random(), 0));
        final byte[] certificate = PublishAdmissionBody.decode(fixture.body()).readyCertificate().canonicalBytes();
        final byte[] outlivingLease = rewriteValidUntil(certificate, 9_001);

        assertThrows(IllegalArgumentException.class, () -> ReadyCertificateV1.decode(outlivingLease));
    }

    private static byte[] rewriteValidUntil(final byte[] encoded, final long validUntil) {
        final java.util.List<CanonicalProtobuf.Reader.Field> fields = new java.util.ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.number() > 15) {
                    break;
                }
                if (field.number() == 11) {
                    CanonicalProtobuf.int64(output, 11, validUntil);
                } else {
                    write(output, field);
                }
            }
        });
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix);
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader prefixReader = new CanonicalProtobuf.Reader(prefix);
            while (prefixReader.hasRemaining()) {
                write(output, prefixReader.next());
            }
            CanonicalProtobuf.bytes(output, 16, digest);
        });
    }

    private static void write(final java.io.ByteArrayOutputStream output,
                              final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }
}
