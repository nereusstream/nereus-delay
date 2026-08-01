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
}
