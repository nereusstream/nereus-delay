package io.nereusstream.delay.gateway;

import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.transport.Digest32;
import io.oxia.client.api.GetResult;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real Oxia coverage for the digest-only Gateway audit sink. */
@Tag("real-service")
class OxiaRealGatewayAuditSinkSmokeTest {
    @Test
    void auditEventIsDurableAndExactlyDeduplicatedAgainstRealService() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-gateway/" + UUID.randomUUID();
        final GatewayAuditEventV1 event = event();

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-real-gateway-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/client")) {
            final OxiaGatewayAuditSink sink = new OxiaGatewayAuditSink(client, prefix);
            sink.record(event);
            sink.record(event);

            final List<GetResult> records;
            try (var scan = client.client().rangeScan(prefix + "/audit/", prefix + "/audit/\uffff")) {
                records = StreamSupport.stream(scan.spliterator(), false).toList();
            }
            assertEquals(1, records.size());
            assertTrue(records.get(0).key().startsWith(prefix + "/audit/"));
            assertArrayEquals(event.canonicalBytes(), records.get(0).value());
        }
    }

    private static GatewayAuditEventV1 event() {
        return new GatewayAuditEventV1(GatewayIngressOperationV1.SCHEDULE,
                new Digest32(Bytes.sha256(Bytes.utf8("gateway-key"))),
                new Digest32(Bytes.sha256(Bytes.utf8("request-body"))),
                GatewayAuditPhaseV1.COMPLETED,
                new Digest32(Bytes.sha256(Bytes.utf8("outcome"))),
                System.currentTimeMillis());
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
