package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.TrustedClock;
import io.oxia.client.api.GetResult;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in real Oxia coverage for the tenant-scoped Gateway admission CAS record. */
@Tag("real-service")
class OxiaRealGatewayAdmissionSmokeTest {
    @Test
    void admissionPoolsAndExpiryWorkAgainstRealService() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-admission/" + UUID.randomUUID();
        final MutableClock clock = new MutableClock(100);
        final AuthenticatedTenantContext tenant = tenant(11);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = OxiaSyncOwnerLeaseBackend.connect(
                endpoint,
                namespace,
                "nereus-delay-real-admission-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                prefix + "/client")) {
            final OxiaGatewayAdmissionController controller = new OxiaGatewayAdmissionController(
                    client, prefix, clock, new OxiaGatewayAdmissionController.Limits(1, 100, 1, 1, 10, 8));
            final GatewayAdmissionLease first = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 80);
            assertEquals(
                    GatewayAdmissionController.State.REJECTED,
                    controller
                            .reserve(new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 1))
                            .state());

            clock.value = 110;
            final GatewayAdmissionLease replacement =
                    reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 100);
            first.close();
            replacement.close();

            final List<GetResult> records;
            try (var scan = client.client().rangeScan(prefix + "/admission/", prefix + "/admission/\uffff")) {
                records = StreamSupport.stream(scan.spliterator(), false).toList();
            }
            assertEquals(1, records.size());
            assertEquals(
                    0,
                    GatewayAdmissionRecordV1.decode(records.get(0).value())
                            .leases()
                            .size());
        }
    }

    private static GatewayAdmissionLease reserve(
            final OxiaGatewayAdmissionController controller,
            final AuthenticatedTenantContext tenant,
            final GatewayIngressOperationV1 operation,
            final long bytes) {
        final GatewayAdmissionController.Decision decision =
                controller.reserve(new GatewayAdmissionRequestV1(tenant, operation, bytes));
        assertEquals(GatewayAdmissionController.State.ACCEPTED, decision.state());
        return decision.lease();
    }

    private static AuthenticatedTenantContext tenant(final int seed) {
        return new AuthenticatedTenantContext(bytes(32, seed), bytes(32, seed + 1), bytes(32, seed + 2));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static final class MutableClock implements TrustedClock {
        private long value;

        private MutableClock(final long value) {
            this.value = value;
        }

        @Override
        public long nowEpochMs() {
            return value;
        }
    }
}
