package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaGatewayAdmissionControllerTest {
    @Test
    void admissionRecordRoundTripsAndRejectsTampering() {
        final Digest32 tenant = new Digest32(bytes(32, 1));
        final GatewayAdmissionRecordV1 record = new GatewayAdmissionRecordV1(tenant, 4, List.of(
                new GatewayAdmissionRecordV1.Lease(bytes(16, 2), GatewayIngressOperationV1.SCHEDULE, 77, 900),
                new GatewayAdmissionRecordV1.Lease(bytes(16, 3), GatewayIngressOperationV1.CONTROL, 9, 901)));
        final byte[] encoded = record.canonicalBytes();
        final GatewayAdmissionRecordV1 decoded = GatewayAdmissionRecordV1.decode(encoded);

        assertArrayEquals(encoded, decoded.canonicalBytes());
        assertEquals(2, decoded.leases().size());
        encoded[encoded.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> GatewayAdmissionRecordV1.decode(encoded));
    }

    @Test
    void durableAdmissionSeparatesPoolsAndReleasesIdempotently() {
        final MutableClock clock = new MutableClock(100);
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayAdmissionController controller = new OxiaGatewayAdmissionController(client, "/nereus/gateway",
                clock, new OxiaGatewayAdmissionController.Limits(1, 100, 1, 1, 1_000, 4));
        final AuthenticatedTenantContext tenant = tenant(10);

        final GatewayAdmissionLease schedule = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 60);
        final GatewayAdmissionLease retry = reserve(controller, tenant, GatewayIngressOperationV1.RETRY_UNCERTAIN, 1);
        final GatewayAdmissionLease control = reserve(controller, tenant, GatewayIngressOperationV1.CONTROL, 1);
        assertEquals(GatewayAdmissionController.State.REJECTED,
                controller.reserve(new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 1))
                        .state());
        assertEquals(io.nereusstream.delay.protocol.StableCode.ADMISSION_CAPACITY_GATED,
                controller.reserve(new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 1))
                        .rejectionCode());

        schedule.close();
        final GatewayAdmissionLease replacement = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 90);
        replacement.close();
        control.close();
        retry.close();
        retry.close();
        assertEquals(0, client.leases(tenant).size());
    }

    @Test
    void durableAdmissionReportsHardScheduleBytesAndReclaimsExpiredLease() {
        final MutableClock clock = new MutableClock(100);
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayAdmissionController controller = new OxiaGatewayAdmissionController(client, "/nereus/gateway",
                clock, new OxiaGatewayAdmissionController.Limits(2, 100, 1, 1, 10, 4));
        final AuthenticatedTenantContext tenant = tenant(20);

        final GatewayAdmissionLease first = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 80);
        final GatewayAdmissionController.Decision tooLarge = controller.reserve(
                new GatewayAdmissionRequestV1(tenant, GatewayIngressOperationV1.SCHEDULE, 30));
        assertEquals(io.nereusstream.delay.protocol.StableCode.HARD_QUOTA_EXCEEDED, tooLarge.rejectionCode());

        clock.value = 110;
        final GatewayAdmissionLease afterExpiry = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 100);
        first.close();
        afterExpiry.close();
        assertEquals(0, client.leases(tenant).size());
    }

    @Test
    void responseLossIsAcceptedOnlyAfterExactRereadForReserveAndRelease() {
        final MutableClock clock = new MutableClock(100);
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayAdmissionController controller = new OxiaGatewayAdmissionController(client, "/nereus/gateway",
                clock, new OxiaGatewayAdmissionController.Limits(1, 100, 1, 1, 1_000, 4));
        final AuthenticatedTenantContext tenant = tenant(30);

        client.loseNextPutResponse = true;
        final GatewayAdmissionLease lease = reserve(controller, tenant, GatewayIngressOperationV1.SCHEDULE, 8);
        assertEquals(1, client.leases(tenant).size());

        client.loseNextPutResponse = true;
        lease.close();
        assertEquals(0, client.leases(tenant).size());
    }

    @Test
    void tenantsUseIndependentDurableRecords() {
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayAdmissionController controller = new OxiaGatewayAdmissionController(client, "/nereus/gateway",
                () -> 100, new OxiaGatewayAdmissionController.Limits(1, 100, 1, 1, 1_000, 4));

        final GatewayAdmissionLease first = reserve(controller, tenant(40), GatewayIngressOperationV1.SCHEDULE, 100);
        final GatewayAdmissionLease second = reserve(controller, tenant(41), GatewayIngressOperationV1.SCHEDULE, 100);
        first.close();
        second.close();
    }

    private static GatewayAdmissionLease reserve(final OxiaGatewayAdmissionController controller,
                                                 final AuthenticatedTenantContext tenant,
                                                 final GatewayIngressOperationV1 operation,
                                                 final long bytes) {
        final GatewayAdmissionController.Decision decision = controller.reserve(
                new GatewayAdmissionRequestV1(tenant, operation, bytes));
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

    private static final class FakeGatewayClient implements OxiaGatewayRecordClient {
        private final Map<String, Stored> records = new TreeMap<>();
        private long nextVersion = 1;
        private boolean loseNextPutResponse;

        @Override
        public GetResult get(final String key) {
            final Stored stored = records.get(key);
            return stored == null ? null : new GetResult(key, Bytes.copy(stored.value()), stored.version());
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            final Stored existing = records.get(key);
            final OptionVersionId expected = options.stream().filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast).findFirst().orElse(null);
            if (expected != null && expected.versionId() == OptionVersionId.KEY_NOT_EXISTS && existing != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (expected != null && expected.versionId() != OptionVersionId.KEY_NOT_EXISTS
                    && (existing == null || existing.version().versionId() != expected.versionId())) {
                throw new UnexpectedVersionIdException(key, expected.versionId());
            }
            final Version version = new Version(nextVersion++, 0, 0, 0, Optional.empty(), Optional.empty());
            records.put(key, new Stored(Bytes.copy(value), version));
            if (loseNextPutResponse) {
                loseNextPutResponse = false;
                throw new IllegalStateException("simulated response loss");
            }
            return new PutResult(key, version);
        }

        @Override
        public void close() {
        }

        private List<GatewayAdmissionRecordV1.Lease> leases(final AuthenticatedTenantContext tenant) {
            final String key = "/nereus/gateway/admission/"
                    + Bytes.hex(new Digest32(tenant.authenticatedTenantScopeHash()).bytes());
            final Stored stored = records.get(key);
            return stored == null ? List.of() : new ArrayList<>(GatewayAdmissionRecordV1.decode(stored.value()).leases());
        }

        private record Stored(byte[] value, Version version) {
        }
    }
}
