package io.nereusstream.delay.gateway;

import io.nereusstream.delay.adapter.WireIngressOutcomeSupport;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SubmissionOutcomeKindV1;
import io.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.semantic.LargeSchedulePreparationV1;
import io.nereusstream.delay.semantic.TrustedClock;
import io.nereusstream.delay.transport.Digest32;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class OxiaGatewayIdempotencyStoreTest {
    @Test
    void gatewayRecordAndAttemptCodecsRoundTripCanonicalBytes() {
        final TrustedClock clock = () -> 100;
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final PreparedSubmissionV1 prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 1));
        final Digest32 bodyHash = new Digest32(bytes(32, 2));

        store.prepareIfAbsent(keyHash, GatewayOperationKindV1.SCHEDULE, bodyHash, prepared, 800);
        final InMemoryGatewayIdempotencyStoreAdapter adapter = new InMemoryGatewayIdempotencyStoreAdapter(store,
                keyHash, prepared);
        final GatewayIdempotencyRecordV1 decoded = GatewayIdempotencyRecordV1.decode(
                adapter.record().canonicalBytes());

        assertArrayEquals(adapter.record().canonicalBytes(), decoded.canonicalBytes());
        assertEquals(1, decoded.attempts().size());
        assertArrayEquals(decoded.attempts().get(0).canonicalBytes(),
                GatewayPhysicalAttemptV1.decode(decoded.attempts().get(0).canonicalBytes()).canonicalBytes());
    }

    @Test
    void oxiaStoreReopensExactRecordAndDoesNotRecreatePermitAfterResponseLoss() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store = new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock,
                10, 20);
        final PreparedSubmissionV1 prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 11));
        final Digest32 bodyHash = new Digest32(bytes(32, 12));
        store.prepareIfAbsent(keyHash, GatewayOperationKindV1.SCHEDULE, bodyHash, prepared, 800);
        final var started = store.startAttempt(keyHash);
        final SubmissionOutcomeMessageV1 uncertain = GatewayOutcomeSupport.uncertain(prepared,
                started.permit().physicalAttemptId());
        store.finish(keyHash, started.permit().physicalAttemptId(), uncertain);

        final OxiaGatewayIdempotencyStore reopened = new OxiaGatewayIdempotencyStore(client, "/nereus/gateway",
                clock, 10, 20);
        assertArrayEquals(store.exact(keyHash).canonicalBytes(), reopened.exact(keyHash).canonicalBytes());
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 91));
        client.loseNextPutResponse = true;
        final var retried = reopened.startRetry(keyHash, started.permit().physicalAttemptId(), retryRequestId);

        assertEquals(GatewayIdempotencyStore.RetryState.EXISTING_RETRY, retried.state());
        assertNull(retried.permit());
        assertEquals(2, reopened.exact(keyHash).attempts().size());
    }

    @Test
    void attemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit() {
        final long[] now = {100};
        final TrustedClock clock = () -> now[0];
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store = new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock,
                10, 20);
        final PreparedSubmissionV1 prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 21));
        final Digest32 bodyHash = new Digest32(bytes(32, 22));
        store.prepareIfAbsent(keyHash, GatewayOperationKindV1.SCHEDULE, bodyHash, prepared, 800);

        client.loseNextPutResponse = true;
        final GatewayIdempotencyStore.AttemptStart responseLost = store.startAttempt(keyHash);
        assertNull(responseLost.permit());
        assertEquals(GatewayIdempotencyPhaseV1.ACTIVE, responseLost.record().phase());
        assertNull(responseLost.record().aggregateOutcomeBytes());
        assertEquals(GatewayPhysicalAttemptStateV1.STARTED, responseLost.record().attempts().get(0).state());

        now[0] = 119;
        final GatewayIdempotencyStore.AttemptStart beforeDeadline = store.startAttempt(keyHash);
        assertNull(beforeDeadline.permit());
        assertEquals(GatewayIdempotencyPhaseV1.ACTIVE, beforeDeadline.record().phase());
        assertNull(beforeDeadline.record().aggregateOutcomeBytes());

        now[0] = 120;
        final GatewayIdempotencyStore.AttemptStart recovered = store.startAttempt(keyHash);
        assertNull(recovered.permit());
        assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, recovered.record().phase());
        assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                recovered.record().attempts().get(0).state());
        assertNotNull(recovered.record().aggregateOutcomeBytes());
        assertEquals(SubmissionOutcomeKindV1.MANAGED,
                SubmissionOutcomeMessageV1.decode(recovered.record().aggregateOutcomeBytes()).kind());
    }

    @Test
    void retryAttemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit() {
        final long[] now = {100};
        final TrustedClock clock = () -> now[0];
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store = new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock,
                10, 20);
        final PreparedSubmissionV1 prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 31));
        final Digest32 bodyHash = new Digest32(bytes(32, 32));
        store.prepareIfAbsent(keyHash, GatewayOperationKindV1.SCHEDULE, bodyHash, prepared, 800);
        final GatewayIdempotencyStore.AttemptStart first = store.startAttempt(keyHash);
        final SubmissionOutcomeMessageV1 uncertain = GatewayOutcomeSupport.uncertain(prepared,
                first.permit().physicalAttemptId());
        store.finish(keyHash, first.permit().physicalAttemptId(), uncertain);
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 93));

        client.loseNextPutResponse = true;
        final GatewayIdempotencyStore.RetryStart responseLost = store.startRetry(keyHash,
                first.permit().physicalAttemptId(), retryRequestId);
        assertEquals(GatewayIdempotencyStore.RetryState.EXISTING_RETRY, responseLost.state());
        assertNull(responseLost.permit());
        assertEquals(GatewayIdempotencyPhaseV1.ACTIVE, responseLost.record().phase());
        assertEquals(GatewayPhysicalAttemptStateV1.STARTED,
                responseLost.record().attempts().get(1).state());

        now[0] = 120;
        final GatewayIdempotencyStore.RetryStart recovered = store.startRetry(keyHash,
                first.permit().physicalAttemptId(), retryRequestId);
        assertEquals(GatewayIdempotencyStore.RetryState.EXISTING_RETRY, recovered.state());
        assertNull(recovered.permit());
        assertEquals(GatewayIdempotencyPhaseV1.QUIESCENT, recovered.record().phase());
        assertEquals(GatewayPhysicalAttemptStateV1.UNCERTAIN,
                recovered.record().attempts().get(1).state());
        assertNotNull(recovered.record().aggregateOutcomeBytes());
    }

    private static PreparedSubmissionV1 prepared() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.scheduleV1(shard, schedule(), 600);
        return PreparedSubmissionV1.managed(CommandCodec.encodeFrameV1(command));
    }

    private static ScheduleIntentV1 schedule() {
        return ScheduleIntentV1.create(new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60),
                        ProfileKindV1.DESTINATION), new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300, 800, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("key"),
                Bytes.utf8("payload"), null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                null, null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class InMemoryGatewayIdempotencyStoreAdapter {
        private final GatewayIdempotencyRecordV1 record;

        private InMemoryGatewayIdempotencyStoreAdapter(final InMemoryGatewayIdempotencyStore store,
                                                       final Digest32 keyHash, final PreparedSubmissionV1 prepared) {
            final var started = store.startAttempt(keyHash);
            final SubmissionOutcomeMessageV1 outcome = GatewayOutcomeSupport.uncertain(prepared,
                    started.permit().physicalAttemptId());
            record = store.finish(keyHash, started.permit().physicalAttemptId(), outcome);
        }

        private GatewayIdempotencyRecordV1 record() {
            return record;
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

        private record Stored(byte[] value, Version version) {
        }
    }
}
