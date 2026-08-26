package com.nereusstream.delay.gateway;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SubmissionOutcomeKind;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.transport.Digest32;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OxiaGatewayIdempotencyStoreTest {
    @Test
    void gatewayRecordAndAttemptCodecsRoundTripCanonicalBytes() {
        final TrustedClock clock = () -> 100;
        final InMemoryGatewayIdempotencyStore store = new InMemoryGatewayIdempotencyStore(clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 1));
        final Digest32 bodyHash = new Digest32(bytes(32, 2));

        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, bodyHash, prepared, 800);
        final InMemoryGatewayIdempotencyStoreAdapter adapter =
                new InMemoryGatewayIdempotencyStoreAdapter(store, keyHash, prepared);
        final GatewayIdempotencyRecord decoded =
                GatewayIdempotencyRecord.decode(adapter.record().canonicalBytes());

        assertArrayEquals(adapter.record().canonicalBytes(), decoded.canonicalBytes());
        assertEquals(1, decoded.attempts().size());
        assertArrayEquals(
                decoded.attempts().get(0).canonicalBytes(),
                GatewayPhysicalAttempt.decode(decoded.attempts().get(0).canonicalBytes())
                        .canonicalBytes());
    }

    @Test
    void gatewayProjectionRejectsImpossibleAttemptAndRecordShapes() {
        final PhysicalEnqueueAttemptId attemptId = PhysicalEnqueueAttemptId.require(bytes(16, 4));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        1, attemptId, GatewayPhysicalAttemptState.STARTED, bytes(1, 5), 100, 120, 2, 110));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        1, attemptId, GatewayPhysicalAttemptState.UNCERTAIN, null, 100, 120, 2, 110));

        final PreparedSubmission prepared = prepared();
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 6)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 7)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.QUIESCENT,
                        List.of(),
                        null,
                        100,
                        200,
                        1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 25)),
                        GatewayOperationKind.CANCEL,
                        new Digest32(bytes(32, 26)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.PREPARED,
                        List.of(),
                        null,
                        100,
                        200,
                        1));

        final SubmissionOutcomeMessage uncertain = GatewayOutcomeSupport.uncertain(prepared, attemptId);
        final GatewayPhysicalAttempt first = new GatewayPhysicalAttempt(
                1, attemptId, GatewayPhysicalAttemptState.UNCERTAIN, uncertain.canonicalBytes(), 100, 120, 2, 110);
        final GatewayPhysicalAttempt duplicate = new GatewayPhysicalAttempt(
                2,
                attemptId,
                GatewayPhysicalAttemptState.UNCERTAIN,
                uncertain.canonicalBytes(),
                100,
                120,
                PhysicalEnqueueAttemptId.require(bytes(16, 36)),
                new Digest32(bytes(32, 37)),
                3,
                110);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 8)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 9)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.QUIESCENT,
                        List.of(first, duplicate),
                        uncertain.canonicalBytes(),
                        100,
                        200,
                        3));

        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        1, attemptId, GatewayPhysicalAttemptState.STARTED, null, 100, 100, 2, 110));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        1, attemptId, GatewayPhysicalAttemptState.STARTED, null, 100, 120, 2, 100));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        1,
                        attemptId,
                        GatewayPhysicalAttemptState.STARTED,
                        null,
                        100,
                        120,
                        PhysicalEnqueueAttemptId.require(bytes(16, 38)),
                        new Digest32(bytes(32, 39)),
                        2,
                        110));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayPhysicalAttempt(
                        2,
                        PhysicalEnqueueAttemptId.require(bytes(16, 40)),
                        GatewayPhysicalAttemptState.STARTED,
                        null,
                        100,
                        120,
                        2,
                        110));

        final PhysicalEnqueueAttemptId firstStartedId = PhysicalEnqueueAttemptId.require(bytes(16, 27));
        final PhysicalEnqueueAttemptId secondStartedId = PhysicalEnqueueAttemptId.require(bytes(16, 28));
        final GatewayPhysicalAttempt firstStarted = new GatewayPhysicalAttempt(
                1, firstStartedId, GatewayPhysicalAttemptState.STARTED, null, 100, 120, 2, 110);
        final GatewayPhysicalAttempt secondStarted = new GatewayPhysicalAttempt(
                2,
                secondStartedId,
                GatewayPhysicalAttemptState.STARTED,
                null,
                121,
                140,
                PhysicalEnqueueAttemptId.require(bytes(16, 41)),
                new Digest32(bytes(32, 42)),
                3,
                130);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 29)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 30)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.ACTIVE,
                        List.of(firstStarted, secondStarted),
                        null,
                        100,
                        200,
                        3));

        final Digest32 nonFinalStartedKey = new Digest32(bytes(32, 31));
        final PhysicalEnqueueAttemptId nonFinalStartedId = PhysicalEnqueueAttemptId.require(bytes(16, 32));
        final PhysicalEnqueueAttemptId laterAttemptId = PhysicalEnqueueAttemptId.require(bytes(16, 33));
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 34));
        final SubmissionOutcomeMessage laterUncertain = GatewayOutcomeSupport.uncertain(prepared, laterAttemptId);
        final GatewayPhysicalAttempt laterUncertainAttempt = new GatewayPhysicalAttempt(
                2,
                laterAttemptId,
                GatewayPhysicalAttemptState.UNCERTAIN,
                laterUncertain.canonicalBytes(),
                121,
                140,
                retryRequestId,
                GatewayIdempotencyHash.retryRequestHash(nonFinalStartedKey, nonFinalStartedId, retryRequestId),
                3,
                130);
        final GatewayPhysicalAttempt nonFinalStarted = new GatewayPhysicalAttempt(
                1, nonFinalStartedId, GatewayPhysicalAttemptState.STARTED, null, 100, 120, 2, 110);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        nonFinalStartedKey,
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 35)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.ACTIVE,
                        List.of(nonFinalStarted, laterUncertainAttempt),
                        laterUncertain.canonicalBytes(),
                        100,
                        200,
                        3));

        final Digest32 queuedTailKey = new Digest32(bytes(32, 43));
        final PhysicalEnqueueAttemptId queuedTailFirstId = PhysicalEnqueueAttemptId.require(bytes(16, 44));
        final PhysicalEnqueueAttemptId queuedTailSecondId = PhysicalEnqueueAttemptId.require(bytes(16, 45));
        final PhysicalEnqueueAttemptId queuedTailRetryId = PhysicalEnqueueAttemptId.require(bytes(16, 46));
        final SubmissionOutcomeMessage queuedTailUncertain =
                GatewayOutcomeSupport.uncertain(prepared, queuedTailFirstId);
        final SubmissionOutcomeMessage queuedTailQueued = queued(prepared, queuedTailSecondId);
        final GatewayPhysicalAttempt queuedTailFirst = new GatewayPhysicalAttempt(
                1,
                queuedTailFirstId,
                GatewayPhysicalAttemptState.UNCERTAIN,
                queuedTailUncertain.canonicalBytes(),
                100,
                120,
                2,
                110);
        final GatewayPhysicalAttempt queuedTailSecond = new GatewayPhysicalAttempt(
                2,
                queuedTailSecondId,
                GatewayPhysicalAttemptState.QUEUED,
                queuedTailQueued.canonicalBytes(),
                121,
                140,
                queuedTailRetryId,
                GatewayIdempotencyHash.retryRequestHash(queuedTailKey, queuedTailFirstId, queuedTailRetryId),
                3,
                130);
        final GatewayPhysicalAttempt queuedTailStarted = new GatewayPhysicalAttempt(
                3,
                PhysicalEnqueueAttemptId.require(bytes(16, 47)),
                GatewayPhysicalAttemptState.STARTED,
                null,
                141,
                160,
                PhysicalEnqueueAttemptId.require(bytes(16, 48)),
                GatewayIdempotencyHash.retryRequestHash(
                        queuedTailKey, queuedTailFirstId, PhysicalEnqueueAttemptId.require(bytes(16, 48))),
                4,
                150);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        queuedTailKey,
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 49)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.ACTIVE,
                        List.of(queuedTailFirst, queuedTailSecond, queuedTailStarted),
                        queuedTailQueued.canonicalBytes(),
                        100,
                        200,
                        4));
    }

    @Test
    void gatewayProjectionRejectsOutcomeStateAndAggregateMismatches() {
        final PreparedSubmission prepared = prepared();
        final PhysicalEnqueueAttemptId attemptId = PhysicalEnqueueAttemptId.require(bytes(16, 14));
        final SubmissionOutcomeMessage uncertain = GatewayOutcomeSupport.uncertain(prepared, attemptId);
        final GatewayPhysicalAttempt stateMismatch = new GatewayPhysicalAttempt(
                1,
                attemptId,
                GatewayPhysicalAttemptState.DEFINITELY_NOT_QUEUED,
                uncertain.canonicalBytes(),
                100,
                120,
                2,
                110);

        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 15)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 16)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.QUIESCENT,
                        List.of(stateMismatch),
                        uncertain.canonicalBytes(),
                        100,
                        200,
                        1));

        final GatewayPhysicalAttempt valid = new GatewayPhysicalAttempt(
                1, attemptId, GatewayPhysicalAttemptState.UNCERTAIN, uncertain.canonicalBytes(), 100, 120, 2, 110);
        final SubmissionOutcomeMessage foreignAggregate =
                GatewayOutcomeSupport.uncertain(prepared, PhysicalEnqueueAttemptId.require(bytes(16, 17)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 18)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 19)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.QUIESCENT,
                        List.of(valid),
                        foreignAggregate.canonicalBytes(),
                        100,
                        200,
                        1));

        final PhysicalEnqueueAttemptId retryPhysicalId = PhysicalEnqueueAttemptId.require(bytes(16, 20));
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 21));
        final GatewayPhysicalAttempt retry = new GatewayPhysicalAttempt(
                2,
                retryPhysicalId,
                GatewayPhysicalAttemptState.STARTED,
                null,
                121,
                140,
                retryRequestId,
                new Digest32(bytes(32, 22)),
                4,
                130);
        assertThrows(
                IllegalArgumentException.class,
                () -> new GatewayIdempotencyRecord(
                        new Digest32(bytes(32, 23)),
                        GatewayOperationKind.SCHEDULE,
                        new Digest32(bytes(32, 24)),
                        prepared.canonicalBytes(),
                        GatewayIdempotencyPhase.ACTIVE,
                        List.of(valid, retry),
                        null,
                        100,
                        200,
                        4));
    }

    @Test
    void oxiaStoreReopensExactRecordAndDoesNotRecreatePermitAfterResponseLoss() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 11));
        final Digest32 bodyHash = new Digest32(bytes(32, 12));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, bodyHash, prepared, 800);
        final var started = store.startAttempt(keyHash);
        final SubmissionOutcomeMessage uncertain =
                GatewayOutcomeSupport.uncertain(prepared, started.permit().physicalAttemptId());
        store.finish(keyHash, started.permit().physicalAttemptId(), uncertain);

        final OxiaGatewayIdempotencyStore reopened =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        assertArrayEquals(
                store.exact(keyHash).canonicalBytes(), reopened.exact(keyHash).canonicalBytes());
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
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 21));
        final Digest32 bodyHash = new Digest32(bytes(32, 22));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, bodyHash, prepared, 800);

        client.loseNextPutResponse = true;
        final GatewayIdempotencyStore.AttemptStart responseLost = store.startAttempt(keyHash);
        assertNull(responseLost.permit());
        assertEquals(GatewayIdempotencyPhase.ACTIVE, responseLost.record().phase());
        assertNull(responseLost.record().aggregateOutcomeBytes());
        assertEquals(
                GatewayPhysicalAttemptState.STARTED,
                responseLost.record().attempts().get(0).state());

        now[0] = 119;
        final GatewayIdempotencyStore.AttemptStart beforeDeadline = store.startAttempt(keyHash);
        assertNull(beforeDeadline.permit());
        assertEquals(GatewayIdempotencyPhase.ACTIVE, beforeDeadline.record().phase());
        assertNull(beforeDeadline.record().aggregateOutcomeBytes());

        now[0] = 120;
        final GatewayIdempotencyStore.AttemptStart recovered = store.startAttempt(keyHash);
        assertNull(recovered.permit());
        assertEquals(GatewayIdempotencyPhase.QUIESCENT, recovered.record().phase());
        assertEquals(
                GatewayPhysicalAttemptState.UNCERTAIN,
                recovered.record().attempts().get(0).state());
        assertNotNull(recovered.record().aggregateOutcomeBytes());
        assertEquals(
                SubmissionOutcomeKind.MANAGED,
                SubmissionOutcomeMessage.decode(recovered.record().aggregateOutcomeBytes())
                        .kind());
    }

    @Test
    void expiredPreparedRecordCannotCreateAnAttemptAtTheStoreBoundary() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 35));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, new Digest32(bytes(32, 36)), prepared, 100);
        final byte[] before = store.exact(keyHash).canonicalBytes();

        final GatewayIdempotencyStore.AttemptStart expired = store.startAttempt(keyHash);

        assertNull(expired.permit());
        assertArrayEquals(before, store.exact(keyHash).canonicalBytes());
        assertEquals(GatewayIdempotencyPhase.PREPARED, expired.record().phase());
        assertEquals(0, expired.record().attempts().size());
    }

    @Test
    void retryAttemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit() {
        final long[] now = {100};
        final TrustedClock clock = () -> now[0];
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 31));
        final Digest32 bodyHash = new Digest32(bytes(32, 32));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, bodyHash, prepared, 800);
        final GatewayIdempotencyStore.AttemptStart first = store.startAttempt(keyHash);
        final SubmissionOutcomeMessage uncertain =
                GatewayOutcomeSupport.uncertain(prepared, first.permit().physicalAttemptId());
        store.finish(keyHash, first.permit().physicalAttemptId(), uncertain);
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 93));

        client.loseNextPutResponse = true;
        final GatewayIdempotencyStore.RetryStart responseLost =
                store.startRetry(keyHash, first.permit().physicalAttemptId(), retryRequestId);
        assertEquals(GatewayIdempotencyStore.RetryState.EXISTING_RETRY, responseLost.state());
        assertNull(responseLost.permit());
        assertEquals(GatewayIdempotencyPhase.ACTIVE, responseLost.record().phase());
        assertNull(responseLost.record().aggregateOutcomeBytes());
        assertEquals(
                GatewayPhysicalAttemptState.STARTED,
                responseLost.record().attempts().get(1).state());

        now[0] = 120;
        final GatewayIdempotencyStore.RetryStart recovered =
                store.startRetry(keyHash, first.permit().physicalAttemptId(), retryRequestId);
        assertEquals(GatewayIdempotencyStore.RetryState.EXISTING_RETRY, recovered.state());
        assertNull(recovered.permit());
        assertEquals(GatewayIdempotencyPhase.QUIESCENT, recovered.record().phase());
        assertEquals(
                GatewayPhysicalAttemptState.UNCERTAIN,
                recovered.record().attempts().get(1).state());
        assertNotNull(recovered.record().aggregateOutcomeBytes());
    }

    @Test
    void lateQueuedEvidencePromotesUncertainWithoutChangingItsAttemptIdentity() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 41));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, new Digest32(bytes(32, 42)), prepared, 800);

        final GatewayIdempotencyStore.AttemptStart started = store.startAttempt(keyHash);
        final PhysicalEnqueueAttemptId attemptId = started.permit().physicalAttemptId();
        final SubmissionOutcomeMessage uncertain = GatewayOutcomeSupport.uncertain(prepared, attemptId);
        store.finish(keyHash, attemptId, uncertain);

        final SubmissionOutcomeMessage queued = queued(prepared, attemptId);
        final GatewayIdempotencyRecord promoted = store.finish(keyHash, attemptId, queued);

        assertEquals(GatewayIdempotencyPhase.QUIESCENT, promoted.phase());
        assertEquals(
                GatewayPhysicalAttemptState.QUEUED, promoted.attempts().get(0).state());
        assertEquals(
                com.nereusstream.delay.protocol.EnqueueOutcomeKind.QUEUED,
                SubmissionOutcomeMessage.decode(promoted.aggregateOutcomeBytes())
                        .managed()
                        .kind());
        final byte[] beforeIdempotentRetry = promoted.canonicalBytes();
        assertArrayEquals(
                beforeIdempotentRetry, store.finish(keyHash, attemptId, queued).canonicalBytes());
    }

    @Test
    void lateOldAttemptEvidenceCannotRegressTheNewestRetryAggregate() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 51));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, new Digest32(bytes(32, 52)), prepared, 800);

        final GatewayIdempotencyStore.AttemptStart first = store.startAttempt(keyHash);
        final PhysicalEnqueueAttemptId firstId = first.permit().physicalAttemptId();
        final SubmissionOutcomeMessage firstUncertain = GatewayOutcomeSupport.uncertain(prepared, firstId);
        store.finish(keyHash, firstId, firstUncertain);
        final PhysicalEnqueueAttemptId retryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 53));
        final GatewayIdempotencyStore.RetryStart retry = store.startRetry(keyHash, firstId, retryRequestId);
        final PhysicalEnqueueAttemptId retryId = retry.permit().physicalAttemptId();
        final SubmissionOutcomeMessage retryUncertain = GatewayOutcomeSupport.uncertain(prepared, retryId);
        final GatewayIdempotencyRecord newest = store.finish(keyHash, retryId, retryUncertain);

        final byte[] beforeLateEvidence = newest.canonicalBytes();
        assertArrayEquals(
                beforeLateEvidence,
                store.finish(keyHash, firstId, firstUncertain).canonicalBytes());
        assertArrayEquals(beforeLateEvidence, store.exact(keyHash).canonicalBytes());
    }

    @Test
    void retryUsesTheHighestUnresolvedAttemptWhenANewerAttemptIsDefinitive() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 55));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, new Digest32(bytes(32, 56)), prepared, 800);

        final GatewayIdempotencyStore.AttemptStart first = store.startAttempt(keyHash);
        final PhysicalEnqueueAttemptId firstId = first.permit().physicalAttemptId();
        store.finish(keyHash, firstId, GatewayOutcomeSupport.uncertain(prepared, firstId));
        final PhysicalEnqueueAttemptId firstRetryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 57));
        final GatewayIdempotencyStore.RetryStart retry = store.startRetry(keyHash, firstId, firstRetryRequestId);
        final PhysicalEnqueueAttemptId retryId = retry.permit().physicalAttemptId();
        store.finish(
                keyHash,
                retryId,
                GatewayOutcomeSupport.localDefinite(
                        prepared, com.nereusstream.delay.protocol.StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED));

        final PhysicalEnqueueAttemptId secondRetryRequestId = PhysicalEnqueueAttemptId.require(bytes(16, 58));
        final GatewayIdempotencyStore.RetryStart secondRetry = store.startRetry(keyHash, firstId, secondRetryRequestId);

        assertEquals(GatewayIdempotencyStore.RetryState.STARTED, secondRetry.state());
        assertNotNull(secondRetry.permit());
        assertEquals(3, secondRetry.record().attempts().size());
    }

    @Test
    void conflictingTerminalEvidenceAndForeignAttemptIdentityAreRejectedWithoutOverwrite() {
        final TrustedClock clock = () -> 100;
        final FakeGatewayClient client = new FakeGatewayClient();
        final OxiaGatewayIdempotencyStore store =
                new OxiaGatewayIdempotencyStore(client, "/nereus/gateway", clock, 10, 20);
        final PreparedSubmission prepared = prepared();
        final Digest32 keyHash = new Digest32(bytes(32, 61));
        store.prepareIfAbsent(keyHash, GatewayOperationKind.SCHEDULE, new Digest32(bytes(32, 62)), prepared, 800);

        final GatewayIdempotencyStore.AttemptStart started = store.startAttempt(keyHash);
        final PhysicalEnqueueAttemptId attemptId = started.permit().physicalAttemptId();
        final SubmissionOutcomeMessage uncertain = GatewayOutcomeSupport.uncertain(prepared, attemptId);
        store.finish(keyHash, attemptId, uncertain);
        final GatewayIdempotencyRecord promoted = store.finish(keyHash, attemptId, queued(prepared, attemptId));

        assertThrows(
                IllegalStateException.class, () -> store.finish(keyHash, attemptId, queued(prepared, attemptId, 4)));
        assertThrows(
                IllegalStateException.class,
                () -> store.finish(
                        keyHash,
                        attemptId,
                        GatewayOutcomeSupport.uncertain(prepared, PhysicalEnqueueAttemptId.require(bytes(16, 63)))));
        assertArrayEquals(promoted.canonicalBytes(), store.exact(keyHash).canonicalBytes());
    }

    private static SubmissionOutcomeMessage queued(
            final PreparedSubmission prepared, final PhysicalEnqueueAttemptId attemptId) {
        return queued(prepared, attemptId, 3);
    }

    private static SubmissionOutcomeMessage queued(
            final PreparedSubmission prepared, final PhysicalEnqueueAttemptId attemptId, final long offset) {
        final PreparedCommand command = CommandCodec.decodeManagedFrame(prepared.managedFrame());
        final UUID topic = UUID.nameUUIDFromBytes(Bytes.utf8("gateway-late-queued-topic"));
        final KafkaSourcePosition source =
                new KafkaSourcePosition(command.shardId(), "gateway", topic, offset, null, 100);
        final CanonicalCommandQueuedReceipt.KafkaQueuedAck ack = new CanonicalCommandQueuedReceipt.KafkaQueuedAck(
                "gateway",
                topic,
                command.shardId().partition(),
                offset,
                null,
                100,
                Bytes.sha256(Bytes.utf8("gateway-late-queued-ack-" + offset)));
        final CanonicalCommandQueuedReceipt receipt =
                CanonicalCommandQueuedReceipt.create(command, source, ack, 5_000, attemptId.bytes());
        return SubmissionOutcomeMessage.managed(EnqueueOutcomeMessage.queued(receipt));
    }

    private static PreparedSubmission prepared() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final PreparedCommand command = PreparedCommand.schedule(shard, schedule(), 600);
        return PreparedSubmission.managed(CommandCodec.encodeManagedFrame(command));
    }

    private static CanonicalScheduleIntent schedule() {
        return CanonicalScheduleIntent.create(
                new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKind.DESTINATION),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 61)),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class InMemoryGatewayIdempotencyStoreAdapter {
        private final GatewayIdempotencyRecord record;

        private InMemoryGatewayIdempotencyStoreAdapter(
                final InMemoryGatewayIdempotencyStore store,
                final Digest32 keyHash,
                final PreparedSubmission prepared) {
            final var started = store.startAttempt(keyHash);
            final SubmissionOutcomeMessage outcome =
                    GatewayOutcomeSupport.uncertain(prepared, started.permit().physicalAttemptId());
            record = store.finish(keyHash, started.permit().physicalAttemptId(), outcome);
        }

        private GatewayIdempotencyRecord record() {
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
            final OptionVersionId expected = options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .findFirst()
                    .orElse(null);
            if (expected != null && expected.versionId() == OptionVersionId.KEY_NOT_EXISTS && existing != null) {
                throw new KeyAlreadyExistsException(key);
            }
            if (expected != null
                    && expected.versionId() != OptionVersionId.KEY_NOT_EXISTS
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
        public void close() {}

        private record Stored(byte[] value, Version version) {}
    }
}
