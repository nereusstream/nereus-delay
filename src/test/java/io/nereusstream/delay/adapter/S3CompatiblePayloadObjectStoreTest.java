package io.nereusstream.delay.adapter;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.PayloadReservation;
import io.nereusstream.delay.runtime.PayloadReservationStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3CompatiblePayloadObjectStoreTest {
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "payload-bucket";
    private static final String ACCESS_KEY = "test-access";
    private static final String SECRET_KEY = "test-secret";
    private static final byte[] PAYLOAD = Bytes.utf8("large-payload");
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);

    @Test
    void resolvesProviderFiveHundredAfterPayloadCommitByExactReadback() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            server.failFirstPutAfterStore = true;
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatiblePayloadObjectStore adapter = adapter(fixture, server.endpoint());
            adapter.register(fixture.reservation(), fixture.trustSet().ref(), fixture.profile().ref());
            final var handle = adapter.issueUploadHandle(fixture.reservation().reservationId(),
                    UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_001).issued();

            adapter.upload(handle, PAYLOAD, 1_002);
            assertTrue(server.firstPutFailureInjected);
            assertTrue(server.requests.contains("GET"));
            assertTrue(server.requestPaths.stream().anyMatch(path -> path.contains(".payload")));

            assertEquals(PayloadAttestationOutcomeV1.ATTESTED, adapter.attest(handle, 1_003).outcome());
        }
    }

    @Test
    void providerFiveHundredBeforePayloadCommitRemainsFailClosed() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            server.failFirstPutBeforeStore = true;
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatiblePayloadObjectStore adapter = adapter(fixture, server.endpoint());
            adapter.register(fixture.reservation(), fixture.trustSet().ref(), fixture.profile().ref());
            final var handle = adapter.issueUploadHandle(fixture.reservation().reservationId(),
                    UploadHandleKindV1.OPAQUE_SINGLE_PUT, 1_001).issued();

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> adapter.upload(handle, PAYLOAD, 1_002));

            assertTrue(failure.getMessage().contains("HTTP 503"));
            assertTrue(server.firstPutFailureInjected);
            assertTrue(server.objects.isEmpty());
        }
    }

    private static S3CompatiblePayloadObjectStore adapter(final Fixture fixture, final URI endpoint) {
        return new S3CompatiblePayloadObjectStore(fixture.profile(), endpoint, REGION, BUCKET, ACCESS_KEY,
                SECRET_KEY, null, Bytes.sha256(Bytes.utf8("tenant")), fixture.trustSet(), 7, 5_000,
                fixture.keyPair().getPrivate(), null, HttpClient.newHttpClient(), CLOCK, Duration.ofSeconds(2));
    }

    private static Fixture fixture(final URI endpoint) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PayloadProofTrustSetSemanticV1 trustSet = new PayloadProofTrustSetSemanticV1(9,
                List.of(PayloadProofVerifierKeyV1.fromPublicKey(7, keyPair.getPublic(), 0, 10_000)));
        final ObjectStoreProfileSemanticV1 objectStore = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatiblePayloadObjectStore.endpointConfigDigest(endpoint, REGION, BUCKET),
                S3CompatiblePayloadObjectStore.credentialAuthorizationScopeDigest(ACCESS_KEY, REGION, BUCKET),
                1, true, true, true, true, Bytes.sha256(Bytes.utf8("encryption")), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, Bytes.sha256(Bytes.utf8("lifecycle")));
        final ProfileSemanticEnvelopeV1 profile = new ProfileSemanticEnvelopeV1(
                ProfileKindV1.OBJECT_STORE, Bytes.utf8("payload-store"), 1, objectStore);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final CommandId commandId = CommandId.random(shard);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final LargeScheduleIntent intent = new LargeScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("lane")), 2_000, 4_000, OrderingMode.BEST_EFFORT,
                PAYLOAD.length, Bytes.sha256(PAYLOAD), 1_000, trustSet.version());
        final byte[] sourcePosition = new KafkaSourcePosition(shard, "embedded",
                UUID.nameUUIDFromBytes(Bytes.utf8("payload-source")), 1, null, 1_000).canonicalBytes();
        final PayloadReservation reservation = new PayloadReservation(shard,
                Bytes.sha256(Bytes.utf8("reservation"), commandId.bytes()), commandId, messageId,
                Bytes.sha256(Bytes.utf8("command")), intent, 5_000, PayloadReservationStatus.RESERVED,
                1, sourcePosition, null);
        return new Fixture(keyPair, trustSet, profile, reservation);
    }

    private record Fixture(KeyPair keyPair, PayloadProofTrustSetSemanticV1 trustSet,
                           ProfileSemanticEnvelopeV1 profile, PayloadReservation reservation) {
    }

    private static final class FakeS3Server implements AutoCloseable {
        private final HttpServer server;
        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        private final List<String> requests = new CopyOnWriteArrayList<>();
        private final List<String> requestPaths = new CopyOnWriteArrayList<>();
        private volatile boolean failFirstPutAfterStore;
        private volatile boolean failFirstPutBeforeStore;
        private volatile boolean firstPutFailureInjected;

        private FakeS3Server() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", this::handle);
            server.start();
        }

        private URI endpoint() {
            return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/api");
        }

        private void handle(final HttpExchange exchange) throws IOException {
            final String method = exchange.getRequestMethod();
            final String path = exchange.getRequestURI().getPath();
            requests.add(method);
            requestPaths.add(path);
            try {
                if (method.equals("PUT")) {
                    final byte[] body = exchange.getRequestBody().readAllBytes();
                    if (failFirstPutBeforeStore && !firstPutFailureInjected) {
                        firstPutFailureInjected = true;
                        respond(exchange, 503, new byte[0], null);
                        return;
                    }
                    if (objects.putIfAbsent(path, body) != null) {
                        respond(exchange, 412, new byte[0], objects.get(path));
                        return;
                    }
                    if (failFirstPutAfterStore && !firstPutFailureInjected) {
                        firstPutFailureInjected = true;
                        respond(exchange, 503, new byte[0], body);
                        return;
                    }
                    respond(exchange, 200, new byte[0], body);
                    return;
                }
                final byte[] body = objects.get(path);
                if (body == null) {
                    respond(exchange, 404, new byte[0], null);
                } else if (method.equals("GET")) {
                    respond(exchange, 200, body, body);
                } else if (method.equals("HEAD")) {
                    respond(exchange, 200, new byte[0], body);
                } else {
                    respond(exchange, 405, new byte[0], null);
                }
            } finally {
                exchange.close();
            }
        }

        private static void respond(final HttpExchange exchange, final int status, final byte[] body,
                                    final byte[] versionBody) throws IOException {
            if (versionBody != null) {
                final String version = Bytes.hex(Bytes.sha256(versionBody)).substring(0, 16);
                exchange.getResponseHeaders().set("x-amz-version-id", version);
                exchange.getResponseHeaders().set("etag", "\"" + version + "\"");
            }
            if (exchange.getRequestMethod().equals("HEAD")) {
                exchange.sendResponseHeaders(status, -1);
                return;
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
