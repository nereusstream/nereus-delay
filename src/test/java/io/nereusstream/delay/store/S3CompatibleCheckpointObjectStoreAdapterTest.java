package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3CompatibleCheckpointObjectStoreAdapterTest {
    private static final String REGION = "us-east-1";
    private static final String BUCKET = "checkpoint-bucket";
    private static final String ACCESS_KEY = "test-access";
    private static final String SECRET_KEY = "test-secret";
    private static final CheckpointManifestLimits LIMITS = new CheckpointManifestLimits(
            10, 1 << 20, 1 << 20, 1024, 1 << 20, 10, 1024);
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-16T00:00:00Z"), ZoneOffset.UTC);

    @TempDir
    Path tempDir;

    @Test
    void uploadsAndRestoresAfterManifestResponseLossWithBoundedSigV4Requests() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            server.dropFirstManifestPutResponse = true;
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture.profile(), server.endpoint());
            final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                    fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());

            final CheckpointResourceV1 resource = adapter.upload(request);

            assertTrue(server.manifestPutResponseDropped);
            assertFalse(resource.immutableVersion().length == 0);
            final List<Request> puts = server.requests.stream().filter(item -> item.method().equals("PUT")).toList();
            assertEquals(3, puts.size());
            for (Request put : puts) {
                assertEquals("*", put.headers().get("if-none-match"));
                assertTrue(put.headers().get("authorization").startsWith("AWS4-HMAC-SHA256 Credential="));
                assertEquals(Bytes.hex(Bytes.sha256(put.body())), put.headers().get("x-amz-content-sha256"));
            }

            final Path restored = adapter.download(new CheckpointDownloadRequest(fixture.manifest(), resource),
                    tempDir.resolve("restored"));
            assertEquals("MANIFEST-1\n", Files.readString(restored.resolve("CURRENT")));
            assertEquals("sst-bytes", Files.readString(restored.resolve("000001.sst")));
            assertTrue(Files.isDirectory(restored));
            assertTrue(server.requests.stream().anyMatch(item -> item.method().equals("GET")
                    && item.path().contains("/manifest.json?versionId=")));
        }
    }

    @Test
    void deletesEveryCheckpointObjectByExactProviderVersion() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture.profile(), server.endpoint());
            final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                    fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());
            final CheckpointResourceV1 resource = adapter.upload(request);

            final CheckpointDeleteResult result = adapter.delete(new CheckpointDeleteRequest(fixture.manifest(),
                    resource));

            assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, result.outcome());
            assertEquals(32, result.providerRequestIdHash().length);
            assertEquals(32, result.responseHash().length);
            final List<Request> deletes = server.requests.stream().filter(item -> item.method().equals("DELETE"))
                    .toList();
            assertEquals(3, deletes.size());
            assertTrue(deletes.stream().allMatch(item -> item.path().contains("?versionId=")));
            assertTrue(deletes.stream().allMatch(item -> item.status() == 204));
            assertTrue(deletes.get(deletes.size() - 1).path().endsWith("/manifest.json?versionId="
                    + new String(resource.immutableVersion(), StandardCharsets.UTF_8)));
            assertTrue(server.objects.isEmpty());
            assertThrows(IllegalStateException.class,
                    () -> adapter.download(new CheckpointDownloadRequest(fixture.manifest(), resource),
                            tempDir.resolve("deleted")));
        }
    }

    @Test
    void rejectsDeleteThatOmitsExactProviderVersionResponse() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture.profile(), server.endpoint());
            final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                    fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());
            final CheckpointResourceV1 resource = adapter.upload(request);
            server.omitDeleteVersionHeaders = true;

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> adapter.delete(new CheckpointDeleteRequest(fixture.manifest(), resource)));
            assertTrue(failure.getMessage().contains("omitted exact immutable version"));
        }
    }

    @Test
    void rejectsImmutableObjectConflictAfterIfNoneMatchPrecondition() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture.profile(), server.endpoint());
            final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                    fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());
            adapter.upload(request);

            server.corruptFirstCheckpointObject();

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> adapter.upload(request));
            assertTrue(failure.getMessage().contains("immutable remote checkpoint object identity conflict"));
            assertTrue(server.requests.stream().anyMatch(item -> item.method().equals("PUT")
                    && item.status() == 412));
        }
    }

    @Test
    void rejectsProfileEndpointOrCredentialScopeDriftBeforeHttp() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            final Fixture fixture = fixture(server.endpoint());
            final URI otherEndpoint = URI.create("http://127.0.0.1:" + server.port() + "/other");
            assertThrows(IllegalArgumentException.class, () -> adapter(fixture.profile(), otherEndpoint));

            final ObjectStoreProfileSemanticV1 semantic = (ObjectStoreProfileSemanticV1) fixture.profile().body();
            final ProfileSemanticEnvelopeV1 wrongCredentialProfile = new ProfileSemanticEnvelopeV1(
                    ProfileKindV1.OBJECT_STORE, Bytes.utf8("checkpoint-store"), 1,
                    new ObjectStoreProfileSemanticV1(ObjectStoreProviderKindV1.S3_COMPATIBLE,
                            semantic.endpointConfigDigest(),
                            S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                                    "other-access", REGION, BUCKET), 1, true, true, true, true,
                            semantic.encryptionPolicyDigest(), semantic.maxObjectBytes(),
                            semantic.allowedUploadHandleBits(), semantic.adapterConformanceVersion(),
                            semantic.lifecyclePolicyDigest()));
            assertThrows(IllegalArgumentException.class,
                    () -> adapter(wrongCredentialProfile, server.endpoint()));
            assertTrue(server.requests.isEmpty());
        }
    }

    @Test
    void rejectsProviderThatOmitsExactVersionHeaders() throws Exception {
        try (FakeS3Server server = new FakeS3Server()) {
            server.omitVersionHeaders = true;
            final Fixture fixture = fixture(server.endpoint());
            final S3CompatibleCheckpointObjectStoreAdapter adapter = adapter(fixture.profile(), server.endpoint());
            final CheckpointUploadRequest request = new CheckpointUploadRequest(fixture.pending(), fixture.manifest(),
                    fixture.checkpointDirectory(), fixture.manifest().canonicalJsonBytes());

            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> adapter.upload(request));
            assertTrue(failure.getMessage().contains("omitted exact immutable version"));
        }
    }

    private S3CompatibleCheckpointObjectStoreAdapter adapter(final ProfileSemanticEnvelopeV1 profile,
                                                              final URI endpoint) {
        return new S3CompatibleCheckpointObjectStoreAdapter(profile, endpoint, REGION, BUCKET, ACCESS_KEY,
                SECRET_KEY, null, LIMITS, java.net.http.HttpClient.newHttpClient(), CLOCK,
                java.time.Duration.ofSeconds(10));
    }

    private Fixture fixture(final URI endpoint) throws Exception {
        final Path directory = tempDir.resolve("checkpoint-" + UUID.randomUUID());
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("CURRENT"), "MANIFEST-1\n");
        Files.writeString(directory.resolve("000001.sst"), "sst-bytes");
        final ProfileSemanticEnvelopeV1 profile = profile(endpoint);
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final byte[] lineage = bytes(16, 2);
        final byte[] checkpoint = bytes(16, 3);
        final UUID storeIncarnation = UUID.randomUUID();
        final OwnerIdentityV1 owner = new OwnerIdentityV1(bytes(8, 5), bytes(8, 6), 42, bytes(32, 7));
        final List<CheckpointFileInventory> inventory = CheckpointFileInventory.collect(directory, LIMITS);
        final List<CheckpointManifest.FileEntry> files = inventory.stream()
                .map(file -> new CheckpointManifest.FileEntry(file.name(), file.length(), file.checksum(),
                        Bytes.utf8("object/" + file.name()), Bytes.utf8("version-1"), null))
                .toList();
        final KafkaSourcePosition position = new KafkaSourcePosition(shard, "cluster", UUID.randomUUID(), 9,
                3, 1_000);
        final CheckpointManifest manifest = new CheckpointManifest(checkpoint, lineage, 0, null, null,
                new CheckpointManifest.CreatedBy(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch()),
                new CheckpointManifest.CreatedAt(900, 1_000, "CERTIFIED_HOST_CLOCK", bytes(8, 8), 1, 2, 3,
                        bytes(32, 9), 0, null), shard, bytes(32, 10), storeIncarnation, 1, 7, position,
                bytes(32, 11), bytes(32, 12), List.of(), files);
        final CheckpointUploadIntentV1 pending = new CheckpointUploadIntentV1(
                new ShardSubjectV1(shard), lineage, checkpoint, owner, uuidBytes(storeIncarnation), bytes(32, 13),
                1, null, null, profile.ref(), evidence(900), 5_000, CheckpointUploadStateV1.PENDING_UPLOAD,
                1, null, null);
        return new Fixture(directory, profile, manifest, pending);
    }

    private static ProfileSemanticEnvelopeV1 profile(final URI endpoint) {
        final ObjectStoreProfileSemanticV1 semantic = new ObjectStoreProfileSemanticV1(
                ObjectStoreProviderKindV1.S3_COMPATIBLE,
                S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, REGION, BUCKET),
                S3CompatibleCheckpointObjectStoreAdapter.credentialAuthorizationScopeDigest(
                        ACCESS_KEY, REGION, BUCKET),
                1, true, true, true, true, bytes(32, 20), 1 << 20,
                ObjectStoreProfileSemanticV1.SINGLE_PUT, 1, bytes(32, 21));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.OBJECT_STORE, Bytes.utf8("checkpoint-store"), 1,
                semantic);
    }

    private record Fixture(Path checkpointDirectory, ProfileSemanticEnvelopeV1 profile,
                           CheckpointManifest manifest, CheckpointUploadIntentV1 pending) {
    }

    private record Request(String method, String path, Map<String, String> headers, byte[] body, int status) {
    }

    private record StoredObject(byte[] body, String version) {
    }

    private static final class FakeS3Server implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final ExecutorService handlers = Executors.newCachedThreadPool();
        private final Map<String, StoredObject> objects = new ConcurrentHashMap<>();
        private final List<Request> requests = new CopyOnWriteArrayList<>();
        private final Thread acceptThread;
        private volatile boolean closed;
        private volatile boolean dropFirstManifestPutResponse;
        private volatile boolean manifestPutResponseDropped;
        private volatile boolean omitVersionHeaders;
        private volatile boolean omitDeleteVersionHeaders;

        private FakeS3Server() throws IOException {
            serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            acceptThread = Thread.startVirtualThread(this::acceptLoop);
        }

        private int port() {
            return serverSocket.getLocalPort();
        }

        private URI endpoint() {
            return URI.create("http://127.0.0.1:" + port() + "/api");
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    final Socket socket = serverSocket.accept();
                    handlers.execute(() -> handle(socket));
                } catch (IOException failure) {
                    if (!closed) {
                        throw new IllegalStateException("fake S3 server accept failed", failure);
                    }
                }
            }
        }

        private void handle(final Socket socket) {
            try (InputStream input = socket.getInputStream(); OutputStream output = socket.getOutputStream()) {
                socket.setSoTimeout(10_000);
                final ParsedRequest parsed = readRequest(input);
                if (parsed.method().equals("PUT")) {
                    handlePut(parsed, output, socket);
                } else if (parsed.method().equals("GET")) {
                    handleGet(parsed, output);
                } else if (parsed.method().equals("DELETE")) {
                    handleDelete(parsed, output);
                } else {
                    respond(output, 405, new byte[0], null);
                }
            } catch (IOException failure) {
                if (!closed) {
                    throw new IllegalStateException("fake S3 server request failed", failure);
                }
            } finally {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // The test server is already shutting down or the client closed the connection.
                }
            }
        }

        private void handlePut(final ParsedRequest request, final OutputStream output, final Socket socket)
                throws IOException {
            final StoredObject existing = objects.get(request.path());
            if (existing != null) {
                record(request, 412);
                respond(output, 412, new byte[0], omitVersionHeaders ? null : existing.version());
                return;
            }
            final String version = version(request.body());
            objects.put(request.path(), new StoredObject(request.body(), version));
            if (dropFirstManifestPutResponse && !manifestPutResponseDropped
                    && request.path().endsWith("/manifest.json")) {
                manifestPutResponseDropped = true;
                record(request, 200);
                socket.close();
                return;
            }
            record(request, 200);
            respond(output, 200, new byte[0], omitVersionHeaders ? null : version);
        }

        private void handleGet(final ParsedRequest request, final OutputStream output) throws IOException {
            final StoredObject object = objects.get(pathWithoutQuery(request.path()));
            final String requestedVersion = queryValue(request.path(), "versionId");
            if (object == null || (requestedVersion != null && !requestedVersion.equals(object.version()))) {
                record(request, 404);
                respond(output, 404, new byte[0], null);
                return;
            }
            record(request, 200);
            respond(output, 200, object.body(), omitVersionHeaders ? null : object.version());
        }

        private void handleDelete(final ParsedRequest request, final OutputStream output) throws IOException {
            final String key = pathWithoutQuery(request.path());
            final String requestedVersion = queryValue(request.path(), "versionId");
            final StoredObject object = objects.get(key);
            if (object == null || requestedVersion == null || !requestedVersion.equals(object.version())) {
                record(request, 404);
                respond(output, 404, new byte[0], null);
                return;
            }
            if (!objects.remove(key, object)) {
                record(request, 409);
                respond(output, 409, new byte[0], null);
                return;
            }
            record(request, 204);
            final String requestId = "fake-" + Bytes.hex(Bytes.sha256(Bytes.utf8(request.path()))).substring(0, 16);
            respond(output, 204, new byte[0], omitVersionHeaders || omitDeleteVersionHeaders
                    ? null : object.version(), requestId);
        }

        private static String pathWithoutQuery(final String path) {
            final int query = path.indexOf('?');
            return query < 0 ? path : path.substring(0, query);
        }

        private static String queryValue(final String path, final String name) {
            final int query = path.indexOf('?');
            if (query < 0) {
                return null;
            }
            for (String pair : path.substring(query + 1).split("&")) {
                final int equals = pair.indexOf('=');
                if (equals > 0 && pair.substring(0, equals).equals(name)) {
                    return pair.substring(equals + 1);
                }
            }
            return null;
        }

        private void respond(final OutputStream output, final int status, final byte[] body,
                             final String version) throws IOException {
            respond(output, status, body, version, null);
        }

        private void respond(final OutputStream output, final int status, final byte[] body,
                             final String version, final String requestId) throws IOException {
            final String reason = status == 200 ? "OK" : status == 204 ? "No Content"
                    : status == 404 ? "Not Found" : status == 409 ? "Conflict"
                    : status == 412 ? "Precondition Failed" : "Method Not Allowed";
            final StringBuilder headers = new StringBuilder()
                    .append("HTTP/1.1 ").append(status).append(' ').append(reason).append("\r\n")
                    .append("Content-Length: ").append(body.length).append("\r\n")
                    .append("Connection: close\r\n");
            if (version != null) {
                headers.append("x-amz-version-id: ").append(version).append("\r\n");
            }
            if (requestId != null) {
                headers.append("x-amz-request-id: ").append(requestId).append("\r\n");
            }
            headers.append("\r\n");
            output.write(headers.toString().getBytes(StandardCharsets.ISO_8859_1));
            output.write(body);
            output.flush();
        }

        private void record(final ParsedRequest request, final int status) {
            requests.add(new Request(request.method(), request.path(), request.headers(), request.body(), status));
        }

        private void corruptFirstCheckpointObject() {
            final String path = objects.keySet().stream().filter(value -> value.contains("/objects/"))
                    .findFirst().orElseThrow();
            final StoredObject original = objects.get(path);
            final byte[] corrupted = original.body().clone();
            corrupted[0] ^= 1;
            objects.put(path, new StoredObject(corrupted, version(corrupted)));
        }

        @Override
        public void close() {
            closed = true;
            try {
                serverSocket.close();
            } catch (IOException failure) {
                throw new IllegalStateException("fake S3 server close failed", failure);
            }
            handlers.shutdownNow();
            try {
                acceptThread.join(TimeUnit.SECONDS.toMillis(5));
                handlers.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("fake S3 server close interrupted", failure);
            }
        }

        private static ParsedRequest readRequest(final InputStream input) throws IOException {
            final byte[] headerBytes = readHeaders(input);
            final String headerText = new String(headerBytes, StandardCharsets.ISO_8859_1);
            final String[] lines = headerText.split("\\r\\n");
            final String[] requestLine = lines[0].split(" ", 3);
            final Map<String, String> headers = new ConcurrentHashMap<>();
            for (int index = 1; index < lines.length; index++) {
                final int colon = lines[index].indexOf(':');
                if (colon > 0) {
                    headers.put(lines[index].substring(0, colon).trim().toLowerCase(),
                            lines[index].substring(colon + 1).trim());
                }
            }
            final int contentLength = Integer.parseInt(headers.getOrDefault("content-length", "0"));
            final byte[] body = input.readNBytes(contentLength);
            if (body.length != contentLength) {
                throw new IOException("fake S3 request body truncated");
            }
            return new ParsedRequest(requestLine[0], requestLine[1], headers, body);
        }

        private static byte[] readHeaders(final InputStream input) throws IOException {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            int matched = 0;
            while (output.size() <= 64 * 1024) {
                final int value = input.read();
                if (value < 0) {
                    throw new IOException("fake S3 request headers truncated");
                }
                output.write(value);
                matched = switch (matched) {
                    case 0 -> value == '\r' ? 1 : 0;
                    case 1 -> value == '\n' ? 2 : value == '\r' ? 1 : 0;
                    case 2 -> value == '\r' ? 3 : 0;
                    case 3 -> value == '\n' ? 4 : value == '\r' ? 1 : 0;
                    default -> matched;
                };
                if (matched == 4) {
                    final byte[] all = output.toByteArray();
                    return java.util.Arrays.copyOf(all, all.length - 4);
                }
            }
            throw new IOException("fake S3 request headers exceed test bound");
        }

        private static String version(final byte[] body) {
            return "version-" + Bytes.hex(Bytes.sha256(body)).substring(0, 16);
        }

        private record ParsedRequest(String method, String path, Map<String, String> headers, byte[] body) {
        }
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static TrustedUtcIntervalEvidence evidence(final long time) {
        return new TrustedUtcIntervalEvidence(time, time + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 14), 1, 2, 3,
                bytes(32, 15), 0, null);
    }
}
