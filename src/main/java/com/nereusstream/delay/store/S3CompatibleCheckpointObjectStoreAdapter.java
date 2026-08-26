package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.ObjectStoreProviderKind;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * S3/S3-compatible HTTP provider for immutable checkpoint objects.
 *
 * <p>The adapter is deliberately provider-shaped rather than profile-shaped:
 * the caller supplies the already authenticated semantic Profile, endpoint
 * configuration and credential scope that were selected by control-plane
 * authority. The adapter verifies those digests before any HTTP request. It
 * uploads every checkpoint object with {@code If-None-Match: *}, writes the
 * manifest last, and verifies an existing object byte-for-byte after a
 * conflict or an ambiguous response. Download uses the same exact object
 * names, streams into a private staging directory and publishes that
 * directory only through an atomic rename.</p>
 *
 * <p>This closes the S3-compatible data-plane identity and response-loss
 * boundary only when the provider returns an exact immutable version header;
 * a Profile requiring exact-version deletion fails closed if that header is
 * absent. The adapter also records a local operation-completion and
 * response-loss uncertainty horizon and supports a one-way local admission
 * fence. That observation is not a provider-side quiescence or consistency
 * attestation. Credential rotation, provider quiescence/consistency
 * attestations, lifecycle deletion and the Oxia catalog transaction remain
 * external authority inputs.</p>
 */
public final class S3CompatibleCheckpointObjectStoreAdapter
        implements CheckpointUploadAdapter,
                CheckpointDownloadAdapter,
                CheckpointDeleteAdapter,
                CheckpointPrefixSweepAdapter {
    private static final String SERVICE = "s3";
    private static final String TERMINATOR = "aws4_request";
    private static final String ENDPOINT_DOMAIN = "nereus-delay-s3-endpoint\0";
    private static final String CREDENTIAL_DOMAIN = "nereus-delay-s3-credential-scope\0";
    private static final int BUFFER_BYTES = 64 * 1024;
    private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter SHORT_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final ProfileSemanticEnvelope profile;
    private final ObjectStoreProfileSemantic objectStore;
    private final URI endpoint;
    private final String region;
    private final String bucket;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final CheckpointManifestLimits limits;
    private final HttpClient client;
    private final Clock clock;
    private final Duration requestTimeout;
    private final ObjectStoreCredentialUseLeaseGate credentialGate;
    private final ObjectStoreProviderOwnershipTracker providerOwnership;

    /**
     * Creates an adapter with a bounded default HTTP client and UTC clock.
     * The supplied Profile must carry the digest of this exact endpoint,
     * region, bucket and credential authorization scope.
     */
    public S3CompatibleCheckpointObjectStoreAdapter(
            final ProfileSemanticEnvelope profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final CheckpointManifestLimits limits) {
        this(
                profile,
                endpoint,
                region,
                bucket,
                accessKeyId,
                secretAccessKey,
                sessionToken,
                limits,
                null,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build(),
                Clock.systemUTC(),
                Duration.ofSeconds(60),
                ObjectStoreProviderOwnershipTracker.DEFAULT_MAXIMUM_PROVIDER_OWNERSHIP_LIFETIME_MS);
    }

    /** Constructor with injectable client/clock for deterministic provider tests. */
    public S3CompatibleCheckpointObjectStoreAdapter(
            final ProfileSemanticEnvelope profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final CheckpointManifestLimits limits,
            final HttpClient client,
            final Clock clock,
            final Duration requestTimeout) {
        this(
                profile,
                endpoint,
                region,
                bucket,
                accessKeyId,
                secretAccessKey,
                sessionToken,
                limits,
                null,
                client,
                clock,
                requestTimeout,
                ObjectStoreProviderOwnershipTracker.DEFAULT_MAXIMUM_PROVIDER_OWNERSHIP_LIFETIME_MS);
    }

    /** Creates a provider adapter whose every upload/download is lease-gated. */
    public S3CompatibleCheckpointObjectStoreAdapter(
            final ProfileSemanticEnvelope profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final CheckpointManifestLimits limits,
            final ObjectStoreCredentialUseLeaseGate credentialGate) {
        this(
                profile,
                endpoint,
                region,
                bucket,
                accessKeyId,
                secretAccessKey,
                sessionToken,
                limits,
                credentialGate,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build(),
                Clock.systemUTC(),
                Duration.ofSeconds(60),
                ObjectStoreProviderOwnershipTracker.DEFAULT_MAXIMUM_PROVIDER_OWNERSHIP_LIFETIME_MS);
    }

    /** Injectable constructor for the lease-gated provider path. */
    public S3CompatibleCheckpointObjectStoreAdapter(
            final ProfileSemanticEnvelope profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final CheckpointManifestLimits limits,
            final ObjectStoreCredentialUseLeaseGate credentialGate,
            final HttpClient client,
            final Clock clock,
            final Duration requestTimeout) {
        this(
                profile,
                endpoint,
                region,
                bucket,
                accessKeyId,
                secretAccessKey,
                sessionToken,
                limits,
                credentialGate,
                client,
                clock,
                requestTimeout,
                ObjectStoreProviderOwnershipTracker.DEFAULT_MAXIMUM_PROVIDER_OWNERSHIP_LIFETIME_MS);
    }

    /** Injectable constructor with an explicit local provider ownership horizon. */
    public S3CompatibleCheckpointObjectStoreAdapter(
            final ProfileSemanticEnvelope profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final CheckpointManifestLimits limits,
            final ObjectStoreCredentialUseLeaseGate credentialGate,
            final HttpClient client,
            final Clock clock,
            final Duration requestTimeout,
            final long maximumProviderOwnershipLifetimeMs) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKind.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemantic semantic)) {
            throw new IllegalArgumentException("S3 checkpoint adapter requires an OBJECT_STORE Profile");
        }
        if (semantic.providerKind() != ObjectStoreProviderKind.S3
                && semantic.providerKind() != ObjectStoreProviderKind.S3_COMPATIBLE) {
            throw new IllegalArgumentException("S3 checkpoint adapter does not support the selected provider");
        }
        this.objectStore = semantic;
        this.endpoint = canonicalEndpoint(Objects.requireNonNull(endpoint, "endpoint"));
        this.region = canonicalText(region, "region");
        this.bucket = canonicalBucket(bucket);
        this.accessKeyId = canonicalText(accessKeyId, "accessKeyId");
        this.secretAccessKey = nonEmpty(secretAccessKey, "secretAccessKey");
        this.sessionToken = sessionToken == null ? null : nonEmpty(sessionToken, "sessionToken");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.credentialGate = credentialGate;
        this.providerOwnership =
                new ObjectStoreProviderOwnershipTracker(this.clock, maximumProviderOwnershipLifetimeMs);
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (!Bytes.constantTimeEquals(
                semantic.endpointConfigDigest(), endpointConfigDigest(this.endpoint, this.region, this.bucket))) {
            throw new IllegalArgumentException("Object Store endpoint configuration does not match Profile");
        }
        if (!Bytes.constantTimeEquals(
                semantic.credentialAuthorizationScopeDigest(),
                credentialAuthorizationScopeDigest(this.accessKeyId, this.region, this.bucket))) {
            throw new IllegalArgumentException("Object Store credential scope does not match Profile");
        }
        if (credentialGate != null && !credentialGate.profile().equals(profile.ref())) {
            throw new IllegalArgumentException("Object Store credential gate Profile differs from adapter");
        }
    }

    /** Returns the endpoint digest bound by the semantic Object Store Profile. */
    public static byte[] endpointConfigDigest(final URI endpoint, final String region, final String bucket) {
        final URI canonical = canonicalEndpoint(Objects.requireNonNull(endpoint, "endpoint"));
        return Bytes.sha256(
                Bytes.utf8(ENDPOINT_DOMAIN),
                Bytes.utf8(canonical.toString()),
                Bytes.utf8(canonicalText(region, "region")),
                Bytes.utf8(canonicalBucket(bucket)));
    }

    /** Returns the non-secret credential authorization scope digest bound by the Profile. */
    public static byte[] credentialAuthorizationScopeDigest(
            final String accessKeyId, final String region, final String bucket) {
        return Bytes.sha256(
                Bytes.utf8(CREDENTIAL_DOMAIN),
                Bytes.utf8(canonicalText(accessKeyId, "accessKeyId")),
                Bytes.utf8(canonicalText(region, "region")),
                Bytes.utf8(canonicalBucket(bucket)));
    }

    /** Returns the local provider ownership observation without changing admission. */
    public synchronized ObjectStoreProviderOwnershipTracker.Observation providerOwnershipObservation() {
        return providerOwnership.observe();
    }

    /** Permanently fences this adapter generation from starting new provider operations. */
    public synchronized void beginProviderQuiescence() {
        providerOwnership.beginQuiescence();
    }

    /** Requires the local fence, operation drain and ambiguity horizon to be closed. */
    public synchronized ObjectStoreProviderOwnershipTracker.Observation requireProviderQuiescence() {
        return providerOwnership.requireLocallyQuiescent();
    }

    @Override
    public synchronized CheckpointResource upload(final CheckpointUploadRequest request) {
        Objects.requireNonNull(request, "request");
        requireCredentialGate();
        if (!profile.ref().equals(request.intent().objectStoreProfile())) {
            throw new IllegalArgumentException("checkpoint upload uses a different Object Store Profile");
        }
        final CheckpointManifest manifest = request.manifest();
        manifest.validateLimits(limits);
        final byte[] manifestBytes = request.manifestBytes();
        if (!Arrays.equals(manifestBytes, manifest.canonicalJsonBytes())) {
            throw new IllegalArgumentException("checkpoint manifest bytes are not canonical");
        }
        if (manifestBytes.length > objectStore.maxObjectBytes()) {
            throw new IllegalArgumentException("checkpoint manifest exceeds Object Store maxObjectBytes");
        }
        final Path checkpointDirectory = normalizeDirectory(request.checkpointDirectory(), "checkpoint directory");
        validateInventory(checkpointDirectory, manifest, limits);
        final String prefix = checkpointPrefix(manifest);
        final ObjectStoreProviderOwnershipTracker.Operation ownership = beginProviderOperation();
        try {
            for (CheckpointManifest.FileEntry file : manifest.files()) {
                if (file.length() > objectStore.maxObjectBytes()) {
                    throw new IllegalArgumentException(
                            "checkpoint file exceeds Object Store maxObjectBytes: " + file.name());
                }
                final Path source = checkpointDirectory.resolve(file.name()).normalize();
                ensureWithin(checkpointDirectory, source);
                putFile(objectKey(prefix, file), source, file.length(), file.checksum());
            }
            final String manifestKey = prefix + "/manifest.json";
            final String immutableVersion =
                    putBytes(manifestKey, manifestBytes, manifest.manifestSha256(), limits.maxManifestBytes());
            final CheckpointResource resource = new CheckpointResource(
                    manifest.recoveryLineageId(),
                    manifest.checkpointId(),
                    request.intent().objectStoreProfile(),
                    Bytes.utf8(bucket),
                    Bytes.utf8(manifestKey),
                    Bytes.utf8(immutableVersion),
                    manifestBytes.length,
                    manifest.manifestSha256());
            limits.validateResource(resource);
            ownership.complete();
            return resource;
        } catch (RuntimeException | Error failure) {
            ownership.uncertain();
            throw failure;
        }
    }

    @Override
    public synchronized CheckpointDeleteResult delete(final CheckpointDeleteRequest request) {
        Objects.requireNonNull(request, "request");
        requireCredentialGate();
        final CheckpointManifest manifest = request.manifest();
        manifest.validateLimits(limits);
        final CheckpointResource resource = request.resource();
        validateCheckpointResource(manifest, resource);

        final ObjectStoreProviderOwnershipTracker.Operation ownership = beginProviderOperation();
        try {
            final String prefix = checkpointPrefix(manifest);
            final String manifestKey = prefix + "/manifest.json";
            final String manifestVersion = decodeProviderVersion(resource.immutableVersion());
            final List<ProviderResponseEvidence> preflightEvidence =
                    new ArrayList<>(manifest.files().size() + 1);
            boolean manifestPresent = false;
            try {
                final RemoteBytes remoteManifest = getBytes(manifestKey, limits.maxManifestBytes(), manifestVersion);
                preflightEvidence.add(remoteManifest.evidence());
                if (remoteManifest.bytes().length != resource.manifestLength()
                        || !Arrays.equals(remoteManifest.bytes(), manifest.canonicalJsonBytes())
                        || !Arrays.equals(resource.manifestSha256(), sha256(remoteManifest.bytes()))
                        || !Arrays.equals(resource.immutableVersion(), Bytes.utf8(remoteManifest.version(null)))) {
                    throw new IllegalStateException("remote checkpoint manifest differs from catalog resource");
                }
                manifestPresent = true;
            } catch (RemoteObjectMissing missing) {
                preflightEvidence.add(missing.evidence());
            }

            final List<DeletePlan> plans = new ArrayList<>(manifest.files().size() + 1);
            boolean filePresent = false;
            for (CheckpointManifest.FileEntry file : manifest.files()) {
                final String key = objectKey(prefix, file);
                final RemoteObjectObservation observation = observeRemoteFile(
                        key, file.length(), file.checksum(), objectBytesLimit(limits.maxIndividualFileBytes()));
                preflightEvidence.add(observation.evidence());
                if (observation.present()) {
                    filePresent = true;
                    plans.add(new DeletePlan(key, observation.version()));
                }
            }
            if (!manifestPresent) {
                if (filePresent) {
                    throw new IllegalStateException("checkpoint manifest version is absent while file objects remain");
                }
                final CheckpointDeleteResult result = new CheckpointDeleteResult(
                        resource,
                        ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                        aggregateProbeRequestIds(preflightEvidence),
                        aggregateProbeResponses(preflightEvidence));
                ownership.complete();
                return result;
            }
            // Keep the manifest visible until every file has passed the identity preflight.
            plans.add(new DeletePlan(manifestKey, manifestVersion));

            final List<DeleteOperation> operations = new ArrayList<>(plans.size());
            for (DeletePlan plan : plans) {
                operations.add(deleteObject(plan.key(), plan.version()));
            }
            final CheckpointDeleteResult result = new CheckpointDeleteResult(
                    resource,
                    ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                    aggregateDeleteRequestIds(operations),
                    aggregateDeleteResponses(operations));
            ownership.complete();
            return result;
        } catch (RuntimeException | Error failure) {
            ownership.uncertain();
            throw failure;
        }
    }

    @Override
    public synchronized CheckpointPrefixSweepResult sweep(final CheckpointPrefixSweepRequest request) {
        Objects.requireNonNull(request, "request");
        requireCredentialGate();
        if (!request.objectStoreProfile().equals(profile.ref())) {
            throw new IllegalArgumentException("checkpoint prefix sweep uses a different Object Store Profile");
        }
        final ObjectStoreProviderOwnershipTracker.Operation ownership = beginProviderOperation();
        try {
            final String prefix = checkpointPrefix(request.recoveryLineageId(), request.checkpointId());
            final VersionList initial = listVersions(prefix, request.maxVersions());
            final List<DeleteOperation> operations =
                    new ArrayList<>(initial.entries().size());
            for (VersionedObject entry : initial.entries()) {
                operations.add(deleteObject(entry.key(), entry.version()));
            }
            final VersionList finalListing = listVersions(prefix, request.maxVersions());
            if (!finalListing.entries().isEmpty()) {
                throw new IllegalStateException("checkpoint prefix sweep did not prove an empty prefix: " + prefix);
            }
            final CheckpointPrefixSweepResult result = new CheckpointPrefixSweepResult(
                    initial.entries().size(),
                    operations.size(),
                    aggregateSweepRequestIds(initial.evidence(), operations, finalListing.evidence()),
                    aggregateSweepResponses(initial.evidence(), operations, finalListing.evidence()));
            ownership.complete();
            return result;
        } catch (RuntimeException | Error failure) {
            ownership.uncertain();
            throw failure;
        }
    }

    @Override
    public synchronized Path download(final CheckpointDownloadRequest request, final Path targetDirectory) {
        Objects.requireNonNull(request, "request");
        requireCredentialGate();
        final CheckpointManifest manifest = request.manifest();
        manifest.validateLimits(limits);
        final CheckpointResource resource = request.resource();
        validateCheckpointResource(manifest, resource);
        final ObjectStoreProviderOwnershipTracker.Operation ownership = beginProviderOperation();
        try {
            final String prefix = checkpointPrefix(manifest);
            final String expectedManifestKey = prefix + "/manifest.json";
            final String manifestVersion = decodeProviderVersion(resource.immutableVersion());
            final RemoteBytes remoteManifest =
                    getBytes(expectedManifestKey, limits.maxManifestBytes(), manifestVersion);
            if (remoteManifest.bytes().length != resource.manifestLength()
                    || !Arrays.equals(remoteManifest.bytes(), manifest.canonicalJsonBytes())
                    || !Arrays.equals(manifest.manifestSha256(), sha256(remoteManifest.bytes()))) {
                throw new IllegalStateException("remote checkpoint manifest differs from catalog manifest");
            }
            final String fallbackVersion = "sha256-" + Bytes.hex(manifest.manifestSha256());
            if (!Arrays.equals(resource.immutableVersion(), Bytes.utf8(remoteManifest.version(fallbackVersion)))) {
                throw new IllegalStateException("remote checkpoint manifest version differs from catalog resource");
            }
            final Path target = normalizeTarget(targetDirectory);
            final Path parent = Objects.requireNonNull(target.getParent(), "target parent");
            ensureDirectory(parent, "checkpoint target parent");
            if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("checkpoint download target already exists: " + target);
            }
            final Path temporary = parent.resolve("." + target.getFileName() + ".s3-checkpoint-" + UUID.randomUUID())
                    .normalize();
            ensureWithin(parent, temporary);
            boolean published = false;
            try {
                ensureDirectory(temporary, "checkpoint staging directory");
                for (CheckpointManifest.FileEntry file : manifest.files()) {
                    final Path destination = temporary.resolve(file.name()).normalize();
                    ensureWithin(temporary, destination);
                    ensureDirectory(
                            Objects.requireNonNull(destination.getParent(), "checkpoint file parent"),
                            "checkpoint staging parent");
                    getFile(objectKey(prefix, file), destination, file.length(), file.checksum());
                }
                validateInventory(temporary, manifest, limits);
                moveCreateNew(temporary, target);
                forceDirectory(parent);
                published = true;
                ownership.complete();
                return target;
            } finally {
                if (!published) {
                    deleteTree(temporary);
                }
            }
        } catch (RuntimeException | Error failure) {
            ownership.uncertain();
            throw failure;
        }
    }

    private void validateCheckpointResource(final CheckpointManifest manifest, final CheckpointResource resource) {
        limits.validateResource(resource);
        if (!resource.objectStoreProfile().equals(profile.ref())
                || !Arrays.equals(resource.container(), Bytes.utf8(bucket))) {
            throw new IllegalArgumentException("checkpoint resource uses a different S3 Object Store");
        }
        if (!Arrays.equals(resource.recoveryLineageId(), manifest.recoveryLineageId())
                || !Arrays.equals(resource.checkpointId(), manifest.checkpointId())) {
            throw new IllegalArgumentException("checkpoint resource does not identify the supplied manifest");
        }
        final String expectedManifestKey = checkpointPrefix(manifest) + "/manifest.json";
        if (!Arrays.equals(resource.objectKey(), Bytes.utf8(expectedManifestKey))) {
            throw new IllegalArgumentException("checkpoint resource manifest key is not canonical");
        }
        if (resource.manifestLength() != manifest.canonicalJsonBytes().length
                || !Arrays.equals(resource.manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("checkpoint resource manifest digest differs from manifest");
        }
        decodeProviderVersion(resource.immutableVersion());
    }

    private void putFile(
            final String key, final Path source, final long expectedLength, final byte[] expectedChecksum) {
        verifyLocalFile(source, expectedLength, expectedChecksum);
        final HttpRequest.BodyPublisher body;
        try {
            body = HttpRequest.BodyPublishers.ofFile(source);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot open checkpoint source: " + source, failure);
        }
        final String payloadHash = Bytes.hex(expectedChecksum);
        final HttpResponse<Void> response;
        try {
            response = send(
                    "PUT",
                    key,
                    payloadHash,
                    body,
                    Map.of("if-none-match", "*", "x-amz-meta-nereus-sha256", payloadHash),
                    HttpResponse.BodyHandlers.discarding());
        } catch (TransportFailure failure) {
            verifyAfterAmbiguousPut(
                    key, expectedLength, expectedChecksum, objectBytesLimit(limits.maxIndividualFileBytes()), failure);
            return;
        }
        if (isSuccess(response.statusCode())) {
            verifyLocalFile(source, expectedLength, expectedChecksum);
            verifyRemoteFile(key, expectedLength, expectedChecksum, objectBytesLimit(limits.maxIndividualFileBytes()));
            return;
        }
        if (isAlreadyExists(response.statusCode())) {
            verifyRemoteFile(key, expectedLength, expectedChecksum, objectBytesLimit(limits.maxIndividualFileBytes()));
            return;
        }
        if (isAmbiguousProviderStatus(response.statusCode())) {
            verifyAfterAmbiguousPut(
                    key,
                    expectedLength,
                    expectedChecksum,
                    objectBytesLimit(limits.maxIndividualFileBytes()),
                    unexpectedStatus("PUT", key, response.statusCode()));
            return;
        }
        throw unexpectedStatus("PUT", key, response.statusCode());
    }

    private String putBytes(final String key, final byte[] bytes, final byte[] checksum, final long maxBytes) {
        final String payloadHash = Bytes.hex(checksum);
        final HttpResponse<Void> response;
        try {
            response = send(
                    "PUT",
                    key,
                    payloadHash,
                    HttpRequest.BodyPublishers.ofByteArray(bytes),
                    Map.of("if-none-match", "*", "x-amz-meta-nereus-sha256", payloadHash),
                    HttpResponse.BodyHandlers.discarding());
        } catch (TransportFailure failure) {
            return verifyAfterAmbiguousPut(key, bytes.length, checksum, objectBytesLimit(maxBytes), failure);
        }
        if (isSuccess(response.statusCode())) {
            return responseVersionOrFallback(response, "sha256-" + payloadHash, "PUT", key);
        }
        if (isAlreadyExists(response.statusCode())) {
            return verifyRemoteFile(key, bytes.length, checksum, objectBytesLimit(maxBytes));
        }
        if (isAmbiguousProviderStatus(response.statusCode())) {
            return verifyAfterAmbiguousPut(
                    key,
                    bytes.length,
                    checksum,
                    objectBytesLimit(maxBytes),
                    unexpectedStatus("PUT", key, response.statusCode()));
        }
        throw unexpectedStatus("PUT", key, response.statusCode());
    }

    private String verifyAfterAmbiguousPut(
            final String key,
            final long expectedLength,
            final byte[] expectedChecksum,
            final long maxBytes,
            final RuntimeException original) {
        try {
            return verifyRemoteFile(key, expectedLength, expectedChecksum, maxBytes);
        } catch (RemoteObjectMissing missing) {
            throw original;
        }
    }

    private String verifyRemoteFile(
            final String key, final long expectedLength, final byte[] expectedChecksum, final long maxBytes) {
        final RemoteObjectObservation observation = observeRemoteFile(key, expectedLength, expectedChecksum, maxBytes);
        if (!observation.present()) {
            throw new RemoteObjectMissing("remote Object Store object does not exist: " + key, observation.evidence());
        }
        return observation.version();
    }

    private RemoteObjectObservation observeRemoteFile(
            final String key, final long expectedLength, final byte[] expectedChecksum, final long maxBytes) {
        final HttpResponse<InputStream> response = send(
                "GET",
                key,
                EMPTY_SHA256,
                HttpRequest.BodyPublishers.noBody(),
                Map.of(),
                HttpResponse.BodyHandlers.ofInputStream());
        final ProviderResponseEvidence missingEvidence =
                responseEvidence("GET", key, response.statusCode(), null, sha256(new byte[0]), response);
        if (response.statusCode() == 404) {
            closeQuietly(response.body());
            return new RemoteObjectObservation(false, null, missingEvidence);
        }
        if (!isSuccess(response.statusCode())) {
            closeQuietly(response.body());
            throw unexpectedStatus("GET", key, response.statusCode());
        }
        try (InputStream input = response.body()) {
            final HashedFile actual = consume(input, null, expectedLength, maxBytes);
            if (actual.length() != expectedLength || !Bytes.constantTimeEquals(actual.checksum(), expectedChecksum)) {
                throw new IllegalStateException("immutable remote checkpoint object identity conflict: " + key);
            }
            final String version =
                    responseVersionOrFallback(response, "sha256-" + Bytes.hex(expectedChecksum), "GET", key);
            return new RemoteObjectObservation(
                    true,
                    version,
                    responseEvidence("GET", key, response.statusCode(), version, actual.checksum(), response));
        } catch (IOException failure) {
            throw new IllegalStateException("cannot verify remote checkpoint object: " + key, failure);
        }
    }

    private void getFile(
            final String key, final Path destination, final long expectedLength, final byte[] expectedChecksum) {
        final HttpResponse<InputStream> response = send(
                "GET",
                key,
                EMPTY_SHA256,
                HttpRequest.BodyPublishers.noBody(),
                Map.of(),
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 404) {
            closeQuietly(response.body());
            throw new IllegalStateException("remote checkpoint object is missing: " + key);
        }
        if (!isSuccess(response.statusCode())) {
            closeQuietly(response.body());
            throw unexpectedStatus("GET", key, response.statusCode());
        }
        responseVersionOrFallback(response, null, "GET", key);
        try (InputStream input = response.body();
                FileChannel output =
                        FileChannel.open(destination, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final HashedFile actual = consume(
                    input,
                    Channels.newOutputStream(output),
                    expectedLength,
                    objectBytesLimit(limits.maxIndividualFileBytes()));
            output.force(true);
            if (actual.length() != expectedLength || !Bytes.constantTimeEquals(actual.checksum(), expectedChecksum)) {
                throw new IllegalStateException("remote checkpoint object differs from manifest: " + key);
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot download remote checkpoint object: " + key, failure);
        }
    }

    private RemoteBytes getBytes(final String key, final long maxBytes) {
        return getBytes(key, maxBytes, null);
    }

    private RemoteBytes getBytes(final String key, final long maxBytes, final String versionId) {
        final HttpResponse<InputStream> response = send(
                "GET",
                key,
                EMPTY_SHA256,
                HttpRequest.BodyPublishers.noBody(),
                Map.of(),
                versionId,
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 404) {
            closeQuietly(response.body());
            throw new RemoteObjectMissing(
                    "remote checkpoint manifest is missing: " + key,
                    responseEvidence("GET", key, response.statusCode(), null, sha256(new byte[0]), response));
        }
        if (!isSuccess(response.statusCode())) {
            closeQuietly(response.body());
            throw unexpectedStatus("GET", key, response.statusCode());
        }
        try (InputStream input = response.body()) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            consume(input, output, -1, objectBytesLimit(maxBytes));
            final byte[] bytes = output.toByteArray();
            final String version = responseVersionOrFallback(response, null, "GET", key);
            return new RemoteBytes(
                    bytes,
                    version,
                    responseEvidence("GET", key, response.statusCode(), version, sha256(bytes), response));
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read remote checkpoint manifest: " + key, failure);
        }
    }

    private VersionList listVersions(final String prefix, final int maxVersions) {
        final String query = "prefix=" + encodeSegment(prefix) + "&versions=";
        final HttpResponse<InputStream> response = sendBucket(
                "GET",
                query,
                EMPTY_SHA256,
                HttpRequest.BodyPublishers.noBody(),
                Map.of(),
                HttpResponse.BodyHandlers.ofInputStream());
        if (!isSuccess(response.statusCode())) {
            closeQuietly(response.body());
            throw unexpectedStatus("GET", "?" + query, response.statusCode());
        }
        try (InputStream input = response.body()) {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            consume(input, output, -1, listResponseLimit(maxVersions));
            final byte[] bytes = output.toByteArray();
            final ProviderResponseEvidence evidence =
                    responseEvidence("GET", prefix, response.statusCode(), null, sha256(bytes), response);
            final List<VersionedObject> entries = parseVersionList(bytes, maxVersions);
            final String requiredPrefix = prefix + "/";
            if (entries.stream().anyMatch(entry -> !entry.key().startsWith(requiredPrefix))) {
                throw new IllegalStateException("checkpoint version listing escaped its exact prefix: " + prefix);
            }
            return new VersionList(entries, evidence);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read checkpoint prefix version listing: " + prefix, failure);
        }
    }

    private long listResponseLimit(final int maxVersions) {
        try {
            final long estimated = Math.multiplyExact((long) maxVersions, 2_048L);
            return objectBytesLimit(Math.max(64 * 1024L, estimated));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("checkpoint prefix listing bound overflow", overflow);
        }
    }

    private static List<VersionedObject> parseVersionList(final byte[] encoded, final int maxVersions) {
        final XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        final List<VersionedObject> result = new ArrayList<>();
        final java.util.Set<String> identities = new java.util.HashSet<>();
        boolean truncated = false;
        String entryKind = null;
        String key = null;
        String version = null;
        String activeText = null;
        final StringBuilder text = new StringBuilder();
        try {
            final XMLStreamReader reader = factory.createXMLStreamReader(new ByteArrayInputStream(encoded));
            try {
                while (reader.hasNext()) {
                    final int event = reader.next();
                    if (event == XMLStreamReader.START_ELEMENT) {
                        final String name = reader.getLocalName();
                        if (name.equals("Version") || name.equals("DeleteMarker")) {
                            if (entryKind != null) {
                                throw new IllegalArgumentException("nested version listing entry");
                            }
                            entryKind = name;
                            key = null;
                            version = null;
                        } else if (name.equals("Key") || name.equals("VersionId") || name.equals("IsTruncated")) {
                            activeText = name;
                            text.setLength(0);
                        }
                    } else if ((event == XMLStreamReader.CHARACTERS || event == XMLStreamReader.CDATA)
                            && activeText != null) {
                        text.append(reader.getText());
                    } else if (event == XMLStreamReader.END_ELEMENT) {
                        final String name = reader.getLocalName();
                        if (activeText != null && activeText.equals(name)) {
                            final String value = text.toString().trim();
                            if (name.equals("Key")) {
                                key = value;
                            } else if (name.equals("VersionId")) {
                                version = value;
                            } else if (name.equals("IsTruncated")) {
                                if (value.equals("true")) {
                                    truncated = true;
                                } else if (!value.equals("false")) {
                                    throw new IllegalArgumentException("invalid ListObjectVersions IsTruncated");
                                }
                            }
                            activeText = null;
                            text.setLength(0);
                        }
                        if (entryKind != null && entryKind.equals(name)) {
                            if (key == null || key.isEmpty() || version == null || version.isEmpty()) {
                                throw new IllegalArgumentException("checkpoint version listing entry is incomplete");
                            }
                            canonicalObjectKey(key);
                            canonicalText(version, "provider version");
                            if (!identities.add(key + "\0" + version)) {
                                throw new IllegalArgumentException("duplicate checkpoint version listing entry");
                            }
                            result.add(new VersionedObject(key, version));
                            if (result.size() > maxVersions) {
                                throw new IllegalStateException("checkpoint prefix version listing exceeds bound");
                            }
                            entryKind = null;
                            key = null;
                            version = null;
                        }
                    }
                }
                if (entryKind != null || activeText != null || reader.hasNext()) {
                    throw new IllegalArgumentException("truncated checkpoint version listing");
                }
            } finally {
                reader.close();
            }
        } catch (XMLStreamException failure) {
            throw new IllegalStateException("cannot parse checkpoint prefix version listing", failure);
        }
        if (truncated) {
            throw new IllegalStateException("checkpoint prefix version listing is truncated");
        }
        return List.copyOf(result);
    }

    private DeleteOperation deleteObject(final String key, final String versionId) {
        final String exactVersion = canonicalText(versionId, "provider version");
        final HttpResponse<Void> response;
        try {
            response = send(
                    "DELETE",
                    key,
                    EMPTY_SHA256,
                    HttpRequest.BodyPublishers.noBody(),
                    Map.of(),
                    exactVersion,
                    HttpResponse.BodyHandlers.discarding());
        } catch (TransportFailure failure) {
            throw failure;
        }
        if (!isSuccess(response.statusCode())) {
            throw unexpectedStatus("DELETE", key, response.statusCode());
        }
        final String responseVersion = responseVersionOrFallback(response, null, "DELETE", key);
        if (!exactVersion.equals(responseVersion)) {
            throw new IllegalStateException("S3 delete response version differs from requested version: " + key);
        }
        final String requestId = response.headers()
                .firstValue("x-amz-request-id")
                .filter(value -> !value.isBlank())
                .orElseThrow(() -> new IllegalStateException("S3 delete response omitted provider request ID: " + key));
        final String secondaryRequestId = response.headers()
                .firstValue("x-amz-id-2")
                .filter(value -> !value.isBlank())
                .orElse("");
        final byte[] requestIdHash = Bytes.sha256(
                Bytes.utf8("nereus-delay-s3-delete-request\0"),
                Bytes.lp32(Bytes.utf8(key)),
                Bytes.lp32(Bytes.utf8(exactVersion)),
                Bytes.lp32(Bytes.utf8(requestId)));
        final byte[] responseHash = Bytes.sha256(
                Bytes.utf8("nereus-delay-s3-delete-response\0"),
                Bytes.lp32(Bytes.utf8(key)),
                Bytes.lp32(Bytes.utf8(exactVersion)),
                Bytes.u32be(response.statusCode()),
                Bytes.lp32(Bytes.utf8(responseVersion)),
                Bytes.lp32(Bytes.utf8(requestId)),
                Bytes.lp32(Bytes.utf8(secondaryRequestId)));
        return new DeleteOperation(requestIdHash, responseHash);
    }

    private static byte[] aggregateSweepRequestIds(
            final ProviderResponseEvidence initialListing,
            final List<DeleteOperation> operations,
            final ProviderResponseEvidence finalListing) {
        final byte[][] fields = new byte[operations.size() + 3][];
        fields[0] = Bytes.utf8("nereus-delay-s3-prefix-sweep-request-ids\0");
        fields[1] = requireRequestIdHash(initialListing, "initial version listing");
        for (int index = 0; index < operations.size(); index++) {
            fields[index + 2] = operations.get(index).requestIdHash();
        }
        fields[fields.length - 1] = requireRequestIdHash(finalListing, "final version listing");
        return Bytes.sha256(fields);
    }

    private static byte[] aggregateSweepResponses(
            final ProviderResponseEvidence initialListing,
            final List<DeleteOperation> operations,
            final ProviderResponseEvidence finalListing) {
        final byte[][] fields = new byte[operations.size() + 3][];
        fields[0] = Bytes.utf8("nereus-delay-s3-prefix-sweep-responses\0");
        fields[1] = Bytes.lp32(initialListing.responseHash());
        for (int index = 0; index < operations.size(); index++) {
            fields[index + 2] = Bytes.lp32(operations.get(index).responseHash());
        }
        fields[fields.length - 1] = Bytes.lp32(finalListing.responseHash());
        return Bytes.sha256(fields);
    }

    private static byte[] requireRequestIdHash(final ProviderResponseEvidence evidence, final String operation) {
        final byte[] requestIdHash = evidence.requestIdHash();
        if (requestIdHash == null) {
            throw new IllegalStateException("S3 " + operation + " omitted provider request ID");
        }
        return Bytes.lp32(requestIdHash);
    }

    private static byte[] aggregateDeleteRequestIds(final List<DeleteOperation> operations) {
        final byte[][] fields = new byte[operations.size() + 1][];
        fields[0] = Bytes.utf8("nereus-delay-s3-delete-request-ids\0");
        for (int index = 0; index < operations.size(); index++) {
            fields[index + 1] = Bytes.lp32(operations.get(index).requestIdHash());
        }
        return Bytes.sha256(fields);
    }

    private static byte[] aggregateDeleteResponses(final List<DeleteOperation> operations) {
        final byte[][] fields = new byte[operations.size() + 1][];
        fields[0] = Bytes.utf8("nereus-delay-s3-delete-responses\0");
        for (int index = 0; index < operations.size(); index++) {
            fields[index + 1] = Bytes.lp32(operations.get(index).responseHash());
        }
        return Bytes.sha256(fields);
    }

    private static byte[] aggregateProbeRequestIds(final List<ProviderResponseEvidence> evidence) {
        final byte[][] fields = new byte[evidence.size() + 1][];
        fields[0] = Bytes.utf8("nereus-delay-s3-delete-probe-request-ids\0");
        for (int index = 0; index < evidence.size(); index++) {
            final byte[] requestIdHash = evidence.get(index).requestIdHash();
            if (requestIdHash == null) {
                throw new IllegalStateException("S3 delete absence probe omitted provider request ID");
            }
            fields[index + 1] = Bytes.lp32(requestIdHash);
        }
        return Bytes.sha256(fields);
    }

    private static byte[] aggregateProbeResponses(final List<ProviderResponseEvidence> evidence) {
        final byte[][] fields = new byte[evidence.size() + 1][];
        fields[0] = Bytes.utf8("nereus-delay-s3-delete-probe-responses\0");
        for (int index = 0; index < evidence.size(); index++) {
            fields[index + 1] = Bytes.lp32(evidence.get(index).responseHash());
        }
        return Bytes.sha256(fields);
    }

    private static ProviderResponseEvidence responseEvidence(
            final String method,
            final String key,
            final int status,
            final String version,
            final byte[] bodyHash,
            final HttpResponse<?> response) {
        final String requestId = response.headers()
                .firstValue("x-amz-request-id")
                .filter(value -> !value.isBlank())
                .orElse(null);
        final String secondaryRequestId = response.headers()
                .firstValue("x-amz-id-2")
                .filter(value -> !value.isBlank())
                .orElse("");
        final byte[] requestIdHash = requestId == null
                ? null
                : Bytes.sha256(
                        Bytes.utf8("nereus-delay-s3-probe-request\0"),
                        Bytes.lp32(Bytes.utf8(method)),
                        Bytes.lp32(Bytes.utf8(key)),
                        Bytes.lp32(Bytes.utf8(requestId)));
        final byte[] responseHash = Bytes.sha256(
                Bytes.utf8("nereus-delay-s3-probe-response\0"),
                Bytes.lp32(Bytes.utf8(method)),
                Bytes.lp32(Bytes.utf8(key)),
                Bytes.u32be(status),
                Bytes.lp32(Bytes.utf8(version == null ? "" : version)),
                bodyHash,
                Bytes.lp32(Bytes.utf8(requestId == null ? "" : requestId)),
                Bytes.lp32(Bytes.utf8(secondaryRequestId)));
        return new ProviderResponseEvidence(requestIdHash, responseHash);
    }

    private <T> HttpResponse<T> send(
            final String method,
            final String key,
            final String payloadHash,
            final HttpRequest.BodyPublisher body,
            final Map<String, String> extraHeaders,
            final HttpResponse.BodyHandler<T> handler) {
        return send(method, key, payloadHash, body, extraHeaders, null, handler);
    }

    private <T> HttpResponse<T> send(
            final String method,
            final String key,
            final String payloadHash,
            final HttpRequest.BodyPublisher body,
            final Map<String, String> extraHeaders,
            final String versionId,
            final HttpResponse.BodyHandler<T> handler) {
        return sendUri(method, key, objectUri(key, versionId), payloadHash, body, extraHeaders, handler);
    }

    private <T> HttpResponse<T> sendBucket(
            final String method,
            final String query,
            final String payloadHash,
            final HttpRequest.BodyPublisher body,
            final Map<String, String> extraHeaders,
            final HttpResponse.BodyHandler<T> handler) {
        return sendUri(method, "bucket?" + query, bucketUri(query), payloadHash, body, extraHeaders, handler);
    }

    private <T> HttpResponse<T> sendUri(
            final String method,
            final String operation,
            final URI uri,
            final String payloadHash,
            final HttpRequest.BodyPublisher body,
            final Map<String, String> extraHeaders,
            final HttpResponse.BodyHandler<T> handler) {
        final Instant now = clock.instant().truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
        final String amzDate = AMZ_DATE.format(now);
        final String shortDate = SHORT_DATE.format(now);
        final Map<String, String> signedHeaders = new TreeMap<>();
        signedHeaders.put("host", hostHeader(uri));
        signedHeaders.put("x-amz-content-sha256", payloadHash);
        signedHeaders.put("x-amz-date", amzDate);
        if (sessionToken != null) {
            signedHeaders.put("x-amz-security-token", sessionToken);
        }
        for (Map.Entry<String, String> header : extraHeaders.entrySet()) {
            final String name = header.getKey().toLowerCase(Locale.ROOT);
            if (name.startsWith("x-amz-")) {
                signedHeaders.put(name, canonicalHeaderValue(header.getValue()));
            }
        }
        final String canonicalHeaders = signedHeaders.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + canonicalHeaderValue(entry.getValue()) + "\n")
                .reduce(new StringBuilder(), StringBuilder::append, StringBuilder::append)
                .toString();
        final String signedHeaderNames = String.join(";", signedHeaders.keySet());
        final String canonicalQuery = uri.getRawQuery() == null ? "" : uri.getRawQuery();
        final String canonicalRequest = method + "\n" + uri.getRawPath() + "\n" + canonicalQuery + "\n"
                + canonicalHeaders + "\n" + signedHeaderNames + "\n" + payloadHash;
        final String scope = shortDate + "/" + region + "/" + SERVICE + "/" + TERMINATOR;
        final String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n"
                + sha256Hex(canonicalRequest.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        final String signature = Bytes.hex(signingKey(shortDate, region, SERVICE, secretAccessKey, stringToSign));
        final String authorization = "AWS4-HMAC-SHA256 Credential=" + accessKeyId + "/" + scope + ", SignedHeaders="
                + signedHeaderNames + ", Signature=" + signature;
        final HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(requestTimeout)
                .method(method, body)
                .header("x-amz-date", amzDate)
                .header("x-amz-content-sha256", payloadHash)
                .header("Authorization", authorization);
        if (sessionToken != null) {
            builder.header("x-amz-security-token", sessionToken);
        }
        for (Map.Entry<String, String> header : extraHeaders.entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }
        try {
            requireCredentialGate();
            return client.send(builder.build(), handler);
        } catch (IOException failure) {
            throw new TransportFailure("S3 request failed: " + method + " " + operation, failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new TransportFailure("S3 request interrupted: " + method + " " + operation, failure);
        } catch (CompletionException failure) {
            throw new TransportFailure("S3 request failed: " + method + " " + operation, failure);
        }
    }

    private URI objectUri(final String key) {
        return objectUri(key, null);
    }

    private URI objectUri(final String key, final String versionId) {
        final String canonicalKey = canonicalObjectKey(key);
        final StringBuilder path = new StringBuilder(endpoint.getRawPath() == null ? "" : endpoint.getRawPath());
        while (path.length() > 0 && path.charAt(path.length() - 1) == '/') {
            path.deleteCharAt(path.length() - 1);
        }
        path.append('/').append(encodeSegment(bucket));
        for (String segment : canonicalKey.split("/", -1)) {
            path.append('/').append(encodeSegment(segment));
        }
        final String query = versionId == null ? "" : "?versionId=" + encodeVersionId(versionId);
        return URI.create(endpoint.getScheme() + "://" + endpoint.getRawAuthority() + path + query);
    }

    private URI bucketUri(final String query) {
        final StringBuilder path = new StringBuilder(endpoint.getRawPath() == null ? "" : endpoint.getRawPath());
        while (path.length() > 0 && path.charAt(path.length() - 1) == '/') {
            path.deleteCharAt(path.length() - 1);
        }
        path.append('/').append(encodeSegment(bucket));
        return URI.create(endpoint.getScheme() + "://" + endpoint.getRawAuthority() + path + "?" + query);
    }

    private static String checkpointPrefix(final CheckpointManifest manifest) {
        return "checkpoints/" + Bytes.hex(manifest.recoveryLineageId()) + "/" + Bytes.hex(manifest.checkpointId());
    }

    private static String checkpointPrefix(final byte[] recoveryLineageId, final byte[] checkpointId) {
        Bytes.requireLength(recoveryLineageId, 16, "recoveryLineageId");
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        return "checkpoints/" + Bytes.hex(recoveryLineageId) + "/" + Bytes.hex(checkpointId);
    }

    private static String objectKey(final String prefix, final CheckpointManifest.FileEntry file) {
        return prefix + "/objects/"
                + FilesystemCheckpointUploadAdapter.objectFileName(file.objectKey(), file.objectVersion());
    }

    private static void validateInventory(
            final Path directory, final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        final List<CheckpointFileInventory> actual = CheckpointFileInventory.collect(directory, limits);
        final List<CheckpointManifest.FileEntry> expected = manifest.files();
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("checkpoint file inventory differs from manifest");
        }
        for (int index = 0; index < actual.size(); index++) {
            final CheckpointFileInventory left = actual.get(index);
            final CheckpointManifest.FileEntry right = expected.get(index);
            if (!left.name().equals(right.name())
                    || left.length() != right.length()
                    || !Bytes.constantTimeEquals(left.checksum(), right.checksum())) {
                throw new IllegalArgumentException("checkpoint file differs from manifest: " + left.name());
            }
        }
    }

    private long objectBytesLimit(final long localLimit) {
        return Math.min(localLimit, objectStore.maxObjectBytes());
    }

    private ObjectStoreProviderOwnershipTracker.Operation beginProviderOperation() {
        return providerOwnership.begin();
    }

    private void requireCredentialGate() {
        if (credentialGate != null) {
            credentialGate.requireBeforeProviderCall();
        }
    }

    private static HashedFile consume(
            final InputStream input, final OutputStream output, final long expectedLength, final long maxBytes)
            throws IOException {
        final MessageDigest digest = sha256Digest();
        final byte[] buffer = new byte[BUFFER_BYTES];
        long length = 0;
        while (true) {
            final int read = input.read(buffer);
            if (read < 0) {
                break;
            }
            if (read == 0) {
                continue;
            }
            length = Math.addExact(length, read);
            if (maxBytes >= 0 && length > maxBytes) {
                throw new IOException("remote checkpoint object exceeds configured bound");
            }
            if (expectedLength >= 0 && length > expectedLength) {
                throw new IOException("remote checkpoint object is longer than expected");
            }
            digest.update(buffer, 0, read);
            if (output != null) {
                output.write(buffer, 0, read);
            }
        }
        if (expectedLength >= 0 && length != expectedLength) {
            throw new IOException("remote checkpoint object length differs from expected value");
        }
        return new HashedFile(length, digest.digest());
    }

    private static void verifyLocalFile(final Path source, final long expectedLength, final byte[] expectedChecksum) {
        try (InputStream input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS)) {
            if (Files.isSymbolicLink(source) || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("checkpoint source is not a regular file: " + source);
            }
            final HashedFile actual = consume(input, null, expectedLength, expectedLength);
            if (!Bytes.constantTimeEquals(actual.checksum(), expectedChecksum)) {
                throw new IllegalStateException("checkpoint source changed while uploading: " + source);
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read checkpoint source: " + source, failure);
        }
    }

    private static String responseVersion(final HttpResponse<?> response, final String fallback) {
        return response.headers()
                .firstValue("x-amz-version-id")
                .filter(value -> !value.isBlank())
                .orElse(fallback);
    }

    private String responseVersionOrFallback(
            final HttpResponse<?> response, final String fallback, final String method, final String key) {
        final String version = responseVersion(response, null);
        if (objectStore.requireExactVersionDelete() && version == null) {
            if (response.body() instanceof InputStream input) {
                closeQuietly(input);
            }
            throw new IllegalStateException("S3 response omitted exact immutable version: " + method + " " + key);
        }
        return version == null ? fallback : version;
    }

    private static boolean isSuccess(final int status) {
        return status >= 200 && status < 300;
    }

    private static boolean isAlreadyExists(final int status) {
        return status == 409 || status == 412;
    }

    /**
     * A provider 5xx after accepting a conditional PUT does not prove that the
     * immutable object is absent. The exact read-back below either resolves
     * the operation to the committed version or preserves the failure for an
     * explicit retry/reaping decision.
     */
    private static boolean isAmbiguousProviderStatus(final int status) {
        return status >= 500 && status < 600;
    }

    private static IllegalStateException unexpectedStatus(final String method, final String key, final int status) {
        return new IllegalStateException("S3 request returned HTTP " + status + ": " + method + " " + key);
    }

    private static String hostHeader(final URI uri) {
        final String host =
                Objects.requireNonNull(uri.getHost(), "endpoint host").toLowerCase(Locale.ROOT);
        final int port = uri.getPort();
        if (port < 0
                || (uri.getScheme().equals("https") && port == 443)
                || (uri.getScheme().equals("http") && port == 80)) {
            return host;
        }
        return host + ":" + port;
    }

    private static byte[] signingKey(
            final String date,
            final String region,
            final String service,
            final String secret,
            final String stringToSign) {
        final byte[] dateKey = hmac(Bytes.utf8("AWS4" + secret), date);
        final byte[] regionKey = hmac(dateKey, region);
        final byte[] serviceKey = hmac(regionKey, service);
        final byte[] signingKey = hmac(serviceKey, TERMINATOR);
        return hmac(signingKey, stringToSign);
    }

    private static byte[] hmac(final byte[] key, final String value) {
        try {
            final Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(Bytes.utf8(value));
        } catch (java.security.GeneralSecurityException impossible) {
            throw new IllegalStateException("HmacSHA256 is unavailable", impossible);
        }
    }

    private static String sha256Hex(final byte[] value) {
        return Bytes.hex(sha256(value));
    }

    private static byte[] sha256(final byte[] value) {
        return Bytes.sha256(value);
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String canonicalHeaderValue(final String value) {
        return Objects.requireNonNull(value, "header value").trim().replaceAll("\\s+", " ");
    }

    private static String encodeSegment(final String value) {
        final byte[] bytes = Bytes.utf8(value);
        final StringBuilder encoded = new StringBuilder(bytes.length);
        final char[] hex = "0123456789ABCDEF".toCharArray();
        for (byte element : bytes) {
            final int unsigned = Byte.toUnsignedInt(element);
            if ((unsigned >= 'a' && unsigned <= 'z')
                    || (unsigned >= 'A' && unsigned <= 'Z')
                    || (unsigned >= '0' && unsigned <= '9')
                    || unsigned == '-'
                    || unsigned == '_'
                    || unsigned == '.'
                    || unsigned == '~') {
                encoded.append((char) unsigned);
            } else {
                encoded.append('%').append(hex[unsigned >>> 4]).append(hex[unsigned & 0x0f]);
            }
        }
        return encoded.toString();
    }

    private static URI canonicalEndpoint(final URI value) {
        if (value.getScheme() == null
                || (!value.getScheme().equalsIgnoreCase("http")
                        && !value.getScheme().equalsIgnoreCase("https"))
                || value.getHost() == null
                || value.getUserInfo() != null
                || value.getQuery() != null
                || value.getFragment() != null) {
            throw new IllegalArgumentException("Object Store endpoint must be an http(s) URI without credentials");
        }
        final String rawPath = value.getRawPath() == null ? "" : value.getRawPath();
        if (rawPath.contains("//") || rawPath.contains("\\") || rawPath.contains("..")) {
            throw new IllegalArgumentException("Object Store endpoint path is not canonical");
        }
        final String scheme = value.getScheme().toLowerCase(Locale.ROOT);
        final String host = value.getHost().toLowerCase(Locale.ROOT);
        final int port = value.getPort();
        final String authority = host + (port < 0 ? "" : ":" + port);
        String path = rawPath;
        while (path.endsWith("/") && !path.isEmpty()) {
            path = path.substring(0, path.length() - 1);
        }
        try {
            return new URI(scheme, authority, path, null, null);
        } catch (java.net.URISyntaxException failure) {
            throw new IllegalArgumentException("Object Store endpoint is not canonical", failure);
        }
    }

    private static String canonicalObjectKey(final String value) {
        Objects.requireNonNull(value, "object key");
        if (value.isEmpty()
                || value.startsWith("/")
                || value.endsWith("/")
                || value.contains("//")
                || value.contains("\\")
                || value.contains("\0")
                || value.contains("..")) {
            throw new IllegalArgumentException("Object Store key is not canonical");
        }
        for (String segment : value.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("Object Store key contains an invalid segment");
            }
        }
        return value;
    }

    private static String decodeProviderVersion(final byte[] encoded) {
        final String version = new String(
                Objects.requireNonNull(encoded, "provider version"), java.nio.charset.StandardCharsets.UTF_8);
        if (version.isBlank() || !Arrays.equals(Bytes.utf8(version), encoded)) {
            throw new IllegalArgumentException("provider version is not canonical UTF-8 text");
        }
        return version;
    }

    private static String encodeVersionId(final String value) {
        return encodeSegment(canonicalText(value, "provider version"));
    }

    private static String canonicalBucket(final String value) {
        final String bucket = canonicalText(value, "bucket");
        if (!bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")) {
            throw new IllegalArgumentException("bucket is not a canonical S3 bucket name");
        }
        return bucket;
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(java.text.Normalizer.normalize(value, java.text.Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC text");
        }
        return value;
    }

    private static String nonEmpty(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return value;
    }

    private static Path normalizeDirectory(final Path value, final String name) {
        final Path directory =
                Objects.requireNonNull(value, name).toAbsolutePath().normalize();
        if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException(name + " is not a real directory: " + value);
        }
        return directory;
    }

    private static Path normalizeTarget(final Path value) {
        final Path target = Objects.requireNonNull(value, "targetDirectory")
                .toAbsolutePath()
                .normalize();
        if (target.getFileName() == null || Files.isSymbolicLink(target)) {
            throw new IllegalArgumentException("checkpoint target is not a real directory path");
        }
        return target;
    }

    private static void ensureDirectory(final Path directory, final String description) {
        try {
            LocalStatePathGuard.ensureRealDirectoryPath(directory, description);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot create " + description, failure);
        }
    }

    private static void ensureWithin(final Path parent, final Path child) {
        if (!child.toAbsolutePath()
                .normalize()
                .startsWith(parent.toAbsolutePath().normalize())) {
            throw new IllegalArgumentException("checkpoint path escapes its staging boundary: " + child);
        }
    }

    private static void moveCreateNew(final Path source, final Path target) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IllegalStateException("S3 checkpoint restore requires atomic rename", unsupported);
        } catch (FileAlreadyExistsException conflict) {
            throw new IllegalStateException("checkpoint restore target appeared during publication", conflict);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot publish restored checkpoint", failure);
        }
    }

    private static void forceDirectory(final Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot fsync checkpoint directory", failure);
        }
    }

    private static void deleteTree(final Path root) {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException failure) {
                    throw new IllegalStateException("cannot clean checkpoint staging directory", failure);
                }
            });
        } catch (IOException failure) {
            throw new IllegalStateException("cannot enumerate checkpoint staging directory", failure);
        }
    }

    private static void closeQuietly(final InputStream input) {
        try {
            input.close();
        } catch (IOException ignored) {
            // Preserve the provider status that caused the close.
        }
    }

    private record HashedFile(long length, byte[] checksum) {}

    private record RemoteBytes(byte[] bytes, String version, ProviderResponseEvidence evidence) {
        private RemoteBytes {
            bytes = Bytes.copy(bytes);
            Objects.requireNonNull(evidence, "evidence");
        }

        @Override
        public byte[] bytes() {
            return Bytes.copy(bytes);
        }

        private String version(final String fallback) {
            return version == null || version.isBlank() ? fallback : version;
        }
    }

    private record RemoteObjectObservation(boolean present, String version, ProviderResponseEvidence evidence) {
        private RemoteObjectObservation {
            Objects.requireNonNull(evidence, "evidence");
            if (present && (version == null || version.isBlank())) {
                throw new IllegalArgumentException("present remote object must carry a provider version");
            }
            if (!present && version != null) {
                throw new IllegalArgumentException("absent remote object must not carry a provider version");
            }
        }
    }

    private record ProviderResponseEvidence(byte[] requestIdHash, byte[] responseHash) {
        private ProviderResponseEvidence {
            Bytes.requireLength(responseHash, 32, "responseHash");
            responseHash = Bytes.copy(responseHash);
            if (requestIdHash != null) {
                Bytes.requireLength(requestIdHash, 32, "requestIdHash");
                requestIdHash = Bytes.copy(requestIdHash);
            }
        }

        @Override
        public byte[] requestIdHash() {
            return requestIdHash == null ? null : Bytes.copy(requestIdHash);
        }

        @Override
        public byte[] responseHash() {
            return Bytes.copy(responseHash);
        }
    }

    private record VersionedObject(String key, String version) {
        private VersionedObject {
            canonicalObjectKey(key);
            canonicalText(version, "provider version");
        }
    }

    private record VersionList(List<VersionedObject> entries, ProviderResponseEvidence evidence) {
        private VersionList {
            entries = List.copyOf(entries);
            Objects.requireNonNull(evidence, "evidence");
        }
    }

    private record DeletePlan(String key, String version) {}

    private record DeleteOperation(byte[] requestIdHash, byte[] responseHash) {
        private DeleteOperation {
            requestIdHash = Bytes.copy(requestIdHash);
            responseHash = Bytes.copy(responseHash);
        }

        @Override
        public byte[] requestIdHash() {
            return Bytes.copy(requestIdHash);
        }

        @Override
        public byte[] responseHash() {
            return Bytes.copy(responseHash);
        }
    }

    private static class TransportFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private TransportFailure(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    private static final class RemoteObjectMissing extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient ProviderResponseEvidence evidence;

        private RemoteObjectMissing(final String message, final ProviderResponseEvidence evidence) {
            super(message);
            this.evidence = Objects.requireNonNull(evidence, "evidence");
        }

        private ProviderResponseEvidence evidence() {
            return evidence;
        }
    }
}
