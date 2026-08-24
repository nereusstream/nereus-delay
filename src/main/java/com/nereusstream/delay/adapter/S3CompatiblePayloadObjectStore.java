package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommittedPayloadDescriptorV1;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import com.nereusstream.delay.protocol.ObjectStoreProviderKindV1;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import com.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileRefV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.UploadHandleKindV1;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PublishAttemptLedger;
import com.nereusstream.delay.store.ObjectStoreCredentialUseLeaseGate;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * S3-compatible large-payload adapter backed by the V1 reservation state
 * machine.
 *
 * <p>The adapter keeps reservation/handle/proof authority in the existing
 * {@link InMemoryPayloadObjectStore} state machine, while this class supplies
 * authenticated remote immutable-byte operations. Every create uses
 * {@code If-None-Match: *}; an existing object is accepted only after an
 * exact byte comparison. Attestation requires a provider-issued version-id,
 * records the provider ETag when present, and verifies the object again on
 * Worker read. The remote namespace is derived from the semantic Profile and
 * the service-owned reservation identity; caller-supplied object paths are
 * never used.</p>
 */
public final class S3CompatiblePayloadObjectStore {
    private static final byte[] ENDPOINT_DOMAIN = Bytes.utf8("nereus-delay-s3-endpoint-v1\0");
    private static final byte[] CREDENTIAL_DOMAIN = Bytes.utf8("nereus-delay-s3-credential-scope-v1\0");
    private static final byte[] CONTAINER_PREFIX = Bytes.utf8("nereus-delay-local/");
    private static final byte[] OBJECT_KEY_PREFIX = Bytes.utf8("reservation/");
    private static final String SERVICE = "s3";
    private static final String TERMINATOR = "aws4_request";
    private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter SHORT_DATE =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final ProfileSemanticEnvelopeV1 profile;
    private final ObjectStoreProfileSemanticV1 objectStore;
    private final URI endpoint;
    private final String region;
    private final String bucket;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final HttpClient client;
    private final Clock clock;
    private final Duration requestTimeout;
    private final ObjectStoreCredentialUseLeaseGate credentialGate;
    private final S3PayloadBackend backend;
    private final InMemoryPayloadObjectStore delegate;

    /** Creates an ungated adapter for an explicitly authorized test/service account. */
    public S3CompatiblePayloadObjectStore(
            final ProfileSemanticEnvelopeV1 profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemanticV1 trustSet,
            final int proofKeyVersion,
            final java.security.PrivateKey proofSigningKey) {
        this(
                profile,
                endpoint,
                region,
                bucket,
                accessKeyId,
                secretAccessKey,
                sessionToken,
                tenantRoutingScope,
                trustSet,
                proofKeyVersion,
                Long.MAX_VALUE,
                proofSigningKey,
                null,
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .followRedirects(HttpClient.Redirect.NEVER)
                        .build(),
                Clock.systemUTC(),
                Duration.ofSeconds(60));
    }

    /** Creates an adapter whose every remote call is guarded by one exact credential-use lease. */
    public S3CompatiblePayloadObjectStore(
            final ProfileSemanticEnvelopeV1 profile,
            final URI endpoint,
            final String region,
            final String bucket,
            final String accessKeyId,
            final String secretAccessKey,
            final String sessionToken,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemanticV1 trustSet,
            final int proofKeyVersion,
            final long maxUploadHandleLifetimeMs,
            final java.security.PrivateKey proofSigningKey,
            final ObjectStoreCredentialUseLeaseGate credentialGate,
            final HttpClient client,
            final Clock clock,
            final Duration requestTimeout) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemanticV1 semantic)) {
            throw new IllegalArgumentException("S3 payload adapter requires an OBJECT_STORE Profile");
        }
        if (semantic.providerKind() != ObjectStoreProviderKindV1.S3
                && semantic.providerKind() != ObjectStoreProviderKindV1.S3_COMPATIBLE) {
            throw new IllegalArgumentException("S3 payload adapter does not support the selected provider");
        }
        this.objectStore = semantic;
        this.endpoint = canonicalEndpoint(Objects.requireNonNull(endpoint, "endpoint"));
        this.region = canonicalText(region, "region");
        this.bucket = canonicalBucket(bucket);
        this.accessKeyId = canonicalText(accessKeyId, "accessKeyId");
        this.secretAccessKey = nonEmpty(secretAccessKey, "secretAccessKey");
        this.sessionToken = sessionToken == null ? null : nonEmpty(sessionToken, "sessionToken");
        this.credentialGate = credentialGate;
        this.client = Objects.requireNonNull(client, "client");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
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
        this.backend = new S3PayloadBackend();
        this.delegate = new InMemoryPayloadObjectStore(
                profile,
                tenantRoutingScope,
                trustSet,
                proofKeyVersion,
                maxUploadHandleLifetimeMs,
                proofSigningKey,
                backend);
    }

    /** Returns the endpoint digest bound by the semantic Object Store Profile. */
    public static byte[] endpointConfigDigest(final URI endpoint, final String region, final String bucket) {
        final URI canonical = canonicalEndpoint(Objects.requireNonNull(endpoint, "endpoint"));
        return Bytes.sha256(
                ENDPOINT_DOMAIN,
                Bytes.utf8(canonical.toString()),
                Bytes.utf8(canonicalText(region, "region")),
                Bytes.utf8(canonicalBucket(bucket)));
    }

    /** Returns the non-secret credential scope digest bound by the Profile. */
    public static byte[] credentialAuthorizationScopeDigest(
            final String accessKeyId, final String region, final String bucket) {
        return Bytes.sha256(
                CREDENTIAL_DOMAIN,
                Bytes.utf8(canonicalText(accessKeyId, "accessKeyId")),
                Bytes.utf8(canonicalText(region, "region")),
                Bytes.utf8(canonicalBucket(bucket)));
    }

    public void register(final PayloadReservation reservation) {
        delegate.register(reservation);
    }

    public void register(
            final PayloadReservation reservation,
            final PayloadProofTrustSetRefV1 pinnedTrustSet,
            final ProfileRefV1 pinnedObjectStoreProfile) {
        delegate.register(reservation, pinnedTrustSet, pinnedObjectStoreProfile);
    }

    public PayloadReservationReceiptV1 reservationReceipt(final PayloadReservation reservation) {
        return delegate.reservationReceipt(reservation);
    }

    public PayloadUploadHandleResponseV1 issueUploadHandle(
            final byte[] reservationId, final UploadHandleKindV1 kind, final long nowEpochMs) {
        return delegate.issueUploadHandle(reservationId, kind, nowEpochMs);
    }

    public PayloadUploadHandleResponseV1 issueUploadHandle(
            final PayloadReservationReceiptV1 receipt, final UploadHandleKindV1 kind, final long nowEpochMs) {
        return delegate.issueUploadHandle(receipt, kind, nowEpochMs);
    }

    public void upload(final OpaquePayloadUploadHandleV1 handle, final byte[] payload, final long nowEpochMs) {
        delegate.upload(handle, payload, nowEpochMs);
    }

    public void upload(
            final PayloadReservationReceiptV1 receipt,
            final OpaquePayloadUploadHandleV1 handle,
            final byte[] payload,
            final long nowEpochMs) {
        delegate.upload(receipt, handle, payload, nowEpochMs);
    }

    public PayloadAttestationResponseV1 attest(final OpaquePayloadUploadHandleV1 handle, final long nowEpochMs) {
        return delegate.attest(handle, nowEpochMs);
    }

    public PayloadAttestationResponseV1 attest(
            final PayloadReservationReceiptV1 receipt,
            final OpaquePayloadUploadHandleV1 handle,
            final long nowEpochMs) {
        return delegate.attest(receipt, handle, nowEpochMs);
    }

    /** Reads and re-verifies the exact committed Object Store identity. */
    public byte[] readPayload(final PayloadReference reference) {
        return readVerified(Objects.requireNonNull(reference, "reference"));
    }

    /**
     * Worker payload provider for a retained PUBLISHING ledger. Inline payloads
     * are returned from the immutable Admission; object payloads are fetched
     * through the exact committed descriptor and re-verified before handoff.
     */
    public Optional<byte[]> load(final PublishAttemptLedger attempt) {
        Objects.requireNonNull(attempt, "attempt");
        final PublishAdmissionBody admission = PublishAdmissionBody.decode(attempt.admissionBytes());
        final CommittedPayloadDescriptorV1 descriptor =
                admission.descriptor().value().payload().hasObject()
                        ? admission.descriptor().value().payload().object()
                        : null;
        if (descriptor == null) {
            return Optional.of(admission.descriptor().value().payload().inlinePayload());
        }
        return Optional.of(readPayload(PayloadReference.fromDescriptor(descriptor)));
    }

    private String remoteObjectKey(final String objectIdentity) {
        requireIdentity(objectIdentity);
        return "payloads/" + Bytes.hex(profile.profileId()) + "/" + objectIdentity + ".payload";
    }

    private byte[] expectedContainer() {
        return Bytes.concat(CONTAINER_PREFIX, Bytes.utf8(Bytes.hex(profile.profileId())));
    }

    private static byte[] expectedObjectKey(final byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        return Bytes.concat(OBJECT_KEY_PREFIX, Bytes.utf8(Bytes.hex(reservationId)));
    }

    private String identityFor(final PayloadReference reference) {
        if (!Bytes.constantTimeEquals(reference.objectStoreProfileHash(), profile.semanticHash())
                || !Arrays.equals(reference.container(), expectedContainer())
                || !reference.hasCommitIdentity()
                || !Arrays.equals(reference.objectKey(), expectedObjectKey(reference.reservationId()))) {
            throw new IllegalArgumentException("payload reference is not bound to this S3 adapter");
        }
        return Bytes.hex(reference.reservationId());
    }

    private byte[] readVerified(final PayloadReference reference) {
        final String identity = identityFor(reference);
        final HttpResponse<InputStream> response = send(
                "GET",
                remoteObjectKey(identity),
                EMPTY_SHA256,
                HttpRequest.BodyPublishers.noBody(),
                Map.of(),
                HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 404) {
            closeQuietly(response.body());
            throw new IllegalStateException("committed payload object is missing: " + identity);
        }
        if (!isSuccess(response.statusCode())) {
            closeQuietly(response.body());
            throw unexpectedStatus("GET", identity, response.statusCode());
        }
        try (InputStream input = response.body()) {
            final byte[] bytes = readBounded(input, objectStore.maxObjectBytes());
            if (bytes.length != reference.length()
                    || !Bytes.constantTimeEquals(Bytes.sha256(bytes), reference.payloadSha256())) {
                throw new IllegalStateException("committed payload bytes differ from the exact reference");
            }
            requireVersion(response, reference.immutableObjectVersion(), identity);
            if (reference.etag() != null) {
                final String etag = response.headers()
                        .firstValue("etag")
                        .map(String::trim)
                        .orElseThrow(() ->
                                new IllegalStateException("S3 payload response omitted pinned ETag: " + identity));
                if (!Arrays.equals(Bytes.utf8(etag), reference.etag())) {
                    throw new IllegalStateException("committed payload ETag differs from the exact reference");
                }
            }
            final String metadataHash = response.headers()
                    .firstValue("x-amz-meta-nereus-sha256")
                    .map(String::trim)
                    .orElse(null);
            if (metadataHash != null && !metadataHash.equals(Bytes.hex(reference.payloadSha256()))) {
                throw new IllegalStateException("committed payload SHA metadata differs from the exact reference");
            }
            return bytes;
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read committed payload object: " + identity, failure);
        }
    }

    private final class S3PayloadBackend implements PayloadObjectBackend {
        @Override
        public byte[] read(final String objectIdentity) {
            final HttpResponse<InputStream> response = send(
                    "GET",
                    remoteObjectKey(objectIdentity),
                    EMPTY_SHA256,
                    HttpRequest.BodyPublishers.noBody(),
                    Map.of(),
                    HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() == 404) {
                closeQuietly(response.body());
                return null;
            }
            if (!isSuccess(response.statusCode())) {
                closeQuietly(response.body());
                throw unexpectedStatus("GET", objectIdentity, response.statusCode());
            }
            try (InputStream input = response.body()) {
                return readBounded(input, objectStore.maxObjectBytes());
            } catch (IOException failure) {
                throw new IllegalStateException("cannot read payload object: " + objectIdentity, failure);
            }
        }

        @Override
        public void putIfAbsent(final String objectIdentity, final byte[] payload, final long maxBytes) {
            requireIdentity(objectIdentity);
            Objects.requireNonNull(payload, "payload");
            if (payload.length > maxBytes || maxBytes != objectStore.maxObjectBytes()) {
                throw new IllegalArgumentException("payload backend size bound changed");
            }
            final String payloadHash = Bytes.hex(Bytes.sha256(payload));
            final HttpResponse<Void> response;
            try {
                response = send(
                        "PUT",
                        remoteObjectKey(objectIdentity),
                        payloadHash,
                        HttpRequest.BodyPublishers.ofByteArray(payload),
                        Map.of("if-none-match", "*", "x-amz-meta-nereus-sha256", payloadHash),
                        HttpResponse.BodyHandlers.discarding());
            } catch (IllegalStateException failure) {
                resolveAfterAmbiguousPut(objectIdentity, payload, failure);
                return;
            }
            if (isSuccess(response.statusCode())) {
                requireVersion(response, null, objectIdentity);
                return;
            }
            if (response.statusCode() == 409 || response.statusCode() == 412) {
                final byte[] existing = read(objectIdentity);
                if (existing != null && Arrays.equals(existing, payload)) {
                    return;
                }
                throw new IllegalStateException("immutable payload object identity conflict: " + objectIdentity);
            }
            if (isAmbiguousProviderStatus(response.statusCode())) {
                resolveAfterAmbiguousPut(
                        objectIdentity, payload, unexpectedStatus("PUT", objectIdentity, response.statusCode()));
                return;
            }
            throw unexpectedStatus("PUT", objectIdentity, response.statusCode());
        }

        private void resolveAfterAmbiguousPut(
                final String objectIdentity, final byte[] payload, final IllegalStateException original) {
            final byte[] existing;
            try {
                existing = read(objectIdentity);
            } catch (RuntimeException lookupFailure) {
                original.addSuppressed(lookupFailure);
                throw original;
            }
            if (existing != null && Arrays.equals(existing, payload)) {
                return;
            }
            if (existing != null) {
                throw new IllegalStateException(
                        "immutable payload object identity conflict: " + objectIdentity, original);
            }
            throw original;
        }

        @Override
        public byte[] immutableObjectVersion(final String objectIdentity, final byte[] payloadSha256) {
            return head(objectIdentity).version();
        }

        @Override
        public byte[] etag(final String objectIdentity, final byte[] payloadSha256) {
            return head(objectIdentity).etag();
        }

        private RemoteIdentity head(final String objectIdentity) {
            requireIdentity(objectIdentity);
            final HttpResponse<Void> response = send(
                    "HEAD",
                    remoteObjectKey(objectIdentity),
                    EMPTY_SHA256,
                    HttpRequest.BodyPublishers.noBody(),
                    Map.of(),
                    HttpResponse.BodyHandlers.discarding());
            if (!isSuccess(response.statusCode())) {
                throw unexpectedStatus("HEAD", objectIdentity, response.statusCode());
            }
            final byte[] version = requireVersion(response, null, objectIdentity);
            final String etag =
                    response.headers().firstValue("etag").map(String::trim).orElse(null);
            return new RemoteIdentity(version, etag == null ? null : Bytes.utf8(etag));
        }
    }

    private <T> HttpResponse<T> send(
            final String method,
            final String key,
            final String payloadHash,
            final HttpRequest.BodyPublisher body,
            final Map<String, String> extraHeaders,
            final HttpResponse.BodyHandler<T> handler) {
        final URI uri = objectUri(key);
        final Instant now = clock.instant().truncatedTo(ChronoUnit.SECONDS);
        final String amzDate = AMZ_DATE.format(now);
        final String shortDate = SHORT_DATE.format(now);
        final Map<String, String> signedHeaders = new TreeMap<>();
        signedHeaders.put("host", hostHeader(uri));
        signedHeaders.put("x-amz-content-sha256", payloadHash);
        signedHeaders.put("x-amz-date", amzDate);
        if (sessionToken != null) {
            signedHeaders.put("x-amz-security-token", sessionToken);
        }
        extraHeaders.forEach((name, value) -> {
            final String lower = name.toLowerCase(Locale.ROOT);
            if (lower.startsWith("x-amz-")) {
                signedHeaders.put(lower, canonicalHeaderValue(value));
            }
        });
        final String canonicalHeaders = signedHeaders.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + canonicalHeaderValue(entry.getValue()) + "\n")
                .collect(Collectors.joining());
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
        extraHeaders.forEach(builder::header);
        try {
            if (credentialGate != null) {
                credentialGate.requireBeforeProviderCall();
            }
            return client.send(builder.build(), handler);
        } catch (IOException failure) {
            throw new IllegalStateException("S3 payload request failed: " + method + " " + key, failure);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("S3 payload request interrupted: " + method + " " + key, failure);
        } catch (CompletionException failure) {
            throw new IllegalStateException("S3 payload request failed: " + method + " " + key, failure);
        }
    }

    private URI objectUri(final String key) {
        final String canonicalKey = canonicalObjectKey(key);
        final StringBuilder path = new StringBuilder(endpoint.getRawPath() == null ? "" : endpoint.getRawPath());
        while (path.length() > 0 && path.charAt(path.length() - 1) == '/') {
            path.deleteCharAt(path.length() - 1);
        }
        path.append('/').append(encodeSegment(bucket));
        for (String segment : canonicalKey.split("/", -1)) {
            path.append('/').append(encodeSegment(segment));
        }
        return URI.create(endpoint.getScheme() + "://" + endpoint.getRawAuthority() + path);
    }

    private static byte[] requireVersion(final HttpResponse<?> response, final byte[] expected, final String identity) {
        final String value = response.headers()
                .firstValue("x-amz-version-id")
                .map(String::trim)
                .filter(item -> !item.isBlank())
                .orElseThrow(
                        () -> new IllegalStateException("S3 payload response omitted immutable version: " + identity));
        final byte[] version = Bytes.utf8(value);
        if (expected != null && !Arrays.equals(expected, version)) {
            throw new IllegalStateException("S3 payload version differs from the exact reference: " + identity);
        }
        return version;
    }

    private static byte[] readBounded(final InputStream input, final long maxBytes) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final byte[] buffer = new byte[64 * 1024];
        long total = 0;
        int read;
        while ((read = input.read(buffer)) != -1) {
            total = Math.addExact(total, read);
            if (total > maxBytes) {
                throw new IllegalStateException("remote payload exceeds Object Store profile maximum");
            }
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static boolean isSuccess(final int status) {
        return status >= 200 && status < 300;
    }

    private static boolean isAmbiguousProviderStatus(final int status) {
        return status >= 500 && status < 600;
    }

    private static IllegalStateException unexpectedStatus(final String method, final String key, final int status) {
        return new IllegalStateException("S3 payload request returned HTTP " + status + ": " + method + " " + key);
    }

    private static void closeQuietly(final InputStream input) {
        try {
            input.close();
        } catch (IOException ignored) {
            // Preserve the provider status that caused the close.
        }
    }

    private static void requireIdentity(final String identity) {
        Objects.requireNonNull(identity, "objectIdentity");
        if (identity.length() != 64
                || !identity.equals(identity.toLowerCase(Locale.ROOT))
                || !identity.chars()
                        .allMatch(character ->
                                (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
            throw new IllegalArgumentException("payload object identity must be a 32-byte lowercase hex value");
        }
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
        return Bytes.hex(Bytes.sha256(value));
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

    private record RemoteIdentity(byte[] version, byte[] etag) {
        private RemoteIdentity {
            version = Bytes.copy(Objects.requireNonNull(version, "version"));
            etag = etag == null ? null : Bytes.copy(etag);
        }

        @Override
        public byte[] version() {
            return Bytes.copy(version);
        }

        @Override
        public byte[] etag() {
            return etag == null ? null : Bytes.copy(etag);
        }
    }
}
