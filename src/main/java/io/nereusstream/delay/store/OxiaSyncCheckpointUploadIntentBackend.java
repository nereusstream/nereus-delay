package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Durable Oxia CAS backend for one checkpoint upload intent.
 *
 * <p>The intent is one canonical record keyed by shard/checkpoint identity.
 * PENDING_UPLOAD to PUBLISHED/REAPING transitions use version CAS, and a
 * response-loss retry is accepted only after an exact successor reread. This
 * class does not claim the separate Owner Lease/session and catalog publication
 * transaction required by V1.</p>
 */
public final class OxiaSyncCheckpointUploadIntentBackend implements CheckpointUploadIntentAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_INTENT_BYTES = 8 * 1024 * 1024;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-oxia-checkpoint-intent-v1\0");

    private final RecordClient client;
    private final String keyPrefix;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncCheckpointUploadIntentBackend(final SyncOxiaClient client, final String keyPrefix) {
        this(new SyncRecordClient(client), keyPrefix);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncCheckpointUploadIntentBackend(final RecordClient client, final String keyPrefix) {
        this.client = Objects.requireNonNull(client, "client");
        this.keyPrefix = canonicalKeyPrefix(keyPrefix);
    }

    @Override
    public CheckpointUploadIntentV1 create(final CheckpointUploadIntentV1 pending) {
        requireState(pending, CheckpointUploadStateV1.PENDING_UPLOAD);
        final String key = intentKey(pending);
        final Entry existing = read(key, pending);
        if (existing != null) {
            if (existing.intent().equals(pending)) {
                return existing.intent();
            }
            throw new IllegalStateException("checkpoint upload intent CAS conflict");
        }
        try {
            putExact(key, encode(pending), Set.of(PutOption.IfRecordDoesNotExist));
            return pending;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, pending);
            if (observed != null && observed.intent().equals(pending)) {
                return observed.intent();
            }
            throw new IllegalStateException("checkpoint upload intent CAS conflict", race);
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, pending);
            if (observed != null && observed.intent().equals(pending)) {
                return observed.intent();
            }
            throw responseFailure;
        }
    }

    @Override
    public CheckpointUploadIntentV1 publish(final CheckpointUploadIntentV1 expectedPending,
                                            final CheckpointResourceV1 resource) {
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        Objects.requireNonNull(resource, "resource");
        final Entry existing = requireExpected(expectedPending);
        final CheckpointUploadIntentV1 next = next(expectedPending, CheckpointUploadStateV1.PUBLISHED,
                resource, null);
        return casSuccessor(existing, next);
    }

    @Override
    public Optional<CheckpointUploadIntentV1> currentPublishedFor(
            final CheckpointUploadIntentV1 expectedPending) {
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        final Entry existing = read(intentKey(expectedPending), expectedPending);
        if (existing == null || existing.intent().state() != CheckpointUploadStateV1.PUBLISHED) {
            return Optional.empty();
        }
        final CheckpointUploadIntentV1 expected = next(expectedPending, CheckpointUploadStateV1.PUBLISHED,
                existing.intent().publishedManifest(), null);
        return existing.intent().equals(expected) ? Optional.of(existing.intent()) : Optional.empty();
    }

    @Override
    public CheckpointUploadIntentV1 beginReaping(final CheckpointUploadIntentV1 expectedPending,
                                                 final TrustedUtcIntervalEvidence evidence) {
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        Objects.requireNonNull(evidence, "evidence");
        evidence.requireEarliestAtLeast(expectedPending.uploadDeadlineEpochMs());
        final Entry existing = read(intentKey(expectedPending), expectedPending);
        final CheckpointUploadIntentV1 next = next(expectedPending, CheckpointUploadStateV1.REAPING,
                null, evidence);
        if (existing != null && existing.intent().state() == CheckpointUploadStateV1.REAPING) {
            if (existing.intent().equals(next)) {
                return existing.intent();
            }
            throw new IllegalStateException("checkpoint reaping successor does not match current state");
        }
        if (existing == null || !existing.intent().equals(expectedPending)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
        return casSuccessor(existing, next);
    }

    @Override
    public CheckpointUploadIntentV1 beginReaping(final CheckpointUploadIntentV1 expectedPending,
                                                 final TrustedUtcIntervalEvidence evidence,
                                                 final RecoveryCatalogAuthority catalog) {
        final CheckpointReapingGuard.Decision decision = CheckpointReapingGuard.evaluate(expectedPending, evidence,
                catalog);
        if (decision != CheckpointReapingGuard.Decision.REAPING_ALLOWED) {
            throw new IllegalStateException("checkpoint reaping guard rejected: " + decision);
        }
        return beginReaping(expectedPending, evidence);
    }

    /** Reads the exact intent identified by its shard/checkpoint tuple. */
    @Override
    public Optional<CheckpointUploadIntentV1> current(final CheckpointUploadIntentV1 identity) {
        Objects.requireNonNull(identity, "identity");
        final Entry existing = read(intentKey(identity), identity);
        return existing == null ? Optional.empty() : Optional.of(existing.intent());
    }

    private CheckpointUploadIntentV1 casSuccessor(final Entry existing,
                                                  final CheckpointUploadIntentV1 next) {
        final String key = intentKey(next);
        final byte[] value = encode(next);
        try {
            putExact(key, value, Set.of(PutOption.IfVersionIdEquals(existing.versionId())));
            return next;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException race) {
            final Entry observed = read(key, next);
            if (observed != null && observed.intent().equals(next)) {
                return observed.intent();
            }
            throw new IllegalStateException("checkpoint upload intent successor CAS conflict", race);
        } catch (RuntimeException responseFailure) {
            final Entry observed = read(key, next);
            if (observed != null && observed.intent().equals(next)) {
                return observed.intent();
            }
            throw responseFailure;
        }
    }

    private Entry requireExpected(final CheckpointUploadIntentV1 expected) {
        final Entry existing = read(intentKey(expected), expected);
        if (existing == null || !existing.intent().equals(expected)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
        return existing;
    }

    private Entry read(final String key, final CheckpointUploadIntentV1 identity) {
        final GetResult result = client.get(key);
        if (result == null) {
            return null;
        }
        if (!key.equals(result.key()) || result.value() == null || result.version() == null) {
            throw new IllegalStateException("Oxia upload intent response has an invalid record identity");
        }
        return decode(result.value(), identity, result.version().versionId());
    }

    private void putExact(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final PutResult result = client.put(key, value, options);
        if (result == null || !key.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia upload intent put returned no exact version");
        }
    }

    private String intentKey(final CheckpointUploadIntentV1 intent) {
        return keyPrefix + "/intent/" + Bytes.hex(Bytes.concat(intent.shard().canonicalHashBytes(),
                intent.checkpointId()));
    }

    private static CheckpointUploadIntentV1 next(final CheckpointUploadIntentV1 expected,
                                                  final CheckpointUploadStateV1 state,
                                                  final CheckpointResourceV1 resource,
                                                  final TrustedUtcIntervalEvidence evidence) {
        return new CheckpointUploadIntentV1(expected.shard(), expected.recoveryLineageId(), expected.checkpointId(),
                expected.owner(), expected.sourceStoreIncarnation(), expected.uploadToken(),
                expected.baseCatalogGeneration(), expected.parentCheckpointId(), expected.parentManifestSha256(),
                expected.objectStoreProfile(), expected.checkpointCreatedAt(), expected.uploadDeadlineEpochMs(), state,
                incrementRevision(expected.stateRevision()), resource, evidence);
    }

    private static long incrementRevision(final long revision) {
        if (revision == -1L) {
            throw new IllegalStateException("checkpoint upload intent state revision exhausted");
        }
        return revision + 1;
    }

    private static void requireState(final CheckpointUploadIntentV1 intent, final CheckpointUploadStateV1 state) {
        Objects.requireNonNull(intent, "intent");
        if (intent.state() != state) {
            throw new IllegalArgumentException("checkpoint upload intent must be " + state);
        }
    }

    private static byte[] encode(final CheckpointUploadIntentV1 intent) {
        final byte[] intentBytes = intent.canonicalBytes();
        if (intentBytes.length == 0 || intentBytes.length > MAX_INTENT_BYTES) {
            throw new IllegalArgumentException("checkpoint upload intent exceeds Oxia size bound");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, intentBytes);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, intentBytes);
            CanonicalProtobuf.bytes(output, 3, digest);
        });
    }

    private static Entry decode(final byte[] encoded, final CheckpointUploadIntentV1 identity,
                                final long versionId) {
        if (encoded == null || encoded.length > MAX_INTENT_BYTES + 256) {
            throw new IllegalStateException("Oxia upload intent record exceeds bounded size");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final CanonicalProtobuf.Reader.Field version = next(reader, 1);
        if (uint(version, 1) != RECORD_VERSION) {
            throw new IllegalStateException("unsupported Oxia upload intent record version");
        }
        final byte[] intentBytes = bytes(next(reader, 2), 2);
        final byte[] digest = bytes(next(reader, 3), 3);
        if (reader.hasRemaining() || !Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, intentBytes))) {
            throw new IllegalStateException("Oxia upload intent record is non-canonical or corrupt");
        }
        try {
            final CheckpointUploadIntentV1 decoded = CheckpointUploadIntentV1.decode(intentBytes);
            if (!decoded.shard().equals(identity.shard())
                    || !Bytes.constantTimeEquals(decoded.checkpointId(), identity.checkpointId())
                    || !java.util.Arrays.equals(encoded, encode(decoded))) {
                throw new IllegalStateException("Oxia upload intent record identity mismatch");
            }
            return new Entry(decoded, versionId);
        } catch (RuntimeException malformed) {
            if (malformed instanceof IllegalStateException stateFailure
                    && "Oxia upload intent record identity mismatch".equals(stateFailure.getMessage())) {
                throw stateFailure;
            }
            throw new IllegalStateException("Oxia upload intent record contains malformed values", malformed);
        }
    }

    private static CanonicalProtobuf.Reader.Field next(final CanonicalProtobuf.Reader reader, final int number) {
        if (!reader.hasRemaining()) {
            throw new IllegalStateException("missing Oxia upload intent record field " + number);
        }
        final CanonicalProtobuf.Reader.Field field = reader.next();
        if (field.number() != number) {
            throw new IllegalStateException("unexpected Oxia upload intent record field " + field.number());
        }
        return field;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("invalid Oxia upload intent bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalStateException("invalid Oxia upload intent varint field " + number);
        }
        return field.unsignedValue();
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
    }

    private record Entry(CheckpointUploadIntentV1 intent, long versionId) {
        private Entry {
            Objects.requireNonNull(intent, "intent");
        }
    }

    private static final class SyncRecordClient implements RecordClient {
        private final SyncOxiaClient delegate;

        private SyncRecordClient(final SyncOxiaClient delegate) {
            this.delegate = Objects.requireNonNull(delegate, "client");
        }

        @Override
        public GetResult get(final String key) {
            return delegate.get(key);
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            return delegate.put(key, value, options);
        }
    }
}
