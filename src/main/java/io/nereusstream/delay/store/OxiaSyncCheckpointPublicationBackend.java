package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Single-record Oxia authority for checkpoint upload publication.
 *
 * <p>Oxia client 0.9 exposes conditional single-record CAS, not a multi-record
 * transaction. This backend therefore stores the Recovery Catalog snapshot and
 * the shard's upload-intent projections in one canonical record. The provider
 * upload remains an external immutable side effect, while the final
 * PUBLISHED-intent plus catalog-manifest binding is one Oxia version CAS. The
 * session-bound Recovery Pin is a separate ephemeral sibling record and is
 * never encoded into this publication snapshot. The
 * existing separate catalog and upload-intent backends remain available for
 * narrow authority tests and fail closed when asked to claim this boundary.</p>
 *
 * <p>The record is scoped to one shard. All reads validate the exact record
 * key, version, canonical bytes and cross-projection identities before a
 * caller can observe a success.</p>
 */
public final class OxiaSyncCheckpointPublicationBackend implements CheckpointAtomicPublicationAuthority {
    private static final int RECORD_VERSION = 1;
    private static final int MAX_STATE_BYTES = 64 * 1024 * 1024;
    private static final int MAX_INTENTS = 100_000;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-oxia-checkpoint-publication-v1\0");
    private static final String RECORD_SUFFIX = "/publication";
    private static final String PIN_SUFFIX = "/recovery-pin";

    private final RecordClient client;
    private final String recordKey;
    private final OxiaSessionBoundRecoveryPinStore pinStore;
    private final CheckpointManifestLimits manifestLimits;

    /** Creates an authority over an already configured Oxia client. */
    public OxiaSyncCheckpointPublicationBackend(final SyncOxiaClient client,
                                                final String keyPrefix,
                                                final CheckpointManifestLimits manifestLimits) {
        this(new SyncRecordClient(client, null), keyPrefix, manifestLimits);
    }

    /**
     * Creates an authority whose Recovery Pin operations use the exact
     * session identity supplied by the connected Oxia client owner.
     */
    public OxiaSyncCheckpointPublicationBackend(final SyncOxiaClient client,
                                                final String keyPrefix,
                                                final CheckpointManifestLimits manifestLimits,
                                                final byte[] sessionIdentity) {
        this(new SyncRecordClient(client, sessionIdentity), keyPrefix, manifestLimits);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncCheckpointPublicationBackend(final RecordClient client,
                                         final String keyPrefix,
                                         final CheckpointManifestLimits manifestLimits) {
        this.client = Objects.requireNonNull(client, "client");
        final String canonicalPrefix = canonicalKeyPrefix(keyPrefix);
        this.recordKey = canonicalPrefix + RECORD_SUFFIX;
        this.pinStore = new OxiaSessionBoundRecoveryPinStore(client, canonicalPrefix + PIN_SUFFIX);
        this.manifestLimits = Objects.requireNonNull(manifestLimits, "manifestLimits");
    }

    @Override
    public CheckpointUploadIntentV1 create(final CheckpointUploadIntentV1 pending) {
        requirePending(pending);
        return mutate(state -> {
            validateScope(state, pending.shard().shardId());
            final CheckpointUploadIntentV1 existing = findIntent(state, pending);
            if (existing != null) {
                if (!existing.equals(pending)) {
                    throw new IllegalStateException("checkpoint upload intent CAS conflict");
                }
                return unchanged(existing, state);
            }
            return changed(pending, state.withIntent(pending));
        });
    }

    @Override
    public CheckpointUploadIntentV1 publish(final CheckpointUploadIntentV1 expectedPending,
                                            final CheckpointResourceV1 resource) {
        requirePending(expectedPending);
        Objects.requireNonNull(resource, "resource");
        return mutate(state -> {
            final CheckpointUploadIntentV1 existing = requireExactPending(state, expectedPending);
            final CheckpointUploadIntentV1 next = nextIntent(expectedPending,
                    CheckpointUploadStateV1.PUBLISHED, resource, null);
            if (existing.equals(next)) {
                return unchanged(existing, state);
            }
            return changed(next, state.withIntent(next));
        });
    }

    @Override
    public Optional<CheckpointUploadIntentV1> currentPublishedFor(
            final CheckpointUploadIntentV1 expectedPending) {
        requirePending(expectedPending);
        final CheckpointUploadIntentV1 current = findIntent(read().state(), expectedPending);
        if (current == null || current.state() != CheckpointUploadStateV1.PUBLISHED) {
            return Optional.empty();
        }
        final CheckpointUploadIntentV1 expected = nextIntent(expectedPending,
                CheckpointUploadStateV1.PUBLISHED, current.publishedManifest(), null);
        return current.equals(expected) ? Optional.of(current) : Optional.empty();
    }

    @Override
    public CheckpointUploadIntentV1 beginReaping(final CheckpointUploadIntentV1 expectedPending,
                                                  final TrustedUtcIntervalEvidence evidence) {
        requirePending(expectedPending);
        Objects.requireNonNull(evidence, "evidence");
        evidence.requireEarliestAtLeast(expectedPending.uploadDeadlineEpochMs());
        return mutate(state -> {
            final CheckpointUploadIntentV1 existing = findIntent(state, expectedPending);
            final CheckpointUploadIntentV1 next = nextIntent(expectedPending,
                    CheckpointUploadStateV1.REAPING, null, evidence);
            if (existing != null && existing.equals(next)) {
                return unchanged(existing, state);
            }
            if (existing == null || !existing.equals(expectedPending)) {
                throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
            }
            return changed(next, state.withIntent(next));
        });
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

    @Override
    public Optional<CheckpointUploadIntentV1> current(final CheckpointUploadIntentV1 identity) {
        Objects.requireNonNull(identity, "identity");
        return Optional.ofNullable(findIntent(read().state(), identity));
    }

    /**
     * Commits the provider object identity, PUBLISHED intent and catalog
     * manifest in one canonical Oxia record CAS.
     */
    @Override
    public CheckpointUploadIntentV1 publishUploadedCheckpointAtomically(
            final CheckpointUploadIntentV1 expectedPending,
            final CheckpointResourceV1 resource,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        requirePending(expectedPending);
        Objects.requireNonNull(resource, "resource");
        Objects.requireNonNull(manifest, "manifest");
        validateUploadedObject(expectedPending, resource, manifest);
        if (expectedCatalogGeneration != expectedPending.baseCatalogGeneration()) {
            throw new IllegalStateException("upload intent base catalog generation does not match publication CAS");
        }
        return mutate(state -> {
            final CheckpointUploadIntentV1 existing = requireExactPendingOrPublished(state, expectedPending);
            final CheckpointUploadIntentV1 published = nextIntent(expectedPending,
                    CheckpointUploadStateV1.PUBLISHED, resource, null);
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            if (existing.state() == CheckpointUploadStateV1.PUBLISHED) {
                if (!existing.equals(published)) {
                    throw new IllegalStateException("checkpoint published intent identity conflicts with request");
                }
                catalog.publishUploadedCheckpoint(existing, manifest, expectedCatalogGeneration);
                return unchanged(existing, state.withCatalog(catalog.snapshot()));
            }
            catalog.publishUploadedCheckpoint(published, manifest, expectedCatalogGeneration);
            return changed(published, state.withCatalog(catalog.snapshot()).withIntent(published));
        });
    }

    @Override
    public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                               final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        manifest.validateLimits(manifestLimits);
        return mutate(state -> {
            validateScope(state, manifest.shardId());
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryCatalog.Publication result = catalog.publish(manifest, expectedCatalogGeneration);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    @Override
    public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                      final byte[] evidenceCursorDigest) {
        return mutate(state -> {
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryFloor result = catalog.advanceFloor(checkpointId, expectedCatalogGeneration,
                    evidenceCursorDigest);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    @Override
    public RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                           final List<EvidenceCursorV1> evidenceCursors) {
        return mutate(state -> {
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryFloorRefV1 result = catalog.advanceFloor(checkpointId, expectedCatalogGeneration,
                    evidenceCursors);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    /**
     * Reconciles an already committed atomic publication. A retry is
     * successful only when both the exact PUBLISHED intent and catalog
     * projection are present in the same authority record.
     */
    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(
            final CheckpointUploadIntentV1 publishedIntent,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        Objects.requireNonNull(publishedIntent, "publishedIntent");
        Objects.requireNonNull(manifest, "manifest");
        if (publishedIntent.state() != CheckpointUploadStateV1.PUBLISHED) {
            throw new IllegalArgumentException("catalog publication requires a PUBLISHED upload intent");
        }
        return mutate(state -> {
            final CheckpointUploadIntentV1 current = findIntent(state, publishedIntent);
            if (current == null || !current.equals(publishedIntent)) {
                throw new IllegalStateException("published upload intent is not the exact authority value");
            }
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            final RecoveryCatalog.Publication result = catalog.publishUploadedCheckpoint(publishedIntent, manifest,
                    expectedCatalogGeneration);
            return changed(result, state.withCatalog(catalog.snapshot()));
        });
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).manifest(checkpointId);
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).currentFloor();
    }

    @Override
    public Optional<RecoveryFloorRefV1> currentFloorRef() {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).currentFloorRef();
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        RecoveryCatalog.fromSnapshot(read().state().catalog()).validatePublishedRestoreCandidate(candidate);
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                       final long requiredMutationSequence,
                                                                       final SourcePosition... requiredPositions) {
        return RecoveryCatalog.fromSnapshot(read().state().catalog()).proveFloorCoverage(candidateCheckpointId,
                requiredMutationSequence, requiredPositions);
    }

    @Override
    public void validateLocalStoreRecovery(final ShardId shardId,
                                           final StoreRecoveryMetadata localMetadata) {
        RecoveryCatalog.fromSnapshot(read().state().catalog()).validateLocalStoreRecovery(shardId, localMetadata);
    }

    @Override
    public RecoveryPinV1 createRecoveryPin(final RecoveryPinV1 pin) {
        final RecoveryPinV1 requested = Objects.requireNonNull(pin, "pin");
        return pinStore.create(requested, () -> {
            final PublicationState state = read().state();
            final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(state.catalog());
            catalog.createRecoveryPin(requested);
        }, () -> read().state().catalog().catalogGeneration());
    }

    @Override
    public void releaseRecoveryPin(final RecoveryPinV1 pin) {
        pinStore.release(Objects.requireNonNull(pin, "pin"));
    }

    @Override
    public Optional<RecoveryPinV1> activeRecoveryPin() {
        return pinStore.active();
    }

    private <T> T mutate(final Function<PublicationState, Change<T>> operation) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            final StoredState before = read();
            final Change<T> change = Objects.requireNonNull(operation.apply(before.state()), "publication change");
            final byte[] afterBytes = encode(change.state());
            if (Arrays.equals(before.encoded(), afterBytes)) {
                return change.result();
            }
            try {
                putExact(afterBytes, before.versionId());
                return change.result();
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException conflict) {
                // Another authority writer won the record CAS. Re-read the
                // complete state and apply the exact request again.
            } catch (RuntimeException responseFailure) {
                final StoredState observed = read();
                if (Arrays.equals(afterBytes, observed.encoded())) {
                    return change.result();
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("Oxia checkpoint publication CAS did not converge");
    }

    private StoredState read() {
        final GetResult result = client.get(recordKey);
        if (result == null) {
            final PublicationState empty = emptyState();
            return new StoredState(empty, null, encode(empty));
        }
        if (!recordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia checkpoint publication response has an invalid key or version");
        }
        final byte[] encoded = result.value();
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_STATE_BYTES) {
            throw new IllegalStateException("Oxia checkpoint publication state has an invalid size");
        }
        return new StoredState(decode(encoded), result.version().versionId(), encoded);
    }

    private void putExact(final byte[] encoded, final Long versionId)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        final Set<PutOption> options = versionId == null
                ? Set.of(PutOption.IfRecordDoesNotExist)
                : Set.of(PutOption.IfVersionIdEquals(versionId));
        final PutResult result = client.put(recordKey, encoded, options);
        if (result == null || !recordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia checkpoint publication put returned no exact version");
        }
    }

    private static PublicationState emptyState() {
        return new PublicationState(null, new RecoveryCatalog.Snapshot(0, null, List.of(),
                java.util.Map.of(), null, null, null), List.of());
    }

    private PublicationState decode(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Oxia checkpoint publication state is empty");
        }
        int index = 0;
        final CanonicalProtobuf.Reader.Field version = nextField(fields, index++, 1);
        if (uint(version, 1) != RECORD_VERSION) {
            throw new IllegalArgumentException("unsupported Oxia checkpoint publication state version");
        }
        final byte[] catalogBytes = bytes(nextField(fields, index++, 2), 2);
        ShardId shard = null;
        final List<CheckpointUploadIntentV1> intents = new ArrayList<>();
        byte[] digest = null;
        while (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            switch (field.number()) {
                case 3 -> {
                    if (shard != null) {
                        throw new IllegalArgumentException("duplicate Oxia publication shard field");
                    }
                    shard = ShardSubjectV1.decode(bytes(field, 3)).shardId();
                }
                case 4 -> {
                    if (intents.size() >= MAX_INTENTS) {
                        throw new IllegalArgumentException("Oxia checkpoint publication intent count exceeds bound");
                    }
                    intents.add(CheckpointUploadIntentV1.decode(bytes(field, 4)));
                }
                case 5 -> {
                    if (digest != null || index != fields.size()) {
                        throw new IllegalArgumentException("Oxia publication digest must be the final field");
                    }
                    digest = bytes(field, 5);
                }
                default -> throw new IllegalArgumentException("unknown Oxia checkpoint publication state field "
                        + field.number());
            }
        }
        if (digest == null) {
            throw new IllegalArgumentException("Oxia checkpoint publication state has no digest");
        }
        final PublicationState state = new PublicationState(shard,
                OxiaSyncRecoveryCatalogBackend.decodeSnapshot(catalogBytes, manifestLimits), intents);
        final byte[] payload = encodePayload(state);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))
                || !Arrays.equals(encoded, encode(state))) {
            throw new IllegalArgumentException("Oxia checkpoint publication state is not canonical");
        }
        return state;
    }

    private static byte[] encode(final PublicationState state) {
        final byte[] payload = encodePayload(state);
        if (payload.length > MAX_STATE_BYTES - 128) {
            throw new IllegalStateException("Oxia checkpoint publication state exceeds size bound");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(payload);
            CanonicalProtobuf.bytes(output, 5, digest);
        });
    }

    private static byte[] encodePayload(final PublicationState state) {
        final byte[] catalogBytes = OxiaSyncRecoveryCatalogBackend.encodeSnapshot(state.catalog());
        final List<CheckpointUploadIntentV1> intents = state.intents().stream()
                .sorted(Comparator.comparing(OxiaSyncCheckpointPublicationBackend::intentIdentity))
                .toList();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECORD_VERSION);
            CanonicalProtobuf.bytes(output, 2, catalogBytes);
            if (state.shard() != null) {
                CanonicalProtobuf.bytes(output, 3, new ShardSubjectV1(state.shard()).canonicalBytes());
            }
            intents.forEach(intent -> CanonicalProtobuf.bytes(output, 4, intent.canonicalBytes()));
        });
    }

    private static String intentIdentity(final CheckpointUploadIntentV1 intent) {
        return Bytes.hex(Bytes.concat(intent.shard().canonicalHashBytes(), intent.checkpointId()));
    }

    private static CheckpointUploadIntentV1 findIntent(final PublicationState state,
                                                       final CheckpointUploadIntentV1 identity) {
        final String requested = intentIdentity(identity);
        for (CheckpointUploadIntentV1 intent : state.intents()) {
            if (requested.equals(intentIdentity(intent))) {
                if (!intent.shard().equals(identity.shard())
                        || !Bytes.constantTimeEquals(intent.recoveryLineageId(), identity.recoveryLineageId())
                        || !Bytes.constantTimeEquals(intent.checkpointId(), identity.checkpointId())) {
                    throw new IllegalStateException("checkpoint publication intent identity collision");
                }
                return intent;
            }
        }
        return null;
    }

    private static CheckpointUploadIntentV1 requireExactPending(final PublicationState state,
                                                                 final CheckpointUploadIntentV1 expected) {
        final CheckpointUploadIntentV1 existing = findIntent(state, expected);
        if (existing == null || !existing.equals(expected)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
        return existing;
    }

    private static CheckpointUploadIntentV1 requireExactPendingOrPublished(final PublicationState state,
                                                                            final CheckpointUploadIntentV1 expected) {
        final CheckpointUploadIntentV1 existing = findIntent(state, expected);
        if (existing == null || (existing.state() != CheckpointUploadStateV1.PENDING_UPLOAD
                && existing.state() != CheckpointUploadStateV1.PUBLISHED)) {
            throw new IllegalStateException("checkpoint upload intent is not pending or published in authority");
        }
        return existing;
    }

    private static void requirePending(final CheckpointUploadIntentV1 pending) {
        Objects.requireNonNull(pending, "pending");
        if (pending.state() != CheckpointUploadStateV1.PENDING_UPLOAD) {
            throw new IllegalArgumentException("checkpoint upload intent must be PENDING_UPLOAD");
        }
    }

    private static void validateScope(final PublicationState state, final ShardId shard) {
        if (state.shard() != null && !state.shard().equals(shard)) {
            throw new IllegalArgumentException("Oxia checkpoint publication record is bound to another shard");
        }
        if (state.catalog().catalogShard() != null && !state.catalog().catalogShard().equals(shard)) {
            throw new IllegalArgumentException("Oxia checkpoint publication catalog is bound to another shard");
        }
    }

    private static void validateUploadedObject(final CheckpointUploadIntentV1 pending,
                                               final CheckpointResourceV1 resource,
                                               final CheckpointManifest manifest) {
        if (!resource.objectStoreProfile().equals(pending.objectStoreProfile())
                || !Bytes.constantTimeEquals(resource.recoveryLineageId(), pending.recoveryLineageId())
                || !Bytes.constantTimeEquals(resource.checkpointId(), pending.checkpointId())
                || resource.manifestLength() != manifest.canonicalJsonBytes().length
                || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
            throw new IllegalArgumentException("uploaded checkpoint manifest object identity mismatch");
        }
    }

    private static CheckpointUploadIntentV1 nextIntent(final CheckpointUploadIntentV1 expected,
                                                       final CheckpointUploadStateV1 state,
                                                       final CheckpointResourceV1 resource,
                                                       final TrustedUtcIntervalEvidence evidence) {
        if (expected.stateRevision() == -1L) {
            throw new IllegalStateException("checkpoint upload intent state revision exhausted");
        }
        return new CheckpointUploadIntentV1(expected.shard(), expected.recoveryLineageId(), expected.checkpointId(),
                expected.owner(), expected.sourceStoreIncarnation(), expected.uploadToken(),
                expected.baseCatalogGeneration(), expected.parentCheckpointId(), expected.parentManifestSha256(),
                expected.objectStoreProfile(), expected.checkpointCreatedAt(), expected.uploadDeadlineEpochMs(), state,
                expected.stateRevision() + 1, resource, evidence);
    }

    private static <T> Change<T> changed(final T result, final PublicationState state) {
        return new Change<>(Objects.requireNonNull(result, "result"), state);
    }

    private static <T> Change<T> unchanged(final T result, final PublicationState state) {
        return new Change<>(Objects.requireNonNull(result, "result"), state);
    }

    private static CanonicalProtobuf.Reader.Field nextField(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("missing Oxia checkpoint publication state field " + number);
        }
        return fields.get(index);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Oxia checkpoint publication bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia checkpoint publication uint field " + number);
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

    interface RecordClient extends OxiaSessionBoundRecoveryPinStore.RecordClient {
    }

    private static final class SyncRecordClient implements RecordClient {
        private final SyncOxiaClient delegate;
        private final byte[] sessionIdentity;

        private SyncRecordClient(final SyncOxiaClient delegate, final byte[] sessionIdentity) {
            this.delegate = Objects.requireNonNull(delegate, "client");
            this.sessionIdentity = sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
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

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options)
                throws UnexpectedVersionIdException {
            return delegate.delete(key, options);
        }

        @Override
        public byte[] sessionIdentity() {
            return sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }
    }

    private record StoredState(PublicationState state, Long versionId, byte[] encoded) {
        private StoredState {
            Objects.requireNonNull(state, "state");
            encoded = Bytes.copy(Objects.requireNonNull(encoded, "encoded"));
        }

        @Override
        public byte[] encoded() {
            return Bytes.copy(encoded);
        }
    }

    private record Change<T>(T result, PublicationState state) {
        private Change {
            Objects.requireNonNull(result, "result");
            Objects.requireNonNull(state, "state");
        }
    }

    private record PublicationState(ShardId shard, RecoveryCatalog.Snapshot catalog,
                                    List<CheckpointUploadIntentV1> intents) {
        private PublicationState {
            Objects.requireNonNull(catalog, "catalog");
            Objects.requireNonNull(intents, "intents");
            if (catalog.activeRecoveryPin() != null) {
                throw new IllegalArgumentException("checkpoint publication record cannot encode RecoveryPin");
            }
            if (shard == null && catalog.catalogShard() != null) {
                shard = catalog.catalogShard();
            }
            if (shard != null && catalog.catalogShard() != null && !shard.equals(catalog.catalogShard())) {
                throw new IllegalArgumentException("checkpoint publication shard differs from catalog shard");
            }
            final Set<String> identities = new HashSet<>();
            final List<CheckpointUploadIntentV1> copied = new ArrayList<>();
            for (CheckpointUploadIntentV1 intent : intents) {
                Objects.requireNonNull(intent, "intent");
                if (shard != null && !shard.equals(intent.shard().shardId())) {
                    throw new IllegalArgumentException("checkpoint publication intent differs from record shard");
                }
                if (!identities.add(intentIdentity(intent))) {
                    throw new IllegalArgumentException("checkpoint publication has duplicate intent identity");
                }
                copied.add(intent);
            }
            if (!copied.isEmpty() && shard == null) {
                throw new IllegalArgumentException("checkpoint publication intents require a record shard");
            }
            intents = List.copyOf(copied);
        }

        private PublicationState withIntent(final CheckpointUploadIntentV1 intent) {
            final List<CheckpointUploadIntentV1> next = new ArrayList<>(intents);
            final String identity = intentIdentity(intent);
            for (int index = 0; index < next.size(); index++) {
                if (identity.equals(intentIdentity(next.get(index)))) {
                    next.set(index, intent);
                    return new PublicationState(shard, catalog, next);
                }
            }
            next.add(intent);
            return new PublicationState(shard == null ? intent.shard().shardId() : shard, catalog, next);
        }

        private PublicationState withCatalog(final RecoveryCatalog.Snapshot nextCatalog) {
            return new PublicationState(shard, nextCatalog, intents);
        }
    }
}
