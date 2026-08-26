package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SourcePosition;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Single-record Oxia CAS backend for the Recovery Catalog.
 *
 * <p>All catalog state for one shard is encoded as one canonical record. A
 * publication, scalar Floor advance, and typed Floor advance therefore share
 * one Oxia version CAS; this class never presents a sequence of independent
 * puts as a transaction. Recovery Pins use a separate Oxia ephemeral record
 * bound to the same client session. Pin creation validates the exact catalog
 * generation before and after the ephemeral CAS, but it is deliberately not a
 * cross-record transaction with the catalog or upload intent.</p>
 *
 * <p>The backend is below {@link OxiaRecoveryCatalog}; the latter validates
 * every returned projection against the request and the exact published
 * manifest. This class only supplies durable CAS/read semantics.</p>
 */
public final class OxiaSyncRecoveryCatalogBackend implements OxiaRecoveryCatalog.CasBackend {
    private static final int SNAPSHOT_VERSION = 1;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final int MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024;
    private static final int MAX_MANIFESTS = 100_000;
    private static final String RECORD_SUFFIX = "/catalog";
    private static final String PIN_SUFFIX = "/recovery-pin";

    private final RecordClient client;
    private final String recordKey;
    private final OxiaSessionBoundRecoveryPinStore pinStore;
    private final CheckpointManifestLimits manifestLimits;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncRecoveryCatalogBackend(
            final SyncOxiaClient client, final String keyPrefix, final CheckpointManifestLimits manifestLimits) {
        this(new SyncRecordClient(client, null), keyPrefix, manifestLimits);
    }

    /** Creates a Recovery Catalog fenced to the exact ephemeral session of a handle. */
    public OxiaSyncRecoveryCatalogBackend(
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits) {
        this(
                new SyncRecordClient(Objects.requireNonNull(handle, "handle").client(), handle.sessionIdentity()),
                keyPrefix,
                manifestLimits,
                handle.backend()::assertConnectedSession);
    }

    /**
     * Creates a backend whose Recovery Pin operations are bound to the exact
     * session identity supplied by the connected Oxia client owner.
     *
     * <p>The ordinary constructor remains useful for catalog-only operations;
     * it deliberately fails closed if a caller tries to create or release a
     * session-bound pin without supplying this identity.</p>
     */
    public OxiaSyncRecoveryCatalogBackend(
            final SyncOxiaClient client,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits,
            final byte[] sessionIdentity) {
        this(new SyncRecordClient(client, sessionIdentity), keyPrefix, manifestLimits);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncRecoveryCatalogBackend(
            final RecordClient client, final String keyPrefix, final CheckpointManifestLimits manifestLimits) {
        this(client, keyPrefix, manifestLimits, () -> {});
    }

    /** Package-private constructor used to exercise the session fence. */
    OxiaSyncRecoveryCatalogBackend(
            final RecordClient client,
            final String keyPrefix,
            final CheckpointManifestLimits manifestLimits,
            final Runnable sessionCheck) {
        this.client = new SessionBoundRecordClient(
                Objects.requireNonNull(client, "client"), Objects.requireNonNull(sessionCheck, "sessionCheck"));
        final String canonicalPrefix = canonicalKeyPrefix(keyPrefix);
        this.recordKey = canonicalPrefix + RECORD_SUFFIX;
        this.pinStore = new OxiaSessionBoundRecoveryPinStore(this.client, canonicalPrefix + PIN_SUFFIX);
        this.manifestLimits = Objects.requireNonNull(manifestLimits, "manifestLimits");
    }

    @Override
    public RecoveryCatalog.Publication publish(
            final CheckpointManifest manifest, final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        manifestLimits.validateManifest(manifest);
        return mutate(expectedCatalogGeneration, catalog -> catalog.publish(manifest, expectedCatalogGeneration));
    }

    @Override
    public RecoveryFloor advanceFloor(
            final byte[] checkpointId, final long expectedCatalogGeneration, final byte[] evidenceCursorDigest) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        final byte[] requestedCheckpointId = Bytes.copy(checkpointId);
        final byte[] requestedEvidence = Bytes.copy(evidenceCursorDigest);
        return mutate(
                expectedCatalogGeneration,
                catalog -> catalog.advanceFloor(requestedCheckpointId, expectedCatalogGeneration, requestedEvidence));
    }

    @Override
    public RecoveryFloorRef advanceFloor(
            final byte[] checkpointId,
            final long expectedCatalogGeneration,
            final List<EvidenceCursor> evidenceCursors) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        final byte[] requestedCheckpointId = Bytes.copy(checkpointId);
        final List<EvidenceCursor> requestedCursors =
                List.copyOf(Objects.requireNonNull(evidenceCursors, "evidenceCursors"));
        return mutate(
                expectedCatalogGeneration,
                catalog -> catalog.advanceFloor(requestedCheckpointId, expectedCatalogGeneration, requestedCursors));
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        return readCatalog().manifest(Bytes.copy(checkpointId));
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return readCatalog().currentFloor();
    }

    @Override
    public Optional<RecoveryFloorRef> currentFloorRef() {
        return readCatalog().currentFloorRef();
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        Objects.requireNonNull(candidate, "candidate");
        readCatalog().validatePublishedRestoreCandidate(candidate);
    }

    /**
     * Validates a reusable local Store projection against the current remote
     * catalog snapshot. This is intentionally a read-only catalog/Floor
     * check; Owner Lease/session fencing is still supplied by the caller's
     * separate authority and is never inferred from local metadata.
     */
    @Override
    public void validateLocalStoreRecovery(final ShardId shardId, final StoreRecoveryMetadata localMetadata) {
        readCatalog()
                .validateLocalStoreRecovery(
                        Objects.requireNonNull(shardId, "shardId"),
                        Objects.requireNonNull(localMetadata, "localMetadata"));
    }

    /**
     * Creates one session-bound ephemeral Recovery Pin after validating the
     * exact catalog generation. The catalog reread after the ephemeral CAS
     * closes the obvious cross-record race; a shared Oxia transaction is still
     * required if pin creation must be linearized with another catalog write.
     */
    @Override
    public RecoveryPin createRecoveryPin(final RecoveryPin pin) {
        final RecoveryPin requested = Objects.requireNonNull(pin, "pin");
        return pinStore.create(
                requested,
                () -> {
                    final RecoveryCatalog catalog = readCatalog();
                    if (catalog.catalogGeneration() != requested.observedCatalogGeneration()) {
                        throw new IllegalStateException("RecoveryPin catalog generation is stale");
                    }
                    catalog.createRecoveryPin(requested);
                },
                () -> readCatalog().catalogGeneration());
    }

    /** Releases only the exact session-bound pin returned by this authority. */
    @Override
    public void releaseRecoveryPin(final RecoveryPin pin) {
        pinStore.release(Objects.requireNonNull(pin, "pin"));
    }

    /** Reads the active ephemeral pin, failing closed on malformed/sessionless bytes. */
    @Override
    public Optional<RecoveryPin> activeRecoveryPin() {
        return pinStore.active();
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
            final byte[] candidateCheckpointId,
            final long requiredMutationSequence,
            final SourcePosition... requiredPositions) {
        Bytes.requireLength(candidateCheckpointId, 16, "candidateCheckpointId");
        final byte[] requestedCandidate = Bytes.copy(candidateCheckpointId);
        final SourcePosition[] requestedPositions =
                Arrays.copyOf(Objects.requireNonNull(requiredPositions, "requiredPositions"), requiredPositions.length);
        return readCatalog().proveFloorCoverage(requestedCandidate, requiredMutationSequence, requestedPositions);
    }

    /**
     * The upload intent is a separate authority record. Treating it as if it
     * were part of this catalog record would allow a non-transactional pair of
     * puts to claim atomicity, so the production adapter fails closed.
     */
    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(
            final CheckpointUploadIntent publishedIntent,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        throw new UnsupportedOperationException(
                "upload-intent/catalog CAS requires a shared Oxia transaction and is not implemented");
    }

    private <T> T mutate(final long expectedCatalogGeneration, final CatalogMutation<T> mutation) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            final GetResult current = client.get(recordKey);
            final RecoveryCatalog before = decodeCatalog(current);
            final byte[] beforeBytes = encodeSnapshot(before.snapshot());
            final T result = mutation.apply(before);
            final byte[] afterBytes = encodeSnapshot(before.snapshot());
            if (afterBytes.length > MAX_SNAPSHOT_BYTES) {
                throw new IllegalStateException("Oxia catalog snapshot exceeds size bound");
            }
            if (Arrays.equals(beforeBytes, afterBytes)) {
                return result;
            }
            try {
                final Set<PutOption> options = current == null
                        ? Set.of(PutOption.IfRecordDoesNotExist)
                        : Set.of(PutOption.IfVersionIdEquals(current.version().versionId()));
                final PutResult stored = client.put(recordKey, afterBytes, options);
                if (stored == null || !recordKey.equals(stored.key()) || stored.version() == null) {
                    throw new IllegalStateException("Oxia catalog put returned no exact version");
                }
                return result;
            } catch (UnexpectedVersionIdException | KeyAlreadyExistsException conflict) {
                // A concurrent owner won the record CAS. Re-read the complete
                // snapshot and apply the request against the new generation.
            } catch (RuntimeException responseFailure) {
                // A transport response can be lost after Oxia committed the
                // exact bytes. Only an exact reread is accepted as success.
                final GetResult observed = client.get(recordKey);
                if (observed != null
                        && recordKey.equals(observed.key())
                        && observed.version() != null
                        && Arrays.equals(afterBytes, observed.value())) {
                    return result;
                }
                throw responseFailure;
            }
        }
        throw new IllegalStateException("Oxia Recovery Catalog CAS did not converge");
    }

    private RecoveryCatalog readCatalog() {
        return decodeCatalog(client.get(recordKey));
    }

    private RecoveryCatalog decodeCatalog(final GetResult result) {
        if (result == null) {
            return RecoveryCatalog.fromSnapshot(
                    new RecoveryCatalog.Snapshot(0, null, List.of(), Map.of(), null, null, null));
        }
        if (!recordKey.equals(result.key()) || result.version() == null) {
            throw new IllegalStateException("Oxia catalog response has an invalid key or version");
        }
        final byte[] encoded = result.value();
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalStateException("Oxia catalog snapshot has an invalid size");
        }
        return RecoveryCatalog.fromSnapshot(decodeSnapshot(encoded));
    }

    private RecoveryCatalog.Snapshot decodeSnapshot(final byte[] encoded) {
        return decodeSnapshot(encoded, manifestLimits);
    }

    static RecoveryCatalog.Snapshot decodeSnapshot(
            final byte[] encoded, final CheckpointManifestLimits manifestLimits) {
        Objects.requireNonNull(manifestLimits, "manifestLimits");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Oxia catalog snapshot is empty");
        }
        int index = 0;
        final CanonicalProtobuf.Reader.Field version = nextField(fields, index++, 1);
        if (uint(version, 1) != SNAPSHOT_VERSION) {
            throw new IllegalArgumentException("unsupported Oxia catalog snapshot version");
        }
        final CanonicalProtobuf.Reader.Field generation = nextField(fields, index++, 2);
        final long catalogGeneration = uint64(generation, 2);
        ShardId catalogShard = null;
        final List<CheckpointManifest> manifests = new ArrayList<>();
        final Map<String, CheckpointResource> resources = new HashMap<>();
        RecoveryFloor floor = null;
        RecoveryFloorRef typedFloor = null;
        while (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            switch (field.number()) {
                case 3 -> {
                    if (catalogShard != null) {
                        throw new IllegalArgumentException("duplicate Oxia catalog shard field");
                    }
                    catalogShard = ShardSubject.decode(bytes(field, 3)).shardId();
                }
                case 4 -> {
                    if (manifests.size() >= MAX_MANIFESTS) {
                        throw new IllegalArgumentException("Oxia catalog manifest count exceeds bound");
                    }
                    final CheckpointManifest manifest =
                            CheckpointManifest.decodeCanonicalJson(bytes(field, 4), manifestLimits);
                    final String checkpointKey = Bytes.hex(manifest.checkpointId());
                    if (manifests.stream()
                            .anyMatch(existing -> checkpointKey.equals(Bytes.hex(existing.checkpointId())))) {
                        throw new IllegalArgumentException("duplicate Oxia catalog checkpoint");
                    }
                    manifests.add(manifest);
                }
                case 5 -> {
                    if (resources.size() >= MAX_MANIFESTS) {
                        throw new IllegalArgumentException("Oxia catalog resource count exceeds bound");
                    }
                    final CheckpointResource resource = CheckpointResource.decode(bytes(field, 5));
                    manifestLimits.validateResource(resource);
                    final String checkpointKey = Bytes.hex(resource.checkpointId());
                    if (resources.put(checkpointKey, resource) != null) {
                        throw new IllegalArgumentException("duplicate Oxia catalog resource");
                    }
                }
                case 6 -> {
                    if (floor != null) {
                        throw new IllegalArgumentException("duplicate Oxia scalar Floor field");
                    }
                    floor = RecoveryFloor.decode(bytes(field, 6));
                }
                case 7 -> {
                    if (typedFloor != null) {
                        throw new IllegalArgumentException("duplicate Oxia typed Floor field");
                    }
                    typedFloor = RecoveryFloorRef.decode(bytes(field, 7));
                }
                default -> throw new IllegalArgumentException("unknown Oxia catalog snapshot field " + field.number());
            }
        }
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(
                catalogGeneration, catalogShard, manifests, resources, floor, typedFloor, null);
        final RecoveryCatalog catalog = RecoveryCatalog.fromSnapshot(snapshot);
        if (!Arrays.equals(encoded, encodeSnapshot(catalog.snapshot()))) {
            throw new IllegalArgumentException("Oxia catalog snapshot is not canonical");
        }
        return catalog.snapshot();
    }

    static byte[] encodeSnapshot(final RecoveryCatalog.Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.manifests().size() > MAX_MANIFESTS) {
            throw new IllegalStateException("Oxia catalog manifest count exceeds bound");
        }
        if (snapshot.manifestResources().size() > MAX_MANIFESTS) {
            throw new IllegalStateException("Oxia catalog resource count exceeds bound");
        }
        if (snapshot.activeRecoveryPin() != null) {
            throw new IllegalStateException("Oxia catalog snapshot cannot encode an unsupported RecoveryPin");
        }
        final List<CheckpointManifest> manifests = snapshot.manifests().stream()
                .sorted(Comparator.comparing(manifest -> Bytes.hex(manifest.checkpointId())))
                .toList();
        final List<CheckpointResource> resources = snapshot.manifestResources().values().stream()
                .sorted(Comparator.comparing(resource -> Bytes.hex(resource.checkpointId())))
                .toList();
        final Map<String, CheckpointManifest> manifestsById = new HashMap<>();
        final HashSet<String> manifestIds = new HashSet<>();
        for (CheckpointManifest manifest : manifests) {
            final String checkpointId = Bytes.hex(manifest.checkpointId());
            if (!manifestIds.add(checkpointId)) {
                throw new IllegalStateException("Oxia catalog contains duplicate checkpoint identity");
            }
            manifestsById.put(checkpointId, manifest);
        }
        final HashSet<String> resourceIds = new HashSet<>();
        for (Map.Entry<String, CheckpointResource> entry :
                snapshot.manifestResources().entrySet()) {
            final CheckpointResource resource = entry.getValue();
            if (!Bytes.hex(resource.checkpointId()).equals(entry.getKey())) {
                throw new IllegalStateException("Oxia catalog resource map key does not match checkpoint identity");
            }
        }
        for (CheckpointResource resource : resources) {
            final String checkpointId = Bytes.hex(resource.checkpointId());
            final CheckpointManifest manifest = manifestsById.get(checkpointId);
            if (!resourceIds.add(checkpointId)
                    || manifest == null
                    || !Bytes.constantTimeEquals(resource.recoveryLineageId(), manifest.recoveryLineageId())
                    || !Bytes.constantTimeEquals(resource.manifestSha256(), manifest.manifestSha256())) {
                throw new IllegalStateException("Oxia catalog resource identity does not match a manifest");
            }
        }
        if (!manifests.isEmpty() && snapshot.catalogShard() == null) {
            throw new IllegalArgumentException("Oxia catalog snapshot is missing its shard identity");
        }
        // Reuse the local catalog's complete projection validator so direct
        // encoder callers cannot emit bytes whose shard, ancestry, Floor or
        // generation relationships would fail on the next decode. The
        // explicit identity checks above remain first so malformed maps fail
        // at this boundary without being normalized by Map.copyOf().
        RecoveryCatalog.fromSnapshot(snapshot);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SNAPSHOT_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, snapshot.catalogGeneration());
            if (snapshot.catalogShard() != null) {
                CanonicalProtobuf.bytes(output, 3, new ShardSubject(snapshot.catalogShard()).canonicalBytes());
            }
            manifests.forEach(manifest -> CanonicalProtobuf.bytes(output, 4, manifest.canonicalJsonBytes()));
            resources.forEach(resource -> CanonicalProtobuf.bytes(output, 5, resource.canonicalBytes()));
            if (snapshot.floor() != null) {
                CanonicalProtobuf.bytes(output, 6, snapshot.floor().canonicalBytes());
            }
            if (snapshot.typedFloorRef() != null) {
                CanonicalProtobuf.bytes(output, 7, snapshot.typedFloorRef().canonicalBytes());
            }
        });
    }

    private static CanonicalProtobuf.Reader.Field nextField(
            final List<CanonicalProtobuf.Reader.Field> fields, final int index, final int number) {
        if (index >= fields.size() || fields.get(index).number() != number) {
            throw new IllegalArgumentException("missing Oxia catalog snapshot field " + number);
        }
        return fields.get(index);
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Oxia catalog bytes field " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("invalid Oxia catalog varint field " + number);
        }
        return field.unsignedValue();
    }

    private static long uint64(final CanonicalProtobuf.Reader.Field field, final int number) {
        return uint(field, number);
    }

    private static String canonicalKeyPrefix(final String value) {
        Objects.requireNonNull(value, "keyPrefix");
        final byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (value.isBlank()
                || value.endsWith("/")
                || value.indexOf('\0') >= 0
                || !value.equals(new String(encoded, StandardCharsets.UTF_8))
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException("keyPrefix must be a nonblank NFC UTF-8 path without trailing '/'");
        }
        return value;
    }

    @FunctionalInterface
    interface CatalogMutation<T> {
        T apply(RecoveryCatalog catalog);
    }

    /** Narrow record surface to keep deterministic tests independent of gRPC. */
    interface RecordClient extends OxiaSessionBoundRecoveryPinStore.RecordClient {}

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
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            return delegate.delete(key, options);
        }

        @Override
        public byte[] sessionIdentity() {
            return sessionIdentity == null ? null : Bytes.copy(sessionIdentity);
        }
    }

    /**
     * Checks the caller's Oxia session around every catalog or pin-record
     * operation. A catalog CAS whose response is lost after the marker
     * disappears is therefore never reported as a successful publication or
     * Floor advance.
     */
    private static final class SessionBoundRecordClient implements RecordClient {
        private final RecordClient delegate;
        private final Runnable sessionCheck;

        private SessionBoundRecordClient(final RecordClient delegate, final Runnable sessionCheck) {
            this.delegate = delegate;
            this.sessionCheck = sessionCheck;
        }

        @Override
        public GetResult get(final String key) {
            sessionCheck.run();
            try {
                final GetResult result = delegate.get(key);
                sessionCheck.run();
                return result;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException {
            sessionCheck.run();
            try {
                final PutResult result = delegate.put(key, value, options);
                sessionCheck.run();
                return result;
            } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public boolean delete(final String key, final Set<DeleteOption> options) throws UnexpectedVersionIdException {
            sessionCheck.run();
            try {
                final boolean deleted = delegate.delete(key, options);
                sessionCheck.run();
                return deleted;
            } catch (UnexpectedVersionIdException expectedCasRace) {
                sessionCheck.run();
                throw expectedCasRace;
            } catch (RuntimeException failure) {
                sessionCheck.run();
                throw failure;
            }
        }

        @Override
        public byte[] sessionIdentity() {
            final byte[] identity = delegate.sessionIdentity();
            return identity == null ? null : Bytes.copy(identity);
        }
    }
}
