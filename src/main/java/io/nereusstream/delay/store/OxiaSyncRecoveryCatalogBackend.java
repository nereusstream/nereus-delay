package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.RecoveryFloorRefV1;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.SyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Single-record Oxia CAS backend for the Recovery Catalog.
 *
 * <p>All catalog state for one shard is encoded as one canonical record.  A
 * publication, scalar Floor advance, and typed Floor advance therefore share
 * one Oxia version CAS; this class never presents a sequence of independent
 * puts as a transaction.  Owner-session pins and upload-intent publication
 * intentionally remain unsupported here because they require a stronger
 * session/transaction contract than this single catalog record provides.</p>
 *
 * <p>The backend is below {@link OxiaRecoveryCatalog}; the latter validates
 * every returned projection against the request and the exact published
 * manifest.  This class only supplies durable CAS/read semantics.</p>
 */
public final class OxiaSyncRecoveryCatalogBackend implements OxiaRecoveryCatalog.CasBackend {
    private static final int SNAPSHOT_VERSION = 1;
    private static final int MAX_CAS_ATTEMPTS = 32;
    private static final int MAX_SNAPSHOT_BYTES = 64 * 1024 * 1024;
    private static final int MAX_MANIFESTS = 100_000;
    private static final String RECORD_SUFFIX = "/catalog";

    private final RecordClient client;
    private final String recordKey;
    private final CheckpointManifestLimits manifestLimits;

    /** Creates a backend over an already configured Oxia client. */
    public OxiaSyncRecoveryCatalogBackend(final SyncOxiaClient client, final String keyPrefix,
                                           final CheckpointManifestLimits manifestLimits) {
        this(new SyncRecordClient(client), keyPrefix, manifestLimits);
    }

    /** Package-private constructor used by deterministic CAS tests. */
    OxiaSyncRecoveryCatalogBackend(final RecordClient client, final String keyPrefix,
                                   final CheckpointManifestLimits manifestLimits) {
        this.client = Objects.requireNonNull(client, "client");
        this.recordKey = canonicalKeyPrefix(keyPrefix) + RECORD_SUFFIX;
        this.manifestLimits = Objects.requireNonNull(manifestLimits, "manifestLimits");
    }

    @Override
    public RecoveryCatalog.Publication publish(final CheckpointManifest manifest,
                                               final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        manifestLimits.validateManifest(manifest);
        return mutate(expectedCatalogGeneration, catalog -> catalog.publish(manifest, expectedCatalogGeneration));
    }

    @Override
    public RecoveryFloor advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                      final byte[] evidenceCursorDigest) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        final byte[] requestedCheckpointId = Bytes.copy(checkpointId);
        final byte[] requestedEvidence = Bytes.copy(evidenceCursorDigest);
        return mutate(expectedCatalogGeneration,
                catalog -> catalog.advanceFloor(requestedCheckpointId, expectedCatalogGeneration, requestedEvidence));
    }

    @Override
    public RecoveryFloorRefV1 advanceFloor(final byte[] checkpointId, final long expectedCatalogGeneration,
                                           final List<EvidenceCursorV1> evidenceCursors) {
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        final byte[] requestedCheckpointId = Bytes.copy(checkpointId);
        final List<EvidenceCursorV1> requestedCursors = List.copyOf(
                Objects.requireNonNull(evidenceCursors, "evidenceCursors"));
        return mutate(expectedCatalogGeneration,
                catalog -> catalog.advanceFloor(requestedCheckpointId, expectedCatalogGeneration, requestedCursors));
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return readCatalog().manifest(Bytes.copy(checkpointId));
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return readCatalog().currentFloor();
    }

    @Override
    public Optional<RecoveryFloorRefV1> currentFloorRef() {
        return readCatalog().currentFloorRef();
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        Objects.requireNonNull(candidate, "candidate");
        readCatalog().validatePublishedRestoreCandidate(candidate);
    }

    /**
     * Validates a reusable local Store projection against the current remote
     * catalog snapshot.  This is intentionally a read-only catalog/Floor
     * check; Owner Lease/session fencing is still supplied by the caller's
     * separate authority and is never inferred from local metadata.
     */
    @Override
    public void validateLocalStoreRecovery(final ShardId shardId,
                                           final StoreRecoveryMetadata localMetadata) {
        readCatalog().validateLocalStoreRecovery(Objects.requireNonNull(shardId, "shardId"),
                Objects.requireNonNull(localMetadata, "localMetadata"));
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(final byte[] candidateCheckpointId,
                                                                        final long requiredMutationSequence,
                                                                        final SourcePosition... requiredPositions) {
        final byte[] requestedCandidate = Bytes.copy(candidateCheckpointId);
        final SourcePosition[] requestedPositions = Arrays.copyOf(
                Objects.requireNonNull(requiredPositions, "requiredPositions"), requiredPositions.length);
        return readCatalog().proveFloorCoverage(requestedCandidate, requiredMutationSequence, requestedPositions);
    }

    /**
     * The upload intent is a separate authority record.  Treating it as if it
     * were part of this catalog record would allow a non-transactional pair of
     * puts to claim atomicity, so the production adapter fails closed.
     */
    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(final CheckpointUploadIntentV1 publishedIntent,
                                                                  final CheckpointManifest manifest,
                                                                  final long expectedCatalogGeneration) {
        throw new UnsupportedOperationException(
                "upload-intent/catalog CAS requires a shared Oxia transaction and is not implemented");
    }

    private <T> T mutate(final long expectedCatalogGeneration,
                         final CatalogMutation<T> mutation) {
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
                if (observed != null && Arrays.equals(afterBytes, observed.value())) {
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
            return RecoveryCatalog.fromSnapshot(new RecoveryCatalog.Snapshot(0, null, List.of(), Map.of(),
                    null, null, null));
        }
        if (!recordKey.equals(result.key())) {
            throw new IllegalStateException("Oxia catalog response belongs to another key");
        }
        final byte[] encoded = result.value();
        if (encoded == null || encoded.length == 0 || encoded.length > MAX_SNAPSHOT_BYTES) {
            throw new IllegalStateException("Oxia catalog snapshot has an invalid size");
        }
        return RecoveryCatalog.fromSnapshot(decodeSnapshot(encoded));
    }

    private RecoveryCatalog.Snapshot decodeSnapshot(final byte[] encoded) {
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
        final Map<String, CheckpointResourceV1> resources = new HashMap<>();
        RecoveryFloor floor = null;
        RecoveryFloorRefV1 typedFloor = null;
        while (index < fields.size()) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index++);
            switch (field.number()) {
                case 3 -> {
                    if (catalogShard != null) {
                        throw new IllegalArgumentException("duplicate Oxia catalog shard field");
                    }
                    catalogShard = ShardSubjectV1.decode(bytes(field, 3)).shardId();
                }
                case 4 -> {
                    if (manifests.size() >= MAX_MANIFESTS) {
                        throw new IllegalArgumentException("Oxia catalog manifest count exceeds bound");
                    }
                    final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(
                            bytes(field, 4), manifestLimits);
                    final String checkpointKey = Bytes.hex(manifest.checkpointId());
                    if (manifests.stream().anyMatch(existing -> checkpointKey.equals(
                            Bytes.hex(existing.checkpointId())))) {
                        throw new IllegalArgumentException("duplicate Oxia catalog checkpoint");
                    }
                    manifests.add(manifest);
                }
                case 5 -> {
                    final CheckpointResourceV1 resource = CheckpointResourceV1.decode(bytes(field, 5));
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
                    typedFloor = RecoveryFloorRefV1.decode(bytes(field, 7));
                }
                default -> throw new IllegalArgumentException("unknown Oxia catalog snapshot field "
                        + field.number());
            }
        }
        final RecoveryCatalog.Snapshot snapshot = new RecoveryCatalog.Snapshot(catalogGeneration, catalogShard,
                manifests, resources, floor, typedFloor, null);
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
        final List<CheckpointManifest> manifests = snapshot.manifests().stream()
                .sorted(Comparator.comparing(manifest -> Bytes.hex(manifest.checkpointId())))
                .toList();
        final List<CheckpointResourceV1> resources = snapshot.manifestResources().values().stream()
                .sorted(Comparator.comparing(resource -> Bytes.hex(resource.checkpointId())))
                .toList();
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SNAPSHOT_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, snapshot.catalogGeneration());
            if (snapshot.catalogShard() != null) {
                CanonicalProtobuf.bytes(output, 3, new ShardSubjectV1(snapshot.catalogShard()).canonicalBytes());
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
        if (value.isBlank() || value.endsWith("/") || value.indexOf('\0') >= 0
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
    interface RecordClient {
        GetResult get(String key);

        PutResult put(String key, byte[] value, Set<PutOption> options)
                throws UnexpectedVersionIdException, KeyAlreadyExistsException;
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
