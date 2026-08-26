package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CheckpointResource;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.EvidenceCursor;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.RecoveryPin;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Crash-durable local projection of one shard's Recovery Catalog.
 *
 * <p>The canonical snapshot contains every published manifest, immutable
 * manifest-object identity, scalar/typed Floor and active RecoveryPin. A
 * JVM lock plus an on-disk lock serializes multiple local authority instances;
 * every successful mutation is published with a checksummed temporary file,
 * atomic rename and directory fsync. This is an embedded/conformance seam,
 * not the production Oxia Owner Lease/session or catalog authority.</p>
 */
public final class PersistentRecoveryCatalog implements RecoveryCatalogAuthority {
    private static final int MAGIC = 0x4E524353; // N R C S
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_LENGTH = Integer.BYTES * 3;
    private static final int DIGEST_LENGTH = 32;
    private static final int MAX_STATE_BYTES = 64 * 1024 * 1024;
    private static final int MAX_MANIFEST_BYTES = 16 * 1024 * 1024;
    private static final int MAX_MANIFESTS = 4096;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-recovery-catalog-state\0");
    private static final Object JVM_LOCK = new Object();

    private final Path stateFile;
    private RecoveryCatalog delegate;

    /** Creates or reopens a crash-durable catalog state file. */
    public PersistentRecoveryCatalog(final Path stateFile) {
        this.stateFile = normalizeStateFile(stateFile);
        try {
            ensureParentDirectory();
            this.delegate = load();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize Recovery Catalog state", failure);
        }
    }

    @Override
    public RecoveryCatalog.Publication publish(
            final CheckpointManifest manifest, final long expectedCatalogGeneration) {
        Objects.requireNonNull(manifest, "manifest");
        return mutate(() -> delegate.publish(manifest, expectedCatalogGeneration));
    }

    @Override
    public RecoveryFloor advanceFloor(
            final byte[] checkpointId, final long expectedCatalogGeneration, final byte[] evidenceCursorDigest) {
        return mutate(() -> delegate.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursorDigest));
    }

    @Override
    public RecoveryFloorRef advanceFloor(
            final byte[] checkpointId,
            final long expectedCatalogGeneration,
            final List<EvidenceCursor> evidenceCursors) {
        return mutate(() -> delegate.advanceFloor(checkpointId, expectedCatalogGeneration, evidenceCursors));
    }

    @Override
    public RecoveryCatalog.Publication publishUploadedCheckpoint(
            final CheckpointUploadIntent publishedIntent,
            final CheckpointManifest manifest,
            final long expectedCatalogGeneration) {
        return mutate(() -> delegate.publishUploadedCheckpoint(publishedIntent, manifest, expectedCatalogGeneration));
    }

    @Override
    public Optional<CheckpointManifest> manifest(final byte[] checkpointId) {
        return read(() -> delegate.manifest(checkpointId));
    }

    @Override
    public Optional<RecoveryFloor> currentFloor() {
        return read(delegate::currentFloor);
    }

    @Override
    public Optional<RecoveryFloorRef> currentFloorRef() {
        return read(delegate::currentFloorRef);
    }

    /** Returns the current catalog generation from the durable snapshot. */
    public long catalogGeneration() {
        return read(delegate::catalogGeneration);
    }

    /** Returns the floor-bounded ancestry in replay order. */
    public List<CheckpointManifest> recoverySet(final byte[] checkpointId) {
        return read(() -> delegate.recoverySet(checkpointId));
    }

    /** Selects the newest checkpoint in the floor-bounded recovery set. */
    public CheckpointManifest selectRecoveryCandidate(final byte[] checkpointId) {
        return read(() -> delegate.selectRecoveryCandidate(checkpointId));
    }

    @Override
    public void validatePublishedRestoreCandidate(final CheckpointManifest candidate) {
        read(() -> {
            delegate.validatePublishedRestoreCandidate(candidate);
            return null;
        });
    }

    @Override
    public Optional<RecoveryCatalog.FloorCoverage> proveFloorCoverage(
            final byte[] candidateCheckpointId,
            final long requiredMutationSequence,
            final SourcePosition... requiredPositions) {
        return read(
                () -> delegate.proveFloorCoverage(candidateCheckpointId, requiredMutationSequence, requiredPositions));
    }

    @Override
    public void validateLocalStoreRecovery(final ShardId shardId, final StoreRecoveryMetadata localMetadata) {
        read(() -> {
            delegate.validateLocalStoreRecovery(shardId, localMetadata);
            return null;
        });
    }

    @Override
    public RecoveryPin createRecoveryPin(final RecoveryPin pin) {
        return mutate(() -> delegate.createRecoveryPin(pin));
    }

    @Override
    public void releaseRecoveryPin(final RecoveryPin pin) {
        mutate(() -> {
            delegate.releaseRecoveryPin(pin);
            return null;
        });
    }

    @Override
    public Optional<RecoveryPin> activeRecoveryPin() {
        return read(delegate::activeRecoveryPin);
    }

    private <T> T mutate(final IoAction<T> action) {
        return withExclusiveLock(() -> {
            delegate = load();
            final RecoveryCatalog.Snapshot before = delegate.snapshot();
            try {
                final T result = action.run();
                persist(delegate.snapshot());
                return result;
            } catch (RuntimeException | IOException | Error failure) {
                delegate = RecoveryCatalog.fromSnapshot(before);
                if (failure instanceof IOException ioFailure) {
                    throw new IllegalStateException("Recovery Catalog state I/O failed", ioFailure);
                }
                throw failure;
            }
        });
    }

    private <T> T read(final IoAction<T> action) {
        return withExclusiveLock(() -> {
            delegate = load();
            return action.run();
        });
    }

    private RecoveryCatalog load() throws IOException {
        final byte[] encoded =
                LocalStatePathGuard.readRegularFileNoFollow(stateFile, MAX_STATE_BYTES, "Recovery Catalog state");
        if (encoded == null) {
            return new RecoveryCatalog();
        }
        return RecoveryCatalog.fromSnapshot(decodeState(encoded));
    }

    private void persist(final RecoveryCatalog.Snapshot snapshot) throws IOException {
        ensureParentDirectory();
        rejectSymbolicLink(stateFile, "Recovery Catalog state");
        final byte[] payload = encodeSnapshot(snapshot);
        if (payload.length > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH) {
            throw new IOException("Recovery Catalog state exceeds bounded size");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + payload.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC)
                .putInt(FORMAT_VERSION)
                .putInt(payload.length)
                .put(payload)
                .put(digest);
        final Path temporary = Files.createTempFile(stateFile.getParent(), ".recovery-catalog-", ".tmp");
        Throwable primaryFailure = null;
        try {
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(output.array());
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("Recovery Catalog state requires atomic rename", unsupported);
            }
            try (FileChannel directory = FileChannel.open(stateFile.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } catch (IOException | RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException | RuntimeException | Error cleanupFailure) {
                if (primaryFailure != null && cleanupFailure != primaryFailure) {
                    primaryFailure.addSuppressed(cleanupFailure);
                } else if (primaryFailure == null) {
                    throwCleanupFailure(cleanupFailure);
                }
            }
        }
    }

    private static void throwCleanupFailure(final Throwable failure) throws IOException {
        if (failure instanceof IOException ioFailure) {
            throw ioFailure;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected cleanup failure", failure);
    }

    private <T> T withExclusiveLock(final IoAction<T> action) {
        synchronized (JVM_LOCK) {
            try {
                ensureParentDirectory();
                final Path lockFile = stateFile.resolveSibling(stateFile.getFileName() + ".lock");
                rejectSymbolicLink(lockFile, "Recovery Catalog lock");
                try (FileChannel channel =
                                FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                        FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("Recovery Catalog lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("Recovery Catalog state I/O failed", failure);
            }
        }
    }

    private void ensureParentDirectory() throws IOException {
        final Path parent = stateFile.getParent();
        if (parent == null) {
            throw new IOException("Recovery Catalog state must have a parent directory");
        }
        LocalStatePathGuard.ensureRealDirectoryPath(parent, "Recovery Catalog state parent");
    }

    static byte[] encodeSnapshot(final RecoveryCatalog.Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        // Validate the complete projection before encoding. The normal
        // delegate path already produces a validated Snapshot, but this
        // boundary is also used while decoding/rechecking state and must not
        // silently normalize an alias resource key, foreign shard or broken
        // Floor/ancestry projection into a different durable value.
        for (Map.Entry<String, CheckpointResource> entry :
                snapshot.manifestResources().entrySet()) {
            final CheckpointResource resource = Objects.requireNonNull(entry.getValue(), "snapshot resource");
            if (!Bytes.hex(resource.checkpointId()).equals(entry.getKey())) {
                throw new IllegalStateException("Recovery Catalog resource map key does not match checkpoint identity");
            }
        }
        RecoveryCatalog.fromSnapshot(snapshot);
        final List<CheckpointManifest> manifests = new ArrayList<>(snapshot.manifests());
        manifests.sort(Comparator.comparing(manifest -> Bytes.hex(manifest.checkpointId())));
        if (manifests.size() > MAX_MANIFESTS) {
            throw new IllegalStateException("Recovery Catalog manifest count exceeds bound");
        }
        final byte[] payload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, FORMAT_VERSION);
            CanonicalProtobuf.uint64Bits(output, 2, snapshot.catalogGeneration());
            for (CheckpointManifest manifest : manifests) {
                final CheckpointResource resource =
                        snapshot.manifestResources().get(Bytes.hex(manifest.checkpointId()));
                CanonicalProtobuf.bytes(output, 3, encodeManifestEntry(manifest, resource));
            }
            if (snapshot.floor() != null) {
                CanonicalProtobuf.bytes(output, 4, snapshot.floor().canonicalBytes());
            }
            if (snapshot.typedFloorRef() != null) {
                CanonicalProtobuf.bytes(output, 5, snapshot.typedFloorRef().canonicalBytes());
            }
            if (snapshot.activeRecoveryPin() != null) {
                CanonicalProtobuf.bytes(output, 6, snapshot.activeRecoveryPin().canonicalBytes());
            }
        });
        return payload;
    }

    private static byte[] encodeManifestEntry(final CheckpointManifest manifest, final CheckpointResource resource) {
        final byte[] manifestBytes = manifest.canonicalJsonBytes();
        if (manifestBytes.length > MAX_MANIFEST_BYTES) {
            throw new IllegalStateException("Recovery Catalog manifest exceeds bound");
        }
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, manifestBytes);
            if (resource != null) {
                CanonicalProtobuf.bytes(output, 2, resource.canonicalBytes());
            }
        });
    }

    private static RecoveryCatalog.Snapshot decodeState(final byte[] encoded) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw new IllegalStateException("Recovery Catalog state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new IllegalStateException("Recovery Catalog state has an unknown header");
        }
        final int payloadLength = input.getInt();
        if (payloadLength <= 0
                || payloadLength > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH
                || encoded.length != HEADER_LENGTH + payloadLength + DIGEST_LENGTH) {
            throw new IllegalStateException("Recovery Catalog state has an invalid length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))) {
            throw new IllegalStateException("Recovery Catalog state checksum mismatch");
        }
        final RecoveryCatalog.Snapshot snapshot;
        try {
            snapshot = decodePayload(payload);
        } catch (RuntimeException malformed) {
            throw new IllegalStateException("Recovery Catalog state is malformed", malformed);
        }
        if (!Arrays.equals(payload, encodeSnapshot(snapshot))) {
            throw new IllegalStateException("Recovery Catalog state is not canonical");
        }
        return snapshot;
    }

    private static RecoveryCatalog.Snapshot decodePayload(final byte[] payload) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(payload, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 2 || fields.get(0).number() != 1 || fields.get(1).number() != 2) {
            throw new IllegalArgumentException("Recovery Catalog snapshot has invalid required fields");
        }
        if (uint32(fields.get(0), 1) != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported Recovery Catalog snapshot version");
        }
        final long generation = uint64Bits(fields.get(1), 2);
        final List<CheckpointManifest> manifests = new ArrayList<>();
        final Map<String, CheckpointResource> resources = new java.util.HashMap<>();
        int index = 2;
        while (index < fields.size() && fields.get(index).number() == 3) {
            if (manifests.size() >= MAX_MANIFESTS) {
                throw new IllegalArgumentException("Recovery Catalog manifest count exceeds bound");
            }
            final Entry entry = decodeManifestEntry(bytes(fields.get(index), 3));
            manifests.add(entry.manifest());
            if (entry.resource() != null) {
                resources.put(Bytes.hex(entry.manifest().checkpointId()), entry.resource());
            }
            index++;
        }
        RecoveryFloor floor = null;
        RecoveryFloorRef typedFloor = null;
        RecoveryPin pin = null;
        if (index < fields.size() && fields.get(index).number() == 4) {
            floor = RecoveryFloor.decode(bytes(fields.get(index++), 4));
        }
        if (index < fields.size() && fields.get(index).number() == 5) {
            typedFloor = RecoveryFloorRef.decode(bytes(fields.get(index++), 5));
        }
        if (index < fields.size() && fields.get(index).number() == 6) {
            pin = RecoveryPin.decode(bytes(fields.get(index++), 6));
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("Recovery Catalog snapshot has unexpected fields");
        }
        return new RecoveryCatalog.Snapshot(generation, null, manifests, resources, floor, typedFloor, pin);
    }

    private static Entry decodeManifestEntry(final byte[] encoded) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 1
                || fields.size() > 2
                || fields.get(0).number() != 1
                || (fields.size() == 2 && fields.get(1).number() != 2)) {
            throw new IllegalArgumentException("Recovery Catalog manifest entry has invalid fields");
        }
        final byte[] manifestBytes = bytes(fields.get(0), 1);
        if (manifestBytes.length > MAX_MANIFEST_BYTES) {
            throw new IllegalArgumentException("Recovery Catalog manifest exceeds bound");
        }
        final CheckpointManifest manifest = CheckpointManifest.decodeCanonicalJson(manifestBytes);
        final CheckpointResource resource =
                fields.size() == 2 ? CheckpointResource.decode(bytes(fields.get(1), 2)) : null;
        return new Entry(manifest, resource);
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint64(field, number);
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("Recovery Catalog uint32 field is out of range: " + number);
        }
        return (int) value;
    }

    private static long uint64Bits(final CanonicalProtobuf.Reader.Field field, final int number) {
        return uint64(field, number);
    }

    private static long uint64(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("Recovery Catalog field is not a varint: " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("Recovery Catalog field is not bytes: " + number);
        }
        return field.rawValue();
    }

    private static Path normalizeStateFile(final Path value) {
        final Path normalized =
                Objects.requireNonNull(value, "stateFile").toAbsolutePath().normalize();
        if (normalized.getFileName() == null) {
            throw new IllegalArgumentException("stateFile must name a file");
        }
        return normalized;
    }

    private static void rejectSymbolicLink(final Path path, final String name) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException(name + " must not be a symbolic link: " + path);
        }
    }

    @FunctionalInterface
    private interface IoAction<T> {
        T run() throws IOException;
    }

    private record Entry(CheckpointManifest manifest, CheckpointResource resource) {
        private Entry {
            Objects.requireNonNull(manifest, "manifest");
        }
    }
}
