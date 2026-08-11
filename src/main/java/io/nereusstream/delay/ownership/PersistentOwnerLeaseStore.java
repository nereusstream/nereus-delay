package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.store.LocalStatePathGuard;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Crash-durable local Owner Lease projection for embedded/conformance runs.
 *
 * <p>The state for each shard keeps the latest consumed owner epoch even when
 * no lease is currently present.  Every CAS is serialized by a JVM lock and a
 * process-shared file lock, then published with a checksummed temporary file,
 * atomic rename and directory fsync.  This gives local restart and response-
 * loss tests the same exact-identity behavior as the in-memory authority.  It
 * is deliberately not a production lease authority: Oxia must own the
 * session-bound ephemeral record and cross-worker CAS.</p>
 */
public final class PersistentOwnerLeaseStore implements OwnerLeaseStore {
    private static final int MAGIC = 0x4E4F4C53; // N O L S
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_LENGTH = Integer.BYTES * 3;
    private static final int DIGEST_LENGTH = 32;
    private static final int SHARD_KEY_LENGTH = 20;
    private static final int TOKEN_LENGTH = 32;
    private static final int MAX_OWNER_ID_BYTES = 4096;
    private static final int MAX_STATE_BYTES = 128 * 1024;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-owner-lease-state-v1\\0");
    private static final Object JVM_LOCK = new Object();

    private final Path root;
    private final SecureRandom random;

    /** Creates or reopens a local state directory reserved for owner leases. */
    public PersistentOwnerLeaseStore(final Path root) {
        this(root, new SecureRandom());
    }

    PersistentOwnerLeaseStore(final Path root, final SecureRandom random) {
        this.root = normalizeRoot(root);
        this.random = Objects.requireNonNull(random, "random");
        try {
            ensureRoot(true);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize owner lease state directory", failure);
        }
    }

    @Override
    public Optional<OwnerLease> acquire(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                        final long leaseDurationMs) {
        validateRequest(shardId, ownerId, nowEpochMs, leaseDurationMs);
        return withExclusiveLock(() -> {
            final State current = load(shardId);
            if (current.lease() != null && current.lease().validAt(nowEpochMs)) {
                return Optional.empty();
            }
            final long expiresAt = Math.addExact(nowEpochMs, leaseDurationMs);
            final long nextEpoch = InMemoryOwnerLeaseStore.nextEpoch(current.lastEpoch());
            final OwnerLease next = new OwnerLease(shardId, ownerId, nextEpoch, randomToken(), expiresAt);
            persist(new State(shardId, nextEpoch, next));
            return Optional.of(next);
        });
    }

    @Override
    public Optional<OwnerLease> acquire(final SourceAssignment assignment, final String ownerId,
                                        final byte[] sessionIdentity, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(assignment, "assignment");
        validateRequest(assignment.shardId(), ownerId, nowEpochMs, leaseDurationMs);
        final OwnerLeaseContext context = new OwnerLeaseContext(assignment.assignmentId(),
                assignment.assignmentEpoch(), sessionIdentity);
        return withExclusiveLock(() -> {
            final State current = load(assignment.shardId());
            if (current.lease() != null && current.lease().validAt(nowEpochMs)) {
                return Optional.empty();
            }
            final long expiresAt = Math.addExact(nowEpochMs, leaseDurationMs);
            final long nextEpoch = InMemoryOwnerLeaseStore.nextEpoch(current.lastEpoch());
            final OwnerLease next = new OwnerLease(assignment.shardId(), ownerId, nextEpoch, randomToken(), expiresAt,
                    context, ShardLifecycleState.ACQUIRING);
            persist(new State(assignment.shardId(), nextEpoch, next));
            return Optional.of(next);
        });
    }

    @Override
    public Optional<OwnerLease> renew(final OwnerLease expected, final long nowEpochMs,
                                      final long leaseDurationMs) {
        Objects.requireNonNull(expected, "expected");
        validateRequest(expected.shardId(), expected.ownerId(), nowEpochMs, leaseDurationMs);
        return withExclusiveLock(() -> {
            final State current = load(expected.shardId());
            if (!sameIdentity(current.lease(), expected) || current.lease().state() != expected.state()
                    || !current.lease().validAt(nowEpochMs)) {
                return Optional.empty();
            }
            final long expiresAt = Math.addExact(nowEpochMs, leaseDurationMs);
            if (expiresAt < current.lease().expiresAtEpochMs()) {
                return Optional.empty();
            }
            final OwnerLease renewed = new OwnerLease(expected.shardId(), expected.ownerId(), expected.ownerEpoch(),
                    expected.leaseToken(), expiresAt, expected.context(), expected.state());
            persist(new State(expected.shardId(), current.lastEpoch(), renewed));
            return Optional.of(renewed);
        });
    }

    @Override
    public boolean release(final OwnerLease expected) {
        Objects.requireNonNull(expected, "expected");
        return withExclusiveLock(() -> {
            final State current = load(expected.shardId());
            if (!sameIdentity(current.lease(), expected)) {
                return false;
            }
            persist(new State(expected.shardId(), current.lastEpoch(), null));
            return true;
        });
    }

    @Override
    public Optional<OwnerLease> transition(final OwnerLease expected, final ShardLifecycleState nextState) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(nextState, "nextState");
        return withExclusiveLock(() -> {
            final State current = load(expected.shardId());
            if (!sameIdentity(current.lease(), expected) || current.lease().state() != expected.state()
                    || !current.lease().state().canTransitionTo(nextState)) {
                return Optional.empty();
            }
            final OwnerLease transitioned = new OwnerLease(expected.shardId(), expected.ownerId(),
                    expected.ownerEpoch(), expected.leaseToken(), expected.expiresAtEpochMs(), expected.context(),
                    nextState);
            persist(new State(expected.shardId(), current.lastEpoch(), transitioned));
            return Optional.of(transitioned);
        });
    }

    @Override
    public Optional<OwnerLease> current(final ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId");
        return withExclusiveLock(() -> Optional.ofNullable(load(shardId).lease()));
    }

    private byte[] randomToken() {
        final byte[] token = new byte[TOKEN_LENGTH];
        random.nextBytes(token);
        return token;
    }

    private <T> T withExclusiveLock(final IoAction<T> action) {
        synchronized (JVM_LOCK) {
            try {
                ensureRoot(false);
                final Path lockPath = root.resolve("owner-leases.lock");
                rejectSymbolicLink(lockPath, "owner lease lock");
                try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE); FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("owner lease lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("owner lease state I/O failed", failure);
            }
        }
    }

    private State load(final ShardId shardId) throws IOException {
        final Path statePath = statePath(shardId);
        final byte[] encoded = LocalStatePathGuard.readRegularFileNoFollow(statePath, MAX_STATE_BYTES,
                "owner lease state");
        if (encoded == null) {
            return new State(shardId, 0, null);
        }
        return decodeState(encoded, shardId, statePath);
    }

    private void persist(final State state) throws IOException {
        ensureRoot(false);
        final Path statePath = statePath(state.shardId());
        rejectSymbolicLink(statePath, "owner lease state");
        final byte[] payload = encodePayload(state);
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + payload.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(FORMAT_VERSION).putInt(payload.length).put(payload).put(digest);
        if (output.position() > MAX_STATE_BYTES) {
            throw new IOException("owner lease state exceeds bounded size");
        }
        final Path temporary = Files.createTempFile(root, ".owner-lease-", ".tmp");
        Throwable primaryFailure = null;
        try {
            try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(output.array());
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            try {
                Files.move(temporary, statePath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("owner lease state requires atomic rename", unsupported);
            }
            forceDirectory();
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

    private void forceDirectory() throws IOException {
        try (FileChannel directory = FileChannel.open(root, StandardOpenOption.READ)) {
            directory.force(true);
        }
    }

    private static byte[] encodePayload(final State state) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, FORMAT_VERSION);
            CanonicalProtobuf.bytes(output, 2, shardKey(state.shardId()));
            CanonicalProtobuf.uint64Bits(output, 3, state.lastEpoch());
            if (state.lease() != null) {
                CanonicalProtobuf.bytes(output, 4, encodeLease(state.lease()));
            }
        });
    }

    private static byte[] encodeLease(final OwnerLease lease) {
        final byte[] ownerBytes = Bytes.utf8(lease.ownerId());
        if (ownerBytes.length == 0 || ownerBytes.length > MAX_OWNER_ID_BYTES) {
            throw new IllegalStateException("owner id exceeds local lease state bound");
        }
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, ownerBytes);
            CanonicalProtobuf.uint64Bits(output, 2, lease.ownerEpoch());
            CanonicalProtobuf.bytes(output, 3, lease.leaseToken());
            CanonicalProtobuf.uint64(output, 4, lease.expiresAtEpochMs());
            CanonicalProtobuf.uint32(output, 5, lease.state().wireValue());
            if (lease.context() != null) {
                CanonicalProtobuf.bytes(output, 6, encodeContext(lease.context()));
            }
        });
    }

    private static byte[] encodeContext(final OwnerLeaseContext context) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, context.sourceAssignmentId());
            CanonicalProtobuf.uint64(output, 2, context.assignmentEpoch());
            CanonicalProtobuf.bytes(output, 3, context.sessionIdentity());
        });
    }

    private static State decodeState(final byte[] encoded, final ShardId expectedShard, final Path statePath) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw corrupt(statePath, "owner lease state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw corrupt(statePath, "owner lease state has an unknown header");
        }
        final int payloadLength = input.getInt();
        if (payloadLength <= 0 || payloadLength > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH
                || encoded.length != HEADER_LENGTH + payloadLength + DIGEST_LENGTH) {
            throw corrupt(statePath, "owner lease state has an invalid length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))) {
            throw corrupt(statePath, "owner lease state checksum mismatch");
        }
        final State state;
        try {
            state = decodePayload(payload, expectedShard);
        } catch (RuntimeException malformed) {
            throw corrupt(statePath, "owner lease state is malformed", malformed);
        }
        if (!Arrays.equals(payload, encodePayload(state))) {
            throw corrupt(statePath, "owner lease state is not canonical");
        }
        return state;
    }

    private static State decodePayload(final byte[] payload, final ShardId expectedShard) {
        final List<CanonicalProtobuf.Reader.Field> fields = readFields(payload, false);
        if (fields.size() < 3 || fields.size() > 4
                || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("owner lease state fields are not canonical");
        }
        requireWire(fields.get(0), 0);
        if (fields.get(0).unsignedValue() != FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported owner lease state version");
        }
        requireWire(fields.get(1), 2);
        final byte[] key = fields.get(1).rawValue();
        if (key.length != SHARD_KEY_LENGTH || !Bytes.constantTimeEquals(key, shardKey(expectedShard))) {
            throw new IllegalArgumentException("owner lease state shard identity mismatch");
        }
        requireWire(fields.get(2), 0);
        final long lastEpoch = fields.get(2).unsignedValue();
        OwnerLease lease = null;
        if (fields.size() == 4) {
            if (fields.get(3).number() != 4) {
                throw new IllegalArgumentException("owner lease state has an unknown field");
            }
            requireWire(fields.get(3), 2);
            lease = decodeLease(fields.get(3).rawValue(), expectedShard);
            if (lease.ownerEpoch() != lastEpoch) {
                throw new IllegalArgumentException("owner lease epoch history does not match active lease");
            }
        }
        return new State(expectedShard, lastEpoch, lease);
    }

    private static OwnerLease decodeLease(final byte[] encoded, final ShardId shardId) {
        final List<CanonicalProtobuf.Reader.Field> fields = readFields(encoded, false);
        if (fields.size() < 5 || fields.size() > 6) {
            throw new IllegalArgumentException("owner lease fields are not canonical");
        }
        for (int index = 0; index < 5; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("owner lease fields are not canonical");
            }
        }
        final String ownerId = decodeOwnerId(fields.get(0));
        requireWire(fields.get(1), 0);
        final long ownerEpoch = fields.get(1).unsignedValue();
        requireWire(fields.get(2), 2);
        final byte[] token = fields.get(2).rawValue();
        Bytes.requireLength(token, TOKEN_LENGTH, "leaseToken");
        requireWire(fields.get(3), 0);
        final long expiresAt = fields.get(3).unsignedValue();
        requireWire(fields.get(4), 0);
        final ShardLifecycleState lifecycle = lifecycleState(fields.get(4).unsignedValue());
        OwnerLeaseContext context = null;
        if (fields.size() == 6) {
            if (fields.get(5).number() != 6) {
                throw new IllegalArgumentException("owner lease context field is not canonical");
            }
            requireWire(fields.get(5), 2);
            context = decodeContext(fields.get(5).rawValue());
        }
        return new OwnerLease(shardId, ownerId, ownerEpoch, token, expiresAt, context, lifecycle);
    }

    private static OwnerLeaseContext decodeContext(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readFields(encoded, false);
        if (fields.size() != 3 || fields.get(0).number() != 1 || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("owner lease context fields are not canonical");
        }
        requireWire(fields.get(0), 2);
        final byte[] assignmentId = fields.get(0).rawValue();
        requireWire(fields.get(1), 0);
        final long assignmentEpoch = fields.get(1).unsignedValue();
        requireWire(fields.get(2), 2);
        return new OwnerLeaseContext(assignmentId, assignmentEpoch, fields.get(2).rawValue());
    }

    private static String decodeOwnerId(final CanonicalProtobuf.Reader.Field field) {
        requireWire(field, 2);
        try {
            final String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(field.rawValue())).toString();
            if (!Arrays.equals(Bytes.utf8(value), field.rawValue())) {
                throw new IllegalArgumentException("owner id is not canonical UTF-8");
            }
            return value;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("owner id is not valid UTF-8", malformed);
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> readFields(final byte[] encoded,
                                                                     final boolean allowRepeated) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, allowRepeated);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static void requireWire(final CanonicalProtobuf.Reader.Field field, final int wireType) {
        if (field.wireType() != wireType) {
            throw new IllegalArgumentException("owner lease field has an invalid wire type");
        }
    }

    private static ShardLifecycleState lifecycleState(final long wireValue) {
        if (wireValue > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("owner lease lifecycle value is out of range");
        }
        for (ShardLifecycleState state : ShardLifecycleState.values()) {
            if (state.wireValue() == (int) wireValue) {
                return state;
            }
        }
        throw new IllegalArgumentException("unknown owner lease lifecycle value: " + wireValue);
    }

    private Path statePath(final ShardId shardId) {
        return root.resolve(Bytes.hex(shardKey(shardId)) + ".state");
    }

    private static byte[] shardKey(final ShardId shardId) {
        return Bytes.concat(shardId.routeIncarnation().bytes(), Bytes.u32beBits(shardId.partition()));
    }

    private void ensureRoot(final boolean create) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            if (!create) {
                throw new IOException("owner lease state root is missing: " + root);
            }
            LocalStatePathGuard.ensureRealDirectoryPath(root, "owner lease state root");
            return;
        }
        LocalStatePathGuard.ensureRealDirectoryPath(root, "owner lease state root");
    }

    private static void rejectSymbolicLink(final Path path, final String name) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException(name + " must not be a symbolic link: " + path);
        }
    }

    private static Path normalizeRoot(final Path value) {
        return Objects.requireNonNull(value, "root").toAbsolutePath().normalize();
    }

    private static void validateRequest(final ShardId shardId, final String ownerId, final long nowEpochMs,
                                        final long leaseDurationMs) {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(ownerId, "ownerId");
        if (ownerId.isBlank() || nowEpochMs < 0 || leaseDurationMs <= 0) {
            throw new IllegalArgumentException("invalid owner lease request");
        }
        final byte[] ownerBytes = Bytes.utf8(ownerId);
        if (ownerBytes.length > MAX_OWNER_ID_BYTES) {
            throw new IllegalArgumentException("owner id exceeds local lease state bound");
        }
        if (!ownerId.equals(new String(ownerBytes, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("owner id is not canonical UTF-8");
        }
    }

    private static boolean sameIdentity(final OwnerLease left, final OwnerLease right) {
        return left != null && right != null && left.sameIdentity(right);
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

    private static IllegalStateException corrupt(final Path path, final String message) {
        return new IllegalStateException(message + ": " + path);
    }

    private static IllegalStateException corrupt(final Path path, final String message, final Throwable cause) {
        return new IllegalStateException(message + ": " + path, cause);
    }

    @FunctionalInterface
    private interface IoAction<T> {
        T run() throws IOException;
    }

    private record State(ShardId shardId, long lastEpoch, OwnerLease lease) {
        private State {
            Objects.requireNonNull(shardId, "shardId");
            if (lease != null && (!shardId.equals(lease.shardId()) || lease.ownerEpoch() != lastEpoch)) {
                throw new IllegalArgumentException("owner lease state identity or epoch mismatch");
            }
        }
    }
}
