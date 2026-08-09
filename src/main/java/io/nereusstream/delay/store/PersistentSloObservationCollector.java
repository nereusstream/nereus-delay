package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloThresholdDirectionV1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Crash-durable local projection of the at-least-once SLO collector merge.
 *
 * <p>The state file retains the exact canonical outbox projection for every
 * sample in deterministic sample-id order.  Each operation rereads the file
 * while holding a JVM and on-disk lock, then publishes a replacement through
 * a checksummed temporary file, atomic rename and directory fsync.  This is
 * an embedded durability seam; it is not the production collector's
 * authorization, rolling-window policy or metric publication authority.</p>
 */
public final class PersistentSloObservationCollector {
    private static final int MAGIC = 0x4E534F43; // N S O C
    private static final int FORMAT_VERSION = 1;
    private static final int HEADER_LENGTH = Integer.BYTES * 3;
    private static final int DIGEST_LENGTH = 32;
    private static final int MAX_STATE_BYTES = 64 * 1024 * 1024;
    private static final int MAX_SAMPLE_BYTES = 8 * 1024 * 1024;
    private static final int MAX_SAMPLES = 65_536;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-slo-collector-state-v1\0");
    private static final Object JVM_LOCK = new Object();

    private final Path stateFile;
    private final SloObservationCollectorLimits limits;
    private SloObservationCollector delegate;

    /** Creates a bounded-by-file-size collector projection without a policy envelope. */
    public PersistentSloObservationCollector(final Path stateFile) {
        this(stateFile, null);
    }

    /** Creates a collector projection with an explicit sample/byte envelope. */
    public PersistentSloObservationCollector(final Path stateFile,
                                              final SloObservationCollectorLimits limits) {
        this.stateFile = normalizeStateFile(stateFile);
        this.limits = limits;
        try {
            ensureParentDirectory();
            this.delegate = load();
        } catch (IOException failure) {
            throw new IllegalStateException("cannot initialize SLO collector state", failure);
        }
    }

    /** Merges one exported outbox record and durably publishes the result. */
    public SloObservationOutboxV1 merge(final SloObservationOutboxV1 incoming,
                                        final SloThresholdDirectionV1 direction) {
        Objects.requireNonNull(incoming, "incoming");
        Objects.requireNonNull(direction, "direction");
        return mutate(() -> delegate.merge(incoming, direction));
    }

    /** Returns a deterministic sample projection sorted by canonical sample ID. */
    public List<SloObservationOutboxV1> snapshot() {
        return read(delegate::snapshot);
    }

    public SloObservationOutboxV1 get(final byte[] sampleId) {
        return read(() -> delegate.get(sampleId));
    }

    public int size() {
        return read(delegate::size);
    }

    public SloObservationCollector.Usage usage() {
        return read(delegate::usage);
    }

    private <T> T mutate(final IoAction<T> action) {
        return withExclusiveLock(() -> {
            delegate = load();
            final List<SloObservationOutboxV1> before = delegate.snapshot();
            try {
                final T result = action.run();
                persist(delegate.snapshot());
                return result;
            } catch (RuntimeException | IOException failure) {
                delegate = rebuild(before);
                if (failure instanceof IOException ioFailure) {
                    throw new IllegalStateException("SLO collector state I/O failed", ioFailure);
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

    private SloObservationCollector load() throws IOException {
        if (!Files.exists(stateFile, LinkOption.NOFOLLOW_LINKS)) {
            return new SloObservationCollector(limits);
        }
        rejectSymbolicLink(stateFile, "SLO collector state");
        if (!Files.isRegularFile(stateFile, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("SLO collector state is not a regular file: " + stateFile);
        }
        if (Files.size(stateFile) > MAX_STATE_BYTES) {
            throw new IOException("SLO collector state exceeds bounded size: " + stateFile);
        }
        return decodeState(Files.readAllBytes(stateFile));
    }

    private SloObservationCollector rebuild(final List<SloObservationOutboxV1> values) {
        final SloObservationCollector result = new SloObservationCollector(limits);
        for (SloObservationOutboxV1 value : values) {
            result.merge(value, value.start().objective().requiredDirection());
        }
        return result;
    }

    private void persist(final List<SloObservationOutboxV1> values) throws IOException {
        ensureParentDirectory();
        rejectSymbolicLink(stateFile, "SLO collector state");
        final byte[] payload = encodeSnapshot(values);
        if (payload.length > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH) {
            throw new IOException("SLO collector state exceeds bounded size");
        }
        final byte[] digest = Bytes.sha256(DIGEST_DOMAIN, payload);
        final ByteBuffer output = ByteBuffer.allocate(HEADER_LENGTH + payload.length + digest.length)
                .order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(FORMAT_VERSION).putInt(payload.length).put(payload).put(digest);
        final Path temporary = Files.createTempFile(stateFile.getParent(), ".slo-collector-", ".tmp");
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
                Files.move(temporary, stateFile, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException unsupported) {
                throw new IOException("SLO collector state requires atomic rename", unsupported);
            }
            try (FileChannel directory = FileChannel.open(stateFile.getParent(), StandardOpenOption.READ)) {
                directory.force(true);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    private <T> T withExclusiveLock(final IoAction<T> action) {
        synchronized (JVM_LOCK) {
            try {
                ensureParentDirectory();
                final Path lockFile = stateFile.resolveSibling(stateFile.getFileName() + ".lock");
                rejectSymbolicLink(lockFile, "SLO collector lock");
                try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE); FileLock fileLock = channel.lock()) {
                    if (!fileLock.isValid()) {
                        throw new IOException("SLO collector lock is not valid");
                    }
                    return action.run();
                }
            } catch (IOException failure) {
                throw new IllegalStateException("SLO collector state I/O failed", failure);
            }
        }
    }

    private void ensureParentDirectory() throws IOException {
        final Path parent = stateFile.getParent();
        if (parent == null) {
            throw new IOException("SLO collector state must have a parent directory");
        }
        rejectSymbolicLink(parent, "SLO collector state parent");
        if (!Files.exists(parent, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(parent);
        }
        if (!Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("SLO collector state parent is not a directory: " + parent);
        }
    }

    private static byte[] encodeSnapshot(final List<SloObservationOutboxV1> values) {
        Objects.requireNonNull(values, "values");
        final List<SloObservationOutboxV1> sorted = new ArrayList<>(values);
        sorted.sort(Comparator.comparing(value -> Bytes.hex(value.sampleId())));
        if (sorted.size() > MAX_SAMPLES) {
            throw new IllegalStateException("SLO collector sample count exceeds bound");
        }
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, FORMAT_VERSION);
            for (SloObservationOutboxV1 value : sorted) {
                final byte[] sample = value.canonicalBytes();
                if (sample.length > MAX_SAMPLE_BYTES) {
                    throw new IllegalStateException("SLO collector sample exceeds bound");
                }
                CanonicalProtobuf.bytes(output, 2, sample);
            }
        });
    }

    private SloObservationCollector decodeState(final byte[] encoded) {
        if (encoded.length < HEADER_LENGTH + DIGEST_LENGTH) {
            throw new IllegalStateException("SLO collector state is truncated");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        if (input.getInt() != MAGIC || input.getInt() != FORMAT_VERSION) {
            throw new IllegalStateException("SLO collector state has an unknown header");
        }
        final int payloadLength = input.getInt();
        if (payloadLength <= 0 || payloadLength > MAX_STATE_BYTES - HEADER_LENGTH - DIGEST_LENGTH
                || encoded.length != HEADER_LENGTH + payloadLength + DIGEST_LENGTH) {
            throw new IllegalStateException("SLO collector state has an invalid length");
        }
        final byte[] payload = new byte[payloadLength];
        input.get(payload);
        final byte[] digest = new byte[DIGEST_LENGTH];
        input.get(digest);
        if (!Bytes.constantTimeEquals(digest, Bytes.sha256(DIGEST_DOMAIN, payload))) {
            throw new IllegalStateException("SLO collector state checksum mismatch");
        }
        final List<SloObservationOutboxV1> values = decodePayload(payload);
        if (!Arrays.equals(payload, encodeSnapshot(values))) {
            throw new IllegalStateException("SLO collector state is not canonical");
        }
        return rebuild(values);
    }

    private static List<SloObservationOutboxV1> decodePayload(final byte[] payload) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(payload, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty() || fields.get(0).number() != 1 || uint32(fields.get(0), 1) != FORMAT_VERSION) {
            throw new IllegalStateException("SLO collector snapshot has invalid version field");
        }
        final List<SloObservationOutboxV1> values = new ArrayList<>();
        String previousId = null;
        for (int index = 1; index < fields.size(); index++) {
            if (fields.get(index).number() != 2) {
                throw new IllegalStateException("SLO collector snapshot has unexpected field");
            }
            final byte[] sampleBytes = bytes(fields.get(index), 2);
            if (sampleBytes.length > MAX_SAMPLE_BYTES) {
                throw new IllegalStateException("SLO collector sample exceeds bound");
            }
            final SloObservationOutboxV1 value = SloObservationOutboxV1.decode(sampleBytes);
            final String currentId = Bytes.hex(value.sampleId());
            if (previousId != null && previousId.compareTo(currentId) >= 0) {
                throw new IllegalStateException("SLO collector samples are not strictly sorted");
            }
            previousId = currentId;
            values.add(value);
            if (values.size() > MAX_SAMPLES) {
                throw new IllegalStateException("SLO collector sample count exceeds bound");
            }
        }
        return List.copyOf(values);
    }

    private static int uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() != FORMAT_VERSION) {
            throw new IllegalStateException("SLO collector version field is invalid");
        }
        return (int) field.unsignedValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalStateException("SLO collector field is not bytes");
        }
        return field.rawValue();
    }

    private static Path normalizeStateFile(final Path value) {
        final Path normalized = Objects.requireNonNull(value, "stateFile").toAbsolutePath().normalize();
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
}
