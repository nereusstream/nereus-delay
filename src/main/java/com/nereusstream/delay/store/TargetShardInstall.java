package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetCheckpointInstallIntent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** The common cross-process publication exclusion for one managed Target shard root. */
final class TargetShardInstall implements AutoCloseable {
    private static final int MAX_RETAINED_FILES = 64;
    private static final java.util.Set<Path> LOCAL_OWNERS = new java.util.HashSet<>();
    private final Path root;
    private final FileChannel channel;
    private FileLock lock;

    private TargetShardInstall(Path root, FileChannel channel) {
        this.root = root;
        this.channel = channel;
    }

    static TargetShardInstall acquire(Path root) throws IOException {
        LocalStatePathGuard.ensureRealDirectoryPath(root, "Target shard install root");
        if (Files.isSymbolicLink(root)) {
            throw new IOException("Target shard install root must not be a symbolic link");
        }
        final Path actualRoot = root.toRealPath();
        synchronized (LOCAL_OWNERS) {
            if (!LOCAL_OWNERS.add(actualRoot)) {
                throw new IOException("Target shard install is already owned in this process");
            }
        }
        final TargetShardInstall owner;
        try {
            final Path file = actualRoot.resolve("TARGET-INSTALL.lock");
            if (Files.exists(file, LinkOption.NOFOLLOW_LINKS)
                    && !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Target install lock is not a regular file");
            }
            owner = new TargetShardInstall(
                    actualRoot,
                    FileChannel.open(
                            file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS));
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (LOCAL_OWNERS) {
                LOCAL_OWNERS.remove(actualRoot);
            }
            throw failure;
        }
        try {
            try {
                owner.lock = owner.channel.tryLock();
            } catch (OverlappingFileLockException busy) {
                throw new IOException("Target shard install is already owned", busy);
            }
            if (owner.lock == null) {
                throw new IOException("Target shard install is already owned by another process");
            }
            return owner;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                owner.close();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            if (owner.channel.isOpen()) {
                failure.addSuppressed(new LockCleanup(owner));
            }
            throw failure;
        }
    }

    void requireHeld() throws IOException {
        if (lock == null || !lock.isValid() || !channel.isOpen()) {
            throw new IOException("Target install exclusion ended");
        }
    }

    private Path intents() throws IOException {
        requireHeld();
        final Path directory = root.resolve("install-intents");
        if (Files.isSymbolicLink(directory)) {
            throw new IOException("Target install intent directory must not be a symbolic link");
        }
        LocalStatePathGuard.ensureRealDirectoryPath(directory, "Target install intent directory");
        return directory;
    }

    private static UUID uuid(byte[] value) {
        final var b = ByteBuffer.wrap(value);
        return new UUID(b.getLong(), b.getLong());
    }

    void create(TargetCheckpointInstallIntent intent) throws IOException {
        if (intent.published()) {
            throw new IOException("new Target install intent is already published");
        }
        final Path directory = intents();
        int files = 0;
        try (var entries = Files.newDirectoryStream(directory)) {
            for (var ignored : entries) {
                if (++files >= MAX_RETAINED_FILES - 1) {
                    throw new IOException("retained Target install intent limit reached");
                }
            }
        }
        final Path path = directory.resolve(uuid(intent.targetIncarnation()) + ".intent");
        writeNew(path, intent.canonicalBytes());
        force(directory);
        force(root);
    }

    TargetCheckpointInstallIntent read(UUID target) throws IOException {
        final Path directory = intents();
        final var intent = TargetCheckpointInstallIntent.decode(readImage(directory.resolve(target + ".intent")));
        if (!target.equals(uuid(intent.targetIncarnation()))) {
            throw new IOException("Target intent filename/identity differs");
        }
        return intent;
    }

    void markPublished(TargetCheckpointInstallIntent original) throws IOException {
        final UUID target = uuid(original.targetIncarnation());
        final var current = read(target);
        final var expected = original.asPublished();
        if (Bytes.constantTimeEquals(current.canonicalBytes(), expected.canonicalBytes())) {
            try (var file = FileChannel.open(
                    intents().resolve(target + ".intent"), StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                file.force(true);
            }
            force(intents());
            return;
        }
        if (!Bytes.constantTimeEquals(current.canonicalBytes(), original.canonicalBytes())) {
            throw new IOException("Target install intent changed its original provenance");
        }
        final Path directory = intents();
        final Path temporary = directory.resolve(target + ".published.tmp");
        final byte[] image = expected.canonicalBytes();
        if (Files.exists(temporary, LinkOption.NOFOLLOW_LINKS)) {
            if (!Bytes.constantTimeEquals(image, readImage(temporary))) {
                throw new IOException("Target publication intent temporary image differs");
            }
            try (var file = FileChannel.open(temporary, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                file.force(true);
            }
        } else {
            writeNew(temporary, image);
        }
        requireHeld();
        Files.move(
                temporary,
                directory.resolve(target + ".intent"),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        force(directory);
    }

    private static byte[] readImage(Path file) throws IOException {
        final byte[] image = LocalStatePathGuard.readRegularFileNoFollow(
                file, TargetCheckpointInstallIntent.MAX_BYTES, "Target install intent");
        if (image == null || image.length == 0) {
            throw new IOException("Target install intent is missing/empty");
        }
        return image;
    }

    private static void writeNew(Path file, byte[] image) throws IOException {
        if (image.length > TargetCheckpointInstallIntent.MAX_BYTES) {
            throw new IOException("Target install intent size exceeds bound");
        }
        try (var output = FileChannel.open(
                file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            final var bytes = ByteBuffer.wrap(image);
            while (bytes.hasRemaining()) {
                output.write(bytes);
            }
            output.force(true);
        }
    }

    private static void force(Path directory) throws IOException {
        try (var dir = FileChannel.open(directory, StandardOpenOption.READ)) {
            dir.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (lock != null && lock.isValid()) {
                lock.release();
            }
        } finally {
            try {
                channel.close();
            } finally {
                if (!channel.isOpen()) {
                    synchronized (LOCAL_OWNERS) {
                        LOCAL_OWNERS.remove(root);
                    }
                }
            }
        }
    }

    private static final class LockCleanup extends IllegalStateException
            implements com.nereusstream.delay.ownership.CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient TargetShardInstall original;

        private LockCleanup(TargetShardInstall original) {
            super("original Target install lock close is pending");
            this.original = original;
        }

        @Override
        public boolean cleanupComplete() {
            return !original.channel.isOpen();
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> retryCleanup() {
            try {
                original.close();
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            } catch (IOException | RuntimeException failure) {
                return java.util.concurrent.CompletableFuture.failedFuture(failure);
            }
        }
    }
}
