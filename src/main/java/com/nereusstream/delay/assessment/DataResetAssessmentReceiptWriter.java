package com.nereusstream.delay.assessment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Writes one local assessment receipt without overwrite or runtime-resource access. */
public final class DataResetAssessmentReceiptWriter {
    private DataResetAssessmentReceiptWriter() {}

    public static Path writeNew(final Path target, final DataResetAssessmentReceipt receipt) throws IOException {
        final Path normalized =
                Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        final Path parent = normalized.getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("assessment receipt parent must be an existing non-symlink directory");
        }
        final byte[] json = Objects.requireNonNull(receipt, "receipt").canonicalJsonBytes();
        final ByteBuffer bytes =
                ByteBuffer.allocate(json.length + 1).put(json).put((byte) '\n').flip();
        try (FileChannel channel = FileChannel.open(
                normalized, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        }
        return normalized;
    }
}
