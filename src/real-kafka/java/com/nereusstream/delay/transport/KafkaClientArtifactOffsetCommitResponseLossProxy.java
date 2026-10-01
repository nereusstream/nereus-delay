package com.nereusstream.delay.transport;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** K1 test proxy that withholds Kafka OffsetCommit responses until a gate is released. */
public final class KafkaClientArtifactOffsetCommitResponseLossProxy {
    private static final int OFFSET_COMMIT_API_KEY = 8;
    private static final int MIN_REQUEST_BYTES = 8;
    private static final int MAX_FRAME_BYTES = 64 << 20;

    private KafkaClientArtifactOffsetCommitResponseLossProxy() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 11) {
            throw new IllegalArgumentException("usage: <listen-1> <listen-2> <listen-3> <target-1> <target-2> "
                    + "<target-3> <hold-file> <release-file> <stop-file> <ready-file> <dropped-file>");
        }
        final int[] listenPorts = {port(arguments[0]), port(arguments[1]), port(arguments[2])};
        final int[] targetPorts = {port(arguments[3]), port(arguments[4]), port(arguments[5])};
        final Path holdFile = Path.of(arguments[6]);
        final Path releaseFile = Path.of(arguments[7]);
        final Path stopFile = Path.of(arguments[8]);
        final Path readyFile = Path.of(arguments[9]);
        final Path droppedFile = Path.of(arguments[10]);
        final var stopped = new AtomicBoolean();
        final var droppedCount = new AtomicInteger();
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final List<ServerSocket> servers = new ArrayList<>(listenPorts.length);
        final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-kafka-offset-commit-proxy");
            thread.setDaemon(true);
            return thread;
        });
        try {
            for (int listenPort : listenPorts) {
                final var server = new ServerSocket();
                server.setReuseAddress(true);
                server.bind(new InetSocketAddress("127.0.0.1", listenPort));
                servers.add(server);
            }
            writeMarker(readyFile, "listening=" + join(listenPorts) + " targets=" + join(targetPorts) + "\n");
            for (int index = 0; index < servers.size(); index++) {
                final int targetPort = targetPorts[index];
                final ServerSocket server = servers.get(index);
                executor.submit(() -> accept(
                        server,
                        targetPort,
                        holdFile,
                        releaseFile,
                        droppedFile,
                        droppedCount,
                        sockets,
                        executor,
                        stopped));
            }
            while (!Files.exists(stopFile)) {
                Thread.sleep(25);
            }
        } finally {
            stopped.set(true);
            closeAll(servers);
            closeAll(sockets);
            executor.shutdownNow();
            try {
                Files.deleteIfExists(readyFile);
            } catch (IOException ignored) {
                // The e2e harness owns its unique temporary directory.
            }
        }
        System.out.println("Kafka OffsetCommit response-loss proxy stopped after dropping " + droppedCount.get()
                + " Broker response(s)");
    }

    private static void accept(
            final ServerSocket server,
            final int targetPort,
            final Path holdFile,
            final Path releaseFile,
            final Path droppedFile,
            final AtomicInteger droppedCount,
            final Set<Socket> sockets,
            final ExecutorService executor,
            final AtomicBoolean stopped) {
        while (!stopped.get()) {
            try {
                final Socket client = server.accept();
                sockets.add(client);
                executor.submit(() -> relay(
                        client, targetPort, holdFile, releaseFile, droppedFile, droppedCount, sockets, executor));
            } catch (IOException closed) {
                if (!stopped.get()) {
                    System.err.println("Kafka OffsetCommit proxy listener failed: " + closed.getMessage());
                }
                return;
            }
        }
    }

    private static void relay(
            final Socket client,
            final int targetPort,
            final Path holdFile,
            final Path releaseFile,
            final Path droppedFile,
            final AtomicInteger droppedCount,
            final Set<Socket> sockets,
            final ExecutorService executor) {
        try (Socket target = new Socket()) {
            target.connect(new InetSocketAddress("127.0.0.1", targetPort), 10_000);
            target.setTcpNoDelay(true);
            client.setTcpNoDelay(true);
            sockets.add(target);
            try {
                final var requestApiKeys = new ConcurrentHashMap<Integer, Integer>();
                final Thread requests = daemon("kafka-offset-proxy-client-to-broker", () -> transferRequests(
                        client, target, requestApiKeys));
                final Thread responses = daemon("kafka-offset-proxy-broker-to-client", () -> transferResponses(
                        client, target, requestApiKeys, holdFile, releaseFile, droppedFile, droppedCount));
                requests.start();
                responses.start();
                try {
                    requests.join();
                    responses.join();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                sockets.remove(target);
            }
        } catch (IOException failure) {
            System.err.println("Kafka OffsetCommit proxy relay failed: " + failure.getMessage());
        } finally {
            sockets.remove(client);
            close(client);
        }
    }

    private static void transferRequests(
            final Socket client, final Socket target, final ConcurrentMap<Integer, Integer> requestApiKeys) {
        try {
            final InputStream input = client.getInputStream();
            final OutputStream output = target.getOutputStream();
            byte[] frame;
            while ((frame = readFrame(input, true)) != null) {
                final int apiKey = Short.toUnsignedInt(ByteBuffer.wrap(frame, 4, Short.BYTES).getShort());
                final int correlationId = ByteBuffer.wrap(frame, 8, Integer.BYTES).getInt();
                if (requestApiKeys.putIfAbsent(correlationId, apiKey) != null) {
                    throw new IOException("duplicate in-flight Kafka correlation ID");
                }
                output.write(frame);
                output.flush();
            }
        } catch (IOException ignored) {
            // Closing either socket terminates both relay directions.
        } finally {
            close(client);
            close(target);
        }
    }

    private static void transferResponses(
            final Socket client,
            final Socket target,
            final ConcurrentMap<Integer, Integer> requestApiKeys,
            final Path holdFile,
            final Path releaseFile,
            final Path droppedFile,
            final AtomicInteger droppedCount) {
        try {
            final InputStream input = target.getInputStream();
            final OutputStream output = client.getOutputStream();
            byte[] frame;
            while ((frame = readFrame(input, false)) != null) {
                final int correlationId = ByteBuffer.wrap(frame, 4, Integer.BYTES).getInt();
                final Integer apiKey = requestApiKeys.remove(correlationId);
                if (apiKey == null) {
                    throw new IOException("Kafka response has no matching request correlation ID");
                }
                if (apiKey == OFFSET_COMMIT_API_KEY && Files.exists(holdFile) && !Files.exists(releaseFile)) {
                    final int ordinal = droppedCount.incrementAndGet();
                    appendMarker(
                            droppedFile,
                            "ordinal=" + ordinal + " apiKey=" + apiKey + " correlationId=" + correlationId
                                    + " brokerResponseReceived=true forwarded=false\n");
                    close(client);
                    close(target);
                    return;
                }
                output.write(frame);
                output.flush();
            }
        } catch (IOException ignored) {
            // Closing either socket terminates both relay directions.
        } finally {
            close(client);
            close(target);
        }
    }

    private static byte[] readFrame(final InputStream input, final boolean request) throws IOException {
        final byte[] lengthBytes = new byte[Integer.BYTES];
        final int first = input.read();
        if (first < 0) {
            return null;
        }
        lengthBytes[0] = (byte) first;
        readFully(input, lengthBytes, 1, lengthBytes.length - 1);
        final int frameLength = ByteBuffer.wrap(lengthBytes).getInt();
        if (frameLength < (request ? MIN_REQUEST_BYTES : Integer.BYTES) || frameLength > MAX_FRAME_BYTES) {
            throw new IOException("Kafka frame length is outside the test proxy bound: " + frameLength);
        }
        final byte[] frame = new byte[Integer.BYTES + frameLength];
        System.arraycopy(lengthBytes, 0, frame, 0, Integer.BYTES);
        readFully(input, frame, Integer.BYTES, frameLength);
        return frame;
    }

    private static void readFully(final InputStream input, final byte[] bytes, final int offset, final int length)
            throws IOException {
        int read = 0;
        while (read < length) {
            final int count = input.read(bytes, offset + read, length - read);
            if (count < 0) {
                throw new EOFException("Kafka frame ended before its declared length");
            }
            read += count;
        }
    }

    private static Thread daemon(final String name, final Runnable action) {
        final Thread thread = new Thread(action, name);
        thread.setDaemon(true);
        return thread;
    }

    private static int port(final String value) {
        try {
            final int port = Integer.parseInt(value);
            if (port <= 0 || port > 65_535) {
                throw new IllegalArgumentException("Kafka OffsetCommit proxy port must be 1..65535");
            }
            return port;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("Kafka OffsetCommit proxy port must be an integer", failure);
        }
    }

    private static String join(final int[] ports) {
        return ports[0] + "," + ports[1] + "," + ports[2];
    }

    private static void writeMarker(final Path path, final String contents) throws IOException {
        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(path, contents);
    }

    private static synchronized void appendMarker(final Path path, final String contents) throws IOException {
        final Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(
                path,
                contents,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
    }

    private static void closeAll(final Iterable<? extends AutoCloseable> closeables) {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Best-effort e2e cleanup.
            }
        }
    }

    private static void close(final Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort close to trigger the client-side connection fault.
        }
    }
}
