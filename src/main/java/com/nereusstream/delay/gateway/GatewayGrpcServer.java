package com.nereusstream.delay.gateway;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Deployable Gateway gRPC lifecycle composition.
 *
 * <p>The Netty factory requires mutual TLS. Authentication of the presented
 * JWT and its mapping to an authenticated tenant remain an explicit
 * {@link GatewayTenantAuthority} composition; no request field is trusted for
 * tenant selection.</p>
 */
public final class GatewayGrpcServer implements AutoCloseable {
    private final Server server;

    private GatewayGrpcServer(final Server server) {
        this.server = Objects.requireNonNull(server, "server");
    }

    /** Builds a server from an already configured gRPC transport builder. */
    public static GatewayGrpcServer create(final ServerBuilder<?> serverBuilder, final GatewayGrpcService service) {
        Objects.requireNonNull(serverBuilder, "serverBuilder");
        Objects.requireNonNull(service, "service");
        final Server configured = serverBuilder
                .intercept(GatewayGrpcContext.capturePeerContext())
                .addService(service)
                .build();
        return new GatewayGrpcServer(configured);
    }

    /** Builds a Netty server with TLS and mandatory client certificate validation. */
    public static GatewayGrpcServer mutualTls(
            final int port,
            final Path certificateChain,
            final Path privateKey,
            final Path trustedClientCertificates,
            final GatewayGrpcService service)
            throws IOException {
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException("port must be 1..65535");
        }
        Objects.requireNonNull(certificateChain, "certificateChain");
        Objects.requireNonNull(privateKey, "privateKey");
        Objects.requireNonNull(trustedClientCertificates, "trustedClientCertificates");
        final io.grpc.netty.shaded.io.netty.handler.ssl.SslContext sslContext = GrpcSslContexts.forServer(
                        certificateChain.toFile(), privateKey.toFile())
                .trustManager(trustedClientCertificates.toFile())
                .clientAuth(ClientAuth.REQUIRE)
                .build();
        return create(NettyServerBuilder.forPort(port).sslContext(sslContext), service);
    }

    public void start() throws IOException {
        server.start();
    }

    public int port() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        return server.awaitTermination(timeout, unit);
    }

    public void shutdown() {
        server.shutdown();
    }

    public void shutdownNow() {
        server.shutdownNow();
    }

    @Override
    public void close() {
        server.shutdownNow();
    }
}
