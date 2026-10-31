package com.nereusstream.delay.ownership;

import com.nereusstream.delay.route.OxiaRouteAuthoritySession;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.SharedRocksDbResources;
import io.oxia.client.api.Authentication;
import io.oxia.client.api.OxiaClientBuilder;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.text.Normalizer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;

/** Owns the actual Owner/Route/watch allocations and transfers their lifetime to the exact admitted Host. */
public final class TargetWorkerOxiaRuntime {
    public record Connection(
            String serviceAddress,
            String namespace,
            String clientIdentifier,
            String ownerPrefix,
            String routeSessionPrefix,
            Duration sessionTimeout,
            Duration requestTimeout,
            Duration startupTimeout,
            Duration cleanupBudget,
            long nativeBytesPerClient) {
        public Connection {
            for (var value : List.of(serviceAddress, namespace, clientIdentifier, ownerPrefix, routeSessionPrefix)) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                if (value.isBlank()
                        || bytes.length > 768
                        || value.indexOf('\0') >= 0
                        || !value.equals(new String(bytes, StandardCharsets.UTF_8))
                        || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
                    throw new IllegalArgumentException("Oxia connection text is not bounded canonical UTF-8");
                }
            }
            if (ownerPrefix.endsWith("/")
                    || routeSessionPrefix.endsWith("/")
                    || overlaps(ownerPrefix, routeSessionPrefix)) {
                throw new IllegalArgumentException(
                        "Oxia Owner and Route session prefixes overlap or have trailing slash");
            }
            if (sessionTimeout == null
                    || sessionTimeout.toMillis() < 1
                    || sessionTimeout.toMillis() > Integer.MAX_VALUE
                    || requestTimeout == null
                    || requestTimeout.toMillis() < 1
                    || requestTimeout.toMillis() > 60_000
                    || startupTimeout == null
                    || startupTimeout.isZero()
                    || startupTimeout.isNegative()
                    || startupTimeout.toNanos() == Long.MAX_VALUE
                    || cleanupBudget == null
                    || cleanupBudget.isZero()
                    || cleanupBudget.isNegative()
                    || cleanupBudget.compareTo(Duration.ofMinutes(1)) > 0
                    || nativeBytesPerClient < 1
                    || nativeBytesPerClient == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Oxia connection requires finite native/time bounds");
            }
            Math.multiplyExact(3L, nativeBytesPerClient);
        }
    }

    public record Registries(
            TargetWorkerOxiaProviders.Prefixes prefixes,
            PublicKey originalRouteKey,
            CredentialAttestationTrustSet credentialTrust,
            long maximumCredentialLeaseMs,
            long maximumAttestationAgeMs,
            int maximumTrustSetBytes,
            com.nereusstream.delay.route.OxiaSignedRouteSnapshotProvider.RefreshLimits routeRefresh) {
        public Registries {
            Objects.requireNonNull(prefixes);
            Objects.requireNonNull(originalRouteKey);
            Objects.requireNonNull(credentialTrust);
            Objects.requireNonNull(routeRefresh);
            if (maximumCredentialLeaseMs < 1
                    || maximumCredentialLeaseMs == Long.MAX_VALUE
                    || maximumAttestationAgeMs < 1
                    || maximumAttestationAgeMs == Long.MAX_VALUE
                    || maximumTrustSetBytes < 1
                    || maximumTrustSetBytes == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Oxia registries require finite value bounds");
            }
        }
    }

    private record Assembly(
            OxiaSyncOwnerLeaseBackend.ClientHandle owner,
            OxiaRouteAuthoritySession route,
            TargetWorkerOxiaProviders providers) {}

    private final Connection connection;
    private final Registries registries;
    private final ThreadPoolExecutor executor;
    private final SharedRocksDbResources resources;
    private final WorkClassExecutionRegistry classes;
    private final TrustedClock clock;
    private final LongSupplier nanos;
    private final Runnable startupGuard;
    private final List<TargetWorkerOxiaClientOwner> clients = new ArrayList<>(3);
    private final long started;
    private OwnedCheckpointIo<Assembly> assemblyIo;
    private Assembly assembly;
    private TargetWorkerOxiaProviders partialProviders;
    private TargetWorkerHostRuntime host;
    private boolean stopping;
    private Throwable firstFailure;

    /** No remote connection or private material is read by the caller; SDK opening/markers use the existing finite pool. */
    public static TargetWorkerOxiaRuntime open(
            Connection connection,
            Registries registries,
            Authentication authentication,
            TargetWorkerRuntimeConfig config,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ThreadPoolExecutor executor,
            TrustedClock clock,
            LongSupplier nanos,
            Runnable startupGuard) {
        Objects.requireNonNull(config).requireRuntime(classes, resources);
        var root = new TargetWorkerOxiaRuntime(
                connection, registries, executor, resources, classes, clock, nanos, startupGuard);
        try {
            for (var suffix : List.of("owner", "route", "watch")) {
                var builder = OxiaClientBuilder.create(connection.serviceAddress())
                        .namespace(connection.namespace())
                        .clientIdentifier(connection.clientIdentifier() + "-" + suffix)
                        .sessionTimeout(connection.sessionTimeout())
                        .requestTimeout(connection.requestTimeout());
                if (authentication != null) {
                    builder.authentication(authentication);
                }
                var client = TargetWorkerOxiaClientOwner.open(
                        builder,
                        executor,
                        resources,
                        connection.clientIdentifier() + "/" + suffix,
                        connection.nativeBytesPerClient(),
                        connection.cleanupBudget(),
                        root::requireStartup);
                client.bindRuntime(root);
                root.clients.add(client);
            }
            return root;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerOxiaClientOwner.OpenFailure partial) {
                partial.owner().bindRuntime(root);
                root.clients.add(partial.owner());
            }
            root.retain(failure);
            root.stopUnattached();
            throw new StartupFailure(failure, root);
        }
    }

    private TargetWorkerOxiaRuntime(
            Connection connection,
            Registries registries,
            ThreadPoolExecutor executor,
            SharedRocksDbResources resources,
            WorkClassExecutionRegistry classes,
            TrustedClock clock,
            LongSupplier nanos,
            Runnable guard) {
        this.connection = Objects.requireNonNull(connection);
        this.registries = Objects.requireNonNull(registries);
        this.executor = Objects.requireNonNull(executor);
        this.resources = Objects.requireNonNull(resources);
        this.classes = Objects.requireNonNull(classes);
        this.clock = Objects.requireNonNull(clock);
        this.nanos = Objects.requireNonNull(nanos);
        this.startupGuard = Objects.requireNonNull(guard);
        for (var prefix : List.of(
                registries.prefixes().routes(),
                registries.prefixes().assignments(),
                registries.prefixes().capabilities(),
                registries.prefixes().operations(),
                registries.prefixes().registrations(),
                registries.prefixes().credentials(),
                registries.prefixes().sourceProfiles(),
                registries.prefixes().retries(),
                registries.prefixes().payloadTrusts())) {
            if (overlaps(prefix, connection.ownerPrefix()) || overlaps(prefix, connection.routeSessionPrefix())) {
                throw new IllegalArgumentException("Oxia registry overlaps original session namespace");
            }
        }
        started = nanos.getAsLong();
    }

    private static boolean overlaps(String left, String right) {
        return left.equals(right) || left.startsWith(right + '/') || right.startsWith(left + '/');
    }

    private void requireStartup() {
        synchronized (this) {
            long elapsed = nanos.getAsLong() - started;
            if (stopping
                    || elapsed < 0
                    || elapsed >= connection.startupTimeout().toNanos()) {
                throw new IllegalStateException("Oxia root startup is stopped or past its deadline");
            }
        }
        startupGuard.run();
    }

    /** One explicit non-waiting startup turn; never starts markers on an SDK callback thread. */
    public synchronized boolean advanceStartup() {
        if (assembly != null && !stopping) {
            return true;
        }
        try {
            requireStartup();
            for (var client : clients) {
                if (client.clientReady().isCompletedExceptionally()) {
                    client.clientReady().join();
                }
                if (!client.readyForUse()) {
                    return false;
                }
            }
            if (assemblyIo == null) {
                assemblyIo = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            var owner =
                                    OxiaSyncOwnerLeaseBackend.fromOwnedClient(clients.get(0), connection.ownerPrefix());
                            guard.run();
                            var route = OxiaRouteAuthoritySession.fromOwnedClients(
                                    clients.get(1), clients.get(2), connection.routeSessionPrefix());
                            route.startSession();
                            guard.run();
                            var providers = TargetWorkerOxiaProviders.fromSessions(
                                    owner,
                                    route,
                                    registries.prefixes(),
                                    registries.originalRouteKey(),
                                    clock,
                                    registries.credentialTrust(),
                                    registries.maximumCredentialLeaseMs(),
                                    registries.maximumAttestationAgeMs(),
                                    registries.maximumTrustSetBytes(),
                                    executor,
                                    registries.routeRefresh(),
                                    nanos);
                            synchronized (this) {
                                partialProviders = providers;
                            }
                            providers.routes().start().toCompletableFuture().join();
                            guard.run();
                            return new Assembly(owner, route, providers);
                        },
                        this::requireStartup,
                        executor);
            }
            if (!assemblyIo.receipt().isDone()) {
                return false;
            }
            if (assemblyIo.receipt().isCompletedExceptionally()) {
                assemblyIo.receipt().join();
            }
            if (!assemblyIo.ioExited()) {
                return false;
            }
            assembly = assemblyIo.completedValue().orElseThrow();
            return true;
        } catch (RuntimeException | Error failure) {
            retain(failure);
            stopUnattached();
            throw new StartupFailure(failure, this);
        }
    }

    public synchronized TargetWorkerOxiaProviders providers() {
        if (assembly == null || stopping) {
            throw new IllegalStateException("original Oxia providers are not ready");
        }
        return assembly.providers();
    }

    /** Root-local concrete content collector; actual Owner/Store guards are added by the Source runtime at bind. */
    public synchronized TargetSourceAuthorityPreparation newSourcePreparation(
            TargetSourceAuthorityLoader.Provider content,
            com.nereusstream.delay.store.TargetStoreBackend.CommitAuthority physical,
            Duration timeout,
            long maximumBytes) {
        providers();
        var loader = new TargetSourceAuthorityLoader(content, physical, maximumBytes);
        return new TargetSourceAuthorityPreparation(loader, executor, nanos, timeout, maximumBytes);
    }

    void requireWorker(TargetWorkerShardRuntime worker) {
        worker.requireOxiaAuthority(providers().leases(), classes, resources);
    }

    synchronized void bindHost(TargetWorkerHostRuntime actual) {
        if (stopping || assembly == null || host != null && host != actual) {
            throw new IllegalStateException("Oxia root is not ready for this original Host");
        }
        host = Objects.requireNonNull(actual);
    }

    synchronized void requireClientCloseAllowed() {
        if (host != null && !stopping) {
            throw new IllegalStateException("shared Oxia authority still serves the original Host dependencies");
        }
    }

    /** No admitted Host exists yet; caller retains all application assembly tasks before invoking this. */
    public void stopUnattached() {
        synchronized (this) {
            if (host != null) {
                throw new IllegalStateException("attached Oxia authority requires original Host closure permit");
            }
            stopping = true;
            if (assemblyIo != null) {
                assemblyIo.requestStop();
            }
        }
        retryCleanup();
    }

    /** Only the actual Host can authorize closing its shared authority clients after its dependencies exit. */
    void closeForHost(TargetWorkerHostRuntime.OxiaClosePermit permit) {
        Objects.requireNonNull(permit).requireOriginalRoot(this);
        synchronized (this) {
            stopping = true;
        }
        retryCleanup();
    }

    public void retryCleanup() {
        final List<TargetWorkerOxiaClientOwner> original;
        synchronized (this) {
            if (!stopping || assemblyIo != null && !assemblyIo.ioExited()) {
                return;
            }
            if (partialProviders != null) {
                partialProviders.routes().stopNotificationIo();
                if (!partialProviders.routes().notificationIoExited()) {
                    return;
                }
            }
            original = List.copyOf(clients);
        }
        for (var client : original) {
            client.requestClose();
            client.retryCleanup();
        }
    }

    public boolean cleanupComplete() {
        final List<TargetWorkerOxiaClientOwner> original;
        synchronized (this) {
            if (!stopping
                    || assemblyIo != null && !assemblyIo.ioExited()
                    || partialProviders != null && !partialProviders.routes().notificationIoExited()) {
                return false;
            }
            original = List.copyOf(clients);
        }
        return original.stream().allMatch(TargetWorkerOxiaClientOwner::cleanupComplete);
    }

    public Optional<Throwable> firstFailure() {
        final List<TargetWorkerOxiaClientOwner> original;
        synchronized (this) {
            original = List.copyOf(clients);
        }
        for (var client : original) {
            client.firstFailure().ifPresent(this::retain);
        }
        synchronized (this) {
            return Optional.ofNullable(firstFailure);
        }
    }

    private synchronized void retain(Throwable failure) {
        if (firstFailure == null) {
            firstFailure = failure;
        } else if (firstFailure != failure) {
            for (Throwable prior : firstFailure.getSuppressed()) {
                if (prior == failure) {
                    return;
                }
            }
            firstFailure.addSuppressed(failure);
        }
    }

    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerOxiaRuntime root;

        private StartupFailure(Throwable cause, TargetWorkerOxiaRuntime root) {
            super("original Oxia root startup failed and is retained", cause);
            this.root = root;
        }

        public TargetWorkerOxiaRuntime root() {
            return root;
        }
    }
}
