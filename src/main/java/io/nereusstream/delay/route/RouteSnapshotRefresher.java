package io.nereusstream.delay.route;

import java.util.concurrent.CompletionStage;

/** Background Oxia/watch composition boundary; preparation never calls start or refresh. */
public interface RouteSnapshotRefresher extends AutoCloseable {
    CompletionStage<Void> start();

    RouteCacheHealth health();

    @Override
    void close();
}
