package com.nereusstream.delay.route;

import io.oxia.client.api.CloseableIterable;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.Notification;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.Set;
import java.util.function.Consumer;

/** Minimal Oxia record/watch surface used by the Route authority composition. */
interface OxiaRouteRecordClient extends AutoCloseable {
    /** Opens or revalidates an optional session fence before authority I/O. */
    default void startSession() {
        // Raw clients remain a compatibility composition without a session fence.
    }

    /** Revalidates or explicitly reopens a session after a known session fence. */
    default void reconnectSession() {
        startSession();
    }

    /** Replaces a lost notification subscription and registers the supplied callback. */
    default void reconnectNotifications(Consumer<Notification> consumer) {
        // Raw clients rely on the Oxia client's own notification retry loop.
    }

    GetResult get(String key);

    CloseableIterable<GetResult> rangeScan(String startKeyInclusive, String endKeyExclusive);

    void notifications(Consumer<Notification> consumer);

    PutResult put(String key, byte[] value, Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException;

    @Override
    void close();
}
