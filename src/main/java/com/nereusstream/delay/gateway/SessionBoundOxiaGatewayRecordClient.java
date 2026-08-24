package com.nereusstream.delay.gateway;

import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;
import java.util.Objects;
import java.util.Set;

/**
 * Oxia record surface bound to the exact marker owned by one ClientHandle.
 * The marker is checked before and after every operation; a session reset is
 * therefore never converted into a guessed CAS result or a reusable permit.
 */
final class SessionBoundOxiaGatewayRecordClient implements OxiaGatewayRecordClient {
    private final OxiaSyncOwnerLeaseBackend.ClientHandle handle;

    SessionBoundOxiaGatewayRecordClient(final OxiaSyncOwnerLeaseBackend.ClientHandle handle) {
        this.handle = Objects.requireNonNull(handle, "handle");
    }

    @Override
    public GetResult get(final String key) {
        verifySession();
        try {
            final GetResult result = handle.client().get(key);
            verifySession();
            return result;
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (Exception failure) {
            throw unavailable(failure);
        }
    }

    @Override
    public PutResult put(final String key, final byte[] value, final Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException {
        verifySession();
        try {
            final PutResult result = handle.client().put(key, value, options);
            verifySession();
            return result;
        } catch (OxiaGatewaySessionUnavailableException failure) {
            throw failure;
        } catch (KeyAlreadyExistsException | UnexpectedVersionIdException expectedCasRace) {
            // Preserve normal CAS classification, but never return it after
            // the session marker has disappeared or changed.
            verifySession();
            throw expectedCasRace;
        } catch (Exception failure) {
            throw unavailable(failure);
        }
    }

    /** The ClientHandle owns the underlying session and closes it. */
    @Override
    public void close() {}

    private void verifySession() {
        try {
            handle.backend().assertConnectedSession();
        } catch (Exception failure) {
            throw new OxiaGatewaySessionUnavailableException(failure);
        }
    }

    private static OxiaGatewaySessionUnavailableException unavailable(final Throwable failure) {
        return new OxiaGatewaySessionUnavailableException(failure);
    }
}
