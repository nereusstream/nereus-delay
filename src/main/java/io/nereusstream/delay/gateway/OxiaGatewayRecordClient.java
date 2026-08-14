package io.nereusstream.delay.gateway;

import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.PutOption;

import java.util.Set;

/** Minimal Oxia record CAS surface used by the Gateway idempotency store. */
interface OxiaGatewayRecordClient extends AutoCloseable {
    GetResult get(String key);

    PutResult put(String key, byte[] value, Set<PutOption> options)
            throws UnexpectedVersionIdException, KeyAlreadyExistsException;

    @Override
    void close();
}
