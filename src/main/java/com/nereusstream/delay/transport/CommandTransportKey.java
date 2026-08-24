package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.AdapterKindV1;

/** Closed key union for exact physical guarded transports. */
public sealed interface CommandTransportKey permits KafkaCommandTransportKey, PulsarCommandTransportKey {
    AdapterKindV1 kind();

    CredentialBindingKey credentialBinding();
}
