package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.AdapterKind;

/** Closed key union for exact physical guarded transports. */
public sealed interface CommandTransportKey permits KafkaCommandTransportKey, PulsarCommandTransportKey {
    AdapterKind kind();

    CredentialBindingKey credentialBinding();
}
