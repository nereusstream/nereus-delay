# Nereus Delay

Nereus Delay is a Java 21 library and service core for durable delayed-message
scheduling across Kafka and Pulsar destinations.

The V1 implementation is built in small, testable layers:

- `protocol`: versioned identities, source positions, canonical hashing, and
  Shard Log framing;
- `store`: one RocksDB instance per Delay Shard with the seven registered
  application column families;
- `runtime`: deterministic command application and the embedded service used by
  conformance tests;
- `scheduler`: persistent Destination Lane scheduling and bounded weighted DRR;
- `adapter`: broker-specific ingress and destination boundaries.

The normative design is [`docs/Nereus Delay V1 设计.md`](docs/Nereus%20Delay%20V1%20设计.md).
Exact wire, key, enum, and stable-code values are in
[`docs/V1-PROTOCOL-REGISTRY.md`](docs/V1-PROTOCOL-REGISTRY.md). The implementation
status and evidence matrix are maintained in
[`docs/IMPLEMENTATION-STATUS.md`](docs/IMPLEMENTATION-STATUS.md).

Run the build with an isolated Gradle cache when the host Gradle native cache is
not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle gradle clean check
```

