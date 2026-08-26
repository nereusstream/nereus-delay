# Nereus Delay

Nereus Delay is a Java 21 library and service core for durable delayed-message
scheduling across Kafka and Pulsar destinations.

The implementation is built in small, testable layers:

- `protocol`: canonical identities, source positions, hashing, and
  Shard Log framing;
- `store`: one RocksDB instance per Delay Shard with the seven registered
  application column families;
- `runtime`: deterministic command application and the embedded service used by
  conformance tests;
- `scheduler`: persistent Destination Lane scheduling and bounded weighted DRR;
- `adapter`: broker-specific ingress and destination boundaries.

The normative design is [`docs/Nereus Delay 设计.md`](docs/Nereus%20Delay%20设计.md).
Major design changes are proposed and reviewed through
[`docs/proposals/`](docs/proposals/README.md), then folded directly into that
single current design baseline.
Exact wire, key, enum, and stable-code values are in
[`docs/PROTOCOL-REGISTRY.md`](docs/PROTOCOL-REGISTRY.md). The implementation
status and evidence matrix are maintained in
[`docs/IMPLEMENTATION-STATUS.md`](docs/IMPLEMENTATION-STATUS.md).
The role and authority order of the design, Registry, ADR, status, and audit
documents is summarized in [`docs/README.md`](docs/README.md).
The terminology glossary is [`CONTEXT.md`](CONTEXT.md); it explains the fixed
meaning of names such as `deliverAt` and `Source Position` without replacing the
normative design.

Run the build with an isolated Gradle cache when the host Gradle native cache is
not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
