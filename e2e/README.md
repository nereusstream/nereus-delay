# Delay V1 isolated Oxia E2E

`run-oxia-real-service.sh` builds the locked Oxia checkout in a unique Docker
Compose project, starts one standalone Oxia shard on a temporary host port,
waits for the gRPC health service, and runs the Delay opt-in real-service
smokes against that container. The smoke set covers Oxia Owner Lease, Control,
Recovery, signed Route publication/refresh and Gateway audit. The compose
service uses only its container filesystem; it does not reuse existing
containers, ports, or volumes.

From the Delay checkout:

```text
./e2e/run-oxia-real-service.sh
```

Use `NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia` and
`NEREUS_DELAY_OXIA_E2E_PORT=<unused-port>` to override the defaults. The
result is Dockerized Oxia plus host-side Delay authority/audit smoke evidence.
It is not a complete Kafka/Pulsar broker, source-consumer, Worker scheduling,
HA or release E2E; those require the locked upstream client/broker artifacts
and separate lifecycle gates.
