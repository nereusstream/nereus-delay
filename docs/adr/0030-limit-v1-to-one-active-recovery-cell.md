# Limit V1 to one active recovery cell

Nereus Delay V1 has one active recovery cell for each Route Incarnation: one authoritative Command Topic identity, one Oxia namespace for ownership/catalog/control, one checkpoint/payload object-store authority, and the Workers that can acquire its shards. Destination clusters may be remote, but active-active scheduling of the same route across independent cells and automatic failover to a differently positioned replica log are out of scope.

## Covered failures

With the authoritative services intact, V1 covers Worker crash or pause, process restart, local disk or host loss, planned rebalance, destination outage/throttling, temporary Object Store or Oxia loss, individual Command Broker node failover within its certified durability policy, corrupt newest checkpoint with valid fallback, and lost responses at all documented CAS/send/upload boundaries.

Acknowledged Commands have zero service RPO across Worker/local-disk loss while the required Command log and a permitted checkpoint remain available. Publish-outcome mutations after a checkpoint can reopen the documented duplicate window but do not justify message loss or invented success. RTO is the bounded placement, lease expiry/acquisition, restore, replay, capability-resolution, and activation time measured against the deployment objective.

## Authority loss

A checkpoint is not a standalone current backup: it needs Command records after its applied Source Position, destination evidence logs where selected, Oxia catalog/floor identity, and referenced payload objects. If source identity changes, retained records have a gap, the Oxia catalog is lost or rolled back, or protected objects are unavailable/corrupt across every Recovery Set member, the shard fails closed.

Kafka MirrorMaker offsets and Pulsar geo-replicated message IDs are not assumed to preserve Source Position or source-time-fence identity. Automatic promotion to such a topic would require a separately designed route-incarnation migration with a proven cut, complete command mapping, ownership fencing, checkpoint/catalog transfer, payload/evidence availability, and explicit ordering/idempotency boundary.

## Deployment requirements

The active cell deploys Command Brokers, Oxia, and object storage with independent failure domains and their own tested backup/restore procedures. Object versioning/immutability, checksums, least-privilege credentials, and catalog backups protect against accidental overwrite; restoring a control-plane backup still requires consistency validation against the Recovery Set and source log.

An operator may choose a documented disaster override that accepts data loss or duplicates, but it creates a new Route Incarnation and visible audit boundary. It is not V1 normal recovery and cannot reuse old receipts as though continuity were proven.
