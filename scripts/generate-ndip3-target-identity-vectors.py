#!/usr/bin/env python3
"""Check NDIP-3 vectors using independent standard-library wire encoders.

Pass --write only when intentionally publishing changed protocol vectors.
"""

import argparse
import hashlib
from pathlib import Path
import struct
import uuid


def u32(number):
    return struct.pack(">I", number)


def u64(number):
    return struct.pack(">Q", number)


def varint(number):
    encoded = bytearray()
    while number > 127:
        encoded.append((number & 127) | 128)
        number >>= 7
    encoded.append(number)
    return bytes(encoded)


def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def uint_field(number, value):
    return varint(number << 3) + varint(value)


def crc32c(value):
    crc = 0xFFFFFFFF
    for byte in value:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def expected_vectors():
    native_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes
    kafka = bytes_field(1, bytes_field(1, b"cluster-a") + bytes_field(2, native_uuid))
    pulsar = bytes_field(
        2,
        bytes_field(1, b"cluster-p")
        + bytes_field(2, bytes(range(32)))
        + bytes_field(3, b"persistent://tenant/ns/topic-partition-5")
        + uint_field(4, 1700000000000),
    )
    vectors = {}
    for label, resource, partition in (("kafka", kafka, 3), ("pulsar", pulsar, 5)):
        raw = b"\x01" + u32(len(resource)) + resource + u32(partition)
        vectors[label + ".canonical"] = raw.hex()
        vectors[label + ".id"] = hashlib.sha256(b"nereus-delay-target-partition\0" + raw).hexdigest()

    prefix = (
        b"\x01"
        + uuid.UUID("11111111-2222-4333-8444-555555555555").bytes
        + u32(3)
        + uuid.UUID("018e0000-0000-7000-8000-000000000001").bytes
    )
    message = prefix + u32(crc32c(prefix))
    vectors["message.id"] = message.hex()
    target = bytes.fromhex(vectors["kafka.id"])
    tokens = (
        ("due.kafka", 8, b"\x01" + u64(7)),
        ("native.pulsar", 9, b"\x02" + u64(7) + u64(11) + u32(2)),
    )
    for label, tag, token in tokens:
        vectors[label + ".key"] = (
            bytes([tag, 1]) + target + b"\x00\x00" + u64(1)
            + u64(100) + token + message + u32(2)
        ).hex()
    vectors["expiry.key"] = (bytes([10, 1]) + u64(200) + target + message + u32(2)).hex()
    vectors["state.key"] = (bytes([9, 1]) + target).hex()
    append_state_vectors(vectors, target, message)
    append_work_vectors(vectors, message)
    append_message_vectors(vectors, message)
    append_order_vectors(vectors, message)
    header = "# NDIP-3 version 1; independent Python hashlib/struct/protobuf wire/CRC32C generation.\n"
    return header + "".join(key + "=" + value + "\n" for key, value in vectors.items())


def with_digest(fields, field_number, domain):
    return fields + bytes_field(field_number, hashlib.sha256(domain + fields).digest())


def head_ref(key, message, generation, time):
    fields = bytes_field(1, key) + bytes_field(2, message) + uint_field(3, generation) + uint_field(4, time)
    return with_digest(fields, 5, b"nereus-delay-target-head\0")


def domain_state(slot, generation, lifecycle, ordinary=None, native=None, native_scope=True):
    fields = uint_field(1, slot) + uint_field(2, generation) + uint_field(3, lifecycle)
    if lifecycle != 3:
        fields += bytes_field(4, bytes([0xAA]) * 32) + bytes_field(5, bytes([0xBB]) * 32)
        if native_scope:
            fields += bytes_field(6, bytes([0xCC]) * 32)
    if ordinary is not None:
        fields += bytes_field(7, ordinary)
    if native is not None:
        fields += bytes_field(8, native)
    return with_digest(fields, 9, b"nereus-delay-target-execution-domain\0")


def queue_state(target, revision, control, admission, domains, maximum=False):
    fields = uint_field(1, 1) + bytes_field(2, target) + uint_field(3, revision) + uint_field(4, control)
    fields += uint_field(5, admission) + bytes_field(6, bytes(range(1, 17)))
    fields += uint_field(7, (1 << 63) - 1 if maximum else 60000)
    fields += b"".join(bytes_field(8, domain) for domain in domains)
    return with_digest(fields, 9, b"nereus-delay-target-queue-state\0")


def value_envelope(value_type, payload):
    prefix = b"NV" + bytes([value_type, 1]) + u32(len(payload)) + payload
    return prefix + u32(crc32c(prefix))


def append_state_vectors(vectors, target, message):
    target = bytes.fromhex(vectors["pulsar.id"])
    token = b"\x01" + u64(7)
    due_key = bytes([8, 1]) + target + b"\0\0" + u64(1) + u64(100) + token + message + u32(2)
    vectors["queue.due.key"] = due_key.hex()
    native_key = bytes([9, 1]) + target + b"\0\0" + u64(1) + u64(100) + token + message + u32(2)
    vectors["native.kafka.key"] = native_key.hex()
    ordering = bytes([0x11]) * 32
    vectors["ordered.key"] = (bytes([11, 1]) + target + ordering + u64(90) + token + message + u32(2)).hex()
    order_head = bytes([12, 1]) + target + b"\0\0" + u64(1) + u64(100) + ordering
    vectors["order.head.key"] = order_head.hex()
    vectors["order.state.key"] = (bytes([10, 1]) + target + ordering).hex()
    vectors["identity.key"] = (bytes([11, 1]) + target).hex()
    vectors["identity.value"] = value_envelope(13, bytes.fromhex(vectors["pulsar.canonical"])).hex()
    ordinary = head_ref(due_key, message, 2, 100)
    native = head_ref(native_key, message, 2, 100)
    ordered = head_ref(order_head, message, 2, 100)
    vectors["head.ordinary"] = ordinary.hex()
    vectors["head.native"] = native.hex()
    vectors["head.ordered"] = ordered.hex()
    domains = {
        "active": domain_state(0, 1, 1, ordinary, native),
        "draining": domain_state(0, 1, 2, native_scope=True),
        "vacant": domain_state(0, 1, 3),
        "ordered": domain_state(0, 1, 1, ordered, native_scope=False),
    }
    for name, domain in domains.items():
        vectors["domain." + name] = domain.hex()
    for name, revision, control, gate, records in (
        ("empty", 1, 1, 1, []),
        ("active", 2, 1, 1, [domains["active"]]),
        ("draining", 3, 1, 1, [domains["draining"]]),
        ("vacant", 4, 1, 1, [domains["vacant"]]),
        ("ordered", 2, 2, 2, [domains["ordered"]]),
    ):
        raw = queue_state(target, revision, control, gate, records)
        vectors["queue." + name] = raw.hex()
        vectors["queue." + name + ".sha256"] = hashlib.sha256(raw).hexdigest()
    vectors["queue.active.value"] = value_envelope(12, bytes.fromhex(vectors["queue.active"])).hex()
    maximum_domains = []
    for slot in range(64):
        keys = []
        for tag in (8, 9):
            key = bytes([tag, 1]) + target + struct.pack(">H", slot) + u64((1 << 64) - 1) + u64((1 << 63) - 1)
            key += b"\x02" + u64((1 << 64) - 1) + u64((1 << 64) - 1) + u32((1 << 32) - 1)
            key += message + u32((1 << 32) - 1)
            keys.append(head_ref(key, message, (1 << 32) - 1, (1 << 63) - 1))
        maximum_domains.append(domain_state(slot, (1 << 64) - 1, 1, keys[0], keys[1]))
    maximum = queue_state(target, (1 << 64) - 1, (1 << 64) - 1, 2, maximum_domains, maximum=True)
    vectors["queue.maximum.sha256"] = hashlib.sha256(maximum).hexdigest()
    vectors["queue.maximum.length"] = str(len(maximum))


def locator(target, message, generation=2, slot=0, domain_generation=1, ordered=False):
    fields = uint_field(1, 1) + bytes_field(2, message) + uint_field(3, generation) + bytes_field(4, target)
    fields += uint_field(5, slot) + uint_field(6, domain_generation) + bytes_field(7, bytes(range(1, 17)))
    fields += uint_field(8, 2 if ordered else 1)
    if ordered:
        fields += bytes_field(9, bytes([0x11]) * 32)
    fields += bytes_field(10, bytes([0xDD]) * 32)
    return with_digest(fields, 11, b"nereus-delay-target-message-locator\0")


def work_ref(loc, kind, deliver, retry, token, attempt, revision, authority=1, control=None, position=None, native=False):
    prefix = uint_field(1, 1) + bytes_field(2, loc) + uint_field(3, kind)
    prefix += uint_field(4, deliver) + uint_field(5, retry) + bytes_field(6, token) + uint_field(7, attempt)
    suffix = uint_field(9, authority)
    if control is not None:
        suffix += bytes_field(10, control) + bytes_field(11, position)
    suffix += uint_field(12, 1 if native else 0)
    semantic = hashlib.sha256(b"nereus-delay-target-work-semantic\0" + prefix + suffix).digest()
    instance_fields = prefix + uint_field(8, revision) + suffix + bytes_field(13, semantic)
    return with_digest(instance_fields, 14, b"nereus-delay-target-work-instance\0")


def kafka_source(route, offset):
    cluster = b"source-cluster"
    return b"\x01" + route + u32(len(cluster)) + cluster + uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes + u32(3) + u64(offset) + b"\x01" + u32(4) + u64(90 if offset == 7 else 110)


def append_work_vectors(vectors, message):
    target = bytes.fromhex(vectors["pulsar.id"])
    token = b"\x01" + u64(7)
    best = locator(target, message)
    fifo = locator(target, message, ordered=True)
    vectors["locator.best"] = best.hex()
    vectors["locator.fifo"] = fifo.hex()
    route = message[1:17]
    schedule = kafka_source(route, 7)
    control_source = kafka_source(route, 9)
    control = bytes_field(1, bytes([0x44]) * 32) + bytes_field(2, bytes([0x55]) * 32) + uint_field(3, 3)
    vectors["work.schedule.source"] = schedule.hex()
    vectors["work.control.source"] = control_source.hex()
    vectors["work.control.ref"] = control.hex()
    records = {
        "initial": work_ref(best, 1, 100, 100, token, 1, 5),
        "native": work_ref(best, 1, 100, 100, token, 1, 5, native=True),
        "definitive": work_ref(best, 2, 100, 150, token, 2, 6),
        "uncertain.pinned": work_ref(best, 3, 100, 150, token, 2, 7, authority=2),
        "uncertain.control": work_ref(best, 3, 100, 150, token, 2, 8, authority=3, control=control, position=control_source),
        "fifo.initial": work_ref(fifo, 1, 90, 90, token, 1, 5),
        "fifo.retry": work_ref(fifo, 2, 90, 150, token, 2, 6),
    }
    for name, raw in records.items():
        vectors["work." + name] = raw.hex()
    vectors["work.native.value"] = value_envelope(14, records["native"]).hex()
    topic = b"x" * (1 << 20)
    max64 = (1 << 64) - 1
    max32 = (1 << 32) - 1
    source = b"\x02" + route + u32(32) + bytes(range(32)) + u32(len(topic)) + topic + u32(3)
    source += u64(max64) + u64(max64) + u32(max32 - 1) + u32(max32) + b"\x02" + u64((1 << 63) - 1)
    loc = locator(target, message, max32, 63, max64)
    max_control = bytes_field(1, bytes([0x44]) * 32) + bytes_field(2, bytes([0x55]) * 32) + uint_field(3, max32)
    max_token = b"\x02" + u64(max64) + u64(max64 - 1) + u32(max32 - 1)
    raw = work_ref(loc, 3, (1 << 63) - 1, (1 << 63) - 1, max_token, (1 << 31) - 1, max64,
                   authority=3, control=max_control, position=source)
    vectors["work.maximum.sha256"] = hashlib.sha256(raw).hexdigest()
    vectors["work.maximum.length"] = str(len(raw))
    vectors["work.maximum.source.sha256"] = hashlib.sha256(source).hexdigest()
    vectors["work.maximum.source.length"] = str(len(source))


def obligation(attempt, generation, state, owner=7):
    key = bytes([2 if state == 1 else 3, 1]) + u64(owner) + u32(32) + attempt
    fields = bytes_field(1, attempt) + uint_field(2, generation) + uint_field(3, state)
    fields += bytes_field(4, key) + bytes_field(5, hashlib.sha256(key).digest())
    return with_digest(fields, 6, b"nereus-delay-attempt-obligation-ref\0")


def target_runtime(generation, aggregate, current, branch, refs, admissions, uncertain, duplicate, revision):
    fields = uint_field(1, 1) + uint_field(2, generation) + uint_field(3, aggregate) + uint_field(4, current)
    if branch is not None:
        fields += bytes_field({2: 5, 3: 6, 4: 7}[current], branch)
    fields += b"".join(bytes_field(8, ref) for ref in refs)
    fields += uint_field(9, admissions) + uint_field(10, uncertain) + uint_field(11, 1 if duplicate else 0) + uint_field(12, revision)
    return with_digest(fields, 13, b"nereus-delay-target-generation-runtime\0")


def target_message(loc, revision, deliver, expiry, retry, native_policy, source, payload, runtime, object_backed=False):
    fields = uint_field(1, 1) + bytes_field(2, loc) + uint_field(3, revision)
    fields += uint_field(4, deliver) + uint_field(5, expiry) + uint_field(6, retry) + uint_field(7, native_policy)
    fields += bytes_field(8, source) + bytes_field(10 if object_backed else 9, payload) + bytes_field(11, runtime)
    return with_digest(fields, 12, b"nereus-delay-target-message\0")


def maximum_message_components(vectors, message):
    max64, max32 = (1 << 64) - 1, (1 << 32) - 1
    target = bytes.fromhex(vectors["pulsar.id"])
    topic = b"x" * (1 << 20)
    prefix = b"\x02" + message[1:17] + u32(32) + bytes(range(32)) + u32(len(topic)) + topic + u32(3)
    control_source = prefix + u64(max64) + u64(max64) + u32(max32 - 1) + u32(max32) + b"\x02" + u64((1 << 63) - 1)
    source = prefix + u64(max64) + u64(max64 - 1) + u32(max32 - 1) + u32(max32) + b"\x02" + u64((1 << 63) - 1)
    loc = locator(target, message, max32, 63, max64)
    token = b"\x02" + u64(max64) + u64(max64 - 1) + u32(max32 - 1)
    control = bytes_field(1, bytes([0x44]) * 32) + bytes_field(2, bytes([0x55]) * 32) + uint_field(3, max32)
    work = work_ref(loc, 3, (1 << 63) - 1, (1 << 63) - 1, token, (1 << 31) - 1, max64,
                    authority=3, control=control, position=control_source)
    refs = [obligation(number.to_bytes(32, "big"), max32, 2, (1 << 63) - 1) for number in range(1, 1025)]
    runtime = target_runtime(max32, 5, 2, work, refs, (1 << 31) - 2, (1 << 31) - 3, True, max64)
    raw = target_message(loc, max64, (1 << 63) - 1, (1 << 63) - 1, (1 << 63) - 1, 1, source, bytes(1 << 24), runtime)
    return runtime, raw


def append_message_vectors(vectors, message):
    initial = bytes.fromhex(vectors["work.native"])
    loc = bytes.fromhex(vectors["locator.best"])
    source = bytes.fromhex(vectors["work.schedule.source"])
    publishing = obligation(bytes([0x77]) * 32, 2, 1)
    uncertain = obligation(bytes([0x77]) * 32, 2, 2)
    vectors["obligation.publishing"] = publishing.hex()
    vectors["obligation.uncertain"] = uncertain.hex()
    runtimes = {
        "initial": target_runtime(2, 1, 2, initial, [], 0, 0, False, 5),
        "claimed": target_runtime(2, 2, 3, bytes([0x66]) * 32, [], 0, 0, False, 6),
        "publishing": target_runtime(2, 3, 4, bytes([0x77]) * 32, [publishing], 1, 0, False, 7),
        "hold": target_runtime(2, 5, 1, None, [uncertain], 1, 0, False, 8),
        "retry": target_runtime(2, 5, 2, bytes.fromhex(vectors["work.uncertain.pinned"]), [uncertain], 1, 0, True, 7),
        "terminal": target_runtime(2, 9, 1, None, [uncertain], 1, 0, True, 9),
    }
    for name, runtime in runtimes.items():
        vectors["runtime." + name] = runtime.hex()
    descriptor = u32(2) + bytes([0xAA]) * 32
    for value in [b"bucket", b"object", b"generation-1", b""]:
        descriptor += u32(len(value)) + value
    descriptor += u64(12) + bytes([0x33]) * 32 + bytes([0x22]) * 32 + bytes([0x44]) * 32
    vectors["message.object.ref"] = descriptor.hex()
    vectors["message.key"] = (b"\x05\x01" + message).hex()
    for name, revision in [("initial", 5), ("claimed", 6), ("terminal", 9)]:
        vectors["message." + name] = target_message(loc, revision, 100, 200, 100, 2, source, b"payload", runtimes[name]).hex()
    vectors["message.object"] = target_message(loc, 5, 100, 200, 100, 2, source, descriptor, runtimes["initial"], object_backed=True).hex()
    vectors["message.initial.value"] = value_envelope(15, bytes.fromhex(vectors["message.initial"])).hex()
    expiry = with_digest(uint_field(1, 1) + bytes_field(2, loc) + uint_field(3, 200), 4, b"nereus-delay-target-expiry\0")
    vectors["message.expiry"] = expiry.hex()
    vectors["message.expiry.key"] = (b"\x0a\x01" + u64(200) + bytes.fromhex(vectors["pulsar.id"]) + message + u32(2)).hex()
    vectors["message.expiry.value"] = value_envelope(16, expiry).hex()
    maximum_runtime, maximum_message = maximum_message_components(vectors, message)
    for name, raw in [("runtime.maximum", maximum_runtime), ("message.maximum", maximum_message)]:
        vectors[name + ".sha256"] = hashlib.sha256(raw).hexdigest()
        vectors[name + ".length"] = str(len(raw))


def order_barrier(loc, order, revision, runtime_digest):
    fields = bytes_field(1, loc) + bytes_field(2, order) + uint_field(3, revision) + bytes_field(4, runtime_digest)
    return with_digest(fields, 5, b"nereus-delay-target-order-barrier\0")


def order_state(target, shard, contract=1, revision=1, control=1, gate=1,
                watermark=None, head=None, barrier=None, slot=0, domain_generation=1):
    fields = uint_field(1, 1) + bytes_field(2, target) + bytes_field(3, bytes([0x11]) * 32) + bytes_field(4, shard)
    fields += uint_field(5, slot) + uint_field(6, domain_generation) + bytes_field(7, bytes(range(1, 17)))
    fields += uint_field(8, contract) + uint_field(9, revision) + uint_field(10, control) + uint_field(11, gate)
    if watermark is not None:
        fields += bytes_field(12, watermark)
    if head is not None:
        fields += bytes_field(13, head)
    if barrier is not None:
        fields += bytes_field(14, barrier)
    return with_digest(fields, 15, b"nereus-delay-target-order-state\0")


def append_order_vectors(vectors, message):
    target = bytes.fromhex(vectors["pulsar.id"])
    domain = bytes([0x11]) * 32
    shard = message[1:21]
    loc = bytes.fromhex(vectors["locator.fifo"])
    source = bytes.fromhex(vectors["work.schedule.source"])
    key = b"\x0b\x01" + target + domain + u64(90) + b"\x01" + u64(7) + message + u32(2)
    head_key = b"\x0c\x01" + target + b"\x00\x00" + u64(1) + u64(90) + domain
    head = head_ref(head_key, message, 2, 90)
    vectors["order.serviceable.key"] = head_key.hex()
    vectors["order.key"] = key.hex()
    vectors["order.empty"] = order_state(target, shard).hex()
    vectors["order.head"] = order_state(target, shard, revision=2, head=head).hex()
    initial = target_runtime(2, 1, 2, bytes.fromhex(vectors["work.fifo.initial"]), [], 0, 0, False, 5)
    vectors["order.message.initial"] = target_message(loc, 5, 90, 200, 90, 1, source, b"payload", initial).hex()
    for name, revision in [("claimed", 6), ("publishing", 7), ("hold", 8), ("terminal", 9)]:
        runtime = bytes.fromhex(vectors["runtime." + name])
        # The runtime digest is its final canonical field; no Java codec is consulted.
        barrier = order_barrier(loc, key, revision, runtime[-32:])
        vectors["order.barrier." + name] = barrier.hex()
        vectors["order.message." + name] = target_message(loc, revision, 90, 200, 90, 1, source, b"payload", runtime).hex()
        vectors["order." + name] = order_state(target, shard, revision=revision, barrier=barrier).hex()
    barrier = bytes.fromhex(vectors["order.barrier.hold"])
    vectors["order.watermark"] = order_state(target, shard, contract=2, revision=8, watermark=key, barrier=barrier).hex()
    vectors["order.closed"] = order_state(target, shard, contract=2, revision=9, control=2, gate=3, watermark=key, barrier=barrier).hex()
    vectors["order.watermark.value"] = value_envelope(17, bytes.fromhex(vectors["order.watermark"])).hex()
    max64, max32 = (1 << 64) - 1, (1 << 32) - 1
    max_loc = locator(target, message, max32, 63, max64, ordered=True)
    max_order = b"\x0b\x01" + target + domain + u64((1 << 63) - 1)
    max_order += b"\x02" + u64(max64) + u64(max64) + u32(max32) + message + u32(max32)
    max_barrier = order_barrier(max_loc, max_order, max64, bytes([0x77]) * 32)
    maximum = order_state(target, shard, contract=2, revision=max64, control=max64, gate=3,
                          watermark=max_order, barrier=max_barrier, slot=63, domain_generation=max64)
    vectors["order.maximum"] = maximum.hex()
    vectors["order.maximum.length"] = str(len(maximum))
    vectors["order.maximum.sha256"] = hashlib.sha256(maximum).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    output = Path(__file__).resolve().parent.parent / "src/test/resources/ndip3/target-identity-vectors.properties"
    expected = expected_vectors().encode("ascii")
    if args.write:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(expected)
    elif output.read_bytes() != expected:
        raise SystemExit("NDIP-3 identity/key golden vectors differ from independent encoding")
    print("NDIP-3 target identity/key vectors PASS sha256=" + hashlib.sha256(expected).hexdigest())


if __name__ == "__main__":
    main()
