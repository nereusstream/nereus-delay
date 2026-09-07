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
