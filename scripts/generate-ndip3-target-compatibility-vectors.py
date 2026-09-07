#!/usr/bin/env python3
"""Independently verify B2 Target execution/control wire vectors; --write deliberately updates the fixture."""

import argparse
import hashlib
from pathlib import Path
import struct
import uuid


def u32(n):
    return struct.pack(">I", n)


def u64(n):
    return struct.pack(">Q", n)


def varint(n):
    result = bytearray()
    while n > 127:
        result.append((n & 127) | 128)
        n >>= 7
    result.append(n)
    return bytes(result)


def b(field, value):
    return varint(field * 8 + 2) + varint(len(value)) + value


def u(field, value):
    return varint(field * 8) + varint(value)


def hashed(fields, number, domain):
    return fields + b(number, hashlib.sha256(domain.encode() + b"\0" + fields).digest())


def resource(adapter, cluster, identity, topic=b"", creation=0):
    fields = b(1, cluster) + b(2, identity)
    if adapter == 2:
        fields += b(3, topic) + u(4, creation)
    return b(adapter, fields)


def capability(adapter, outcome, timing, evidence=None, maximum=False):
    fields = u(1, adapter) + u(2, outcome) + u(3, timing)
    if evidence is not None:
        fields += b(4, evidence)
    values = ([0, 0, 0, 0] if evidence is None else
              [(1 << 31) - 1, (1 << 63) - 1, (1 << 63) - 1, (1 << 63) - 1] if maximum else
              [8, 86400000, 600000, 64])
    fields += b"".join(u(n, value) for n, value in zip(range(5, 9), values))
    fields += b(9, bytes([0x22]) * 32) + b(10, bytes([0x33]) * 32)
    version = 0 if evidence is None else (1 << 31) - 1 if maximum else 1
    return fields + u(11, version) + u(12, version)


def dispatch(target, cap, maximum=False):
    fields = u(1, 1) + b(2, target) + b(3, bytes([0xAA]) * 32) + u(4, (1 << 31) - 1 if maximum else 2)
    fields += b(5, cap) + b(6, bytes([0xBB]) * 32)
    fields += u(7, (1 << 63) - 1 if maximum else 86400000) + u(8, (1 << 63) - 1 if maximum else 172800000)
    return hashed(fields, 9, "nereus-delay-target-dispatch-compatibility")


def scope(target, shard, controls, permits):
    fields = u(1, 1) + b(2, target) + b(3, shard)
    fields += b"".join(b(4, u(1, kind) + b(2, identity)) for kind, identity in controls)
    fields += b"".join(b(5, identity) for identity in permits)
    return hashed(fields, 6, "nereus-delay-target-control-scope")


def envelope(kind, payload):
    raw = b"NV" + bytes([kind, 1]) + u32(len(payload)) + payload
    crc = 0xFFFFFFFF
    for byte in raw:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return raw + u32(crc ^ 0xFFFFFFFF)


def expected():
    kafka = resource(1, b"cluster-a", uuid.UUID("00112233-4455-6677-8899-aabbccddeeff").bytes)
    pulsar = resource(2, b"cluster-p", bytes(range(32)), b"persistent://tenant/ns/topic-partition-5", 1700000000000)
    receipt = resource(1, b"cluster-a", uuid.UUID("11223344-5566-7788-99aa-bbccddeeff00").bytes)
    journal = resource(2, b"cluster-p", bytes([0x44]) * 32, b"persistent://tenant/ns/journal-partition-5", 1700000000000)
    targets = {}
    vectors = {}
    for name, res, partition in [("kafka", kafka, 3), ("pulsar", pulsar, 5)]:
        raw = b"\x01" + u32(len(res)) + res + u32(partition)
        targets[name] = hashlib.sha256(b"nereus-delay-target-partition\0" + raw).digest()
        vectors[name + ".target"] = raw.hex()
    variants = [("kafka.baseline", "kafka", capability(1, 1, 1)),
                ("kafka.receipt", "kafka", capability(1, 2, 1, receipt)),
                ("pulsar.baseline", "pulsar", capability(2, 1, 3)),
                ("pulsar.journal", "pulsar", capability(2, 3, 3, journal))]
    for name, target, cap in variants:
        value = dispatch(targets[target], cap)
        vectors[name + ".capability"] = cap.hex()
        vectors[name + ".dispatch"] = value.hex()
        vectors[name + ".key"] = (b"\x0c\x01" + value[-32:]).hex()
    vectors["dispatch.value"] = envelope(18, bytes.fromhex(vectors["pulsar.journal.dispatch"])).hex()
    shard = uuid.UUID("11111111-2222-4333-8444-555555555555").bytes + u32(3)
    controls = [(1, bytes([0x11]) * 32), (3, bytes([0x33]) * 32)]
    permits = [bytes([0x44]) * 32, bytes([0x55]) * 32]
    for name, groups, budgets in [("empty", [], []), ("shared", controls, permits),
                                  ("private", [(1, bytes([0x12]) * 32), controls[1]], permits),
                                  ("tenant", [controls[0], (2, bytes([0x22]) * 32), controls[1]], permits),
                                  ("permit", controls, [bytes([0x44]) * 32, bytes([0x56]) * 32])]:
        value = scope(targets["pulsar"], shard, groups, budgets)
        vectors["scope." + name] = value.hex()
    vectors["scope.key"] = (b"\x0d\x01" + bytes.fromhex(vectors["scope.shared"])[-32:]).hex()
    vectors["scope.value"] = envelope(19, bytes.fromhex(vectors["scope.shared"])).hex()
    maximum = scope(targets["pulsar"], shard, [(1, n.to_bytes(32, "big")) for n in range(1, 33)],
                    [n.to_bytes(32, "big") for n in range(33, 65)])
    evidence = resource(2, b"x" * 256, bytes(range(32)), b"x" * (1 << 20), (1 << 63) - 1)
    max_dispatch = dispatch(targets["pulsar"], capability(2, 3, 7, evidence, maximum=True), maximum=True)
    for name, value in [("scope.maximum", maximum), ("dispatch.maximum", max_dispatch)]:
        vectors[name + ".length"] = str(len(value))
        vectors[name + ".sha256"] = hashlib.sha256(value).hexdigest()
    for name, number in [("TARGET_BINDING_INCOMPATIBLE", 0x1117), ("TARGET_EXECUTION_DOMAIN_LIMIT_EXCEEDED", 0x1118),
                          ("TARGET_EXECUTION_DOMAIN_DRAINING", 0x1119), ("TARGET_EXECUTION_DOMAIN_GENERATION_EXHAUSTED", 0x111A),
                          ("TARGET_CLOSED", 0x111B)]:
        vectors["code." + name] = str(number)
    return ("# NDIP-3 B2 version 1; independent Python wire/CRC32C/hash vectors.\n" +
            "".join(key + "=" + value + "\n" for key, value in vectors.items())).encode("ascii")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    path = Path(__file__).resolve().parent.parent / "src/test/resources/ndip3/target-compatibility-vectors.properties"
    value = expected()
    if args.write:
        path.write_bytes(value)
    elif path.read_bytes() != value:
        raise SystemExit("NDIP-3 Target compatibility vectors differ from independent encoding")
    print("NDIP-3 Target compatibility vectors PASS sha256=" + hashlib.sha256(value).hexdigest())


if __name__ == "__main__":
    main()
