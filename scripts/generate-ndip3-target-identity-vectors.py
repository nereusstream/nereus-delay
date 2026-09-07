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
    header = "# NDIP-3 version 1; independent Python hashlib/struct/protobuf wire/CRC32C generation.\n"
    return header + "".join(key + "=" + value + "\n" for key, value in vectors.items())


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
