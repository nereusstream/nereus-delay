#!/usr/bin/env python3
"""Independent NDIP-3 Target counter encoders (Python standard library only)."""
import argparse
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
I64 = (1 << 63) - 1
U64 = (1 << 64) - 1
sha = lambda raw: hashlib.sha256(raw).digest()
repeat = lambda count, value: bytes([value]) * count
be = lambda value, count: value.to_bytes(count, 'big')


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result) + bytes([value])


def u(field, value):
    return varint(field << 3) + varint(value)


def b(field, value):
    return varint((field << 3) | 2) + varint(len(value)) + value


def hashed(raw, field, name):
    return raw + b(field, sha(('nereus-delay-' + name + '\0').encode('ascii') + raw))


def capacity(amounts):
    raw = u(1, 1) + b''.join(b(2, u(1, n) + u(2, amounts.get(n, 0))) for n in range(1, 67))
    return hashed(raw, 3, 'capacity-vector')


def usage(amounts, targets=0, domains=0, strict=0, incarnations=0):
    raw = u(1, 1) + b(2, capacity(amounts)) + u(3, targets) + u(4, domains) + u(5, strict) + u(6, incarnations)
    return hashed(raw, 7, 'target-quota-usage')


SHARD = repeat(16, 0x11) + be(3, 4)
INC = repeat(16, 0x22)
TARGET = repeat(32, 0x33)
TENANT = repeat(32, 0x44)
SOURCE = b'\x01' + SHARD[:16] + be(3, 4) + b'kfk' + repeat(16, 0x55) + SHARD[16:] + be(10, 8) + b'\0' + be(100, 8)


def identity(kind):
    raw = u(1, 1) + u(2, kind) + b(3, SHARD) + b(4, INC)
    if kind in (1, 2):
        raw += b(5, TARGET)
    if kind in (2, 4):
        raw += b(6, TENANT)
    return hashed(raw, 7, 'target-quota-identity')


def stamp(sequence=1, source=SOURCE):
    return u(1, sequence) + b(2, source) + b(3, repeat(32, 0x66))


def counter(ident, value, revision=1, mutation=None):
    return hashed(u(1, 1) + b(2, ident) + b(3, value) + u(4, revision) + b(5, mutation or stamp()), 6, 'target-quota-counter')


def aggregate(value, revision=1, mutation=None):
    raw = u(1, 1) + b(2, SHARD) + b(3, INC) + b(4, value) + u(5, revision)
    if revision:
        raw += b(6, mutation or stamp())
    return hashed(raw, 7, 'target-quota-aggregate')


def expected():
    result = {'source': SOURCE.hex(), 'mutation': stamp().hex()}
    primary = usage({1: 2, 2: 20, 7: 1, 8: 7, 9: 1, 10: 64}, 1, 1, 0, 1)
    mirror = usage({1: 2, 2: 20, 7: 1, 8: 7, 9: 1, 10: 64}, 1, 0, 0, 0)
    for kind, name in [(1, 'target'), (2, 'tenantTarget'), (3, 'shard'), (4, 'tenantShard')]:
        result[name + '.identity'] = identity(kind).hex()
        key = b'\x13\x01' + bytes([kind]) + SHARD + INC
        if kind in (1, 2):
            key += TARGET
        if kind in (2, 4):
            key += TENANT
        result[name + '.key'] = key.hex()
    for name, raw in [('usage', primary), ('mirrorUsage', mirror), ('emptyUsage', usage({})),
                      ('counter', counter(identity(1), primary)), ('mirror', counter(identity(2), mirror)),
                      ('aggregate', aggregate(primary)), ('genesis', aggregate(usage({}), 0))]:
        result[name] = raw.hex()
    result['aggregate.key'] = (b'\x14\x01' + SHARD).hex()
    maximum_usage = usage({n: I64 for n in range(1, 16)}, 1, 64, I64, 1)
    topic = b'x' * (1 << 20)
    maximum_source = (b'\x02' + SHARD[:16] + be(32, 4) + repeat(32, 0x77) + be(len(topic), 4)
                      + topic + SHARD[16:] + be(U64, 8) * 2 + be(0, 4) + be(1, 4) + b'\x01' + be(I64, 8))
    for name, raw in [('usage', usage({n: I64 for n in list(range(1, 16)) + list(range(51, 56))}, I64, I64, I64, I64)),
                      ('counter', counter(identity(1), maximum_usage, U64, stamp(U64, maximum_source))),
                      ('aggregate', aggregate(maximum_usage, U64, stamp(U64, maximum_source)))]:
        result['maximum.' + name + '.length'] = str(len(raw))
        result['maximum.' + name + '.sha256'] = sha(raw).hex()
    return ('# Independent Target quota wire inputs; maximum samples are bound by length and SHA-256.\n'
            + ''.join(key + '=' + value + '\n' for key, value in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    elif path.read_bytes() != raw:
        raise SystemExit('Target quota vectors differ from independent encoding')
    print('Target quota vectors PASS sha256=' + sha(raw).hex())


if __name__ == '__main__':
    main()
