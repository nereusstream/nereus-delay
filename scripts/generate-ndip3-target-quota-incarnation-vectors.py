#!/usr/bin/env python3
"""Independent source-derived quota incarnation, drain and fixed-record commitment vectors."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
scope_module = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-scope-vectors.py'))
base, account = scope_module['base'], scope_module['account']
u, b, hashed, usage, be, repeat = (base[k] for k in ['u', 'b', 'hashed', 'usage', 'be', 'repeat'])
SHARD, TENANT, TARGET = (base[k] for k in ['SHARD', 'TENANT', 'TARGET'])
LINEAGE = repeat(16, 0xcc)
SOURCE_MAX = 94 + (1 << 20)
MUTATION_MAX = 11 + 4 + SOURCE_MAX + 34
IDENTITY_MAX = 2 + 2 + 22 + 18 + 34 + 34 + 34
ACCOUNT_MAX = 2 + 34 + 2 + 4 * 11 + 34
MAXIMUM = 2 + 3 + IDENTITY_MAX + 34 + 2 + ACCOUNT_MAX + 2 * (4 + MUTATION_MAX) + 18 + 34
sha = lambda raw: hashlib.sha256(raw).digest()


def source(n):
    return base['SOURCE'][:-17] + be(9+n, 8) + b'\0' + be(100, 8)


def stamp(n):
    return base['stamp'](n, source(n))


def incarnation(target=True, allocation=None, drain=None):
    allocation = allocation or stamp(1)
    scope = scope_module['scope'](target)
    origin = u(1, 1) + b(2, scope) + b(3, account['accounting']()) + b(4, LINEAGE) + b(5, allocation)
    identifier = sha(b'nereus-delay-target-quota-incarnation-id\0' + origin)[:16]
    kind = 1 if target else 3
    ident = u(1, 1) + u(2, kind) + b(3, SHARD) + b(4, identifier)
    if target:
        ident += b(5, TARGET)
    ident = hashed(ident, 7, 'target-quota-identity')
    raw = u(1, 1) + b(2, ident) + b(3, TENANT) + b(4, account['accounting']()) + b(5, allocation) + b(6, LINEAGE)
    if drain:
        raw += b(7, drain)
    key = b'\x1a\x01' + bytes([kind]) + SHARD + identifier + (TARGET if target else b'')
    return hashed(raw, 8, 'target-quota-incarnation'), key, ident


def maximum_source(entry):
    topic = b'x' * (1 << 20)
    return (b'\x02' + SHARD[:16] + be(32, 4) + repeat(32, 0x77) + be(len(topic), 4) + topic
            + SHARD[16:] + be(base['U64'], 8) + be(entry, 8) + be(0, 4) + be(1, 4) + b'\x01' + be(base['I64'], 8))


def fee(key, source_bytes):
    return len(key) + MAXIMUM - 2 * SOURCE_MAX + 2 * source_bytes + 12 + 32


def expected():
    result = {}
    for target in [True, False]:
        name = 'target' if target else 'shard'
        for phase, drain in [('open', None), ('draining', stamp(2))]:
            raw, key, identity = incarnation(target, drain=drain)
            prefix = name + '.' + phase
            result[prefix] = raw.hex()
            result[prefix + '.key'] = key.hex()
            result[prefix + '.identity'] = identity.hex()
            cost = fee(key, len(source(1)) + 4)
            result[prefix + '.primary'] = usage({3: cost}, incarnations=1).hex()
            result[prefix + '.mirror'] = usage({3: cost}).hex()
    allocation = base['stamp'](base['U64'] - 1, maximum_source(base['U64'] - 1))
    drain = base['stamp'](base['U64'], maximum_source(base['U64']))
    raw, key, identity = incarnation(True, allocation, drain)
    result['maximum.length'] = str(len(raw))
    result['maximum.sha256'] = sha(raw).hex()
    result['maximum.primary'] = usage({3: fee(key, SOURCE_MAX)}, incarnations=1).hex()
    return ('# Independent source-derived quota incarnation vectors.\n'
            + ''.join(k + '=' + v + '\n' for k, v in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-incarnation-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    if path.read_bytes() != raw:
        raise SystemExit('Target quota incarnation vectors mismatch')
    print('Target quota incarnation vectors PASS sha256=' + sha(raw).hex())


if __name__ == '__main__':
    main()
