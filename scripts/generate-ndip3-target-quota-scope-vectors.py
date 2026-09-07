#!/usr/bin/env python3
"""Independent all-incarnation Target quota totals and complete grant artifacts."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
base = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-vectors.py'))
account = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-accounting-vectors.py'))
u, b, hashed, usage, repeat, be = (base[k] for k in ['u', 'b', 'hashed', 'usage', 'repeat', 'be'])
SHARD, TARGET, TENANT = (base[k] for k in ['SHARD', 'TARGET', 'TENANT'])
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def scope(target=True):
    raw = u(1, 1) + b(2, SHARD) + b(3, TENANT)
    if target:
        raw += b(4, TARGET)
    return hashed(raw, 5, 'target-quota-scope')


def total(value, revision=1, mutation=None):
    raw = u(1, 1) + b(2, scope()) + b(3, value) + u(4, revision) + b(5, mutation or base['stamp']())
    return hashed(raw, 6, 'target-quota-total')


def grant(value, target=True, version=1, policy=7, maximum=False):
    raw = (u(1, 1) + b(2, scope(target)) + b(3, repeat(32, 0x99)) + u(4, version)
           + b(5, account['accounting'](maximum)) + b(6, value) + u(7, policy) + b(8, repeat(32, 0xaa)))
    return hashed(raw, 9, 'target-quota-grant')


def expected():
    value = usage({1: 2, 2: 20, 7: 1, 8: 7, 9: 1, 10: 64}, 1, 1, 0, 2)
    result = {key: raw.hex() for key, raw in [
        ('scope.shard', scope(False)), ('scope.target', scope()), ('usage', value),
        ('source', base['SOURCE']), ('mutation', base['stamp']()),
        ('total', total(value)), ('total.zero', total(usage({}))),
        ('total.key', b'\x16\x01\x02' + SHARD + TENANT + TARGET),
        ('grant.shard', grant(value, False)), ('grant.target', grant(value)),
        ('grant.zero', grant(usage({}), version=2)), ('accounting', account['accounting']())]}
    high = base['I64']
    maximum_target = usage({n: high for n in range(1, 16)}, 1, 64, high, high)
    maximum_shard = usage({n: high for n in list(range(1, 16)) + list(range(51, 56))}, high, high, high, high)
    maximum_source = (b'\x02' + SHARD[:16] + be(32, 4) + repeat(32, 0x77) + be(1 << 20, 4)
                      + b'x' * (1 << 20) + SHARD[16:] + be(base['U64'], 8) * 2
                      + be(0, 4) + be(1, 4) + b'\x01' + be(high, 8))
    for name, raw in [
        ('total', total(maximum_target, base['U64'], base['stamp'](base['U64'], maximum_source))),
        ('grant.target', grant(maximum_target, version=base['U64'], policy=base['U64'], maximum=True)),
        ('grant.shard', grant(maximum_shard, False, base['U64'], base['U64'], True))]:
        result['maximum.' + name + '.length'] = str(len(raw))
        result['maximum.' + name + '.sha256'] = sha(raw)
    return ('# Independent all-incarnation Target totals and grant artifacts.\n'
            + ''.join(key + '=' + value + '\n' for key, value in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-scope-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    elif path.read_bytes() != raw:
        raise SystemExit('Target quota scope vectors differ from independent encoding')
    print('Target quota scope vectors PASS sha256=' + sha(raw))


if __name__ == '__main__':
    main()
