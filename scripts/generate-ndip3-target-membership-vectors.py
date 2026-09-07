#!/usr/bin/env python3
"""Independent Target membership wire vectors; no Java output is consumed."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
base = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-binding-channel-vectors.py'))
compat = base['compat']
b, u, hashed, repeat = base['b'], base['u'], base['hashed'], base['repeat']


def properties(raw):
    return dict(line.split('=', 1) for line in raw.decode().splitlines() if line and not line.startswith('#'))


def grant(profile, required, offered, controls, source):
    registration = u(1, 1) + b(2, repeat(32, 0x70)) + b(3, profile)
    registration += b(4, required) + b(5, offered) + b(6, controls)
    registration += b(7, repeat(32, 0x71)) + b(8, repeat(32, 0x72))
    value = hashed(registration + b(9, repeat(32, 0x73)) + b(10, source), 11,
                   'nereus-delay-target-membership-grant')
    return registration, value


def expected():
    values = properties(compat['expected']())
    result = {}
    source = base['identity']['kafka_source'](base['route'], 6)[:-8] + base['u64'](90)
    for name in ['kafka.baseline', 'kafka.receipt', 'pulsar.baseline', 'pulsar.journal']:
        target = hashlib.sha256(b'nereus-delay-target-partition\0' + bytes.fromhex(values[name.split('.')[0] + '.target'])).digest()
        scope = compat['scope'](target, base['shard'], [(1, repeat(32, 0x11)), (3, repeat(32, 0x33))],
                                [repeat(32, 0x44), repeat(32, 0x55)])
        dispatch = bytes.fromhex(values[name + '.dispatch'])
        registration, value = grant(base['profile'](), dispatch, dispatch, scope, source)
        result[name + '.registration'] = registration.hex()
        result[name + '.grant'] = value.hex()
        result[name + '.key'] = (b'\x0f\x01' + value[-32:]).hex()
    result['grant.value'] = compat['envelope'](22, value).hex()
    target = hashlib.sha256(b'nereus-delay-target-partition\0' + bytes.fromhex(values['pulsar.target'])).digest()
    evidence = compat['resource'](2, b'x'*256, bytes(range(32)), b'x'*(1 << 20), base['I64'])
    dispatch = compat['dispatch'](target, compat['capability'](2, 3, 7, evidence, maximum=True), maximum=True)
    scope = compat['scope'](target, base['shard'], [(1, n.to_bytes(32, 'big')) for n in range(1, 33)],
                            [n.to_bytes(32, 'big') for n in range(33, 65)])
    source = b'\x02' + base['route'] + base['u32'](32) + bytes(range(32))
    source += base['u32'](1 << 20) + b'x'*(1 << 20) + base['u32'](3)
    source += base['u64'](base['U64']) * 2 + base['u32'](base['U32']-1) + base['u32'](base['U32'])
    source += b'\x02' + base['u64'](base['I64'])
    registration, value = grant(base['profile'](maximum=True), dispatch, dispatch, scope, source)
    for name, raw in [('maximum.registration', registration), ('maximum.grant', value)]:
        result[name + '.length'] = str(len(raw))
        result[name + '.sha256'] = hashlib.sha256(raw).hexdigest()
    return ('# NDIP-3 B2 membership schema 1; independent Python wire vectors.\n' +
            ''.join(k + '=' + v + '\n' for k, v in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    output = ROOT / 'src/test/resources/ndip3/target-membership-vectors.properties'
    value = expected()
    if args.write:
        output.write_bytes(value)
    elif output.read_bytes() != value:
        raise SystemExit('NDIP-3 Target membership vectors differ from independent encoding')
    print('NDIP-3 Target membership vectors PASS sha256=' + hashlib.sha256(value).hexdigest())


if __name__ == '__main__':
    main()
