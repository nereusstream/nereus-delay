#!/usr/bin/env python3
"""Independent fixed accounting projection envelopes and bookkeeping inventory vectors."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
account = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-accounting-vectors.py'))
base = account['base']
u, b, hashed, repeat, be, capacity = (base[k] for k in ['u', 'b', 'hashed', 'repeat', 'be', 'capacity'])
SHARD, TENANT = base['SHARD'], base['TENANT']
# Conservative schema envelopes, independent of the Java implementation or its encoded values.
SOURCE_MAX = 94 + (1 << 20)
MUTATION_MAX = 11 + 4 + SOURCE_MAX + 34
IDENTITY_MAX = 2 + 2 + 22 + 18 + 34 + 34 + 34
VECTOR_MAX = 36 + 66 * 14
USAGE_MAX = 2 + 3 + VECTOR_MAX + 4 * 11 + 34
ACCOUNT_MAX = 2 + 34 + 2 + 4 * 11 + 34
SCOPE_MAX = 2 + 22 + 34 + 34 + 34
GRANT_MAX = 2 + 3 + SCOPE_MAX + 34 + 11 + 3 + ACCOUNT_MAX + 3 + USAGE_MAX + 11 + 34 + 34
REQUEST_MAX = 2 + 2 * (3 + GRANT_MAX) + 2 + 113
COUNTER_MAX = 2 + 3 + IDENTITY_MAX + 3 + USAGE_MAX + 11 + 4 + MUTATION_MAX + 34
TOTAL_MAX = 2 + 3 + SCOPE_MAX + 3 + USAGE_MAX + 11 + 4 + MUTATION_MAX + 34
AGGREGATE_MAX = 2 + 22 + 18 + 3 + USAGE_MAX + 11 + 4 + MUTATION_MAX + 34
ALLOCATION_MAX = 2 + 3 + IDENTITY_MAX + 34 + 2 + ACCOUNT_MAX + 4 + MUTATION_MAX + 18 + 34
ACTIVATION_MAX = 2 + 3 + REQUEST_MAX + 76 + 4 + MUTATION_MAX + 3 * 34 + 4 + ALLOCATION_MAX
ANCHOR_MAX = 2 + 3 + IDENTITY_MAX + 34 + 2 + ACCOUNT_MAX + 4 * 11 + 4 + MUTATION_MAX + 34
LOCATOR_MAX = 2 + 43 + 6 + 34 + 4 + 11 + 18 + 2 + 34 + 34 + 34
BUDGET_MAX = 2 + 3 + LOCATOR_MAX + 3 * 34 + 2 + ACCOUNT_MAX + 11 + 2 * (3 + VECTOR_MAX) + 2 + 11 + 2 * (4 + MUTATION_MAX) + 34 + 18 + 35


def fees(source_bytes):
    fee = lambda key, payload, copies=1: key + payload - copies * SOURCE_MAX + copies * source_bytes + 12 + 32
    return {'anchor': fee(22, ANCHOR_MAX), 'aggregate': fee(22, AGGREGATE_MAX),
            'counter': fee(103, COUNTER_MAX), 'total': fee(87, TOTAL_MAX),
            'activation': fee(87, ACTIVATION_MAX, 2), 'budget': fee(34, BUDGET_MAX, 2)}


def anchor(source, inventory, sequence=1):
    raw = u(1, 1) + b(2, base['identity'](3)) + b(3, TENANT) + b(4, account['accounting']())
    raw += u(5, inventory[0]) + u(6, inventory[1]) + u(7, inventory[2]) + u(8, sequence)
    return hashed(raw + b(9, base['stamp'](sequence, source)), 10, 'target-quota-bookkeeping')


def pulsar(topic, maximum=False):
    value = base['U64'] if maximum else 4
    return (b'\x02' + SHARD[:16] + be(32, 4) + repeat(32, 0x77) + be(len(topic), 4) + topic + SHARD[16:]
            + be(value, 8) + be(value, 8) + be(0, 4) + be(1, 4) + b'\x01' + be(base['I64'] if maximum else 100, 8))


def expected():
    kafka = base['SOURCE']
    epoch = kafka[:-9] + b'\x01' + be((1 << 32)-1, 4) + kafka[-8:]
    result = {'key': (b'\x18\x01' + SHARD).hex()}
    for name, source, inventory, seq in [('initial', kafka, (2,0,0), 1), ('populated', kafka, (6,2,3), 1),
                                        ('epoch', epoch, (2,0,0), base['U64']),
                                        ('pulsar', pulsar(b'persistent://t/n/source'), (4,1,2), 1)]:
        bound = len(source) + (4 if source == kafka else 0)
        cost = fees(bound)
        total = cost['anchor'] + cost['aggregate'] + inventory[0]*cost['counter'] + inventory[1]*cost['total'] + inventory[2]*cost['activation']
        result[name+'.source'] = source.hex()
        result[name+'.anchor'] = anchor(source, inventory, seq).hex()
        result[name+'.charge'] = capacity({3: total}).hex()
        result[name+'.sourceBound'] = str(bound)
        for key,value in cost.items():
            result[name+'.fee.'+key] = str(value)
    large_source = pulsar(b'x'*(1 << 20), True)
    large = anchor(large_source, (2,0,0), base['U64'])
    result['maximum.length'] = str(len(large))
    result['maximum.sha256'] = hashlib.sha256(large).hexdigest()
    result['maximum.sourceBound'] = str(len(large_source))
    result['maximum.fee.counter'] = str(fees(len(large_source))['counter'])
    return ('# Independent Python quota bookkeeping envelopes and inventory vectors.\n'
            + ''.join(key+'='+value+'\n' for key,value in result.items())).encode('ascii')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-quota-bookkeeping-vectors.properties'
    raw=expected()
    if args.write:
        path.write_bytes(raw)
    if path.read_bytes()!=raw:
        raise SystemExit('Target quota bookkeeping vectors mismatch')
    print('Target quota bookkeeping vectors PASS sha256='+hashlib.sha256(raw).hexdigest())


if __name__=='__main__':
    main()
