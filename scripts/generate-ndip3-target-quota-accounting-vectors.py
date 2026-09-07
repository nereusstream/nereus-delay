#!/usr/bin/env python3
"""Independent Target measurement and attempt-budget lifecycle vectors."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
base = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-vectors.py'))
u, b, hashed, capacity, repeat, be = (base[k] for k in ['u', 'b', 'hashed', 'capacity', 'repeat', 'be'])
sha = lambda raw: hashlib.sha256(raw).digest()
SHARD, INC, TARGET, TENANT = (base[k] for k in ['SHARD', 'INC', 'TARGET', 'TENANT'])
COMMITTED = {3: 4096, 9: 4, 10: 512, 11: 4, 12: 1024, 13: 2048, 14: 4, 15: 512}
INITIAL = {3: 256, 11: 1, 12: 128}
UNKNOWN = {3: 300, 11: 2, 12: 256, 13: 100}
RESOLVED = {3: 400, 9: 1, 10: 80, 11: 3, 12: 384, 13: 200, 14: 1, 15: 64}
LINEAGE = repeat(16, 0xcc)


def crc(raw):
    result = 0xffffffff
    for byte in raw:
        result ^= byte
        for _ in range(8):
            result = (result >> 1) ^ (0x82f63b78 if result & 1 else 0)
    return be(result ^ 0xffffffff, 4)


prefix = b'\x01' + SHARD + bytes.fromhex('00000000006470008000000000000001')
MESSAGE = prefix + crc(prefix)


def source(n):
    return b'\x01' + SHARD[:16] + be(3, 4) + b'kfk' + repeat(16, 0x55) + SHARD[16:] + be(9+n, 8) + b'\0' + be(99+n, 8)


def mutation(n):
    return u(1, n) + b(2, source(n)) + b(3, repeat(32, 0x66))


def accounting(maximum=False):
    charges = [base['I64']] * 4 if maximum else [32, 24, 40, 64]
    raw = u(1, 1) + b(2, repeat(32, 0xbb)) + u(3, 1)
    raw += b''.join(u(index+4, value) for index, value in enumerate(charges))
    return hashed(raw, 8, 'target-quota-accounting')


def locator():
    raw = u(1, 1) + b(2, MESSAGE) + u(3, 1) + b(4, TARGET) + u(5, 0) + u(6, 1) + b(7, INC) + u(8, 1) + b(10, repeat(32, 0x88))
    return hashed(raw, 11, 'target-message-locator')


def floor(n):
    position = b(1, b(1, SHARD[:16]) + b(2, b'kfk') + b(3, repeat(16, 0x55)) + u(4, 3) + u(5, 9+n) + u(7, 99+n))
    raw = b(1, LINEAGE) + b(2, repeat(16, 0xd0+n)) + b(3, repeat(32, 0xdd)) + u(4, n) + b(5, position) + u(6, n)
    return hashed(raw, 8, 'recovery-floor-ref')


def budget(phase, allocation):
    raw = u(1, 1) + b(2, locator()) + b(3, TENANT) + b(4, repeat(32, 0x99)) + b(5, repeat(32, 0xaa)) + b(6, accounting())
    raw += u(7, 52) + b(8, capacity(COMMITTED)) + b(9, capacity(allocation)) + u(10, phase) + u(11, phase) + b(12, mutation(phase))
    if phase >= 3:
        raw += b(13, mutation(3))
    if phase >= 4:
        raw += b(14, floor(phase-1)[-32:])
    return hashed(raw + b(15, LINEAGE), 16, 'target-quota-attempt-budget')


def expected():
    result = {'accounting': accounting(), 'accounting.maximum': accounting(True), 'locator': locator(), 'message': MESSAGE,
              'commitment': capacity(COMMITTED), 'initial': capacity(INITIAL), 'unknownAllocation': capacity(UNKNOWN),
              'resolvedAllocation': capacity(RESOLVED), 'floor3': floor(3), 'floor4': floor(4)}
    for phase, name, allocation in [(1, 'admitted', INITIAL), (2, 'unknown', UNKNOWN), (3, 'resolved', RESOLVED),
                                     (4, 'retained', RESOLVED), (5, 'released', {})]:
        result[name + '.budget'] = budget(phase, allocation)
        charge = {} if phase == 5 else dict(allocation if phase == 4 else COMMITTED)
        if phase < 3:
            charge.update({7: 1, 8: 52})
        result[name + '.charge'] = capacity(charge)
    result['budget.key'] = b'\x15\x01' + repeat(32, 0x99)
    for name, amounts in [('state', {3: 152}), ('result', {9: 1, 10: 152}), ('system', {11: 1, 12: 152}),
                          ('evidence', {14: 1, 15: 152}), ('wal', {13: 132}), ('reservation', {5: 1, 6: 20}),
                          ('active', {1: 1, 2: 20}), ('retainedPayload', {4: 20}), ('execution', {7: 1, 8: 52})]:
        result['fee.' + name] = capacity(amounts)
    return ('# Independent Python Target quota accounting and attempt lifecycle vectors.\n'
            + ''.join(key + '=' + value.hex() + '\n' for key, value in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-accounting-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    elif path.read_bytes() != raw:
        raise SystemExit('Target quota accounting vectors differ from independent encoding')
    print('Target quota accounting vectors PASS sha256=' + sha(raw).hex())


if __name__ == '__main__':
    main()
