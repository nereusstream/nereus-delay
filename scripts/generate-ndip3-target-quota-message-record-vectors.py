#!/usr/bin/env python3
"""Independent Message-family record keys, canonical payload digests and STATE charges."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
payload = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-payload-vectors.py'))
u, b, capacity, be = (payload[k] for k in ['u', 'b', 'capacity', 'be'])
binding = payload['binding']
identity = binding['identity']
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def expected():
    # Existing full independently generated record bodies are inputs, not Java-produced lengths.
    iv = dict(line.split('=', 1) for line in identity['expected_vectors']().splitlines() if '=' in line)
    get = lambda key: bytes.fromhex(iv[key])
    pv = dict(line.split('=', 1) for line in payload['expected']().decode().splitlines() if '=' in line)
    bv = dict(line.split('=', 1) for line in binding['expected']().decode().splitlines() if '=' in line)
    target, message = get('pulsar.id'), get('message.id')
    suffix = target + be(0, 2) + be(1, 8) + be(100, 8) + b'\x01' + be(7, 8) + message + be(2, 4)
    records = {
        'owner': (bytes.fromhex(pv['key']), bytes.fromhex(pv['inline.active'])),
        'binding': (b'\x06\x01' + bytes.fromhex(bv['binding.best'])[-32:], bytes.fromhex(bv['binding.best'])),
        'message': (get('message.key'), get('message.initial')),
        'due': (b'\x08\x01' + suffix, get('work.native')),
        'native': (b'\x09\x01' + suffix, get('work.native')),
        'expiry': (get('message.expiry.key'), get('message.expiry')),
        'ordered': (get('order.key'), get('work.fifo.initial')),
        'orderHead': (get('order.serviceable.key'), get('work.fifo.initial')),
    }
    result = {}
    costs = {}
    for name, (key, raw) in records.items():
        costs[name] = len(key) + len(raw) + 12 + 32
        result[name + '.key'] = key.hex()
        result[name + '.payload.sha256'] = sha(raw)
        result[name + '.charge'] = capacity({3: costs[name]}).hex()
    result['nativeSubset.charge'] = capacity({3: sum(costs[n] for n in ['owner', 'binding', 'message', 'due', 'native', 'expiry'])}).hex()
    return ('# Independent Message record footprints; structural evidence, not source authorization.\n'
            + ''.join(k + '=' + v + '\n' for k, v in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-message-record-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    if path.read_bytes() != raw:
        raise SystemExit('Target quota Message record vectors mismatch')
    print('Target quota Message record vectors PASS sha256=' + sha(raw))


if __name__ == '__main__':
    main()
