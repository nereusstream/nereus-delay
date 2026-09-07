#!/usr/bin/env python3
"""Independent Target quota grant Control, signed System Mutation and activation vectors."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
scope = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-scope-vectors.py'))
signer = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-native-policy-vectors.py'))
base, account = scope['base'], scope['account']
u, b, hashed, usage, repeat, be = (scope[k] for k in ['u', 'b', 'hashed', 'usage', 'repeat', 'be'])
SHARD, TARGET, TENANT = (scope[k] for k in ['SHARD', 'TARGET', 'TENANT'])
sha = lambda raw: hashlib.sha256(raw).digest()
lp = lambda raw: be(len(raw), 4) + raw
LIMIT = usage({1: 2, 2: 20, 7: 1, 8: 7, 9: 1, 10: 64}, 1, 1, 0, 2)
ROLE_HASH = sha(b'nereus-delay-control-role-set\0' + u(1, 5))
AUTHOR = b(2, b(1, repeat(32, 0xa1)) + b(2, ROLE_HASH) + b(3, repeat(32, 0xa2)))


def values(target=True, successor=False, transfer=False, zero=False, maximum=False):
    version = base['U64'] if maximum else 2 if successor else 1
    policy = base['U64'] if maximum else 7
    high = base['I64']
    limit = usage({n: high for n in range(1, 16)}, 1, 64, high, high) if maximum else LIMIT
    if maximum and not target:
        limit = usage({n: high for n in list(range(1, 16)) + list(range(51, 56))}, high, high, high, high)
    next_grant = scope['grant'](usage({}) if zero else limit, target, version, policy, maximum)
    prior = scope['grant'](limit, target, version-1, policy, maximum) if successor else None
    plan = b(1, repeat(32, 0xb4)) + b(2, repeat(32, 0xb5)) + u(3, policy) + b(4, repeat(32, 0xb6))
    request = u(1, 1) + b(2, next_grant) + (b(3, prior) if prior else b'') + (b(4, plan) if transfer else b'')
    outer = b(18, request)
    request_hash = sha(b'nereus-delay-control-request\0' + be(18, 2) + lp(outer))
    ref = b(1, repeat(32, 0x72)) + b(2, request_hash) + u(3, 0)
    semantic = sha(b'nereus-delay-target-quota-control\0' + be(17, 2) + request)
    subject = b(1, SHARD[:16]) + u(2, 3)
    until = high if maximum else 500
    body = (b(1, subject) + u(2, 1) + u(3, until) + b(10, ref) + u(11, 17) + u(12, version)
            + b(13, semantic) + b(15, b(17, request)))
    logical = sha(b'nereus-delay-control-target-logical-id\0' + repeat(32, 0x72) + be(0, 4) + be(17, 2))
    mh = sha(b'nereus-delay-system-mutation-hash\0' + b'\x01' + be(1, 4)*3 + be(1, 2) + SHARD + be(until, 8) + lp(body))
    mid = sha(b'nereus-delay-system-mutation-id' + be(1, 2) + lp(logical) + SHARD + mh)
    signed = sha(b'nereus-delay-system-mutation-signature\0' + be(0x4e444c31, 4) + b'\x01\x02'
                 + be(1, 4)*3 + be(1, 2) + lp(mid) + SHARD + be(until, 8) + lp(body) + lp(mh) + lp(AUTHOR) + be(1, 4))
    public, signature = signer['sign'](signed)
    envelope = u(1, 1) + b(3, u(1, 1) + b(2, mid) + b(3, subject) + u(4, 1) + u(5, until)
                          + b(6, body) + b(7, mh) + u(8, 1) + b(9, AUTHOR) + u(10, 1) + b(11, signature))
    source = account['source'](version) if not maximum else (
        b'\x02' + SHARD[:16] + be(32, 4) + repeat(32, 0x77) + be(1 << 20, 4) + b'x'*(1 << 20)
        + SHARD[16:] + be(base['U64'], 8)*2 + be(0, 4) + be(1, 4) + b'\x01' + be(high, 8))
    stamp = u(1, version) + b(2, source) + b(3, sha(envelope))
    activated = hashed(u(1, 1) + b(2, request) + b(3, ref) + b(4, stamp) + b(5, mid) + b(6, mh), 7,
                       'target-quota-grant-activation')
    return dict(request=request, outer=outer, ref=ref, body=body, semantic=semantic, logical=logical,
                mutationHash=mh, mutationId=mid, envelope=envelope, activation=activated, public=public,
                key=b'\x17\x01' + (b'\x02' if target else b'\x01') + SHARD + TENANT + (TARGET if target else b''))


def expected():
    result = {'seed': signer['SEED'].hex(), 'limit': LIMIT.hex()}
    for name, args in [('target.initial', {}), ('shard.initialTransfer', dict(target=False, transfer=True)),
                       ('target.replaceTransfer', dict(successor=True, transfer=True)),
                       ('shard.replace', dict(target=False, successor=True)),
                       ('target.zero', dict(successor=True, zero=True))]:
        for key, raw in values(**args).items():
            result[name+'.'+key] = raw.hex()
    for target in [True, False]:
        maximum = values(target=target, successor=True, transfer=True, maximum=True)
        for key in ['request', 'body', 'activation']:
            name = 'maximum.' + ('target' if target else 'shard') + '.' + key
            result[name+'.length'] = str(len(maximum[key]))
            result[name+'.sha256'] = sha(maximum[key]).hex()
    return ('# Independent Target quota grant Control and signed activation vectors.\n'
            + ''.join(key+'='+value+'\n' for key, value in result.items())).encode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    path = ROOT / 'src/test/resources/ndip3/target-quota-grant-vectors.properties'
    raw = expected()
    if args.write:
        path.write_bytes(raw)
    elif path.read_bytes() != raw:
        raise SystemExit('Target quota grant vectors differ from independent encoding')
    print('Target quota grant vectors PASS sha256=' + sha(raw).hex())


if __name__ == '__main__':
    main()
