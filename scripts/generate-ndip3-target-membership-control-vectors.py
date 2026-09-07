#!/usr/bin/env python3
"""Independent membership policy, Control request/body and System Mutation identity vectors."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parent.parent
previous = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-membership-vectors.py'))
base, compat = previous['base'], previous['compat']
b, u, hashed, repeat = previous['b'], previous['u'], previous['hashed'], previous['repeat']
sha = lambda x: hashlib.sha256(x).digest()


def policy(profile, dispatch, controls):
    raw = u(1, 1) + b(2, repeat(32, 0x70)) + b(3, profile) + b(4, dispatch[-32:])
    raw += b(5, dispatch) + b(6, controls) + u(7, 1) + b(8, repeat(32, 0x74))
    return hashed(raw, 9, 'nereus-delay-target-membership-policy')


def registration(profile, dispatch, controls, policy):
    return u(1, 1) + b(2, repeat(32, 0x70)) + b(3, profile) + b(4, dispatch) + b(5, dispatch) + b(6, controls) + b(7, policy[-32:]) + b(8, repeat(32, 0x72))


def control(policy, value, issue=True, maximum=False):
    kind, control_kind = (16, 15) if issue else (17, 16)
    operation = repeat(32, 0x72 if issue else 0x75)
    request = b(1, policy) + b(2, value) + (b'' if issue else b(3, u(1, 2)))
    outer = b(kind, request)
    request_hash = sha(b'nereus-delay-control-request\0' + kind.to_bytes(2, 'big') + base['u32'](len(outer)) + outer)
    ref = b(1, operation) + b(2, request_hash) + u(3, 0)
    semantic_hash = sha(b'nereus-delay-target-membership-control\0' + control_kind.to_bytes(2, 'big') + request)
    until = base['I64'] if maximum else 500
    body = b(1, b(1, base['route']) + u(2, 3)) + u(2, 1) + u(3, until)
    body += b(10, ref) + u(11, control_kind) + u(12, 1) + b(13, semantic_hash)
    body += (b'' if issue else u(14, 0)) + b(15, b(control_kind, request))
    logical = sha(b'nereus-delay-control-target-logical-id\0' + operation + base['u32'](0) + control_kind.to_bytes(2, 'big'))
    mh = sha(b'nereus-delay-system-mutation-hash\0' + b'\x01' + base['u32'](1)*3 + b'\x00\x01' + base['shard'] + base['u64'](until) + base['u32'](len(body)) + body)
    mid = sha(b'nereus-delay-system-mutation-id' + b'\x00\x01' + base['u32'](32) + logical + base['shard'] + mh)
    return {'request':request,'outer':outer,'requestHash':request_hash,'ref':ref,'body':body,'semanticHash':semantic_hash,'logical':logical,'mutationHash':mh,'mutationId':mid}


def expected():
    values = previous['properties'](compat['expected']())
    result = {}
    for name in ['kafka.baseline','kafka.receipt','pulsar.baseline','pulsar.journal']:
        target = sha(b'nereus-delay-target-partition\0' + bytes.fromhex(values[name.split('.')[0]+'.target']))
        scope = compat['scope'](target, base['shard'], [(1,repeat(32,0x11)),(3,repeat(32,0x33))], [repeat(32,0x44),repeat(32,0x55)])
        dispatch = bytes.fromhex(values[name+'.dispatch'])
        p = policy(base['profile'](), dispatch, scope)
        result[name+'.policy'] = p.hex()
        result[name+'.key'] = (b'\x10\x01'+p[-32:]).hex()
    reg = registration(base['profile'](), dispatch, scope, p)
    result['registration'] = reg.hex()
    result['policy.value'] = compat['envelope'](23,p).hex()
    for label, value, issue in [('issue',reg,True),('close',repeat(32,0x76),False)]:
        for name, raw in control(p,value,issue).items(): result[label+'.'+name] = raw.hex()
    evidence = compat['resource'](2,b'x'*256,bytes(range(32)),b'x'*(1<<20),base['I64'])
    dispatch = compat['dispatch'](target,compat['capability'](2,3,7,evidence,maximum=True),maximum=True)
    scope = compat['scope'](target,base['shard'],[(1,n.to_bytes(32,'big')) for n in range(1,33)], [n.to_bytes(32,'big') for n in range(33,65)])
    p = policy(base['profile'](maximum=True),dispatch,scope)
    reg = registration(base['profile'](maximum=True),dispatch,scope,p)
    c = control(p,reg,maximum=True)
    for name, raw in [('policy',p),('request',c['request']),('body',c['body'])]:
        result['maximum.'+name+'.length'] = str(len(raw)); result['maximum.'+name+'.sha256'] = hashlib.sha256(raw).hexdigest()
    return ('# NDIP-3 B2 membership policy/control schema 1; independent Python vectors.\n'+''.join(k+'='+v+'\n' for k,v in result.items())).encode('ascii')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write',action='store_true');args=parser.parse_args()
    output=ROOT/'src/test/resources/ndip3/target-membership-control-vectors.properties';value=expected()
    if args.write: output.write_bytes(value)
    elif output.read_bytes()!=value: raise SystemExit('NDIP-3 membership policy/control vectors differ from independent encoding')
    print('NDIP-3 membership policy/control vectors PASS sha256='+hashlib.sha256(value).hexdigest())


if __name__=='__main__': main()
