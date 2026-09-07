#!/usr/bin/env python3
"""Independent Target binding/channel wire vectors. Existing Python encoders are reused; no Java output is read."""
import argparse
import hashlib
from pathlib import Path
import runpy
import uuid

ROOT = Path(__file__).resolve().parent.parent
compat = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-compatibility-vectors.py'))
identity = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-identity-vectors.py'))
b, u, hashed = compat['b'], compat['u'], compat['hashed']
u32, u64 = compat['u32'], compat['u64']
U64, U32, I64, AUX = (1 << 64) - 1, (1 << 32) - 1, (1 << 63) - 1, 1 << 20
route = uuid.UUID('11111111-2222-4333-8444-555555555555').bytes
shard = route + u32(3)
prefix = b'\x01' + shard + uuid.UUID('018e0000-0000-7000-8000-000000000001').bytes
message = prefix + u32(identity['crc32c'](prefix))


def repeat(n, x): return bytes([x]) * n

def profile(kind=1, maximum=False):
    return b(1, b'x' * 256 if maximum else b'dest' if kind == 1 else b'obj') + u(2, U64 if maximum else 1) + b(3, repeat(32, 0x61)) + u(4, kind)


def channel(target, dispatch, controls, kind, maximum=False, generation=1):
    slot, domain_gen, channel_slot = (63, U64, U32) if maximum else (0, 1, 0)
    generation = U64 if maximum else generation
    producer_scope = b(1, shard) + b(2, target) + u(3, slot) + u(4, domain_gen) + b(5, repeat(16, 1))
    producer_scope += u(6, kind) + u(7, channel_slot) + b(8, dispatch) + b(9, controls)
    producer = b'nd-target-' + hashlib.sha256(b'nereus-delay-target-producer\0' + producer_scope).hexdigest().encode()
    context = u(1, 1) + b(2, shard) + b(3, target) + u(4, slot) + u(5, domain_gen) + b(6, repeat(16, 1))
    context += b(7, dispatch) + b(8, controls) + u(9, kind) + u(10, channel_slot) + u(11, generation)
    context += b(12, producer) + b(13, hashlib.sha256(producer).digest())
    if kind in (2, 3): context += u(14, U64 if maximum else 1)
    context += b(15, repeat(32, 0x22))
    holder = hashlib.sha256(b'nereus-delay-credential-holder-target-channel\0' + context).digest()
    clock = u(1, I64-3 if maximum else 100) + u(2, I64-2 if maximum else 101) + u(3, 4 if maximum else 3)
    clock += b(4, b'x'*256 if maximum else b'clock') + u(5, U64 if maximum else 1) + u(6, U64 if maximum else 2)
    clock += u(7, U64 if maximum else 3) + b(8, repeat(32, 0x44)) + u(9, U32 if maximum else 0)
    if maximum: clock += b(10, repeat(64, 0x55))
    lease = u(1, 1) + b(2, profile(maximum=maximum)) + u(3, 1) + b(4, holder)
    lease += u(5, U64 if maximum else 1) + b(6, repeat(32, 0x33)) + b(7, repeat(32, 0x66)) + b(8, clock)
    lease += u(9, I64 if maximum else 1000) + u(10, U64 if maximum else 1)
    lease = hashed(lease, 11, 'nereus-delay-credential-use-lease')
    raw = hashed(context + b(16, lease), 17, 'nereus-delay-target-channel-identity')
    return raw, producer, holder, clock, lease


def intent(kind='best', maximum=False):
    retry = b(1, b'x'*256 if maximum else b'retry') + u(2, U64 if maximum else 1) + b(3, repeat(32, 0x62))
    raw = b(1, profile(maximum=maximum)) + b(2, retry)
    raw += u(3, I64 if maximum else 100) + u(4, I64 if maximum else 200) + u(5, 1) + u(6, 2 if kind == 'strict' else 1)
    raw += b(7, b'x'*AUX if maximum else b'key')
    if kind == 'object':
        descriptor = b(1, profile(3)) + b(2, b'container') + b(3, b'key') + b(4, b'version') + b(5, b'etag')
        descriptor += u(6, 7) + b(7, hashlib.sha256(b'payload').digest()) + b(8, repeat(32, 0x63)) + b(9, repeat(32, 0x64))
        raw += b(9, descriptor)
    elif kind != 'prepare': raw += b(8, b'x'*(1 << 24) if maximum else b'payload')
    metadata = b(2, b(4, b(1, b'p') + b(2, b'x'*(AUX-15)))) if maximum else b(2, b'')
    assert not maximum or len(metadata) == AUX
    raw += b(10, metadata) + b(11, b'x'*AUX if maximum else b'biz') + u(12, I64 if maximum else 99) + u(13, 1)
    raw += u(14, 2 if kind == 'native' else 1)
    return raw


def binding(target, dispatch, control, kind, maximum=False):
    body = b(1, message) + u(2, 2 if kind == 'prepare' else 1) + u(3, I64 if maximum else 500) + b(10, intent(kind, maximum))
    if kind == 'prepare':
        body += u(11, 7) + b(12, hashlib.sha256(b'payload').digest()) + u(13, 1000)
        body += b(14, u(1, 1) + b(2, repeat(32, 0x65))) + b(15, profile(3))
    source = identity['kafka_source'](route, 7)
    if maximum:
        source = b'\x02' + route + u32(32) + bytes(range(32)) + u32(AUX) + b'x'*AUX + u32(3)
        source += u64(U64) + u64(U64) + u32(U32-1) + u32(U32) + b'\x02' + u64(I64)
    raw = u(1, 1) + b(2, message) + u(3, 2 if kind == 'prepare' else 1) + b(4, body) + b(5, source) + b(6, target)
    raw += u(7, 63 if maximum else 0) + u(8, U64 if maximum else 1) + b(9, repeat(16, 1))
    raw += b(10, dispatch) + b(11, dispatch) + b(12, control) + b(13, repeat(32, 0x77))
    if kind == 'native': raw += b(14, repeat(32, 0xEE))
    if kind == 'strict': raw += b(15, repeat(32, 0x88))
    return hashed(raw, 16, 'nereus-delay-target-schedule-binding'), body, source


def expected():
    values = dict(line.split('=', 1) for line in compat['expected']().decode().splitlines() if line and not line.startswith('#'))
    result = {'message.id': message.hex()}
    control = bytes.fromhex(values['scope.shared'])[-32:]
    for name, kind in [('kafka.baseline', 1), ('kafka.receipt', 2), ('pulsar.baseline', 1), ('pulsar.journal', 3)]:
        target = hashlib.sha256(b'nereus-delay-target-partition\0' + bytes.fromhex(values[name.split('.')[0]+'.target'])).digest()
        dispatch = bytes.fromhex(values[name+'.dispatch'])[-32:]
        # Kafka control scope has the same groups and Shard, projected to its own target.
        controls = control if name.startswith('pulsar') else compat['scope'](target, shard, [(1, repeat(32, 0x11)), (3, repeat(32, 0x33))], [repeat(32, 0x44), repeat(32, 0x55)])[-32:]
        raw, producer, holder, clock, lease = channel(target, dispatch, controls, kind)
        result['channel.'+name] = raw.hex()
        result['channel.'+name+'.producer'] = producer.hex()
        result['channel.'+name+'.holder'] = holder.hex()
    target = hashlib.sha256(b'nereus-delay-target-partition\0' + bytes.fromhex(values['pulsar.target'])).digest()
    dispatch = bytes.fromhex(values['pulsar.journal.dispatch'])[-32:]
    for kind in ['best','native','strict','prepare','object']:
        raw, body, source = binding(target, dispatch, control, kind)
        result['binding.'+kind] = raw.hex(); result['body.'+kind] = body.hex()
        result['binding.'+kind+'.key'] = (b'\x06\x01'+raw[-32:]).hex()
    result['source'] = source.hex()
    raw, producer, holder, clock, lease = channel(target, dispatch, control, 3, maximum=True)
    result['channel.maximum'] = raw.hex()
    result['channel.maximum.length'] = str(len(raw))
    result['channel.maximum.sha256'] = hashlib.sha256(raw).hexdigest()
    result['channel.maximum.lease.length'] = str(len(lease))
    result['channel.maximum.time.length'] = str(len(clock))
    raw, body, source = binding(target, dispatch, control, 'strict', maximum=True)
    for name, value in [('binding.maximum',raw),('body.maximum',body)]:
        result[name+'.length'] = str(len(value)); result[name+'.sha256'] = hashlib.sha256(value).hexdigest()
    raw = bytes.fromhex(result['channel.pulsar.journal'])
    result['channel.key'] = (b'\x0e\x01'+raw[-32:]).hex()
    result['channel.value'] = compat['envelope'](20,raw).hex()
    result['binding.value'] = compat['envelope'](21,bytes.fromhex(result['binding.native'])).hex()
    return ('# NDIP-3 B2 Target binding/channel schema 1; independent Python wire vectors.\n' + ''.join(k+'='+v+'\n' for k,v in result.items())).encode('ascii')


def main():
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument('--write',action='store_true'); args=parser.parse_args()
    output=ROOT/'src/test/resources/ndip3/target-binding-channel-vectors.properties'; value=expected()
    if args.write: output.write_bytes(value)
    elif output.read_bytes()!=value: raise SystemExit('NDIP-3 Target binding/channel vectors differ from independent encoding')
    print('NDIP-3 Target binding/channel vectors PASS sha256='+hashlib.sha256(value).hexdigest())


if __name__=='__main__': main()
