#!/usr/bin/env python3
"""Independent frozen Message payload ownership and lifecycle vectors."""
import argparse, hashlib, runpy
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
q=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-quota-accounting-vectors.py'))
binding=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-binding-channel-vectors.py'))
base=q['base']
u,b,hashed,capacity,repeat,be=(base[k] for k in ['u','b','hashed','capacity','repeat','be'])
sha=lambda raw:hashlib.sha256(raw).digest()
LINEAGE=repeat(16,0xcc)

def source(n): return binding['identity']['kafka_source'](binding['route'],6+n)
def stamp(n): return u(1,n)+b(2,source(n))+b(3,repeat(32,0x66))
def floor(n):
    rawsource=b(1,b(1,binding['route'])+b(2,b'source-cluster')+b(3,bytes.fromhex('00112233445566778899aabbccddeeff'))+u(4,3)+u(5,6+n)+u(6,4)+u(7,90 if n==1 else 110))
    raw=b(1,LINEAGE)+b(2,repeat(16,0xd0+n))+b(3,repeat(32,0xdd))+u(4,n)+b(5,rawsource)+u(6,n)
    return hashed(raw,8,'recovery-floor-ref')
def object_ref():
    parts=[b'container',b'key',b'version',b'etag']
    return be(2,4)+repeat(32,0x61)+b''.join(be(len(p),4)+p for p in parts)+be(7,8)+sha(b'payload')+repeat(32,0x63)+repeat(32,0x64)
def expected():
    values=dict(line.split('=',1) for line in binding['expected']().decode().splitlines() if '=' in line)
    compat=dict(line.split('=',1) for line in binding['compat']['expected']().decode().splitlines() if '=' in line)
    target=sha(b'nereus-delay-target-partition\0'+bytes.fromhex(compat['pulsar.target']))
    owner=hashed(u(1,1)+u(2,1)+b(3,binding['shard'])+b(4,repeat(16,1))+b(5,target),7,'target-quota-identity')
    result={'owner':owner.hex(),'key':(b'\x19\x01'+binding['message']).hex(),'committed':object_ref().hex(),'floor2':floor(2).hex(),'floor3':floor(3).hex()}
    variants=[('inline.active','best',1,2,1,False),('inline.retained','best',1,3,2,False),('inline.released','best',1,4,3,False),
              ('object.direct','object',2,2,1,True),('object.reserved','prepare',2,1,1,False),('object.active','prepare',2,2,2,True),
              ('object.retained','prepare',2,3,3,True),('object.released','prepare',2,4,4,True),('object.expired','prepare',2,3,2,False),('object.abandoned','prepare',2,4,3,False)]
    for name,kind,wire,phase,n,committed in variants:
        raw=u(1,1)+b(2,binding['message'])+b(3,owner)+b(4,repeat(32,0x44))+b(5,q['accounting']())+b(6,bytes.fromhex(values['binding.'+kind])[-32:])
        raw+=u(7,wire)+u(8,7)+b(9,sha(b'payload'))
        if wire==2: raw+=b(10,repeat(32,0x63))+b(11,repeat(32,0x61))
        if committed: raw+=b(12,object_ref())
        raw+=u(13,phase)+u(14,n)+b(15,stamp(n))+b(16,LINEAGE)
        if phase==4:raw+=b(17,floor(n-1)[-32:])
        raw=hashed(raw,18,'target-quota-payload-owner')
        charge={5:1,6:7} if phase==1 else {1:1,2:7} if phase==2 else {4:7} if phase==3 else {}
        result[name]=raw.hex();result[name+'.payloadCharge']=capacity(charge).hex()
        result[name+'.recordCharge']=capacity({3:43+len(raw)+12+32}).hex()
    # Structural upper-bound sample only; not an authenticated initial binding or source execution trace.
    topic=b'x'*(1 << 20); component=b'y'*(1 << 20); u64=(1 << 64)-1; i64=(1 << 63)-1
    maximum_source=(b'\x02'+binding['route']+be(32,4)+repeat(32,0x77)+be(len(topic),4)+topic+be(3,4)
                    +be(u64,8)+be(u64,8)+be(0,4)+be(1,4)+b'\x01'+be(i64,8))
    maximum_ref=(be(2,4)+repeat(32,0x61)+b''.join(be(len(component),4)+component for _ in range(4))
                 +be(i64,8)+sha(b'payload')+repeat(32,0x63)+repeat(32,0x64))
    raw=u(1,1)+b(2,binding['message'])+b(3,owner)+b(4,repeat(32,0x44))+b(5,q['accounting']())+b(6,bytes.fromhex(values['binding.object'])[-32:])
    raw+=u(7,2)+u(8,i64)+b(9,sha(b'payload'))+b(10,repeat(32,0x63))+b(11,repeat(32,0x61))+b(12,maximum_ref)
    raw+=u(13,2)+u(14,u64)+b(15,u(1,u64)+b(2,maximum_source)+b(3,repeat(32,0x66)))+b(16,LINEAGE)
    raw=hashed(raw,18,'target-quota-payload-owner')
    result['maximum.length']=str(len(raw));result['maximum.sha256']=sha(raw).hex()
    result['maximum.recordCharge']=capacity({3:43+len(raw)+12+32}).hex()
    return ('# Independent Python payload owner and lifecycle vectors.\n'+''.join(k+'='+v+'\n' for k,v in result.items())).encode('ascii')
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write',action='store_true');args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-quota-payload-vectors.properties';raw=expected()
    if args.write:path.write_bytes(raw)
    if path.read_bytes()!=raw:raise SystemExit('Target quota payload vectors mismatch')
    print('Target quota payload vectors PASS sha256='+sha(raw).hex())
if __name__=='__main__':main()
