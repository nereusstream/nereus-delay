#!/usr/bin/env python3
"""Independent frozen Target Claim charge projection, without Java serializers."""
import argparse, hashlib, runpy
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
q=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-quota-accounting-vectors.py'))
i=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-identity-vectors.py'))
u,b,hashed,capacity,repeat,be=(q['base'][k] for k in ['u','b','hashed','capacity','repeat','be'])
sha=lambda raw:hashlib.sha256(raw).hexdigest()
def expected():
    iv=dict(line.split('=',1) for line in i['expected_vectors']().splitlines() if '=' in line)
    get=lambda k:bytes.fromhex(iv[k])
    message=get('message.id');shard=message[1:21]
    identity=hashed(u(1,1)+u(2,1)+b(3,shard)+b(4,bytes(range(1,17)))+b(5,get('pulsar.id')),7,'target-quota-identity')
    owner=b(1,b'deployment')+b(2,b'worker')+u(3,7)+b(4,repeat(32,0x55))
    # Same physical source and Shard as the work; this is a structural projection, not Claim authority.
    source=i['kafka_source'](message[1:17],8)
    stamp=u(1,2)+b(2,source)+b(3,repeat(32,0x66))+u(4,1)
    key=b'\x1b\x01'+shard+be(7,8)+repeat(32,0x77)
    result={'identity':identity.hex(),'owner':owner.hex(),'creation':stamp.hex(),'key':key.hex()}
    for name,work,seq,cost in [('ordinary',get('work.initial'),1,64),('native',get('work.native'),2,128),('ordered',get('work.fifo.initial'),3,96)]:
        raw=u(1,1)+b(2,repeat(32,0x77))+b(3,work)+b(4,identity)+b(5,repeat(32,0x44))+b(6,q['accounting']())
        raw+=b(7,owner)+b(8,repeat(16,0x88))+u(9,seq)+u(10,1000)+u(11,cost)+b(12,repeat(32,0x99))+b(13,stamp)+b(14,repeat(16,0xcc))
        raw=hashed(raw,15,'target-quota-claim-charge')
        result[name]=raw.hex();result[name+'.execution']=capacity({7:1,8:cost}).hex()
        result[name+'.record']=capacity({3:len(key)+len(raw)+12+32}).hex()
    return ('# Independent Claim charge projections; not business Claim or SEND authority.\n'+''.join(k+'='+v+'\n' for k,v in result.items())).encode('ascii')
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write',action='store_true');args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-quota-claim-vectors.properties';raw=expected()
    if args.write:path.write_bytes(raw)
    if path.read_bytes()!=raw:raise SystemExit('Target quota Claim vectors mismatch')
    print('Target quota Claim vectors PASS sha256='+sha(raw))
if __name__=='__main__':main()
