#!/usr/bin/env python3
"""Independent frozen logical results, allocation attachment and physical audit records."""
import argparse,hashlib,runpy
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
inc=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-quota-incarnation-vectors.py'))
u,b,hashed,be,repeat=(inc[k] for k in ['u','b','hashed','be','repeat'])
capacity=inc['base']['capacity']
sha=lambda raw:hashlib.sha256(raw).hexdigest()
crc32c=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-identity-vectors.py'))['crc32c']
command_prefix=b'\x01'+inc['SHARD']+bytes.fromhex('019a1122334477778877665544332211')
COMMAND=command_prefix+be(crc32c(command_prefix),4)
MID=repeat(32,0x88)
def expected():
    target=inc['incarnation'](True);shard=inc['incarnation'](False)
    source=inc['source'](2);stamp=inc['stamp'](2)
    outcome=be(1,4)+b'\x01'+be(0,4)+be(2,4)+be(3,8)+b'\x01'+be(len(source),4)+source
    tuple_bytes=u(1,1)+u(2,1)+u(3,1)+u(4,1)+u(5,2)
    command_payload=be(2,4)+be(len(tuple_bytes),4)+tuple_bytes+repeat(32,0x99)+be(len(outcome),4)+outcome
    author=b(2,b(1,repeat(32,0x11))+b(2,repeat(32,0x22))+b(3,repeat(32,0x33)))
    system_payload=be(1,4)+MID+repeat(32,0xaa)+be(1,4)+be(1000,8)+b'\x01'+be(0,4)+be(len(author),4)+author+be(len(source),4)+source
    allocation=inc['incarnation'](True,stamp)[0]
    def record(kind,logical,owner,payload,mutation=stamp,first=None,allocated=None):
        raw=u(1,1)+u(2,kind)+b(3,logical)+b(4,owner)+b(5,inc['TENANT'])+b(6,inc['account']['accounting']())+b(7,inc['LINEAGE'])+b(8,mutation)+b(9,payload)
        if first is not None:raw+=b(10,first[-32:])
        if allocated is not None:raw+=b(11,allocated)
        return hashed(raw,12,'target-result-record')
    command=record(1,COMMAND,target[2],command_payload)
    result=record(2,COMMAND,target[2],outcome,first=command)
    system=record(3,MID,shard[2],system_payload,allocated=allocation)
    position=record(4,COMMAND,shard[2],COMMAND,first=command)
    duplicate=record(4,COMMAND,shard[2],COMMAND,mutation=inc['stamp'](3),first=command)
    system_position=record(5,MID,shard[2],MID,first=system)
    rows={'command.id':COMMAND.hex(),'system.id':MID.hex(),'command.payload':command_payload.hex(),'result.payload':outcome.hex(),'system.payload':system_payload.hex(),'allocation':allocation.hex()}
    for name,raw,key,cls in [
        ('command',command,b'\x06\x01'+COMMAND,14),('result',result,b'\x07\x01'+COMMAND,9),
        ('system',system,b'\x08\x01'+MID,9),('position',position,b'\x09\x01'+source,14),
        ('duplicate',duplicate,b'\x09\x01'+inc['source'](3),14),('systemPosition',system_position,b'\x09\x01'+source,14)]:
        rows[name]=raw.hex();rows[name+'.key']=key.hex();rows[name+'.charge']=capacity({cls:1,cls+1:len(key)+len(raw)+12+32}).hex()
    command_fee=43+len(command)+12+32
    result_fee=43+len(result)+12+32
    position_fee=2+len(source)+len(position)+12+32
    duplicate_fee=2+len(inc['source'](3))+len(duplicate)+12+32
    rows['audit.target']=capacity({9:1,10:result_fee,14:1,15:command_fee}).hex()
    rows['audit.shard']=capacity({14:2,15:position_fee+duplicate_fee}).hex()
    rows['audit.primary']=capacity({9:1,10:result_fee,14:3,15:command_fee+position_fee+duplicate_fee}).hex()
    rows['audit.encodedBytes']=str(43+len(command)+12+43+len(result)+12+2+len(source)+len(position)+12
                                 +2+len(inc['source'](3))+len(duplicate)+12
                                 +len(target[1])+len(target[0])+12+len(shard[1])+len(shard[0])+12)
    return ('# Independent immutable Target results and physical audits.\n'+''.join(k+'='+v+'\n' for k,v in rows.items())).encode('ascii')
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write',action='store_true');args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-result-vectors.properties';raw=expected()
    if args.write:path.write_bytes(raw)
    if path.read_bytes()!=raw:raise SystemExit('Target result vectors mismatch')
    print('Target result vectors PASS sha256='+sha(raw))
if __name__=='__main__':main()
