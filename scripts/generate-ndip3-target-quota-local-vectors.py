#!/usr/bin/env python3
"""Independent quota vectors for local Claim/revoke at a fixed applied source frontier."""
import argparse
import hashlib
from pathlib import Path
import runpy
ROOT = Path(__file__).resolve().parent.parent
base = runpy.run_path(str(ROOT / 'scripts/generate-ndip3-target-quota-vectors.py'))
u,b,repeat,be,usage,counter,aggregate,identity = (base[k] for k in ['u','b','repeat','be','usage','counter','aggregate','identity'])
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def expected():
    original = {1:2,2:20,7:1,8:7,9:1,10:64}
    claimed = dict(original)
    claimed.update({3:100,7:2,8:71})
    rows = {}
    for name,seq,ordinal,revision,source,values,digest in [
            ('claim',1,1,2,base['SOURCE'],claimed,0x77),
            ('revoke',1,2,3,base['SOURCE'],original,0x78),
            ('source',2,0,4,base['SOURCE'][:-17]+be(11,8)+b'\0'+be(101,8),{**original,3:1},0x79)]:
        stamp = u(1,seq)+b(2,source)+b(3,repeat(32,digest))+(u(4,ordinal) if ordinal else b'')
        primary = usage(values,1,1,0,1)
        mirror = usage(values,1,0,0,0)
        rows[name+'.mutation'] = stamp.hex()
        rows[name+'.counter'] = counter(identity(1),primary,revision,stamp).hex()
        rows[name+'.mirror'] = counter(identity(2),mirror,revision,stamp).hex()
        rows[name+'.aggregate'] = aggregate(primary,revision,stamp).hex()
    return ('# Independent fixed-source-frontier quota mutation vectors.\n'+''.join(k+'='+v+'\n' for k,v in rows.items())).encode('ascii')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write',action='store_true')
    args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-quota-local-vectors.properties'
    raw=expected()
    if args.write:
        path.write_bytes(raw)
    if path.read_bytes()!=raw:
        raise SystemExit('Target quota local vectors mismatch')
    print('Target quota local vectors PASS sha256='+sha(raw))


if __name__=='__main__':
    main()
