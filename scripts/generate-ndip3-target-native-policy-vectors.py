#!/usr/bin/env python3
"""Independent Target Native schema, SHA-256 and Ed25519 golden vectors (Python standard library only)."""
import argparse
import hashlib
from pathlib import Path
import runpy

ROOT=Path(__file__).resolve().parent.parent
prior=runpy.run_path(str(ROOT/'scripts/generate-ndip3-target-membership-vectors.py'))
base,compat=prior['base'],prior['compat']
b,u,hashed,repeat=prior['b'],prior['u'],prior['hashed'],prior['repeat']
sha=lambda v:hashlib.sha256(v).digest()
Q=2**255-19
L=2**252+27742317777372353535851937790883648493
D=-121665*pow(121666,Q-2,Q)%Q
I=pow(2,(Q-1)//4,Q)
SEED=bytes.fromhex('9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60')


def add(p,q):
    x,y=p;xx,yy=q;v=D*x*xx*y*yy%Q
    return ((x*yy+xx*y)*pow(1+v,Q-2,Q)%Q,(y*yy+x*xx)*pow(1-v,Q-2,Q)%Q)


def scalar(p,n):
    result=(0,1)
    while n:
        if n&1:result=add(result,p)
        p=add(p,p);n>>=1
    return result


def basepoint():
    y=4*pow(5,Q-2,Q)%Q
    xx=(y*y-1)*pow(D*y*y+1,Q-2,Q)%Q
    x=pow(xx,(Q+3)//8,Q)
    if (x*x-xx)%Q:x=x*I%Q
    if x&1:x=Q-x
    return x,y


def encode(p):return (p[1]|((p[0]&1)<<255)).to_bytes(32,'little')


def sign(message):
    h=hashlib.sha512(SEED).digest();a=int.from_bytes(h[:32],'little');a=(a&((1<<254)-8))|(1<<254)
    public=encode(scalar(basepoint(),a))
    r=int.from_bytes(hashlib.sha512(h[32:]+message).digest(),'little')%L
    R=encode(scalar(basepoint(),r))
    k=int.from_bytes(hashlib.sha512(R+public+message).digest(),'little')%L
    return public,R+((r+k*a)%L).to_bytes(32,'little')


def artifacts(maximum=False):
    lock=sha(b'nereus/delay-resource-guard@0a2536484cd3932801a98dc88ff112b2df88a1c7')
    raw=u(1,1)+b(2,repeat(32,0x81))+b(3,lock)+u(4,base['U64'] if maximum else 7)+u(5,1)+u(6,2)+u(7,2)
    return hashed(raw,8,'nereus-delay-target-native-artifacts')


def scope(target,dispatch,controls,maximum=False):
    shard=base['route']+(base['u32'](base['U32']) if maximum else base['u32'](3))
    raw=u(1,1)+b(2,repeat(32,0x82))+b(3,repeat(32,0x74))+b(4,shard)+b(5,target)+b(6,repeat(16,1))
    raw+=u(7,63 if maximum else 0)+u(8,base['U64'] if maximum else 1)+b(9,dispatch)+b(10,controls)
    raw+=u(11,base['I64'] if maximum else 60000)+u(12,1)+b(13,artifacts(maximum))
    return hashed(raw,14,'nereus-delay-target-native-policy-scope')


def clock(maximum=False):
    raw=u(1,base['I64']-3 if maximum else 100)+u(2,base['I64']-2 if maximum else 101)+u(3,4 if maximum else 3)
    raw+=b(4,b'x'*256 if maximum else b'clock')+u(5,base['U64'] if maximum else 1)+u(6,base['U64'] if maximum else 2)
    raw+=u(7,base['U64'] if maximum else 3)+b(8,repeat(32,0x44))+u(9,base['U32'] if maximum else 0)
    return raw+(b(10,repeat(64,0x55)) if maximum else b'')


def snapshot(scope,mode,generation=1,maximum=False):
    lead=0 if mode==1 else base['I64'] if maximum else 30000
    paths=0 if mode==1 else 1
    key=base['U32'] if maximum else 9
    start,end=(base['I64']-1,base['I64']) if maximum else (1000,120000)
    raw=u(1,2)+b(2,scope[-32:])+u(3,base['U64'] if maximum else generation)+u(4,mode)+u(5,lead)
    raw+=u(6,start)+u(7,end)+u(8,paths)+b(9,clock(maximum))+u(10,key)+b(11,artifacts(maximum)[-32:])
    digest=sha(b'nereus-delay-target-native-policy-snapshot\0'+raw)
    signature_input=b'nereus-delay-target-native-policy-snapshot-signature\0'+digest+base['u32'](key)
    public,signature=sign(signature_input)
    return raw+b(12,digest)+b(13,signature),digest,signature_input,public


def head(snapshot,until):return hashed(u(1,2)+b(2,snapshot)+u(3,until),4,'nereus-delay-target-native-policy-head')


def expected():
    # RFC 8032 test 1 checks the independent signer before creating project vectors.
    public,sig=sign(b'')
    assert public.hex()=='d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a'
    assert sig.hex()=='e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b'
    values=prior['properties'](compat['expected']());result={'issuer.public':public.hex(),'issuer.seed':SEED.hex(),'artifacts':artifacts().hex()}
    target=sha(b'nereus-delay-target-partition\0'+bytes.fromhex(values['pulsar.target']))
    control=bytes.fromhex(values['scope.shared'])[-32:]
    for name in ['pulsar.baseline','pulsar.journal']:
        s=scope(target,bytes.fromhex(values[name+'.dispatch'])[-32:],control)
        result[name+'.scope']=s.hex();result[name+'.key']=(b'\x11\x01'+s[-32:]).hex()
    for mode,name in [(1,'disabled'),(2,'shadow'),(3,'enabled')]:
        snap,digest,preimage,_=snapshot(s,mode)
        result[name+'.snapshot']=snap.hex();result[name+'.snapshotDigest']=digest.hex();result[name+'.signatureInput']=preimage.hex()
        result[name+'.key']=(b'\x12\x01'+digest).hex()
        result[name+'.head']=head(snap,120000 if mode==3 else 0).hex()
    snap,digest,_,_=snapshot(s,1,generation=2)
    result['disabled.after.enabled.head']=head(snap,120000).hex()
    result['enabled.headRef']=(b(1,s[-32:])+u(2,1)+b(3,bytes.fromhex(result['enabled.snapshotDigest']))+u(4,5)).hex()
    result['scope.value']=compat['envelope'](24,s).hex();result['snapshot.value']=compat['envelope'](25,bytes.fromhex(result['enabled.snapshot'])).hex()
    s=scope(target,bytes.fromhex(values['pulsar.journal.dispatch'])[-32:],control,True)
    snap,digest,preimage,_=snapshot(s,3,maximum=True)
    for name,raw in [('artifacts',artifacts(True)),('scope',s),('snapshot',snap),('head',head(snap,base['I64']))]:
        result['maximum.'+name]=raw.hex();result['maximum.'+name+'.length']=str(len(raw));result['maximum.'+name+'.sha256']=sha(raw).hex()
    return ('# NDIP-3 B3 Target common Native policy; independent Python/RFC 8032 vectors.\n'+''.join(k+'='+v+'\n' for k,v in result.items())).encode('ascii')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--write',action='store_true');args=parser.parse_args()
    path=ROOT/'src/test/resources/ndip3/target-native-policy-vectors.properties';value=expected()
    if args.write:path.write_bytes(value)
    elif path.read_bytes()!=value:raise SystemExit('Target Native policy vectors differ from independent encoding')
    print('Target Native policy vectors PASS sha256='+sha(value).hex())


if __name__=='__main__':main()
