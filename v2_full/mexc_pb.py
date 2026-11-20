"""Minimal strict decoder for the three OFFICIAL MEXC Spot Protobuf message schemas:
PushDataV3ApiWrapper (fields 1/3/313/314), PublicAggreDealsV3Api,
PublicAggreDepthsV3Api. Unknown fields are safely ignored as specified by protobuf.
No executable code generation, no untrusted pickle, no trading functionality.
"""
from __future__ import annotations

class DecodeError(ValueError):pass

def _varint(buf,pos):
    val=0
    for shift in range(0,70,7):
        if pos>=len(buf):raise DecodeError('TRUNCATED_VARINT')
        a=buf[pos];pos+=1
        val|=(a&0x7f)<<shift
        if a<128:return val,pos
    raise DecodeError('VARINT_TOO_LONG')

def fields(b):
    if not isinstance(b,bytes) or len(b)>5_000_000:raise DecodeError('INVALID_PB_SIZE')
    pos=0;data={}
    while pos<len(b):
        key,pos=_varint(b,pos);n=key>>3;w=key&7
        if n<=0:raise DecodeError('INVALID_FIELD_NUMBER')
        if w==0:v,pos=_varint(b,pos)
        elif w==2:
            length,pos=_varint(b,pos)
            if length<0 or pos+length>len(b):raise DecodeError('TRUNCATED_BYTES')
            v=b[pos:pos+length];pos+=length
        elif w==1:
            if pos+8>len(b):raise DecodeError('TRUNCATED_FLOAT64')
            v=b[pos:pos+8];pos+=8
        elif w==5:
            if pos+4>len(b):raise DecodeError('TRUNCATED_FLOAT32')
            v=b[pos:pos+4];pos+=4
        else:raise DecodeError('UNSUPPORTED_WIRE_TYPE')
        data.setdefault(n,[]).append(v)
    return data

def v(d,i,default=None):return d.get(i,[default])[0]
def s(x):
    if not isinstance(x,bytes):raise DecodeError('EXPECTED_PB_STRING')
    return x.decode('utf-8','strict')

def decode(b):
    e=fields(b);channel=s(v(e,1));pair=s(v(e,3));sendtime=v(e,6,0)
    if not (channel.startswith('spot@public.aggre.') and pair.isalnum()):
        raise DecodeError('UNEXPECTED_CHANNEL_OR_SYMBOL')
    if 313 in e:
        x=fields(v(e,313))
        def rows(seq):
            out=[]
            for raw in seq:
                y=fields(raw);out.append([s(v(y,1)),s(v(y,2))])
            return out
        return {'type':'depth','channel':channel,'pair':pair,'send_ms':sendtime,
                'asks':rows(x.get(1,[])),'bids':rows(x.get(2,[])),
                'fromVersion':int(s(v(x,4))),'toVersion':int(s(v(x,5)))}
    if 314 in e:
        x=fields(v(e,314));deals=[]
        for raw in x.get(1,[]):
            y=fields(raw)
            typ=v(y,3)
            if typ not in (1,2):raise DecodeError('UNKNOWN_MEXC_TRADE_TYPE')
            deals.append({'price':s(v(y,1)),'quantity':s(v(y,2)),
                          'side':'buy' if typ==1 else 'sell',
                          'ms':int(v(y,4)),'id':s(v(y,5))})
        return {'type':'trade','channel':channel,'pair':pair,'send_ms':sendtime,
                'deals':deals,'direction_semantics':'TRADE_TYPE_1_BUY_2_SELL_OFFICIAL'}
    raise DecodeError('MEXC_OTHER_MESSAGE_TYPE')
