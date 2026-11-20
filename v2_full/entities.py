"""Allow only reviewed chain-specific CEX attributions to enter economic proof.

A label alone never implies withdrawal, private ownership or market-maker control.
"""
from __future__ import annotations
import json, pathlib, urllib.parse

APPROVED_PUBLISHERS={'Arkham':'arkm.com','BscScan':'bscscan.com',
                     'BaseScan':'basescan.org','Blockscout':'blockscout.com'}

def load_entities(path):
    raw=json.loads(pathlib.Path(path).read_text(encoding='utf-8'))
    if raw.get('schema')!='radar-v2-cex-entity-attribution-1':
        raise ValueError('CEX_PROVENANCE_SCHEMA_NOT_REVIEWED')
    result={}
    for k,v in raw.get('entities',{}).items():
        chain,sep,addr=k.partition('|')
        if not sep or chain not in ('bsc','base') or not addr.startswith('0x') or len(addr)!=42:
            raise ValueError('INVALID_CHAIN_SCOPED_CEX_ADDRESS')
        if chain!=v.get('chain') or addr!=str(v.get('address','')).lower():
            raise ValueError('CEX_CHAIN_ADDRESS_MISMATCH')
        if not v.get('entity') or v.get('proof')!='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':
            raise ValueError('CEX_PROOF_NOT_QUALIFIED')
        sources=v.get('sources',[])
        publishers=set()
        for s in sources:
            name=s.get('publisher')
            url=s.get('url','')
            parsed=urllib.parse.urlparse(url)
            if (name not in APPROVED_PUBLISHERS or parsed.scheme!='https' or
                parsed.hostname!=APPROVED_PUBLISHERS[name]):
                raise ValueError('CEX_SOURCE_DOMAIN_NOT_INDEPENDENT')
            publishers.add(name)
        if len(publishers)<2 or int(v.get('source_count',0))<2:
            raise ValueError('CEX_REQUIRES_TWO_INDEPENDENT_SOURCES')
        if v.get('withdrawal_verified') or v.get('recipient_beneficial_owner_verified'):
            raise ValueError('ENTITY_ATTRIBUTION_CANNOT_PROVE_WITHDRAWAL')
        result[k]={
            'entity':v['entity'],
            'proof':v['proof'],
            'chain':chain,
            'evidence_urls':[s['url'] for s in sources],
            'withdrawal_verified':False,
            'recipient_beneficial_owner_verified':False
        }
    return result
