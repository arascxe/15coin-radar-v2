"""Automatically check exchange's PUBLIC chain contract metadata against 15 configured contracts.
Only Gate and Bitget documented metadata endpoints used. MEXC/other markets fail closed.
A symbol or pair existing on an exchange is not sufficient evidence of token identity.
"""
from __future__ import annotations
from .streams import http_json
from .core import stamp


def matching_chain(chain_name,expected):
    u=str(chain_name).upper().replace(' ','').replace('-','').replace('_','')
    if expected=='bsc':
        return u in {'BSC','BEP20','BNBSMARTCHAIN','BNBCHAIN','BNBBEP20'}
    if expected=='base':return u in {'BASE','BASEMAINNET','BASEETH'}
    return False

def extract_contracts(entries,expected):
    matched=[];seen=[]
    for x in entries or []:
        network=x.get('chain',x.get('name',x.get('chainName','')))
        address=x.get('contractAddress',x.get('addr',x.get('contract_address','')))
        seen.append({'network':network,'contract':address})
        if matching_chain(network,expected) and isinstance(address,str) and address.startswith('0x') and len(address)==42:
            matched.append(address.lower())
    return matched,seen

def gate_match(c,pair,fetch=http_json):
    symbol=c['symbol']; expected=c['token'].lower()
    q=fetch('https://api.gateio.ws/api/v4/spot/currency_pairs/'+pair)
    if not isinstance(q,dict) or q.get('base')!=symbol or q.get('quote') not in ('USDT','USDC'):
        return {'status':'GATE_PAIR_MISMATCH','pair_response':q}
    x=fetch('https://api.gateio.ws/api/v4/spot/currencies/'+symbol)
    got,networks=extract_contracts(x.get('chains',[]),c['chain'])
    # Older API may use a single contract_address, but network identity would be ambiguous:
    # do not trust this fallback as an automatic full verification.
    return {'status':'VERIFIED_SAME_CONTRACT_NETWORK' if expected in got else 'CONTRACT_NOT_MATCHED_OR_UNPUBLISHED',
            'expected_contract':expected,'published_contracts_on_expected_chain':got,
            'chain_metadata':networks,'source':'OFFICIAL_GATE_PUBLIC_CURRENCY_CHAIN','pair':pair}

def bitget_match(c,pair,fetch=http_json):
    symbol=c['symbol'];expected=c['token'].lower()
    inst=fetch('https://api.bitget.com/api/v3/market/instruments',{'category':'SPOT','symbol':pair})
    if str(inst.get('code'))!='00000':return {'status':'BITGET_INSTRUMENT_ERROR'}
    instruments=inst.get('data',[])
    matches=[z for z in instruments if z.get('symbol')==pair and
             str(z.get('baseCoin',z.get('baseCurrency',symbol))).upper()==symbol]
    if not matches:return {'status':'BITGET_PAIR_NOT_VERIFIED'}
    meta=fetch('https://api.bitget.com/api/v3/market/coins',{'coin':symbol})
    if str(meta.get('code'))!='00000':return {'status':'BITGET_COIN_META_ERROR'}
    all_coins=meta.get('data',[])
    got=[];network_details=[]
    for coin in all_coins:
        if coin.get('coin')!=symbol:continue
        a,b=extract_contracts(coin.get('chains',[]),c['chain'])
        got.extend(a);network_details.extend(b)
    return {'status':'VERIFIED_SAME_CONTRACT_NETWORK' if expected in got else 'CONTRACT_NOT_MATCHED_OR_UNPUBLISHED',
            'expected_contract':expected,'published_contracts_on_expected_chain':got,
            'chain_metadata':network_details,'source':'OFFICIAL_BITGET_PUBLIC_COIN_CHAINS','pair':pair}

def verify_all(config,fetch=http_json):
    bysym={x['symbol']:x for x in config['coins']}
    statuses={};proofs={}
    for x in config['venues']:
        if x['venue'] not in ('gate','bitget') or not x.get('pair'):continue
        key=x['coin']+'|'+x['venue']
        try:
            proof=(gate_match(bysym[x['coin']],x['pair'],fetch)
                   if x['venue']=='gate' else bitget_match(bysym[x['coin']],x['pair'],fetch))
        except Exception as e:
            proof={'status':'ERROR_SOURCE_UNAVAILABLE','error':str(e)[:120]}
        statuses[key]=proof['status'];proofs[key]=proof
    return {'checked_ms':stamp(),'statuses':statuses,'proofs':proofs,
        'note':'Official chain contract equality verifies token identity, not the owner of an exchange hot wallet.'}
