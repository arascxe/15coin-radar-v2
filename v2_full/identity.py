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


# Curated historical MEXC *spot* listings: exact spot pair, BEP20 contract
# and MEXC first-party announcement jointly reviewed on 2026-11-20.
# A listing does NOT prove current CEX custody, deposit route, or unchanged code.
# Reject if the live public spot exchangeInfo no longer advertises the pair.
MEXC_OFFICIAL_LISTINGS={
  'DOS':('0xb0f09ea9ae0515c3551080d4a745c8115aa30e37',
         'https://www.mexc.com/announcements/article/first-in-market-17827791537404'),
  'LYN':('0x302dfaf2cdbe51a18d97186a7384e87cf599877d',
         'https://www.mexc.com/announcements/article/initial-listing-mexc-will-list-everlyn-ai-lyn-in-innovation-zone-with-convert-feature-17827791530626'),
  'STAR':('0x8fce7206e3043dd360f115afa956ee31b90b787c',
         'https://www.mexc.com/announcements/article/initial-listing-mexc-will-list-starpower-star-in-innovation-zone-with-convert-feature-17827791529929'),
  'IN':('0x61fac5f038515572d6f42d4bcb6b581642753d50',
         'https://www.mexc.com/announcements/article/initial-listing-mexc-will-list-infinit-in-in-innovation-zone-with-convert-feature-17827791527903'),
  'APR':('0x299ad4299da5b2b93fba4c96967b040c7f611099',
         'https://www.mexc.com/announcements/article/initial-listing-mexc-will-list-apriori-apr-in-innovation-zone-with-convert-feature-17827791531061'),
  'BSB':('0x595deaad1eb5476ff1e649fdb7efc36f1e4679cc',
         'https://www.mexc.com/announcements/article/first-in-market-17827791534007')
}
def mexc_match(c,pair,fetch=http_json):
    symbol=c['symbol']
    listing=MEXC_OFFICIAL_LISTINGS.get(symbol)
    if not listing or c['chain']!='bsc' or listing[0]!=c['token'].lower() or pair!=symbol+'USDT':
        return {'status':'NO_EXACT_OFFICIAL_SPOT_LISTING_PROOF'}
    data=fetch('https://api.mexc.com/api/v3/exchangeInfo',{'symbol':pair})
    instruments=data.get('symbols',[]) if isinstance(data,dict) else []
    live=[x for x in instruments if x.get('symbol')==pair and
          x.get('baseAsset')==symbol and x.get('quoteAsset')=='USDT' and
          str(x.get('status','')).upper() in ('1','TRADING') and
          x.get('isSpotTradingAllowed',True) is not False]
    return {'status':('VERIFIED_OFFICIAL_HISTORICAL_MEXC_SPOT_LISTING_WITH_ACTIVE_PAIR'
                       if live else 'OFFICIAL_LISTING_BUT_PAIR_INACTIVE_OR_UNVERIFIED'),
            'source':listing[1],
            'expected_contract':listing[0],
            'pair':pair,'scope':'Official historical MEXC spot listing; current network deposit or CEX custody NOT proven'}

def verify_all(config,fetch=http_json):
    bysym={x['symbol']:x for x in config['coins']}
    statuses={};proofs={}
    for x in config['venues']:
        if x['venue'] not in ('gate','bitget','mexc') or not x.get('pair'):continue
        key=x['coin']+'|'+x['venue']
        try:
            proof=(gate_match(bysym[x['coin']],x['pair'],fetch)
                   if x['venue']=='gate' else bitget_match(bysym[x['coin']],x['pair'],fetch)
                   if x['venue']=='bitget' else mexc_match(bysym[x['coin']],x['pair'],fetch))
        except Exception as e:
            proof={'status':'ERROR_SOURCE_UNAVAILABLE','error':str(e)[:120]}
        statuses[key]=proof['status'];proofs[key]=proof
    return {'checked_ms':stamp(),'statuses':statuses,'proofs':proofs,
        'note':'Official chain contract equality verifies token identity, not the owner of an exchange hot wallet.'}
