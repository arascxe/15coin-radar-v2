"""Token inventory analysis from verified contiguous chain Transfer events.
Attributed exchange ownership is an *external evidence input*, never guessed.
Transfers from a CEX labelled address to an unknown address are not a proven withdrawal.
"""
from collections import defaultdict
from decimal import Decimal
from .core import dec
EXCLUDE={'dex_pool','dex_vault','pool_address_unverified','platform_proxy','large_contract_unclassified'}

def entity_map(config):
    groups={};roles={};names={}
    for x in config.get('wallets',[]):
        key=(x['coin'],x['chain'],x['address'].lower())
        role=x.get('role','unknown')
        roles[key]=role
        if role=='exchange_custody':names[key]=x.get('label','UNVERIFIED_EXCHANGE')
    return roles,names

def analyze(config,tail,verified_entities=None,codeproofs=None,balanceproofs=None):
    """verified_entities: mapping 'chain|address' -> {entity, proof:'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC',...}
    codeproofs, balanceproofs must be explicit current RPC facts; omitted means unknown.
    """
    verified_entities=verified_entities or {};codeproofs=codeproofs or {};balanceproofs=balanceproofs or {}
    roles,names=entity_map(config)
    chains=tail.get('chains',{})
    coins={x['symbol']:{'coin':x['symbol'],'chain':x['chain'],'status':'UNKNOWN',
        'verified_cex_external_net_raw':'0','candidate_cex_out_raw':'0',
        'verified_retained_eoa_count':0,'independent_recipients':[],
        'candidate_movements':[],'blocked':[]} for x in config['coins']}
    evidence=defaultdict(lambda:defaultdict(int))
    rows=defaultdict(list)
    for x in tail.get('events',[]):
        sym=x['coin'];chain='bsc' if sym!='TRUST' else 'base'
        z=coins[sym];frm=x['from'].lower();to=x['to'].lower()
        raw=int(x['raw']);a=(sym,chain,frm);b=(sym,chain,to)
        # Stop processing pool trade transfers as CEX external custody.
        if roles.get(a) in EXCLUDE or roles.get(b) in EXCLUDE:continue
        src=verified_entities.get(chain+'|'+frm)
        dst=verified_entities.get(chain+'|'+to)
        if src and src.get('proof')!='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':src=None
        if dst and dst.get('proof')!='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':dst=None
        if src:
            if dst and dst['entity']==src['entity']:
                continue  # exchange internal transfer, NOT a real net exit
            if dst and dst['entity']!=src['entity']:
                continue  # CEX-to-CEX movement does NOT imply supply removed
            evidence[sym][to]+=raw
            rows[(sym,to)].append(x)
        elif roles.get(a)=='exchange_custody' and dst is None:
            z['candidate_cex_out_raw']=str(int(z['candidate_cex_out_raw'])+raw)
            if len(z['candidate_movements'])<4:
                z['candidate_movements'].append({'tx':x['tx'],'from':frm,'to':to,
                    'raw':str(raw),'classification':'UNVERIFIED_CEX_LABEL_AND_DESTINATION'})
    for c in config['coins']:
        sym=c['symbol'];z=coins[sym];chain=c['chain'];ch=chains.get(chain,{})
        if ch.get('status')!='FRESH_CONTIGUOUS_TAIL':
            z['blocked'].append('NO_FRESH_COMPLETE_TAIL');continue
        if not ch.get('complete'):
            z['blocked'].append('CHAIN_COVERAGE_GAP');continue
        for to,amt in evidence[sym].items():
            code=codeproofs.get(chain+'|'+to)
            bals=balanceproofs.get(sym+'|'+to)
            if code!='EOA_ETH_GETCODE_EMPTY_AT_CONFIRMED_BLOCK':
                z['blocked'].append('DESTINATION_NOT_PROVEN_EOA');continue
            if not (bals and bals.get('reconciled') is True and
                    bals.get('two_distinct_finalized_block_snapshots') is True and
                    bals.get('source_balance_reconciled') is True and
                    int(bals.get('retained_raw',0))>=amt*7//10):
                z['blocked'].append('CEX_SOURCE_AND_EOA_RETENTION_NOT_JOINTLY_PROVED');continue
            if not all(isinstance(x.get('block_ms'),int) for x in rows[(sym,to)]):
                z['blocked'].append('MISSING_SOURCE_BLOCK_TIMESTAMPS');continue
            if len({x['tx'] for x in rows[(sym,to)]})<2:
                z['blocked'].append('ONLY_ONE_TRANSFER');continue
            z['independent_recipients'].append({'address':to,'raw':str(int(bals['retained_raw'])),
                'last_block_ms':max(x['block_ms'] for x in rows[(sym,to)]),
                'source_net_decrease_raw':bals['source_net_decrease_raw'],
                'source_balance_reconciled':True,
                'txs':sorted({x['tx'] for x in rows[(sym,to)]})})
        z['verified_retained_eoa_count']=len(z['independent_recipients'])
        z['verified_cex_external_net_raw']=str(sum(int(r['raw']) for r in z['independent_recipients']))
        z['status']='VERIFIED_RETENTION_PREPARATION' if z['independent_recipients'] else (
            'UNVERIFIED_EXCHANGE_MOVEMENTS' if z['candidate_movements'] else 'NO_POSITIVE_EVIDENCE_OBSERVED')
        if not verified_entities:z['blocked'].append('NO_VERIFIED_CEX_ENTITY_CLUSTERS_PROVIDED')
    return {'schema':'radar-v2-inventory-verification-1','coins':list(coins.values()),
        'note':'Transfer events and candidate labels cannot establish beneficial ownership or purchase.'}
