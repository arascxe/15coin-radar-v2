"""V2 near-tip on-chain economics: balance-proved accumulation, never price prediction.

Two independent evidence paths:
  (A) verified CEX custody loss + retained EOA (existing inventory/proofs).
  (B) newly discovered multi-EOA net accumulation with two current finalized
      balance snapshots, contiguous token logs and source-aware exclusions.
Classification never treats a DEX transfer, bridge, mint or unverified exchange
label as a verified CEX withdrawal or LP withdrawal.
"""
from __future__ import annotations
from collections import Counter,defaultdict
from decimal import Decimal,InvalidOperation
import hashlib
from .core import stamp, D

ZERO='0x'+'0'*40
NO_BUY_SIGNAL=True
INFRA={'dex_pool','dex_vault','pool_address_unverified','platform_proxy',
       'large_contract_unclassified','exchange_custody',
       'base_attribution_unverified','unverified_entity'}
POOL={'dex_pool','dex_vault','pool_address_unverified'}
MAX_NEW_CANDIDATES_PER_COIN=3
MAX_FOLLOW_UP_PER_COIN=2
MEMORY_MS=72*60*60*1000
MAX_TIP_AGE_MS=300000

def _label(config):
    return {(w['coin'],w['chain'],w['address'].lower()):w for w in config.get('wallets',[])
            if isinstance(w.get('address'),str) and len(w['address'])==42}

def _pool_addresses(config):
    return {(p['coin'],p['chain'],p['pool'].lower()) for p in config.get('dex_pools',[])
            if isinstance(p.get('pool'),str) and len(p['pool'])==42}

def classify_transfer(coin,chain,e,labels,pools,verified):
    a=e['from'].lower();b=e['to'].lower()
    sa=verified.get(chain+'|'+a);sb=verified.get(chain+'|'+b)
    if a==ZERO:return 'MINT_OR_ISSUANCE_NOT_ACCUMULATION'
    if b==ZERO:return 'BURN_OR_REDEMPTION_NOT_ACCUMULATION'
    ar=labels.get((coin,chain,a),{}).get('role')
    br=labels.get((coin,chain,b),{}).get('role')
    if ((coin,chain,a) in pools or (coin,chain,b) in pools or
        ar in POOL or br in POOL):
        return 'DEX_SWAP_OR_LP_INTERACTION_UNRESOLVED'
    if ar in ('platform_proxy','large_contract_unclassified') or br in ('platform_proxy','large_contract_unclassified'):
        return 'CONTRACT_OR_ROUTER_INTERACTION_UNRESOLVED'
    if sa and sb:
        if sa.get('entity')==sb.get('entity'):
            return 'VERIFIED_CEX_INTERNAL_TRANSFER_NOT_OUTFLOW'
        return 'VERIFIED_CEX_TO_CEX_TRANSFER_NOT_NET_OFFMARKET'
    if sa and sa.get('proof')=='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':
        return 'VERIFIED_CEX_TO_UNKNOWN_DESTINATION_NOT_PROVEN_WITHDRAWAL'
    if sb and sb.get('proof')=='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':
        return 'UNKNOWN_TO_VERIFIED_CEX_POSSIBLE_DEPOSIT'
    if ar=='exchange_custody' or br=='exchange_custody':
        return 'UNVERIFIED_EXCHANGE_LABEL_ROUTE'
    return 'UNKNOWN_OWNER_TRANSFER'

def classify(config,tail,verified_entities=None):
    """Deduplicate by token+tx+log-index and never sum relay hops as purchases."""
    verified_entities=verified_entities or {}
    labels=_label(config);pools=_pool_addresses(config)
    bycoin={c['symbol']:{'coin':c['symbol'],'chain':c['chain'],
                        'classes':Counter(),'net_by_address':defaultdict(int),
                        'inbound':defaultdict(list),'outbound':defaultdict(list),
                        'transfer_count':0,'unique_transfer_count':0,
                        'events':[]} for c in config['coins']}
    seen=set()
    for e in tail.get('events',[]):
        coin=e.get('coin')
        if coin not in bycoin:continue
        ch=bycoin[coin]['chain']
        if ch!='base' and ch!='bsc':continue
        a=str(e.get('from','')).lower();b=str(e.get('to','')).lower()
        try:
            raw=int(e['raw']);block=int(e['block'])
        except (TypeError,ValueError,KeyError):continue
        if raw<=0 or a==b or len(a)!=42 or len(b)!=42:continue
        key=(coin,e.get('tx'),e.get('log_index'))
        if key in seen:continue
        seen.add(key)
        category=classify_transfer(coin,ch,e,labels,pools,verified_entities)
        z=bycoin[coin];z['classes'][category]+=1
        x={'coin':coin,'chain':ch,'tx':e.get('tx'),'log_index':e.get('log_index'),
           'block':block,'from':a,'to':b,'raw':raw,'classification':category}
        z['events'].append(x);z['transfer_count']+=1
        if a!=ZERO:
            z['net_by_address'][a]-=raw
            z['outbound'][a].append(x)
        if b!=ZERO:
            z['net_by_address'][b]+=raw
            z['inbound'][b].append(x)
    return bycoin,labels,pools

def _clean_eligible(coin,ch,addr,z,labels,pools):
    if addr==ZERO or (coin,ch,addr) in pools:return False
    role=labels.get((coin,ch,addr),{}).get('role')
    return role not in INFRA

def _rpc_at(http,provider,token,addr,block):
    b=hex(int(block))
    code=http.rpc(provider,'eth_getCode',[addr,b])
    if code not in ('0x','0x0'):return None,'CONTRACT_OR_UNKNOWN_CODE'
    data='0x70a08231'+'0'*24+addr[2:]
    val=http.rpc(provider,'eth_call',[{'to':token,'data':data},b])
    if not isinstance(val,str) or not val.startswith('0x'):raise ValueError('INVALID_TOKEN_BALANCE')
    return int(val,16),None

def _supply(http,provider,token,block):
    try:
        val=http.rpc(provider,'eth_call',[{'to':token,'data':'0x18160ddd'},hex(block)])
        n=int(val,16)
        return n if n>0 else None
    except (TypeError,ValueError,RuntimeError,KeyError):
        return None

def _visible_ask(market,coin):
    """Quote-stable visible CEX depth in tokens; NOT aggregate sellable supply."""
    total=D(0);venues=[];thinner=[]
    for m in market.get('markets',[]):
        if m.get('coin')!=coin:continue
        pair=str(m.get('pair','')).upper()
        if not (pair.endswith('USDT') or pair.endswith('USDC')):continue
        b=m.get('book',{})
        if b.get('status')!='VERIFIED_SEQUENCED_BOOK_INTERVAL':continue
        try:
            p=D(str(b['reference_price']))
            q=D(str(b['ask_after_quote']))
            if p<=0 or q<0:continue
            total+=q/p
            venues.append(m['venue'])
            if D(str(b.get('fraction_depleted','0')))>=D('.20'):
                thinner.append(m['venue'])
        except (KeyError,InvalidOperation,ValueError,TypeError,ZeroDivisionError):
            continue
    return {'token_units_within_observed_2pct_ask_band':str(total) if venues else None,
            'venues':sorted(set(venues)),
            'thinning_venues':sorted(set(thinner)),
            'scope':'Observed anchored sequenced CEX visible asks; excludes hidden orders, other venues and all DEXes.'}

def observe(config,tail,market=None,verified_entities=None,prior=None,http=None,now_ms=None):
    """Carry forward real finalized-block EOA snapshots across short-lived runners.

    An existing snapshot is valid for delta proof ONLY when all ERC-20 Transfer
    logs between its block and the current finalized head are contiguous. A
    first snapshot NEVER proves earlier accumulation by itself.
    """
    now=stamp() if now_ms is None else int(now_ms)
    verified_entities=verified_entities or {}
    prior=prior or {}
    old=prior.get('snapshots',{})
    old_episodes=prior.get('episodes',{})
    classified,labels,pools=classify(config,tail,verified_entities)
    snapshots={};episodes={}
    coins=[]
    for c in config['coins']:
        sym=c['symbol'];ch=c['chain'];z=classified[sym]
        meta=tail.get('chains',{}).get(ch,{})
        head=meta.get('safe_head')
        fresh=(meta.get('status')=='FRESH_CONTIGUOUS_TAIL' and
               meta.get('complete') is True and
               isinstance(head,int) and isinstance(meta.get('from_block'),int) and
               isinstance(meta.get('safe_head_ms'),int) and
               0<=now-meta['safe_head_ms']<=MAX_TIP_AGE_MS)
        statuses=Counter(z['classes'])
        byaddr=z['net_by_address']
        inbound=z['inbound']
        flow_ranked=[]
        for a,net in byaddr.items():
            if net<=0 or not _clean_eligible(sym,ch,a,z,labels,pools):continue
            incoming=inbound.get(a,[])
            if not incoming:continue
            if any(x['from']==ZERO for x in incoming):continue
            nonpool=[x for x in incoming if x['classification'] not in
                     ('DEX_SWAP_OR_LP_INTERACTION_UNRESOLVED','CONTRACT_OR_ROUTER_INTERACTION_UNRESOLVED')]
            if not nonpool:continue
            dexpart=sum(x['raw'] for x in incoming if
                        x['classification']=='DEX_SWAP_OR_LP_INTERACTION_UNRESOLVED')
            total=sum(x['raw'] for x in incoming)
            if 2*dexpart>=total:continue
            flow_ranked.append((a,net,incoming))
        flow_ranked.sort(key=lambda x:x[1],reverse=True)
        # Recheck old candidates as well as the largest fresh independent receivers.
        previous_keys=[k for k,v in old.items() if
                       v.get('coin')==sym and v.get('chain')==ch and
                       0<=now-int(v.get('observed_ms',0))<=MEMORY_MS and
                       _clean_eligible(sym,ch,v.get('address',''),z,labels,pools)]
        chosen=[x[0] for x in flow_ranked[:MAX_NEW_CANDIDATES_PER_COIN]]
        previous_keys.sort(key=lambda k:int(old[k].get('observed_ms',0)),reverse=True)
        for k in previous_keys[:MAX_FOLLOW_UP_PER_COIN]:
            addr=old[k]['address']
            if addr not in chosen:chosen.append(addr)
        attempts=Counter()
        for address in chosen[:MAX_NEW_CANDIDATES_PER_COIN+MAX_FOLLOW_UP_PER_COIN]:
            key=sym+'|'+address;prior_read=old.get(key)
            if not fresh or http is None:
                attempts['NO_VERIFIED_FRESH_CHAIN_OR_RPC']+=1
                continue
            try:
                current,reason=_rpc_at(http,meta['provider'],c['token'],address,head)
                if reason:
                    attempts[reason]+=1;continue
                snapshots[key]={'coin':sym,'chain':ch,'address':address,'block':head,
                    'raw':str(current),'observed_ms':now,'proof':'FINALIZED_BALANCE_AND_EOA_CODE'}
                attempts['CURRENT_BALANCE_VERIFIED']+=1
                if not prior_read:continue
                prevblock=prior_read.get('block')
                if not (isinstance(prevblock,int) and prevblock<head and
                        meta['from_block']<=prevblock+1):continue
                # Snapshot delta and EVERY transfer in (old block, current head]
                # must agree exactly. If chain ledger missed any blocks, fail closed.
                rows=[e for e in z['events'] if prevblock<e['block']<=head]
                delta=current-int(prior_read['raw'])
                transfer_net=sum(e['raw'] for e in rows if e['to']==address)
                transfer_net-=sum(e['raw'] for e in rows if e['from']==address)
                if delta<=0 or delta!=transfer_net:continue
                inflow=[e for e in rows if e['to']==address]
                sources={e['from'] for e in inflow if e['from']!=ZERO}
                blocks={e['block'] for e in inflow}
                if len({e['tx'] for e in inflow})<2 or len(blocks)<2 or len(sources)<2:
                    continue
                if any(e['from']==ZERO for e in inflow):continue
                poolamount=sum(e['raw'] for e in inflow
                    if e['classification'] in
                       ('DEX_SWAP_OR_LP_INTERACTION_UNRESOLVED',
                        'CONTRACT_OR_ROUTER_INTERACTION_UNRESOLVED'))
                if poolamount*2>=sum(e['raw'] for e in inflow):continue
                event={'coin':sym,'chain':ch,'address':address,
                    'delta_raw':str(delta),'retained_post_raw':str(current),
                    'first_block':prevblock,'last_block':head,
                    'last_observed_ms':now,
                    'source_addresses':sorted(sources),
                    'inbound_transactions':len({e['tx'] for e in inflow}),
                    'evidence_txs':sorted({e['tx'] for e in inflow})[:8],
                    'proof':'TWO_FINALIZED_EOA_SNAPSHOTS_AND_CONTIGUOUS_TRANSFER_RECONCILIATION',
                    'owner_verified':False,'cex_outflow_verified':False}
                episodes[key]=event
                attempts['NET_DELTA_AND_INBOUND_RECONCILED']+=1
            except Exception as e:
                attempts['RPC_READ_FAIL_'+type(e).__name__]+=1
                if prior_read:snapshots[key]=prior_read
        for key,episode in old_episodes.items():
            if episode.get('coin')!=sym or key in episodes:continue
            if not (0<=now-int(episode.get('last_observed_ms',0))<=MEMORY_MS):
                continue
            newer=snapshots.get(key)
            if newer and fresh and int(newer['raw'])>=int(episode['retained_post_raw'])*7//10:
                episodes[key]={**episode,'last_reverified_ms':now,
                               'retained_current_raw':newer['raw']}
        accepted=[e for e in episodes.values() if e['coin']==sym and
                  0<=now-int(e.get('last_reverified_ms',e.get('last_observed_ms',0)))<=MAX_TIP_AGE_MS]
        # Prevent a single sender distributing across two of its own wallets
        # from being called "independent cluster accumulation".
        totalsources=set().union(*(set(e['source_addresses']) for e in accepted)) if accepted else set()
        two_receiver_cluster=(len(accepted)>=2 and len(totalsources)>=3 and
                              all(len(e['source_addresses'])>=2 for e in accepted))
        amount=sum(int(e['delta_raw']) for e in accepted)
        supply=_supply(http,meta['provider'],c['token'],head) if fresh and http is not None and (chosen or accepted) else None
        ask=_visible_ask(market or {},sym)
        ratio_supply=str(D(amount)/D(supply)) if supply and amount else None
        ratio_ask=None
        if ask['token_units_within_observed_2pct_ask_band'] and amount:
            try:
                denominator=D(ask['token_units_within_observed_2pct_ask_band'])
                if denominator>0:ratio_ask=str((D(amount)/(D(10)**int(c['decimals'])))/denominator)
            except (InvalidOperation,ValueError):pass
        # Transparent illustrative, uncalibrated research materiality tests.
        material=bool((ratio_supply is not None and D(ratio_supply)>=D('.0001')) or
                      (ratio_ask is not None and D(ratio_ask)>=D('.5')))
        unexplained=None
        if fresh and supply:
            for e in sorted(z['events'],key=lambda x:x['raw'],reverse=True):
                if (e['classification']=='UNKNOWN_OWNER_TRANSFER' and
                    e['raw']*1000>=supply and
                    _clean_eligible(sym,ch,e['to'],z,labels,pools)):
                    unexplained={'tx':e['tx'],'block':e['block'],
                        'amount_raw':str(e['raw']),
                        'fraction_of_onchain_total_supply':str(D(e['raw'])/D(supply)),
                        'owner_unknown':True,'no_cex_or_buy_inference':True}
                    break
        if not fresh:research='DATA_GAP_UNKNOWN'
        elif two_receiver_cluster and material:research='CLUSTER_ACCUMULATION_RESEARCH'
        elif two_receiver_cluster:research='CLUSTER_ACCUMULATION_SMALL_OR_MATERIALITY_UNKNOWN'
        elif unexplained:research='UNEXPLAINED_LARGE_TRANSFER_REVIEW'
        else:research='NO_RESEARCH_TRIGGER_OBSERVED'
        coins.append({'coin':sym,'chain':ch,'chain_fresh_contiguous':fresh,
            'chain_scope':'VERIFIED_RECENT_TIP_ONLY_NOT_ALL_HISTORICAL_BLOCKS',
            'classified_transfer_counts':dict(statuses),
            'observed_eoa_snapshot_count':sum(1 for k in snapshots if k.startswith(sym+'|')),
            'balances':dict(attempts),'eligible_followed_addresses':chosen,
            'confirmed_independent_eoa_accumulation_count':len(accepted),
            'recipient_evidence':accepted[:5],
            'multi_receiver_unknown_owner_cluster':two_receiver_cluster,
            'research_state':research,'unknown_owner_not_independent_buyer_proof':True,
            'accumulation_raw':str(amount),'fraction_of_total_onchain_supply':ratio_supply,
            'visible_spot_2pct_ask_token_capacity':ask,
            'accumulation_to_visible_ask_ratio':ratio_ask,
            'unexplained_transfer':unexplained,
            'observed_onchain_total_supply_raw':str(supply) if supply else None,
            'total_supply_not_circulating_supply':True,
            'historical_predictive_precision':None,
            'buy_signal':False})
    return {'schema':'radar-v2-independent-onchain-economics-1',
            'asof_ms':now,'coins':coins,'strong_buy_signal':False,
            'important':'No private wallet beneficial-owner proof, complete CEX stock or future 10x guarantee.'},{
                'schema':'radar-v2-onchain-snapshots-1',
                'asof_ms':now,'snapshots':snapshots,'episodes':episodes}
