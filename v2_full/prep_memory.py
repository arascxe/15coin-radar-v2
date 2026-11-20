"""Bounded, revalidated inventory-preparation memory across ephemeral GitHub runners.

Evidence lives at most 72h and is rechecked with fresh public RPC each window.
No stale cached preparation can promote an alert. This does not prove beneficial
ownership of an EOA, hidden exchange inventory, or a future price increase.
"""
from __future__ import annotations
from .core import stamp

MAX_AGE_MS=72*60*60*1000

def _balance(http,url,token,address,block):
    data='0x70a08231'+'0'*24+address[2:].lower()
    return int(http.rpc(url,'eth_call',[{'to':token,'data':data},hex(block)]),16)

def revalidate(config,report,chain,previous=None,now=None,http=None):
    """Apply only events with a fresh contiguous chain, positive onchain balance proof."""
    now=stamp() if now is None else now
    previous=previous or {}
    old=previous.get('episodes',{})
    tokens={x['symbol']:x for x in config['coins']}
    episodes={}
    health={}
    for row in report['coins']:
        coin=row['coin'];cfg=tokens[coin];ch=cfg['chain']
        meta=chain.get('chains',{}).get(ch,{})
        safe=(meta.get('status')=='FRESH_CONTIGUOUS_TAIL' and
              meta.get('complete') is True and
              isinstance(meta.get('safe_head'),int) and
              isinstance(meta.get('safe_head_ms'),int) and
              now-meta['safe_head_ms']<=300000 and
              now>=meta['safe_head_ms'])
        if row.get('preparation_proved'):
            recipients=row['coin_inventory'].get('independent_recipients',[])
            for dest in recipients:
                if dest.get('source_balance_reconciled') is not True:continue
                event={
                    'coin':coin,'chain':ch,'recipient':dest['address'].lower(),
                    'source_net_decrease_raw':dest.get('source_net_decrease_raw',{}),
                    'retained_raw':dest['raw'],
                    'baseline_raw':dest.get('baseline_raw'),
                    'last_block_ms':dest['last_block_ms'],
                    'first_verified_ms':now,
                    'last_reverified_ms':now,
                    'last_reverified_block':meta.get('safe_head')}
                key=coin+'|'+event['recipient']
                previous_valid=old.get(key)
                if previous_valid and previous_valid.get('last_block_ms')==event['last_block_ms']:
                    event['first_verified_ms']=int(previous_valid.get('first_verified_ms',now))
                if safe and event['baseline_raw'] is not None and now-event['last_block_ms']<=MAX_AGE_MS:episodes[key]=event
        for key,prior in old.items():
            if key in episodes or prior.get('coin')!=coin:continue
            ts=prior.get('last_block_ms')
            if not isinstance(ts,int) or ts>now or now-ts>MAX_AGE_MS:continue
            if prior.get('chain')!=ch:continue
            address=prior.get('recipient','')
            if not (isinstance(address,str) and len(address)==42 and
                    address.startswith('0x')):continue
            if prior.get('baseline_raw') is None:
                health[coin]='PREVIOUS_EPISODE_MISSING_PRE_TRANSFER_BALANCE';continue
            if not safe or http is None:
                health[coin]='STALE_OR_MISSING_RECHECK_CANNOT_PROMOTE'
                episodes[key]=prior
                continue
            # Never skip proof merely because its clock age is short: the
            # recipient might have sold newly received inventory meanwhile.
            # Reuse a proof only for the EXACT SAME finalized block.
            if prior.get('last_reverified_block')==meta.get('safe_head'):
                episodes[key]=prior
                continue
            try:
                block=meta['safe_head'];url=meta['provider']
                if http.rpc(url,'eth_getCode',[address,hex(block)])!='0x':
                    health[coin]='EOA_CODE_CHECK_FAILED';episodes[key]=prior;continue
                bal=_balance(http,url,cfg['token'],address,block)
                if prior.get('baseline_raw') is None:
                    health[coin]='MISSING_ORIGINAL_RECIPIENT_BASELINE';continue
                needed=int(prior['baseline_raw'])+(int(prior['retained_raw'])*7)//10
                if bal<needed:
                    health[coin]='RETENTION_NO_LONGER_PROVED';continue
                verified={**prior,'last_reverified_ms':now,
                          'last_reverified_block':block,
                          'current_balance_raw':str(bal)}
                episodes[key]=verified
            except Exception as ex:
                health[coin]='RPC_RECHECK_FAILED:'+type(ex).__name__
                episodes[key]=prior
                continue
        live=[e for e in episodes.values() if e['coin']==coin and safe and
              coin not in health and 0<=now-int(e.get('last_reverified_ms',0))<=180000]
        if live and not row.get('preparation_proved'):
            proof=max(live,key=lambda e:e['last_block_ms'])
            # Evidence of earlier receipt is refreshed, but not inferred from
            # historical rumors or unverified candidate addresses.
            row['preparation_proved']=True
            row['preparation_revalidated_from_prior_window']=True
            row['coin_inventory']['status']='VERIFIED_RETAINED_EOA_FROM_PRIOR_WINDOW'
            row['coin_inventory']['revalidated_receivers']=live
            spot=row.get('first_spot_pressure_observed',False)
            timing=False
            for market in row.get('market_evidence',[]):
                start=market.get('window_start_ms')
                if (spot and isinstance(start,int) and
                    proof['last_block_ms']<=start and
                    start-proof['last_block_ms']<=MAX_AGE_MS):
                    timing=True;break
            if timing:
                row['state']='COMBINED_PREP_THEN_SPOT_OBSERVED'
                row['temporal_sequence_proved']=True
            elif not spot:
                row['state']='VERIFIED_INVENTORY_PREPARATION_OBSERVED'
        if not live and not row.get('preparation_proved') and coin not in health:
            health[coin]='NO_CURRENT_REVALIDATED_PREPARATION'
    return report,{'schema':'radar-v2-revalidated-preparation-1',
                   'asof_ms':now,'episodes':episodes,'health':health,
                   'note':'Per-run chain head + EOA balance recheck required; never infer all sellable inventory.'}
