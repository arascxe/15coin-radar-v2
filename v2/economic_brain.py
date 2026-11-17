#!/usr/bin/env python3
"""15-coin V2 economic evidence engine, read-only, fail-closed.

Preliminary WATCH candidates require multi-transfer accumulation confirmed by
two token balance snapshots AND contiguous chain event coverage. An EOA label
or an exchange tag is NOT ownership verification. No trading orders or alerts.
"""
import argparse
import collections
import datetime as dt
import decimal
import hashlib
import json
import pathlib
import sqlite3

from radar_stage1 import validate_config

ZERO='0x'+'0'*40
EXCHANGE_PROOFS={'official_exchange_verified','confirmed_onchain_proof'}
POOLS={'dex_pool','dex_vault','pool_address_unverified'}
CONTRACTS={'large_contract_unclassified','platform_proxy'}
CUSTODY={'exchange_custody','base_attribution_unverified'}
CANDIDATE_EOA={'eoa_unverified'}
HOURS=(24,168,720)
decimal.getcontext().prec=65

def kinds(wallet):
    if not wallet:return 'UNKNOWN_OWNER'
    kind=wallet.get('role','')
    if kind=='exchange_custody':
        if wallet.get('identity_confidence') in EXCHANGE_PROOFS and wallet.get('entity_id'):
            return 'EXCHANGE_VERIFIED'
        return 'EXCHANGE_TAG_UNVERIFIED'
    if kind in CUSTODY:return 'EXCHANGE_TAG_UNVERIFIED'
    if kind in POOLS:return 'DEX_OR_POOL_UNVERIFIED'
    if kind in CONTRACTS:return 'CONTRACT_OR_PROXY_NOT_INDEPENDENT'
    if kind in CANDIDATE_EOA:return 'EOA_CANDIDATE_OWNER_UNVERIFIED'
    return 'HOLDER_OWNER_UNVERIFIED'

def transfer_kind(frm,to,labels):
    if frm==ZERO:return 'MINT_OR_ZERO_ADDRESS_TRANSFER'
    if to==ZERO:return 'BURN_OR_ZERO_ADDRESS_TRANSFER'
    a=labels.get(frm,{});b=labels.get(to,{})
    x,y=kinds(a),kinds(b)
    if frm==to:return 'SAME_ADDRESS_TRANSFER'
    if x=='EXCHANGE_VERIFIED' and y=='EXCHANGE_VERIFIED':
        return ('VERIFIED_EXCHANGE_INTERNAL_TRANSFER'
                if a.get('entity_id')==b.get('entity_id') else 'VERIFIED_EXCHANGE_TO_EXCHANGE')
    if x=='EXCHANGE_VERIFIED':return 'VERIFIED_EXCHANGE_TO_OWNER_UNVERIFIED'
    if y=='EXCHANGE_VERIFIED':return 'OWNER_UNVERIFIED_TO_VERIFIED_EXCHANGE'
    if 'EXCHANGE_TAG_UNVERIFIED' in (x,y):return 'UNVERIFIED_EXCHANGE_TAG_MOVEMENT'
    if 'DEX_OR_POOL_UNVERIFIED' in (x,y):return 'DEX_SWAP_OR_LP_UNRESOLVED'
    if 'CONTRACT_OR_PROXY_NOT_INDEPENDENT' in (x,y):return 'CONTRACT_OR_PROXY_UNRESOLVED'
    return 'UNKNOWN_OWNER_TO_UNKNOWN_OWNER'

def covered(c,chain,start,end):
    """A contiguous *event-verified* chain interval is required for a balance delta."""
    if start>end:return False
    intervals=c.execute("""SELECT from_block,to_block FROM chain_checks
      WHERE chain=? AND from_block IS NOT NULL AND to_block IS NOT NULL
      AND to_block>=? AND from_block<=? ORDER BY from_block,to_block""",
      (chain,start,end)).fetchall()
    pos=start
    for a,b in intervals:
        if a>pos:return False
        if b>=pos:pos=b+1
        if pos>end:return True
    return False

def as_token(amount,decimals):
    value=decimal.Decimal(int(amount))/(decimal.Decimal(10)**decimals)
    return format(value.normalize(),'f')

def sample_window(snapshots,hours):
    """Observed-time window; never call a partial window a full 24h/7d/30d history."""
    if len(snapshots)<2:return None
    latest=snapshots[-1]
    threshold=latest['seen_ms']-hours*3600000
    earlier=[s for s in snapshots[:-1] if s['seen_ms']<=threshold]
    baseline=earlier[-1] if earlier else snapshots[0]
    if baseline['block']>=latest['block']:return None
    return {'baseline':baseline,'latest':latest,
            'full_window':bool(earlier),
            'observed_hours':round(max(0,(latest['seen_ms']-baseline['seen_ms'])/3600000),3)}

def snapshots_by_address(c,coin,chain,addresses):
    out={}
    for addr in addresses:
        data=c.execute("""SELECT block,raw_amount,seen_ms FROM wallet_balances
          WHERE coin=? AND chain=? AND address=? ORDER BY block""",
          (coin,chain,addr)).fetchall()
        out[addr]=[{'block':int(a),'raw':int(b),'seen_ms':int(t)} for a,b,t in data]
    return out

def net_transfers(c,coin,from_block,to_block,address):
    rows=c.execute("""SELECT tx_hash,log_index,block,from_address,to_address,amount_raw
      FROM token_transfers WHERE coin=? AND block>? AND block<=?
      AND (from_address=? OR to_address=?) ORDER BY block,log_index""",
      (coin,from_block,to_block,address,address)).fetchall()
    inbound=outbound=0;incoming=[];all_tx=set()
    for tx,idx,block,frm,to,raw in rows:
        raw=int(raw)
        if frm==to:continue
        if to==address:
            inbound+=raw;incoming.append({'tx':tx,'block':block,'amount_raw':str(raw),
                                           'from':frm})
        if frm==address:outbound+=raw
        all_tx.add(tx)
    return {'in_raw':inbound,'out_raw':outbound,'net_raw':inbound-outbound,
            'distinct_incoming_blocks':len({x['block'] for x in incoming}),
            'distinct_incoming_sources':len({x['from'] for x in incoming}),
            'distinct_transactions':len(all_tx),
            'incoming_transactions':len({x['tx'] for x in incoming}),
            'incoming':incoming}

def build(db_path,config):
    validate_config(config)
    c=sqlite3.connect('file:'+pathlib.Path(db_path).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':
            raise RuntimeError('SQLITE_INTEGRITY_NOT_OK')
        latest=c.execute('SELECT run_id,status,started_ms FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
        cursors={x[0]:(x[1],x[2]) for x in c.execute('SELECT chain,last_complete_block,updated_ms FROM chain_cursor')}
        health={x[0]:(x[1],x[2],x[3]) for x in c.execute("""
          SELECT chain,status,head_block,to_block FROM chain_checks
          WHERE run_id=?""",(latest[0] if latest else '',))}
        result=[]
        for conf in config['coins']:
            sym=conf['symbol'];chain=conf['chain'];decimals=conf['decimals']
            labels={w['address'].lower():w for w in config['wallets']
                    if w['coin']==sym and w['chain']==chain}
            # Dynamic addresses are monitored only after a same-block eth_getCode
            # check returned EOA code and a real balanceOf was recorded. This does
            # NOT prove beneficial owner, independent buyer, or exchange withdrawal.
            dynamic_addresses=[r[0] for r in c.execute(
                """SELECT DISTINCT address FROM wallet_checks
                   WHERE coin=? AND chain=? AND status='OK' AND detail=?
                   ORDER BY address""",
                (sym,chain,'DYNAMIC_EOA_CODE_VERIFIED_OWNER_UNKNOWN'))]
            for dyn_addr in dynamic_addresses:
                labels.setdefault(dyn_addr,{'role':'eoa_unverified',
                    'identity_confidence':'onchain_code_zero_owner_unknown',
                    'label':'DYNAMIC_EOA_CODE_VERIFIED_OWNER_UNKNOWN'})

            cursor=cursors.get(chain,(None,None))[0]
            ch_status=health.get(chain,(None,None,None))[0]
            last_head=health.get(chain,(None,None,None))[1]
            chain_data_ok=(cursor is not None and ch_status=='OK' and
                           last_head is not None and cursor>=last_head)
            from_b=max(0,cursor-4999) if cursor is not None else None
            if cursor is not None:
                transfers=c.execute("""SELECT tx_hash,log_index,block,from_address,
                  to_address,amount_raw FROM token_transfers WHERE coin=?
                  AND block BETWEEN ? AND ? ORDER BY block,log_index""",
                  (sym,from_b,cursor)).fetchall()
            else:transfers=[]
            counts=collections.Counter()
            events=[]
            for tx,idx,block,frm,to,raw in transfers:
                typ=transfer_kind(frm,to,labels)
                counts[typ]+=1
                events.append({'tx':tx,'block':block,'kind':typ,'from':frm,'to':to,'amount_raw':str(raw)})
            events.sort(key=lambda x:int(x['amount_raw']),reverse=True)
            candidate_wallets=[]
            excluded=[]
            snap=snapshots_by_address(c,sym,chain,labels)
            for address,w in labels.items():
                samples=snap[address]
                if len(samples)<2:continue
                main=sample_window(samples,24)
                if main is None:continue
                old=main['baseline'];new=main['latest'];delta=new['raw']-old['raw']
                if delta<=0:continue
                wallet_kind=kinds(w)
                row={'address':address,'wallet_kind':wallet_kind,'label':w.get('label'),
                     'identity_confidence':w.get('identity_confidence','unknown'),
                     'first_block':old['block'],'last_block':new['block'],
                     'balance_before':as_token(old['raw'],decimals),
                     'balance_after':as_token(new['raw'],decimals),
                     'balance_increase':as_token(delta,decimals),
                     'relative_increase_pct':str(round(decimal.Decimal(100)*delta/old['raw'],2)) if old['raw']>0 else None,
                     'history_hours':main['observed_hours'],'full_24h_baseline':main['full_window'],
                     'dynamically_discovered':address in dynamic_addresses,
                     'ownership_verified':False}
                history={}
                for hours in HOURS:
                    x=sample_window(samples,hours)
                    if x is not None:
                        history[str(hours)+'h']={'baseline_complete':x['full_window'],
                                                'actual_observed_hours':x['observed_hours'],
                                                'balance_delta':as_token(x['latest']['raw']-x['baseline']['raw'],decimals)}
                row['windows']=history
                if wallet_kind!='EOA_CANDIDATE_OWNER_UNVERIFIED':
                    if len(excluded)<5:
                        excluded.append({**row,'exclusion':'NOT_VERIFIED_INDEPENDENT_EOA'})
                    continue
                safe_end=cursor is not None and new['block']<=cursor
                proof=covered(c,chain,old['block']+1,new['block']) if safe_end else False
                flow=net_transfers(c,sym,old['block'],new['block'],address) if proof else None
                row['chain_interval_verified']=proof
                row['transfer_net_reconciled']=flow is not None and flow['net_raw']==delta
                row['incoming_transactions']=flow['incoming_transactions'] if flow else 0
                row['net_transfer_amount']=as_token(flow['net_raw'],decimals) if flow else None
                # A balance increase can come wholly from a DEX pool/vault:
                # this may be normal swap churn, NOT independent market-maker
                # accumulation or evidence of an exchange withdrawal.
                all_in=flow['incoming'] if flow is not None else []
                known_pool_addresses={a for a,x in labels.items() if x.get('role') in POOLS}
                known_pool_addresses.update(p['pool'].lower() for p in config.get('dex_pools',[])
                       if p.get('coin')==sym and p.get('chain')==chain and p.get('pool'))
                dex_in=[x for x in all_in if x['from'] in known_pool_addresses]
                dex_in_raw=sum(int(x['amount_raw']) for x in dex_in)
                incoming_raw=sum(int(x['amount_raw']) for x in all_in)
                dex_txs=len({x['tx'] for x in dex_in})
                total_txs=len({x['tx'] for x in all_in})
                dex_dominated=(incoming_raw>0 and dex_in_raw*2>=incoming_raw or
                               total_txs>0 and dex_txs*2>=total_txs)
                independent_sources={x['from'] for x in all_in
                  if x['from']!=ZERO and x['from'] not in known_pool_addresses and
                  kinds(labels.get(x['from'])) not in
                    ('EXCHANGE_TAG_UNVERIFIED','CONTRACT_OR_PROXY_NOT_INDEPENDENT')}
                verified_exchange_sources={x['from'] for x in all_in
                    if kinds(labels.get(x['from']))=='EXCHANGE_VERIFIED'}
                source_breadth=(len(independent_sources)>=2 or
                                len(verified_exchange_sources)>=1 and total_txs>=2)
                block_breadth=flow is not None and flow['distinct_incoming_blocks']>=2
                row['inbound_provenance']={
                    'unique_inflow_sources':flow['distinct_incoming_sources'] if flow else 0,
                    'unique_inflow_blocks':flow['distinct_incoming_blocks'] if flow else 0,
                    'known_dex_inflow_transactions':dex_txs,
                    'known_dex_inflow_share_of_amount':str(round(
                        decimal.Decimal(dex_in_raw)/decimal.Decimal(incoming_raw),4)) if incoming_raw else None,
                    'source_breadth_for_research':bool(source_breadth),
                    'dex_dominated':bool(dex_dominated),
                    'origin_ownership_still_unverified':True}

                row['evidence_txs']=(flow['incoming'][-4:] if flow else [])
                change_big=(old['raw']==0 or decimal.Decimal(delta)/old['raw']>=decimal.Decimal('0.20'))
                valid=(proof and flow is not None and flow['net_raw']==delta
                    and flow['incoming_transactions']>=2 and change_big
                    and block_breadth and source_breadth and not dex_dominated)
                row['status']='WALLET_ACCUMULATION_WATCH_UNVERIFIED_OWNER' if valid else 'NOT_CORROBORATED'
                row['reason']=('MULTI_SOURCE_MULTI_BLOCK_BALANCE_AND_TRANSFER_RECONCILED_OWNER_UNVERIFIED'
                    if valid else ('KNOWN_DEX_POOL_OR_VAULT_DOMINATED_NOT_INDEPENDENT_PREPARATION'
                    if dex_dominated else
                    'SINGLE_SOURCE_OR_SINGLE_BLOCK_OR_INSUFFICIENT_CORROBORATION'))
                if valid:row['dedupe_id']=hashlib.sha256(
                    (sym+':'+address+':'+str(old['block'])+':'+str(new['block'])).encode()).hexdigest()[:20]
                if valid or len(candidate_wallets)<12:candidate_wallets.append(row)
            # Do not silently equate a zero candidate list with absence of buildup.
            candidate_wallets.sort(key=lambda x:(x['status']!='WALLET_ACCUMULATION_WATCH_UNVERIFIED_OWNER',
                                                  -decimal.Decimal(x['balance_increase'])))
            active=[w for w in candidate_wallets if w['status']=='WALLET_ACCUMULATION_WATCH_UNVERIFIED_OWNER']
            missing=[]
            if not chain_data_ok:missing.append('CHAIN_LATEST_SCAN_PARTIAL_OR_UNKNOWN')
            if not any(kinds(w)=='EXCHANGE_VERIFIED' for w in labels.values()):
                missing.append('NO_VERIFIED_EXCHANGE_OWNERSHIP')
            missing+=['DEX_LP_EVENTS_NOT_CLASSIFIED','SPOT_TAKER_DIRECTION_NOT_CAPTURED',
                      'ORDERBOOK_DEPTH_NOT_CAPTURED','CEX_INTERNAL_EXTERNAL_CLUSTER_NOT_PROVEN']
            status=('PREPARATION_WATCH_HYPOTHESIS_ONLY' if active else
                    'INSUFFICIENT_EVIDENCE_NOT_NO_PREPARATION')
            result.append({'coin':sym,'chain':chain,'contract':conf['token'],
                'classification':status,'strong_signal':False,'buy_signal':False,
                'chain_cursor':cursor,'latest_chain_status':ch_status,
                'chain_head':last_head,'chain_currently_complete':chain_data_ok,
                'verified_cex_wallet_count':sum(kinds(w)=='EXCHANGE_VERIFIED' for w in labels.values()),
                'exchange_tag_unverified_count':sum(kinds(w)=='EXCHANGE_TAG_UNVERIFIED' for w in labels.values()),
                'dynamically_monitored_eoa_addresses':len(dynamic_addresses),
                'observed_events_last_5000_blocks':len(transfers),
                'flow_event_type_counts':dict(counts),
                'largest_movements':events[:5],
                'wallet_accumulation_candidates':active[:8],
                'wallet_observations_not_corrobated':candidate_wallets[len(active):][:5],
                'other_holder_increases_excluded':excluded[:4],
                'missing_evidence':missing,
                'note':'Transfer logs are not market purchases; EOA and CEX ownership are not proven.'})
        return {'schema':'radar-v2-economic-brain-1',
            'generated_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
            'last_run_id':latest[0] if latest else None,
            'last_run_status':latest[1] if latest else 'NO_RUN',
            'ready_for_automated_strong_alerts':False,
            'meaning':'preliminary observed on-chain accumulation watch; no verified whale owner or buying recommendation',
            'candidate_coins':sum(x['classification']=='PREPARATION_WATCH_HYPOTHESIS_ONLY' for x in result),
            'coins':result}
    finally:c.close()

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--config',required=True);p.add_argument('--out',required=True)
    a=p.parse_args();report=build(a.db,json.loads(pathlib.Path(a.config).read_text()))
    dest=pathlib.Path(a.out);dest.parent.mkdir(parents=True,exist_ok=True)
    temp=dest.with_suffix(dest.suffix+'.tmp')
    temp.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    temp.replace(dest)
    print(json.dumps({'coin_count':len(report['coins']),'candidate_coins':report['candidate_coins'],
      'strong_signal_count':0,'last_run':report['last_run_id']}))

if __name__=='__main__':main()
