#!/usr/bin/env python3
"""Durable multi-day token-balance research for the existing V2 SQLite archive.

No RPC, schema migration, new account or order placement. Every observation
must be tied to a successful wallet check and a chain head consistent with
the measured block. Incomplete historical periods are never called 30-day
accumulation, and different addresses are not assumed different owners.
"""
import argparse
import collections
import datetime
import decimal
import json
import pathlib
import sqlite3
import time

from radar_stage1 import validate_config
from economic_brain import covered, net_transfers

D = decimal.Decimal
DAY = 86400000
WINDOWS = (1, 3, 7, 30)
RECENT_MS = 3 * 3600000
MAX_SNAPSHOT_HEAD_GAP = {'bsc': 3600, 'base': 10000}
EXCLUDED_ROLES = {'dex_pool','dex_vault','pool_address_unverified',
                  'large_contract_unclassified','platform_proxy',
                  'exchange_custody','base_attribution_unverified'}
GOOD = 'DYNAMIC_EOA_CODE_VERIFIED_OWNER_UNKNOWN'
SCHEMA = 'radar-v2-observed-multiday-accumulation-1'

def number(raw,decimals):
    return format((D(str(raw)) / (D(10) ** int(decimals))).normalize(),'f')

def daily_series(samples):
    """Pick the latest real reading of each UTC day, not synthesized intervals."""
    daily = {}
    for x in samples:
        day = datetime.datetime.fromtimestamp(x['seen_ms']/1000,datetime.timezone.utc).date().isoformat()
        daily[day] = x
    return [daily[k] for k in sorted(daily)]

def validated_snapshots(conn,coin,chain,address,now_ms):
    rows=conn.execute("""
      SELECT b.block,b.raw_amount,b.seen_ms,c.head_block,w.detail
      FROM wallet_balances b JOIN wallet_checks w
        ON w.coin=b.coin AND w.chain=b.chain AND w.address=b.address
       AND w.block=b.block AND w.status='OK'
      LEFT JOIN chain_checks c ON c.run_id=w.run_id AND c.chain=b.chain
      WHERE b.coin=? AND b.chain=? AND b.address=? AND b.seen_ms>=?
      ORDER BY b.seen_ms,b.block
      """,(coin,chain,address,now_ms-40*DAY)).fetchall()
    seen=set();out=[]
    for block,raw,seen_ms,head,detail in rows:
        if not isinstance(head,int) or block>head or head-block>MAX_SNAPSHOT_HEAD_GAP[chain]:
            continue
        if (block,seen_ms) in seen or seen_ms>now_ms+60000:continue
        seen.add((block,seen_ms))
        if out and (block<=out[-1]['block'] or seen_ms<=out[-1]['seen_ms']):
            continue
        out.append({'block':block,'raw':int(raw),'seen_ms':seen_ms,
                    'verified_dynamic_eoa':detail==GOOD})
    return out

def series_stats(samples,days,now_ms,decimals):
    if len(samples)<2:return {'state':'INSUFFICIENT_SNAPSHOTS'}
    latest=samples[-1]
    cutoff=latest['seen_ms']-days*DAY
    before=[s for s in samples[:-1] if s['seen_ms']<=cutoff]
    baseline=before[-1] if before else samples[0]
    chosen=[s for s in samples if baseline['seen_ms']<=s['seen_ms']<=latest['seen_ms']]
    day_readings=daily_series(chosen)
    daily_deltas=[y['raw']-x['raw'] for x,y in zip(day_readings,day_readings[1:])]
    gaps=[(y['seen_ms']-x['seen_ms'])/3600000
          for x,y in zip(chosen,chosen[1:])]
    observed_hours=(latest['seen_ms']-baseline['seen_ms'])/3600000
    positive=sum(v>0 for v in daily_deltas)
    negative=sum(v<0 for v in daily_deltas)
    delta=latest['raw']-baseline['raw']
    fresh=(0<=now_ms-latest['seen_ms']<=RECENT_MS)
    # A complete window needs a baseline close to its requested edge, frequent
    # genuine observations and an actual fresh ending balance. A two-snapshot
    # jump does not prove steady multi-day accumulation.
    baseline_near=bool(before and 0<=cutoff-baseline['seen_ms']<=DAY)
    complete=bool(baseline_near and fresh and
                  max(gaps,default=1e10)<=48 and
                  len(day_readings)>=min(days+1,3 if days==1 else days//2))
    min_positive=2 if days==3 else 4
    regular=bool(days>=3 and complete and delta>0 and positive>=min_positive
                 and positive>=max(1,len(daily_deltas)*0.6))
    return {'state':'COMPLETE_OBSERVED_WINDOW' if complete else 'PARTIAL_OBSERVED_WINDOW',
            'requested_days':days,'observed_hours':round(observed_hours,2),
            'start_observed_ms':baseline['seen_ms'],'end_observed_ms':latest['seen_ms'],
            'start_block':baseline['block'],'end_block':latest['block'],
            'start_balance':number(baseline['raw'],decimals),
            'end_balance':number(latest['raw'],decimals),
            'net_delta':number(delta,decimals),'net_delta_raw':str(delta),
            'observed_calendar_days':len(day_readings),
            'positive_observed_daily_intervals':positive,
            'negative_observed_daily_intervals':negative,
            'longest_gap_hours':round(max(gaps,default=0),2),
            'current_snapshot_fresh':fresh,'regular_multi_day_increase_observed':regular,
            'baseline_near_period_start':baseline_near,
            'transfer_flow_reconciled_across_full_period':False,
            'not_proof_of_market_purchase_or_common_owner':True}

def build(db_path,config,asof_ms=None,verified_cex_path=None):
    validate_config(config)
    now_ms=int(time.time()*1000) if asof_ms is None else asof_ms
    cex={}
    if verified_cex_path and pathlib.Path(verified_cex_path).exists():
        obj=json.loads(pathlib.Path(verified_cex_path).read_text())
        for key,row in obj.get('entities',{}).items():
            if row.get('proof')=='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':
                cex[key.lower()]=row.get('entity')
    conn=sqlite3.connect('file:'+pathlib.Path(db_path).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('SQLITE_NOT_INTEGRAL')
        coins=[]
        for conf in config['coins']:
            sym=conf['symbol'];chain=conf['chain'];decimals=conf['decimals']
            reg={w['address'].lower():w for w in config['wallets'] if
                 w['coin']==sym and w['chain']==chain}
            valid_dyn={x[0] for x in conn.execute("""
                SELECT DISTINCT address FROM wallet_checks
                WHERE coin=? AND chain=? AND status='OK' AND detail=?""",
                (sym,chain,GOOD))}
            tracked=sorted(set(reg)|valid_dyn)
            wallets=[]
            for address in tracked:
                meta=reg.get(address,{})
                role=meta.get('role','dynamic_eoa_unverified')
                if role in EXCLUDED_ROLES:continue
                snaps=validated_snapshots(conn,sym,chain,address,now_ms)
                if not snaps:continue
                periods={str(days)+'d':series_stats(snaps,days,now_ms,decimals) for days in WINDOWS}
                # Full-period chain logs, when available, can independently
                # reconcile balance changes; lack of logs remains UNKNOWN.
                for days in WINDOWS:
                    window=periods[str(days)+'d']
                    if window.get('state')!='COMPLETE_OBSERVED_WINDOW':
                        continue
                    lo=window['start_block'];hi=window['end_block']
                    if not covered(conn,chain,lo+1,hi):
                        window['transfer_proof_status']='CHAIN_INTERVAL_NOT_COMPLETE'
                        continue
                    flow=net_transfers(conn,sym,lo,hi,address)
                    matched=flow['net_raw']==int(window['net_delta_raw'])
                    window['transfer_flow_reconciled_across_full_period']=matched
                    window['transfer_proof_status']=('FULL_INTERVAL_TOKEN_TRANSFERS_RECONCILED'
                        if matched else 'NET_BALANCE_TRANSFER_MISMATCH')
                    window['verified_incoming_transactions']=flow['incoming_transactions']
                    window['verified_inflow_source_count']=flow['distinct_incoming_sources']
                # Freeze a bounded, real transaction sample for the longest
                # proven multi-day window. A sample never claims all transfers.
                transaction_proof=None
                for days in (30,7,3):
                    window=periods[str(days)+'d']
                    if window.get('regular_multi_day_increase_observed') is not True:
                        continue
                    lo,hi=window['start_block'],window['end_block']
                    txs=list(conn.execute("""
                        SELECT tx_hash,block,from_address,to_address,amount_raw
                        FROM token_transfers
                        WHERE coin=? AND chain=? AND block>? AND block<=?
                        AND (from_address=? OR to_address=?)
                        ORDER BY block DESC,tx_hash DESC LIMIT 9
                        """,(sym,chain,lo,hi,address,address)))
                    transaction_proof={
                       'window_days':days,'from_block_exclusive':lo,
                       'to_block_inclusive':hi,'selected_event_count':min(8,len(txs)),
                       'more_events_may_exist':len(txs)>8,
                       'covered_every_intervening_block':window.get('transfer_proof_status') in
                            ('FULL_INTERVAL_TOKEN_TRANSFERS_RECONCILED','NET_BALANCE_TRANSFER_MISMATCH'),
                       'balance_delta_reconciled_to_complete_transfers':window.get('transfer_flow_reconciled_across_full_period') is True,
                       'entries':[{'tx':tx,'block':block,'from':frm,'to':to,
                            'amount_raw':str(amount),'chain':chain,
                            'direction_relative_to_wallet':'IN' if to==address else 'OUT'}
                         for tx,block,frm,to,amount in txs[:8]],
                       'scope':'BOUNDED_REAL_TOKEN_TRANSFER_SAMPLE_NOT_FULL_HISTORY'}
                    break
                latest=snaps[-1]
                direct_cex=[]
                for frm,raw,tx,block in conn.execute("""
                    SELECT from_address,amount_raw,tx_hash,block FROM token_transfers
                    WHERE coin=? AND chain=? AND to_address=? AND block<=?
                    ORDER BY block DESC LIMIT 120
                    """,(sym,chain,address,latest['block'])):
                    if chain+'|'+frm in cex:
                        direct_cex.append({'entity':cex[chain+'|'+frm],
                          'amount':number(raw,decimals),'tx':tx,'block':block,
                          'only_direct_transfer_not_proven_withdrawal':True})
                wallets.append({'address':address,'role':role,'beneficial_owner_verified':False,
                    'dynamic_eoa_code_verified_in_archive':address in valid_dyn,
                    'latest_balance':number(latest['raw'],decimals),
                    'latest_observed_ms':latest['seen_ms'],'latest_block':latest['block'],
                    'validated_snapshot_count':len(snaps),'periods':periods,
                    'transaction_proof_sample':transaction_proof,
                    'direct_verified_cex_address_sources':direct_cex[:5],
                    'cex_sources_completeness':'PARTIAL_NOT_ALL_CEX_ADDRESSES'})
            wallets.sort(key=lambda x:(x['periods']['30d'].get('regular_multi_day_increase_observed',False),
                D(x['periods']['30d'].get('net_delta','0') or '0')),reverse=True)
            material=[w for w in wallets if
                 w['periods']['7d'].get('regular_multi_day_increase_observed',False)
                 and w['periods']['7d']['current_snapshot_fresh']]
            thirty=[w for w in wallets if
                 w['periods']['30d'].get('regular_multi_day_increase_observed',False)]
            early3=[w for w in wallets if
                 w['periods']['3d'].get('regular_multi_day_increase_observed',False)
                 and w['periods']['3d']['current_snapshot_fresh']]
            # Add wallet balances, never sum transaction inflows (which double
            # count hops). Still NOT evidence of distinct beneficial owners.
            sum_7d=sum((D(w['periods']['7d']['net_delta']) for w in material),D(0))
            coins.append({'coin':sym,'chain':chain,'registered_or_discovered_address_count':len(tracked),
                'observed_non_infrastructure_wallet_count':len(wallets),
                'recent_regular_3d_wallets_unknown_owners':len(early3),
                'recent_regular_7d_wallets_unknown_owners':len(material),
                'regular_30d_wallets_unknown_owners':len(thirty),
                'candidate_7d_net_observed_change_token':str(sum_7d),
                'long_term_status':'MULTI_WALLET_BALANCE_ACCUMULATION_RESEARCH'
                    if len(material)>=2 and sum_7d>0 else
                    'SINGLE_WALLET_BALANCE_ACCUMULATION_RESEARCH'
                    if material else
                    'EARLY_3D_BALANCE_ACCUMULATION_RESEARCH'
                    if early3 else 'NO_VERIFIED_MULTIDAY_ACCUMULATION_OR_DATA_INSUFFICIENT',
                'no_independent_beneficial_owner_proof':True,
                'transfer_full_period_provenance':'INDIVIDUAL_WALLET_WINDOW_DEPENDENT',
                'relative_to_total_sellable_supply':'UNKNOWN_NO_COMPLETE_SELLABLE_SUPPLY',
                'wallets':wallets[:25]})
        return {'schema':SCHEMA,
          'generated_utc':datetime.datetime.fromtimestamp(now_ms/1000,datetime.timezone.utc).isoformat(),
          'asof_ms':now_ms,'coin_count':len(coins),'coins':coins,
          'never_assert_continuous_30d_without_actual_daily_snapshots':True,
          'scope':'Observed verified block-consistent balance history, not all chain accounts',
          'cannot_prove':'Buy intent, beneficial ownership, all CEX withdrawals, full circulating/sellable supply, causation, future pump',
          'buy_signal':False,'strong_alert':False}
    finally:conn.close()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',required=True)
    ap.add_argument('--config',required=True)
    ap.add_argument('--verified-cex',default=None)
    ap.add_argument('--out',required=True)
    a=ap.parse_args()
    config=json.loads(pathlib.Path(a.config).read_text())
    report=build(a.db,config,verified_cex_path=a.verified_cex)
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_name(target.name+'.tmp')
    tmp.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    tmp.replace(target)
    print(json.dumps({'coins':len(report['coins']),'counts':{
      k:sum(c['long_term_status']==k for c in report['coins']) for k in sorted({
      x['long_term_status'] for x in report['coins']})},
      'buy_signal':False,'full_30d_proofs':sum(c['regular_30d_wallets_unknown_owners']
                                               for c in report['coins'])},ensure_ascii=False))

if __name__=='__main__':main()
