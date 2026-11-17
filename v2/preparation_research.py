#!/usr/bin/env python3
"""Evidence-first early-preparation research, never a trading recommendation.

Uses stored transfer and wallet records to quantify flows but refuses to
promote weak labels into verified exchange withdrawals, or 1m OHLCV into
taker buy/sell data. The initial result is an evidence report, not an alarm.
"""
import argparse,collections,datetime,json,pathlib,sqlite3
from radar_stage1 import validate_config

ACCEPTED_CEX_CONFIDENCE={'confirmed_onchain_proof','official_exchange_verified'}
def role(w):
    label=w.get('role','unclassified')
    confidence=w.get('identity_confidence','')
    if label=='exchange_custody':
        return 'VERIFIED_CEX' if confidence in ACCEPTED_CEX_CONFIDENCE else 'CEX_CANDIDATE_UNVERIFIED'
    if label=='dex_pool':return 'DEX_POOL_CANDIDATE'
    if label in ('large_holder_unclassified','historical_candidate','unverified_entity','platform_proxy'):
        return 'UNVERIFIED'
    return 'OTHER_UNVERIFIED'
def classify_transfers(rows,labels):
    sums=collections.Counter();participants=collections.defaultdict(set);top=[]
    for _,tx,idx,block,frm,to,amount in rows:
        amount=int(amount)
        f=role(labels.get(frm,{}));t=role(labels.get(to,{}))
        sums['events']+=1;sums['raw_total']+=amount
        participants['senders'].add(frm);participants['receivers'].add(to)
        if f=='VERIFIED_CEX' and t!='VERIFIED_CEX':
            sums['verified_cex_outflow_raw']+=amount
        elif t=='VERIFIED_CEX' and f!='VERIFIED_CEX':
            sums['verified_cex_inflow_raw']+=amount
        if f=='CEX_CANDIDATE_UNVERIFIED' and t!='CEX_CANDIDATE_UNVERIFIED':
            sums['unverified_cex_candidate_out_raw']+=amount
        elif t=='CEX_CANDIDATE_UNVERIFIED' and f!='CEX_CANDIDATE_UNVERIFIED':
            sums['unverified_cex_candidate_in_raw']+=amount
        top.append((amount,tx,block,frm,to,f,t))
    top.sort(reverse=True)
    return {
        'events':sums['events'],
        'distinct_senders':len(participants['senders']),
        'distinct_receivers':len(participants['receivers']),
        'verified_cex_outflow_raw':str(sums['verified_cex_outflow_raw']),
        'verified_cex_inflow_raw':str(sums['verified_cex_inflow_raw']),
        'candidate_cex_outflow_raw_unverified':str(sums['unverified_cex_candidate_out_raw']),
        'candidate_cex_inflow_raw_unverified':str(sums['unverified_cex_candidate_in_raw']),
        'largest_transfers':[{'amount_raw':str(amount),'tx':tx,'block':block,'from':frm,'to':to,'from_type':f,'to_type':t} for amount,tx,block,frm,to,f,t in top[:3]]
    }
def analyze(db,config):
    validate_config(config)
    c=sqlite3.connect('file:'+pathlib.Path(db).resolve().as_posix()+'?mode=ro',uri=True)
    c.row_factory=sqlite3.Row
    try:
        chains={r['chain']:{'last_complete_block':r['last_complete_block'],'updated_ms':r['updated_ms']} for r in c.execute('SELECT * FROM chain_cursor')}
        checks=collections.defaultdict(dict)
        latest=c.execute('SELECT run_id,status,started_ms FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
        if latest:
            for r in c.execute('SELECT coin,venue,status,received_bars,missing_minutes FROM market_checks WHERE run_id=?',(latest['run_id'],)):
                checks[r['coin']][r['venue']]={'status':r['status'],'bars':r['received_bars'],'missing':r['missing_minutes']}
        results=[]
        for coin in config['coins']:
            sym=coin['symbol'];chain=coin['chain']
            labels={w['address'].lower():w for w in config['wallets'] if w['coin']==sym}
            verified=sum(role(w)=='VERIFIED_CEX' for w in labels.values())
            unverified=sum(role(w)=='CEX_CANDIDATE_UNVERIFIED' for w in labels.values())
            # Last 5000 covered blocks is a *block window*, NOT a 24h wallclock window.
            cursor=chains.get(chain,{}).get('last_complete_block')
            from_block=max(0,cursor-4999) if cursor is not None else None
            if cursor is None:rows=[]
            else:rows=[tuple(r) for r in c.execute(
                'SELECT coin,tx_hash,log_index,block,from_address,to_address,amount_raw FROM token_transfers WHERE coin=? AND block>=? AND block<=? ORDER BY block,log_index',
                (sym,from_block,cursor))]
            transfers=classify_transfers(rows,labels)
            snaps=c.execute('SELECT COUNT(*),MIN(block),MAX(block) FROM wallet_balances WHERE coin=?',(sym,)).fetchone()
            market=checks.get(sym,{})
            coverage={'chain_cursor_known':cursor is not None,'scan_end_block':cursor,'scan_start_block_window':from_block,
                      'snapshot_rows':snaps[0],'snapshot_min_block':snaps[1],'snapshot_max_block':snaps[2],
                      'market_venues_checked':len(market),'market_statuses':market}
            # No LP mint/burn, tick depth, taker trades or independently verified CEX
            # labels: no stage P1/P2 may be emitted from this dataset alone.
            reasons=[]
            if verified==0:reasons.append('NO_INDEPENDENTLY_VERIFIED_EXCHANGE_CUSTODY_LABEL')
            if cursor is None:reasons.append('NO_CONFIRMED_CHAIN_CURSOR')
            if snaps[0]<2:reasons.append('INSUFFICIENT_WALLET_SNAPSHOTS')
            reasons+=['LP_LIQUIDITY_EVENTS_NOT_VALIDATED','TAKER_SIDE_SPOT_FLOW_NOT_CAPTURED','ORDER_BOOK_DEPTH_NOT_CAPTURED']
            results.append({'coin':sym,'chain':chain,
                'evidence_window':'LAST_5000_CONFIRMED_BLOCKS_NOT_WALLCLOCK',
                'classification':'RESEARCH_ONLY_NOT_BUY_SIGNAL',
                'verified_exchange_addresses':verified,'unverified_exchange_candidates':unverified,
                'transfers':transfers,'coverage':coverage,'missing_evidence':reasons})
        return {'schema':'radar-v2-preparation-research-1',
            'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'source':'PERSISTED_SQLITE_NOT_EXTERNAL_MARKET_QUERY',
            'last_run_id':latest['run_id'] if latest else None,
            'last_run_status':latest['status'] if latest else 'NO_RUN',
            'note':'Block-based counts only. Seen time reflects collection, not necessarily event time. No buy recommendation.',
            'coins':results}
    finally:c.close()
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--db',required=True);ap.add_argument('--config',required=True);ap.add_argument('--out',required=True)
    a=ap.parse_args();report=analyze(a.db,json.loads(pathlib.Path(a.config).read_text()))
    out=pathlib.Path(a.out);out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'coin_count':len(report['coins']),'last_run':report['last_run_id'],
        'verified_cex_labels':sum(x['verified_exchange_addresses'] for x in report['coins']),
        'count_with_transfer_events':sum(x['transfers']['events']>0 for x in report['coins']),
        'signal_count':0}))
if __name__=='__main__':main()
