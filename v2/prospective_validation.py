#!/usr/bin/env python3
"""Leakage-free prospective evaluation on genuinely archived public spot samples.

The archive began 2026-11-17. It does NOT contain BTW/AKE/IRYS historical
spot order books or verified past CEX inventories. Never claim retrospective
10x detection or meaningful false-positive rates from a few hourly records.

Decision is reconstructed from snapshot time using past (not future) snapshots;
price outcomes are observed only from later complete OHLCV windows.
"""
import argparse, datetime, gzip, json, pathlib, sqlite3
from collections import Counter,defaultdict
from decision_engine import build as decide,EXPECTED
from economic_fusion import build as fuse,MIN_INTERVAL_MS,MAX_INTERVAL_MS
M=60000
HORIZONS=(15,60,240)
RET_THRESHOLDS=(5,20,100,900)
MIN_INDEPENDENT=30
MIN_POSITIVES=5
VENUE_ORDER=('gate','mexc','bitget','kucoin','bybit','okx')

def read_archive(dirpath):
    out=[];bad=[]
    for file in sorted(pathlib.Path(dirpath).glob('*.json.gz')):
        try:
            if file.stat().st_size>300000:raise ValueError('OVERSIZE')
            x=json.loads(gzip.decompress(file.read_bytes()))
            if (x.get('schema')!='radar-v2-public-spot-microstructure-1' or
                not isinstance(x.get('asof_ms'),int) or
                not isinstance(x.get('data'),list)):raise ValueError('INVALID_SAMPLE')
            out.append(x)
        except Exception as exc:bad.append({'file':file.name,'reason':type(exc).__name__})
    distinct={x['run_id']:x for x in out}
    return sorted(distinct.values(),key=lambda x:x['asof_ms']),bad

def decision_at(current,prior):
    """Only snapshot-local spot information, no future wallet/technical data."""
    brain={'schema':'radar-v2-economic-brain-1','last_run_id':current['run_id'],
        'coins':[{'coin':c,'chain_currently_complete':False,'wallet_accumulation_candidates':[]}
                 for c in EXPECTED]}
    technical={'schema':'radar-v2-technical-confirmation-1',
        'coins':[{'coin':c,'markets':[]} for c in EXPECTED]}
    preliminary=fuse(current,brain,prior,technical)
    decision=decide(preliminary,current,technical)
    return {x['coin']:x['state'] for x in decision['coins']}

def choose_price_venue(conn,coin,asof_ms):
    # Fixed preference avoids looking into future to select best-performing venue.
    m=(asof_ms//M)*M
    for venue in VENUE_ORDER:
        row=conn.execute("""SELECT minute_ms,close FROM market_candles
           WHERE coin=? AND venue=? AND minute_ms<=?
           ORDER BY minute_ms DESC LIMIT 1""",(coin,venue,m-2*M)).fetchone()
        if row is not None and 0<=m-2*M-row[0]<=3*M and float(row[1])>0:
            return venue,row[0],float(row[1])
    return None

def future_outcome(conn,coin,asof_ms,horizon,venue_info=None):
    """Max subsequent printed 1m high = upper bound, NOT realizable profit."""
    if venue_info is None:venue_info=choose_price_venue(conn,coin,asof_ms)
    if venue_info is None:return {'status':'UNKNOWN_NO_FRESH_PRE_EVENT_PRICE'}
    venue,base_minute,baseline=venue_info
    m=(asof_ms//M)*M
    begin=m+M;end=m+horizon*M
    rows=conn.execute("""SELECT minute_ms,high,low,close FROM market_candles
       WHERE coin=? AND venue=? AND minute_ms>=? AND minute_ms<=?
       ORDER BY minute_ms""",(coin,venue,begin,end)).fetchall()
    if (len(rows)!=horizon or any(rows[i][0]!=begin+i*M for i in range(horizon))):
        return {'status':'UNKNOWN_FUTURE_WINDOW_NOT_COMPLETE',
               'venue':venue,'baseline_minute':base_minute,
               'expected_minutes':horizon,'observed_minutes':len(rows)}
    maximum=max(float(x[1]) for x in rows)
    close=float(rows[-1][3])
    return {'status':'MATURED_COMPLETE_FUTURE_BARS','venue':venue,
            'baseline_minute':base_minute,'baseline_close':round(baseline,10),
            'future_max_high_return_pct':round(100*(maximum/baseline-1),4),
            'future_end_close_return_pct':round(100*(close/baseline-1),4),
            'warning':'BEST_CASE_HISTORICAL_HIGH_NOT_EXECUTABLE_RETURN_NO_FEES'}

def review(conn,samples):
    trials=[]
    for i,current in enumerate(samples):
        past=[x for x in samples[:i] if
              MIN_INTERVAL_MS<=current['asof_ms']-x['asof_ms']<=MAX_INTERVAL_MS]
        if not past:continue
        previous=past[-1]
        states=decision_at(current,previous)
        for coin in EXPECTED:
            item={'coin':coin,'asof_ms':current['asof_ms'],
                 'sample_run_id':current['run_id'],
                 'prior_run_id':previous['run_id'],
                 'source':'SPOT_ONLY_REPLAY_NO_WALLET_HISTORY_AVAILABLE',
                 'state':states[coin],
                 'research_candidate':states[coin]=='SPOT_IGNITION_RESEARCH_WATCH'}
            price=choose_price_venue(conn,coin,current['asof_ms'])
            item['forward']={str(h):future_outcome(conn,coin,current['asof_ms'],h,price)
                             for h in HORIZONS}
            trials.append(item)
    return trials

def summarise(trials,files,invalid):
    evidence=[]
    for horizon in HORIZONS:
        completed=[x for x in trials if x['forward'][str(horizon)]['status']=='MATURED_COMPLETE_FUTURE_BARS']
        missing=len(trials)-len(completed)
        for threshold in RET_THRESHOLDS:
            # Descriptive only, repeated time windows NOT independent trials.
            tp=fp=fn=tn=0
            for x in completed:
                up=x['forward'][str(horizon)]['future_max_high_return_pct']>=threshold
                watch=x['research_candidate']
                if up and watch:tp+=1
                elif not up and watch:fp+=1
                elif up and not watch:fn+=1
                else:tn+=1
            n=len(completed);positives=tp+fn
            # Even 30 overlapping snapshots are NOT 30 independent examples.
            evidence.append({'horizon_minutes':horizon,'threshold_future_max_high_pct':threshold,
                'matured_windows':n,'unknown_or_unmatured_windows':missing,
                'research_tp':tp,'research_fp':fp,'research_fn':fn,'research_tn':tn,
                'statistically_valid_precision':None,
                'statistically_valid_false_positive_rate':None,
                'why_no_valid_rate':'OVERLAPPING_WINDOWS; SMALL_HISTORY; HISTORICAL_10X_GROUND_TRUTH_NOT_ESTABLISHED'})
    return {'schema':'radar-v2-forward-replay-1',
        'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'archive_samples':files,'bad_archive_files':invalid,
        'replayable_snapshot_trials':len(trials),'distinct_coins':15,
        'first_snapshot_ms':min([x['asof_ms'] for x in trials],default=None),
        'last_snapshot_ms':max([x['asof_ms'] for x in trials],default=None),
        'reference_exits_backtested':False,
        'historical_false_alarm_probability_validated':False,
        'wallet_preparation_historical_replay_available':False,
        'return_measure_is_realizable_profit':False,
        'limitations':['NO_RECORDED_2023_2024_ORDERBOOK_HISTORY_FOR_REFERENCE_PUMPS',
            'SPOT_SAMPLE_IS_NOT_FULL_TAKER_TAPE','MATURED_FUTURE_HIGHS_ARE_NOT_EXECUTABLE',
            'EACH_COIN_HOURLY_WINDOW_OVERLAPS_NEIGHBORING_WINDOWS',
            'SOURCE_IDENTITY_UNVERIFIED','NO_FEES_SLIPPAGE_ORDERBOOK_EXECUTION'],
        'horizon_metrics_exploratory_only':evidence,
        'examples':[{'coin':x['coin'],'asof_ms':x['asof_ms'],'state':x['state'],
            'forward_15m':x['forward']['15']['status'],
            'return_15m_pct':x['forward']['15'].get('future_max_high_return_pct')}
            for x in trials if x['research_candidate']][:25]}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--archive',required=True)
    p.add_argument('--out',required=True);a=p.parse_args()
    samples,invalid=read_archive(a.archive)
    conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('SQLITE_CORRUPT')
        trials=review(conn,samples)
    finally:conn.close()
    report=summarise(trials,len(samples),invalid)
    dest=pathlib.Path(a.out);dest.parent.mkdir(parents=True,exist_ok=True)
    tmp=dest.with_suffix(dest.suffix+'.tmp')
    tmp.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');tmp.replace(dest)
    print(json.dumps({'snapshots':report['archive_samples'],
        'replay_trials':report['replayable_snapshot_trials'],
        'matured_15m':report['horizon_metrics_exploratory_only'][0]['matured_windows'],
        'validated_false_positive_rate':None}))
if __name__=='__main__':main()
