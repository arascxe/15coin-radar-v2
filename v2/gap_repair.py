#!/usr/bin/env python3
"""Bounded spot OHLCV gap retry. Missing bars remain UNKNOWN, never fabricated as zero trades.
Independent of the live source checks and the confirmed on-chain block cursor.
"""
import argparse
import collections
import datetime
import json
import pathlib
import sqlite3
import time

from radar_stage1 import HTTPClient, Store, MINUTE_MS, get_market_spec, parse_candles, now_ms

DDL = """
CREATE TABLE IF NOT EXISTS spot_gap_repairs(
  run_id TEXT NOT NULL, coin TEXT NOT NULL, venue TEXT NOT NULL,
  window_start_ms INTEGER NOT NULL, window_end_ms INTEGER NOT NULL,
  status TEXT NOT NULL, recovered_minutes INTEGER NOT NULL,
  remaining_minutes INTEGER NOT NULL, detail TEXT NOT NULL,
  observed_ms INTEGER NOT NULL,
  PRIMARY KEY(run_id,coin,venue)
);
"""

def market_sources(config):
    result={(v['coin'],v['venue']):v for v in config['venues'] if v.get('pair')}
    for p in config['dex_pools']:
        if p.get('pool'):
            result[(p['coin'],'geckoterminal')]={
                'coin':p['coin'],'venue':'geckoterminal',
                'chain':p['chain'],'pool':p['pool'],
                'pair':p.get('pair_label',p['coin']+'/DEX')}
    return result

def candidates(store):
    # One least-recently-attempted missing minute per venue; no permanent
    # starvation if a sparse venue really had no trades.
    return store.db.execute("""
      SELECT coin,venue,minute_ms FROM (
        SELECT coin,venue,minute_ms,last_seen_ms,
          ROW_NUMBER() OVER (
            PARTITION BY coin,venue ORDER BY last_seen_ms,minute_ms
          ) AS rank_in_venue
        FROM market_missing_minutes
      ) WHERE rank_in_venue=1
      ORDER BY last_seen_ms,minute_ms LIMIT 120
    """).fetchall()

def repair_gaps(store,run_id,config,http,ts=None,max_windows=6,window_minutes=30):
    ts=now_ms() if ts is None else ts
    closed_to=(ts//MINUTE_MS)*MINUTE_MS-2*MINUTE_MS
    sources=market_sources(config)
    store.db.execute(DDL)
    store.db.commit()
    attempts=[]
    for coin,venue,start in candidates(store):
        if len(attempts)>=max_windows:break
        market=sources.get((coin,venue))
        cursor=store.last_market_end(coin,venue)
        if market is None or cursor is None:continue
        end=min(start+window_minutes*MINUTE_MS,cursor,closed_to)
        if start>=end:continue
        status="ERROR";detail="";observed={}
        try:
            url,params=get_market_spec(market,start,end)
            raw=http.get(url,params)
            parsed=parse_candles(venue,raw)
            observed={b['minute_ms']:b for b in parsed if start<=b['minute_ms']<end}
            detail="OHLCV_RETRY_ONLY; NO_VERIFIED_TAKER_TAPE_OR_ZERO_TRADE_PROOF"
        except Exception as exc:
            detail=type(exc).__name__+":"+str(exc)[:180]
        with store.db:
            prior=store.db.execute(
                "SELECT COUNT(*) FROM market_missing_minutes WHERE coin=? AND venue=? AND minute_ms>=? AND minute_ms<?",
                (coin,venue,start,end)).fetchone()[0]
            for b in observed.values():
                store.db.execute("""
                    INSERT INTO market_candles
                    (coin,venue,pair,minute_ms,open,high,low,close,quote_vol,source_timestamp_ms,seen_ms)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(coin,venue,minute_ms) DO UPDATE SET
                      open=excluded.open,high=excluded.high,low=excluded.low,
                      close=excluded.close,quote_vol=excluded.quote_vol,
                      source_timestamp_ms=excluded.source_timestamp_ms,seen_ms=excluded.seen_ms
                    """,(coin,venue,market['pair'],b['minute_ms'],b['open'],b['high'],
                         b['low'],b['close'],b['quote_vol'],b['minute_ms'],ts))
                store.db.execute(
                    "DELETE FROM market_missing_minutes WHERE coin=? AND venue=? AND minute_ms=?",
                    (coin,venue,b['minute_ms']))
            remaining=store.db.execute(
                "SELECT COUNT(*) FROM market_missing_minutes WHERE coin=? AND venue=? AND minute_ms>=? AND minute_ms<?",
                (coin,venue,start,end)).fetchone()[0]
            # Rotate still-unresolved minutes; never erase them or call them no-trade.
            store.db.execute(
                "UPDATE market_missing_minutes SET last_seen_ms=? WHERE coin=? AND venue=? AND minute_ms>=? AND minute_ms<?",
                (ts,coin,venue,start,end))
            restored=prior-remaining
            if not detail.startswith(("HTTPError:","URLError:","TimeoutError:","ValueError:","RuntimeError:","ConnectionError:")) and not (detail.startswith("Exception:")):
                status="OK" if remaining==0 else ("PARTIAL" if restored else "UNKNOWN")
            store.db.execute("""
                INSERT OR REPLACE INTO spot_gap_repairs VALUES(?,?,?,?,?,?,?,?,?,?)
            """,(run_id,coin,venue,start,end,status,restored,remaining,detail,ts))
        attempts.append({'coin':coin,'venue':venue,'status':status,
                         'recovered_minutes':restored,'remaining_minutes':remaining,
                         'start_ms':start,'end_ms':end,'detail':detail})
    total=store.db.execute("SELECT COUNT(*) FROM market_missing_minutes").fetchone()[0]
    return {'status_counts':dict(collections.Counter(x['status'] for x in attempts)),
            'attempts':attempts,'resolved_this_run':sum(x['recovered_minutes'] for x in attempts),
            'unresolved_spot_minutes':total,
            'note':'Absence of candle does not establish zero trade activity.'}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True)
    p.add_argument('--config',required=True)
    p.add_argument('--report-path',required=True)
    a=p.parse_args()
    if not pathlib.Path(a.db).is_file():
        raise FileNotFoundError('REFUSE_BOOTSTRAP_MISSING_LIVE_DATABASE')
    config=json.loads(pathlib.Path(a.config).read_text())
    store=Store(a.db)
    try:
        row=store.db.execute("SELECT run_id FROM runs ORDER BY started_ms DESC LIMIT 1").fetchone()
        if not row:raise RuntimeError('NO_PERSISTED_RUN_FOR_REPAIR')
        report=repair_gaps(store,row[0],config,HTTPClient(config['limits']['request_timeout_seconds']))
    finally:store.close()
    report_path=pathlib.Path(a.report_path)
    if report_path.exists():
        doc=json.loads(report_path.read_text())
        if doc.get('run_id')==row[0]:
            doc['gap_repair']=report
            if isinstance(doc.get('health'),dict):
                doc['health']['unresolved_spot_minutes']=report['unresolved_spot_minutes']
            report_path.write_text(json.dumps(doc,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'gap_repair':report},indent=2,ensure_ascii=False))

if __name__=='__main__':main()
