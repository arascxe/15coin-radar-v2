#!/usr/bin/env python3
"""Coverage-aware price/volume confirmation; strictly NOT a standalone signal.

All features are computed from already persisted 1m spot OHLCV. Missing minutes
are UNKNOWN, never filled with zero. No new onchain/API ingestion or tables.
"""
import argparse
import datetime
import json
import math
import pathlib
import sqlite3
import statistics

MINUTE=60000
def finite_positive(x):return x is not None and math.isfinite(float(x)) and float(x)>0

def evaluate(rows,expected_end_ms):
    """rows: (minute_ms,open,high,low,close,quote_vol), 120 strictly contiguous minutes."""
    at={int(r[0]):r for r in rows}
    missing=[t for t in range(expected_end_ms-120*MINUTE,expected_end_ms,MINUTE) if t not in at]
    if missing:
        return {'status':'INSUFFICIENT_CONTIGUOUS_MINUTE_COVERAGE',
                'expected_minutes':120,'received_minutes':120-len(missing),
                'missing_minutes':len(missing),
                'rvol_15m':None,'rvol_1h':None,'higher_lows':None,
                'range_compression':None,'volume_price_response':None,
                'volatility_expansion':None}
    bars=[at[t] for t in range(expected_end_ms-120*MINUTE,expected_end_ms,MINUTE)]
    if any(not (finite_positive(r[1]) and finite_positive(r[2]) and finite_positive(r[3]) and
                    finite_positive(r[4]) and math.isfinite(float(r[5])) and float(r[5])>=0 and
                    float(r[3])<=min(float(r[1]),float(r[4]))<=max(float(r[1]),float(r[4]))<=float(r[2]))
           for r in bars):
        return {'status':'INVALID_CANDLE_VALUES_NO_FEATURES',
            'expected_minutes':120,'received_minutes':120,'missing_minutes':0}
    vols=[float(z[5]) for z in bars]
    close=[float(z[4]) for z in bars]
    last15=sum(vols[-15:]);last60=sum(vols[-60:])
    prior60=sum(vols[-120:-60]);prior45=sum(vols[-60:-15])
    # Both compare equal-duration rate, not mismatched 15m vs 60m totals.
    rvol15=(last15/15)/(prior45/45) if prior45>0 else None
    rvol60=last60/prior60 if prior60>0 else None
    lows=[min(float(z[3]) for z in bars[-30+i*10:-20+i*10]) for i in (0,1,2)]
    higher_lows=lows[0]<lows[1]<lows[2]
    price_range=lambda data:(max(float(z[2]) for z in data)-min(float(z[3]) for z in data))/float(data[0][1])
    short_range=price_range(bars[-15:])
    older_ranges=[price_range(bars[i:i+15]) for i in (45,60,75,90)]
    prior_median=statistics.median(older_ranges)
    compression=short_range/prior_median if prior_median>0 else None
    ret15=(close[-1]/close[-16]-1)*100
    # A descriptive price movement per $1000 in observed quote turnover.
    price_volume_impact=ret15/(last15/1000) if last15>0 else None
    logreturns=[math.log(close[i]/close[i-1]) for i in range(1,len(close))]
    recent_vol=statistics.pstdev(logreturns[-15:])
    older_vol=statistics.pstdev(logreturns[-75:-15])
    vol_expansion=recent_vol/older_vol if older_vol>0 else None
    return {'status':'COMPLETE_120_CONSECUTIVE_1M_BARS','expected_minutes':120,
       'received_minutes':120,'missing_minutes':0,
       'rvol_15m':round(rvol15,4) if rvol15 is not None else None,
       'rvol_1h':round(rvol60,4) if rvol60 is not None else None,
       'higher_lows':higher_lows,'last_three_10m_lows':list(map(lambda x:round(x,9),lows)),
       'range_compression':round(compression,4) if compression is not None else None,
       'range_compression_interpretation':'RATIO_ONLY_NOT_PROOF_OF_MANIPULATION',
       'return_15m_pct':round(ret15,4),
       'volume_price_response_pct_per_1000_quote':round(price_volume_impact,6) if price_volume_impact is not None else None,
       'volatility_expansion_ratio':round(vol_expansion,4) if vol_expansion is not None else None,
       'volume_is_not_taker_buy_pressure':True}

def cross_market_dispersion(conn,symbol,config):
    """Compare equal-minute USDT venue closes only, not unrelated timestamps.

    Pair/token identity still has not been independently verified across venues.
    A 2% difference is not a buy signal or an executable arbitrage quote.
    """
    allowed={v['venue'] for v in config['venues'] if v['coin']==symbol
             and str(v.get('pair','')).upper().endswith('USDT')}
    if len(allowed)<2:return {'status':'INSUFFICIENT_MATCHED_USDT_VENUES'}
    last=conn.execute("SELECT MAX(minute_ms) FROM market_candles WHERE coin=?",(symbol,)).fetchone()[0]
    if last is None:return {'status':'NO_STORED_CANDLES'}
    for off in range(4):
        t=last-off*MINUTE
        rows=conn.execute("SELECT venue,close FROM market_candles WHERE coin=? AND minute_ms=?",
                          (symbol,t)).fetchall()
        prices=[float(p) for venue,p in rows if venue in allowed and finite_positive(p)]
        if len(prices)>=2:
            return {'status':'MATCHED_MINUTE_RESEARCH_ONLY','minute_ms':t,
                'matched_venues':len(prices),
                'max_min_close_difference_pct':round(100*(max(prices)/min(prices)-1),4),
                'warning':'SYMBOL_IDENTITY_UNVERIFIED; CANDLE_DIFFERENCE_NOT_EXECUTABLE_BID_ASK'}
    return {'status':'NO_MATCHED_MINUTE_WITHIN_LAST_4M'}

def build(conn,config):
    syms={x['symbol'] for x in config['coins']}
    entries=[]
    for c in config['coins']:
        sym=c['symbol'];markets=[]
        for spec in config['venues']:
            if spec['coin']!=sym:continue
            venue=spec['venue']
            last=conn.execute('SELECT MAX(minute_ms) FROM market_candles WHERE coin=? AND venue=?',(sym,venue)).fetchone()[0]
            if last is None:
                markets.append({'venue':venue,'status':'NO_STORED_CANDLES'})
                continue
            end=(last//MINUTE+1)*MINUTE
            rows=conn.execute('''SELECT minute_ms,open,high,low,close,quote_vol
               FROM market_candles WHERE coin=? AND venue=? AND minute_ms>=? AND minute_ms<?
               ORDER BY minute_ms''',(sym,venue,end-120*MINUTE,end)).fetchall()
            markets.append({'venue':venue,'latest_minute_ms':last,'features':evaluate(rows,end)})
        entries.append({'coin':sym,'markets':markets,
             'cross_venue_close_dispersion':cross_market_dispersion(conn,sym,config),
             'strong_alert':False,'buy_signal':False})
    return {'schema':'radar-v2-technical-confirmation-1',
        'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'coin_count':len(entries),'strong_alert':False,
        'data_source':'ALREADY_STORED_SPOT_OHLCV_NO_NEW_API',
        'warning':'TECHNICAL_METRICS_ARE_SECONDARY_CONTEXT_NOT_BUY_SIGNALS',
        'coins':entries}

def main():
    p=argparse.ArgumentParser();p.add_argument('--db',required=True)
    p.add_argument('--config',required=True);p.add_argument('--out',required=True)
    a=p.parse_args()
    config=json.loads(pathlib.Path(a.config).read_text())
    conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('CORRUPT_DB')
        result=build(conn,config)
    finally:conn.close()
    dest=pathlib.Path(a.out);dest.parent.mkdir(parents=True,exist_ok=True)
    tmp=dest.with_suffix(dest.suffix+'.tmp')
    tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');tmp.replace(dest)
    print(json.dumps({'coins':len(result['coins']),
       'complete_markets':sum(x.get('features',{}).get('status')=='COMPLETE_120_CONSECUTIVE_1M_BARS' for c in result['coins'] for x in c['markets']),
       'strong_alert':False}))
if __name__=='__main__':main()
