#!/usr/bin/env python3
"""Conservative public spot trade-side and top-book sampler (Bitget, Gate, MEXC).

Official public endpoints only; NO private API keys, order submission, trades,
backfilled minute-by-minute tape, taker-direction certainty, market-wide volume
coverage, or strong alerts. The last N fills may cover seconds or many hours.
A short *aggregate* gzip snapshot is immutable and suitable for cheap Git.
"""
import argparse
import concurrent.futures
import datetime
import decimal
import gzip
import hashlib
import json
import pathlib
import sqlite3
import time
from radar_stage1 import validate_config,HTTPClient

D=decimal.Decimal
decimal.getcontext().prec=40
SUPPORTED={'bitget','gate','mexc'}
MAX_TRADES=100
MAX_BOOK=40
RECENT_MS=3*60*1000

def dval(raw):
    n=D(str(raw))
    if not n.is_finite() or n<=0:raise ValueError('INVALID_POSITIVE_AMOUNT')
    return n

def num(x):
    return format(x.normalize(),'f')

def spec(venue,pair,kind):
    if venue=='bitget':
        if kind=='fills':return 'https://api.bitget.com/api/v3/market/fills',{'category':'SPOT','symbol':pair,'limit':MAX_TRADES}
        return 'https://api.bitget.com/api/v3/market/orderbook',{'category':'SPOT','symbol':pair,'limit':MAX_BOOK}
    if venue=='gate':
        if kind=='fills':return 'https://api.gateio.ws/api/v4/spot/trades',{'currency_pair':pair,'limit':MAX_TRADES}
        return 'https://api.gateio.ws/api/v4/spot/order_book',{'currency_pair':pair,'limit':MAX_BOOK}
    if venue=='mexc':
        if kind=='fills':return 'https://api.mexc.com/api/v3/trades',{'symbol':pair,'limit':MAX_TRADES}
        return 'https://api.mexc.com/api/v3/depth',{'symbol':pair,'limit':MAX_BOOK}
    raise ValueError('UNSUPPORTED_PUBLIC_SPOT_MARKET')

def trades(data,venue,asof_ms):
    if venue=='bitget':
        if not isinstance(data,dict) or str(data.get('code'))!='00000' or not isinstance(data.get('data'),list):
            raise ValueError('BITGET_INVALID_PUBLIC_FILL_RESPONSE')
        raw=data['data']
    else:
        if not isinstance(data,list):raise ValueError('INVALID_PUBLIC_TRADE_RESPONSE')
        raw=data
    rows={};missing_trade_ids=0
    for index,x in enumerate(raw):
        if not isinstance(x,dict):raise ValueError('INVALID_PUBLIC_TRADE')
        if venue=='mexc':
            if not isinstance(x.get('isBuyerMaker'),bool):
                raise ValueError('MEXC_MISSING_BUYER_MAKER_DIRECTION')
            # If buyer was the maker, the seller took liquidity.
            side='sell' if x['isBuyerMaker'] else 'buy'
            name=x.get('id')
            amount=x.get('qty')
            stamp=x.get('time')
        else:
            side=x.get('side')
            if side not in ('buy','sell'):raise ValueError('NO_VERIFIED_PUBLIC_TRADE_SIDE')
            name=x.get('execId') if venue=='bitget' else x.get('id')
            amount=x.get('size') if venue=='bitget' else x.get('amount')
            stamp=x.get('ts') if venue=='bitget' else x.get('create_time_ms')
        if name is None:
            if venue!='mexc':raise ValueError('MISSING_EXCHANGE_TRADE_ID')
            missing_trade_ids+=1
            name='NO_ID_ENTRY_'+str(index)
        if not str(name):raise ValueError('INVALID_TRADE_ID')
        name=str(name)
        amount=dval(amount)
        price=dval(x.get('price'))
        if stamp is None:raise ValueError('MISSING_PUBLIC_TRADE_TIMESTAMP')
        ms=int(D(str(stamp)))
        if ms<=0 or ms>asof_ms+60000:raise ValueError('INVALID_PUBLIC_FILL_TIMESTAMP')
        item=(ms,side,price*amount)
        if name in rows and rows[name]!=item:raise ValueError('CONFLICTING_DUPLICATE_TRADE_ID')
        rows[name]=item
    allrows=list(rows.values())
    recent=[x for x in allrows if asof_ms-RECENT_MS<=x[0]<=asof_ms]
    def values(sample):
        buy=sum((x[2] for x in sample if x[1]=='buy'),D(0))
        sell=sum((x[2] for x in sample if x[1]=='sell'),D(0))
        total=buy+sell
        return {'count':len(sample),'buy_quote':num(buy),'sell_quote':num(sell),
            'observed_quote_notional':num(total),
            'buy_side_fraction':num(buy/total) if total>0 else None}
    newest=max((x[0] for x in allrows),default=None)
    oldest=min((x[0] for x in allrows),default=None)
    status='UNKNOWN_EMPTY_TRADES' if not allrows else (
        'STALE_SAMPLE' if not recent else 'RECENT_INCOMPLETE_SAMPLE')
    return {'status':status,'sample_limit':MAX_TRADES,'sample_reached_limit':len(raw)>=MAX_TRADES,
        'deduplication_possible':missing_trade_ids==0,
        'missing_exchange_trade_ids':missing_trade_ids,
        'oldest_trade_ms':oldest,'newest_trade_ms':newest,
        'newest_age_seconds':round((asof_ms-newest)/1000,2) if newest is not None else None,
        'sample_span_seconds':round((newest-oldest)/1000,2) if oldest is not None else None,
        'last_3min_observed':values(recent),
        'last_N_trades_observed':values(allrows),
        'trade_side_semantics':('MEXC_BUYER_MAKER_FLAG_INFERRED_TAKER_SIDE'
           if venue=='mexc' else 'PUBLIC_EXCHANGE_TRADE_SIDE_NOT_INDEPENDENTLY_VERIFIED_TAKER_AGGRESSOR'),
        'coverage_note':'ONLY_LAST_UP_TO_100_TRADES; missing trades and time intervals unknown'}


def book(data,venue,asof_ms):
    if venue=='bitget':
        if not isinstance(data,dict) or str(data.get('code'))!='00000':
            raise ValueError('BITGET_INVALID_ORDERBOOK_RESPONSE')
        x=data.get('data')
        if not isinstance(x,dict):raise ValueError('BITGET_NO_DEPTH_DATA')
        asks=x.get('a',[]);bids=x.get('b',[])
        snap_ms=int(x['ts']) if x.get('ts') is not None else None
    else:
        if not isinstance(data,dict):raise ValueError('GATE_OR_MEXC_INVALID_ORDERBOOK_RESPONSE')
        asks=data.get('asks',[]);bids=data.get('bids',[])
        snap_ms=int(data.get('current')) if data.get('current') is not None else None
    def rows(data):
        out=[]
        for row in data:
            if not isinstance(row,(list,tuple)) or len(row)<2:raise ValueError('INVALID_BOOK_ROW')
            out.append((dval(row[0]),dval(row[1])))
        return out
    a=rows(asks);b=rows(bids)
    if not a or not b:raise ValueError('EMPTY_BOOK_SIDE')
    if any(a[i][0]>a[i+1][0] for i in range(len(a)-1)):raise ValueError('ASKS_NOT_ASCENDING')
    if any(b[i][0]<b[i+1][0] for i in range(len(b)-1)):raise ValueError('BIDS_NOT_DESCENDING')
    best_ask=a[0][0];best_bid=b[0][0]
    if best_bid>=best_ask:raise ValueError('CROSSED_OR_LOCKED_BOOK')
    mid=(best_ask+best_bid)/2
    ask2=sum((price*qty for price,qty in a if price<=mid*D('1.02')),D(0))
    bid2=sum((price*qty for price,qty in b if price>=mid*D('0.98')),D(0))
    return {'status':'ONE_INSTANTANEOUS_BOOK_SNAPSHOT',
            'best_bid':num(best_bid),'best_ask':num(best_ask),
            'spread_bps':num((best_ask-best_bid)*D(10000)/mid),
            'ask_quote_notional_within_2pct':num(ask2),
            'bid_quote_notional_within_2pct':num(bid2),
            'snapshot_exchange_ms':snap_ms,
            'snapshot_age_seconds':round((asof_ms-snap_ms)/1000,2) if snap_ms is not None else None,
            'ask_levels_returned':len(a),'bid_levels_returned':len(b),
            'limitations':'TOP_LEVEL_ONLY; no historical ask withdrawal or full order book proof'}

def sample(config,http,run_id,asof_ms=None):
    validate_config(config)
    now=int(time.time()*1000) if asof_ms is None else asof_ms
    venues=[v for v in config['venues'] if v['venue'] in SUPPORTED]
    def each(venue):
        coin=venue['coin'];market=venue['venue'];pair=venue.get('pair')
        result={'coin':coin,'venue':market,'pair':pair,
             'spot_pair_identity':venue.get('identity_status','UNVERIFIED'),
             'independent_spot_token_identity_verified':False,
             'market_purchase_early_alert':False,
             'sample_asof_ms':now}
        if not pair:
            result['trade_status']='UNSUPPORTED_PAIR_NOT_CONFIGURED'
            result['book_status']='UNSUPPORTED_PAIR_NOT_CONFIGURED'
            return result
        for kind in ('fills','depth'):
            key='trade' if kind=='fills' else 'book'
            try:
                url,params=spec(market,pair,kind)
                response=http.get(url,params)
                x=trades(response,market,now) if kind=='fills' else book(response,market,now)
                result[key]=x
                result[key+'_status']=x['status']
            except Exception as e:
                result[key+'_status']='ERROR_UNVERIFIED'
                result[key+'_error']=type(e).__name__+':'+str(e)[:170]
        return result
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        rows=list(executor.map(each,venues))
    return {'schema':'radar-v2-public-spot-microstructure-1',
        'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'asof_ms':now,'run_id':run_id,
        'venues_sampled':len(rows),'coins_with_public_market':len(set(z['coin'] for z in rows)),
        'limitation':'Public last-100 fill windows and instantaneous top-40 books are neither complete taker tape nor time-continuous liquidity.',
        'verified_early_ignition_alerts':0,'strong_alert':False,'buy_signal':False,
        'data':rows}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--config',required=True)
    p.add_argument('--out',required=True);p.add_argument('--archive',required=True)
    a=p.parse_args()
    config=json.loads(pathlib.Path(a.config).read_text())
    conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('CORRUPT_DB')
        r=conn.execute('SELECT run_id FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
        if r is None:raise RuntimeError('NO_COLLECTOR_RUN')
        rid=r[0]
    finally:conn.close()
    report=sample(config,HTTPClient(config['limits']['request_timeout_seconds']),rid)
    raw=(json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
    digest=hashlib.sha256(raw).hexdigest()
    report['sha256_aggregate_payload']=digest
    raw=(json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()
    out=pathlib.Path(a.out);out.parent.mkdir(parents=True,exist_ok=True)
    tmp=out.with_suffix(out.suffix+'.tmp');tmp.write_bytes(raw);tmp.replace(out)
    archive=pathlib.Path(a.archive);archive.parent.mkdir(parents=True,exist_ok=True)
    temp=archive.with_suffix(archive.suffix+'.tmp')
    temp.write_bytes(gzip.compress(raw,compresslevel=8,mtime=0));temp.replace(archive)
    print(json.dumps({'markets':len(report['data']),
        'trades_available':sum(z.get('trade_status')=='RECENT_INCOMPLETE_SAMPLE' for z in report['data']),
        'book_available':sum(z.get('book_status')=='ONE_INSTANTANEOUS_BOOK_SNAPSHOT' for z in report['data']),
        'errors':sum(z.get('trade_status')=='ERROR_UNVERIFIED' or z.get('book_status')=='ERROR_UNVERIFIED' for z in report['data']),
        'archive_bytes':archive.stat().st_size,'strong_alert':False},ensure_ascii=False))

if __name__=='__main__':main()
