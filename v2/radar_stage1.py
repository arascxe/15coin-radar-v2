#!/usr/bin/env python3
"""15-coin early-radar v2 / STAGE 1. Read-only evidence ingestion, no signals, orders or notifications.

NO MAGIC GUARANTEES: this stage checks source coverage and persists raw observations. A successful HTTP
request is NOT proof of continuous trades, full CEX reserves or holder identity.
Python 3.11+, standard library only.
"""
import argparse
import concurrent.futures
import datetime as dt
import decimal
import hashlib
import json
import math
import pathlib
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict

ROOT = pathlib.Path(__file__).resolve().parent
TOKEN_RE = re.compile(r'^0x[0-9a-f]{40}$')
MINUTE_MS = 60000
BSC_RPC = ('https://bsc-dataseed.bnbchain.org', 'https://bsc.publicnode.com')
BASE_RPC = ('https://mainnet.base.org', 'https://base.publicnode.com')
CHAINS = {'bsc': {'id':56, 'rpc':BSC_RPC,'block_seconds':1.0},'base': {'id':8453,'rpc':BASE_RPC,'block_seconds':2.0}}


def utc_ms(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat(timespec='seconds')

def now_ms():return int(time.time()*1000)

def finite_num(v):
    x=float(v)
    if not math.isfinite(x):raise ValueError('non-finite market number')
    return x

def json_file(p):return json.loads(pathlib.Path(p).read_text(encoding='utf-8'))

def validate_config(c):
    expected=['ON','DOS','HANA','COLLECT','LIGHT','APR','IN','TRUST','B2','CYS','STBL','BSB','AIO','STAR','LYN']
    cs=c['coins']
    if len(cs)!=15 or set(x['symbol'] for x in cs)!=set(expected):raise ValueError('MUST_HAVE_EXACT_15_TOKEN_SYMBOLS')
    if len({(x['chain'],x['token']) for x in cs})!=15:raise ValueError('COIN_CONTRACT_COLLISION')
    for x in cs:
        if not TOKEN_RE.fullmatch(x['token']) or x['chain'] not in CHAINS or x['chain_id']!=CHAINS[x['chain']]['id'] or x['decimals'] not in range(0,31):raise ValueError('BAD_TOKEN_CONFIG')
    for w in c['wallets']:
        if not TOKEN_RE.fullmatch(w['address']) or w['coin'] not in expected:raise ValueError('BAD_WALLET_CONFIG')
    kinds=set()
    for v in c['venues']:
        key=(v['coin'],v['venue']);
        if key in kinds or v['market_type']!='SPOT' or v['coin'] not in expected:raise ValueError('BAD_OR_DUPLICATE_MARKET')
        kinds.add(key)
    return True

SCHEMA='''
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY,started_ms INTEGER NOT NULL,finished_ms INTEGER,status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS run_lease(lock_key TEXT PRIMARY KEY,owner TEXT NOT NULL,expires_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS market_candles(coin TEXT NOT NULL,venue TEXT NOT NULL,pair TEXT NOT NULL,minute_ms INTEGER NOT NULL,
  open REAL NOT NULL,high REAL NOT NULL,low REAL NOT NULL,close REAL NOT NULL,quote_vol REAL NOT NULL,
  source_timestamp_ms INTEGER NOT NULL,seen_ms INTEGER NOT NULL,
  PRIMARY KEY(coin,venue,minute_ms));
CREATE TABLE IF NOT EXISTS market_cursor(coin TEXT NOT NULL,venue TEXT NOT NULL,last_queried_end_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,
  PRIMARY KEY(coin,venue));
CREATE TABLE IF NOT EXISTS market_bootstrap(coin TEXT NOT NULL,venue TEXT NOT NULL,first_requested_ms INTEGER NOT NULL, PRIMARY KEY(coin,venue));
CREATE TABLE IF NOT EXISTS market_checks(run_id TEXT NOT NULL,coin TEXT NOT NULL,venue TEXT NOT NULL,pair TEXT,status TEXT NOT NULL,
  window_start_ms INTEGER,window_end_ms INTEGER,received_bars INTEGER NOT NULL DEFAULT 0,expected_minutes INTEGER NOT NULL DEFAULT 0,
  missing_minutes INTEGER,detail TEXT, PRIMARY KEY(run_id,coin,venue));
CREATE TABLE IF NOT EXISTS wallet_balances(coin TEXT NOT NULL,chain TEXT NOT NULL,address TEXT NOT NULL,block INTEGER NOT NULL,raw_amount TEXT NOT NULL,
  seen_ms INTEGER NOT NULL, PRIMARY KEY(coin,chain,address,block));
CREATE TABLE IF NOT EXISTS wallet_checks(run_id TEXT NOT NULL,coin TEXT NOT NULL,address TEXT NOT NULL,chain TEXT NOT NULL,status TEXT NOT NULL,block INTEGER,detail TEXT,
  PRIMARY KEY(run_id,coin,address,chain));
CREATE TABLE IF NOT EXISTS token_transfers(chain TEXT NOT NULL,chain_id INTEGER NOT NULL,token TEXT NOT NULL,coin TEXT NOT NULL,
  tx_hash TEXT NOT NULL,log_index INTEGER NOT NULL,block INTEGER NOT NULL,from_address TEXT NOT NULL,to_address TEXT NOT NULL,
  amount_raw TEXT NOT NULL,seen_ms INTEGER NOT NULL,
  PRIMARY KEY(chain_id,tx_hash,log_index,token));
CREATE TABLE IF NOT EXISTS chain_cursor(chain TEXT PRIMARY KEY,last_complete_block INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS chain_checks(run_id TEXT NOT NULL,chain TEXT NOT NULL,status TEXT NOT NULL,
  from_block INTEGER,to_block INTEGER,head_block INTEGER,missing_from INTEGER,missing_to INTEGER,detail TEXT,
  PRIMARY KEY(run_id,chain));
CREATE TABLE IF NOT EXISTS source_gaps(run_id TEXT NOT NULL,source_type TEXT NOT NULL,source_key TEXT NOT NULL,from_coord TEXT NOT NULL,to_coord TEXT NOT NULL,
  reason TEXT NOT NULL,PRIMARY KEY(run_id,source_type,source_key,from_coord,to_coord));
CREATE INDEX IF NOT EXISTS idx_candles_time ON market_candles(minute_ms);
CREATE INDEX IF NOT EXISTS idx_token_transfer_coin ON token_transfers(coin,block);
CREATE INDEX IF NOT EXISTS idx_wallet_snap ON wallet_balances(coin,address,block);
'''

class Store:
    def __init__(self,path):
        path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
        self.path=path
        self.db=sqlite3.connect(path,timeout=10)
        self.db.execute('PRAGMA busy_timeout=10000')
        try:
            integrity=self.db.execute('PRAGMA quick_check').fetchone()[0]
            if integrity!='ok':raise RuntimeError('CORRUPT_DB_FAIL_CLOSED: '+integrity)
        except BaseException:
            self.db.close()
            raise
        self.db.executescript(SCHEMA)
        self.db.commit()
    def close(self):self.db.close()
    def acquire_lease(self):
        owner=hashlib.sha256(f'{time.time_ns()}:{self.path}'.encode()).hexdigest()[:24]
        now=now_ms()
        try:
            self.db.execute('BEGIN IMMEDIATE')
            row=self.db.execute('SELECT owner,expires_ms FROM run_lease WHERE lock_key=?',('collector',)).fetchone()
            if row is not None and row[1]>now:
                self.db.rollback()
                raise RuntimeError('CONCURRENT_RUN_BLOCKED: another collector owns the live lease')
            self.db.execute('INSERT OR REPLACE INTO run_lease VALUES(?,?,?)',('collector',owner,now+45*60*1000))
            self.db.commit()
            return owner
        except BaseException:
            self.db.rollback()
            raise
    def release_lease(self,owner):
        with self.db:self.db.execute('DELETE FROM run_lease WHERE lock_key=? AND owner=?',('collector',owner))
    def fail_run(self,rid):
        with self.db:self.db.execute('UPDATE runs SET finished_ms=?,status=? WHERE run_id=?',(now_ms(),'FAILED_NOT_COMPLETE',rid))
    def begin_run(self,ts):
        rid=hashlib.sha256(f'{ts}:{time.time_ns()}'.encode()).hexdigest()[:18]
        with self.db:
            self.db.execute('INSERT INTO runs VALUES(?,?,NULL,?)',(rid,ts,'RUNNING'))
        return rid
    def finish_run(self,rid,now):
        with self.db:
            n=self.db.execute('SELECT COUNT(*) FROM market_checks WHERE run_id=? AND status NOT IN (\'OK\')',(rid,)).fetchone()[0]
            n+=self.db.execute('SELECT COUNT(*) FROM chain_checks WHERE run_id=? AND status!=\'OK\' ',(rid,)).fetchone()[0]
            n+=self.db.execute('SELECT COUNT(*) FROM wallet_checks WHERE run_id=? AND status!=\'OK\' ',(rid,)).fetchone()[0]
            self.db.execute('UPDATE runs SET finished_ms=?, status=? WHERE run_id=?',(now,'COMPLETE_SOURCE_OBSERVATIONS' if n==0 else 'PARTIAL_OR_FAILED_SOURCE_OBSERVATIONS',rid))
    def last_market_end(self,coin,venue):
        x=self.db.execute('SELECT last_queried_end_ms FROM market_cursor WHERE coin=? AND venue=?',(coin,venue)).fetchone()
        return x[0] if x else None
    def market_initial_start(self,coin,venue):
        x=self.db.execute('SELECT first_requested_ms FROM market_bootstrap WHERE coin=? AND venue=?',(coin,venue)).fetchone()
        return x[0] if x else None
    def market_save(self,rid,coin,venue,pair,status,t0,t1,bars,detail='',observed_ms=None,backlog_to_ms=None):
        now=observed_ms or now_ms();expected=max(0,(t1-t0)//MINUTE_MS);n=len({b['minute_ms'] for b in bars});missing=max(0,expected-n)
        with self.db:
            self.db.execute('INSERT INTO market_bootstrap VALUES(?,?,?) ON CONFLICT(coin,venue) DO UPDATE SET first_requested_ms=MIN(first_requested_ms,excluded.first_requested_ms)',(coin,venue,t0))
            for b in bars:
                self.db.execute('INSERT INTO market_candles VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(coin,venue,minute_ms) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,quote_vol=excluded.quote_vol,source_timestamp_ms=excluded.source_timestamp_ms,seen_ms=excluded.seen_ms',
                        (coin,venue,pair,b['minute_ms'],b['open'],b['high'],b['low'],b['close'],b['quote_vol'],b['minute_ms'],now))
            # Missing minute is UNKNOWN, not zero volume. Never silently discard holes.
            if status in ('OK','BACKLOG','PARTIAL','UNKNOWN'):
                present={b['minute_ms'] for b in bars}
                self.db.executemany('DELETE FROM market_missing_minutes WHERE coin=? AND venue=? AND minute_ms=?',((coin,venue,m) for m in present))
                self.db.executemany('INSERT INTO market_missing_minutes VALUES(?,?,?,?,?) ON CONFLICT(coin,venue,minute_ms) DO UPDATE SET last_seen_ms=excluded.last_seen_ms',((coin,venue,m,now,now) for m in range(t0,t1,MINUTE_MS) if m not in present))
            self.db.execute('INSERT INTO market_checks VALUES(?,?,?,?,?,?,?,?,?,?,?)',(rid,coin,venue,pair,status,t0,t1,n,expected,missing,detail))
            if status in ('OK','BACKLOG','PARTIAL','UNKNOWN'):
                self.db.execute('INSERT INTO market_cursor VALUES(?,?,?,?) ON CONFLICT(coin,venue) DO UPDATE SET last_queried_end_ms=MAX(last_queried_end_ms,excluded.last_queried_end_ms),updated_ms=excluded.updated_ms',(coin,venue,t1,now))
            if status!='OK':
                gap_start=t1 if status=='BACKLOG' else t0
                gap_end=backlog_to_ms if status=='BACKLOG' and backlog_to_ms is not None else t1
                self.db.execute('INSERT OR REPLACE INTO source_gaps VALUES(?,?,?,?,?,?)',(rid,'spot_1m',f'{coin}:{venue}',str(gap_start),str(gap_end),detail or status))
        return {'coin':coin,'venue':venue,'status':status,'bars':n,'expected_minutes':expected,'missing_minutes':missing,'from_utc':utc_ms(t0),'to_utc':utc_ms(t1),'detail':detail}

    def wallet_save(self,rid,c,addr,block,raw,err=None,observed_ms=None):
        now=observed_ms or now_ms();status='ERROR' if err is not None else 'OK'
        with self.db:
            self.db.execute('INSERT INTO wallet_checks VALUES(?,?,?,?,?,?,?)',(rid,c['symbol'],addr,c['chain'],status,block,str(err)[:200] if err else None))
            if not err:self.db.execute('INSERT OR IGNORE INTO wallet_balances VALUES(?,?,?,?,?,?)',(c['symbol'],c['chain'],addr,block,str(raw),now))
        return status
    def chain_save(self,rid,chain,head,from_b,to_b,transfers,status,detail='',observed_ms=None):
        now=observed_ms or now_ms()
        with self.db:
            for x in transfers:
                self.db.execute('INSERT OR IGNORE INTO token_transfers VALUES(?,?,?,?,?,?,?,?,?,?,?)',(
                    chain,CHAINS[chain]['id'],x['token'],x['coin'],x['tx'],x['log_index'],x['block'],x['from'],x['to'],str(x['raw']),now))
            last=head if to_b is None else to_b
            self.db.execute('INSERT INTO chain_checks VALUES(?,?,?,?,?,?,?,?,?)',(rid,chain,status,from_b,to_b,head,None if status=='OK' else (to_b+1 if to_b is not None else from_b),None if status=='OK' else head,detail))
            if to_b is not None and to_b>=from_b:
                self.db.execute('INSERT INTO chain_cursor VALUES(?,?,?) ON CONFLICT(chain) DO UPDATE SET last_complete_block=MAX(last_complete_block,excluded.last_complete_block),updated_ms=excluded.updated_ms',(chain,to_b,now))
            if status!='OK':self.db.execute('INSERT OR REPLACE INTO source_gaps VALUES(?,?,?,?,?,?)',(rid,'onchain_transfer',chain,str(to_b+1 if to_b is not None else from_b),str(head),detail or status))
    def last_chain_block(self,chain):
        x=self.db.execute('SELECT last_complete_block FROM chain_cursor WHERE chain=?',(chain,)).fetchone();return x[0] if x else None
    def report(self,rid):
        market=list(self.db.execute('SELECT coin,venue,status,received_bars,expected_minutes,missing_minutes,detail FROM market_checks WHERE run_id=? ORDER BY coin,venue',(rid,)))
        chain=list(self.db.execute('SELECT chain,status,from_block,to_block,head_block,detail FROM chain_checks WHERE run_id=? ORDER BY chain',(rid,)))
        wallets=list(self.db.execute('SELECT coin,status,COUNT(*) FROM wallet_checks WHERE run_id=? GROUP BY coin,status ORDER BY coin,status',(rid,)))
        gaps=self.db.execute('SELECT COUNT(*) FROM source_gaps WHERE run_id=?',(rid,)).fetchone()[0]
        return {'run_id':rid,'market_counts':dict(__import__('collections').Counter(x[2] for x in market)),'market_checks': [{'coin':x[0],'venue':x[1],'status':x[2],'bars':x[3],'expected':x[4],'missing':x[5],'note':x[6]} for x in market], 'chain_checks':[{'chain':x[0],'status':x[1],'from':x[2],'to':x[3],'head':x[4],'detail':x[5]} for x in chain],'wallet_checks':[{'coin':x[0],'status':x[1],'count':x[2]} for x in wallets], 'gap_records':gaps,'total_candles':self.db.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0], 'total_transfers':self.db.execute('SELECT COUNT(*) FROM token_transfers').fetchone()[0], 'unresolved_spot_minutes':self.db.execute('SELECT COUNT(*) FROM market_missing_minutes').fetchone()[0]}

class HTTPClient:
    def __init__(self,timeout=8):self.timeout=timeout
    def get(self,url,params):
        q=urllib.parse.urlencode({k:str(v) for k,v in params.items() if v is not None})
        req=urllib.request.Request(url+('?' +q if q else ''),headers={'Accept':'application/json','User-Agent':'RadarV2-Research/0.1'})
        with urllib.request.urlopen(req,timeout=self.timeout) as r:
            return json.load(r)
    def rpc(self,url,method,params):
        req=urllib.request.Request(url,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers={'Content-Type':'application/json'},method='POST')
        with urllib.request.urlopen(req,timeout=self.timeout) as r:resp=json.load(r)
        if resp.get('error'):raise RuntimeError(str(resp['error'])[:200])
        if 'result' not in resp:raise RuntimeError('RPC_MISSING_RESULT')
        return resp['result']


def _bar(t,o,h,l,c,vol):
    x={'minute_ms':int(t),'open':finite_num(o),'high':finite_num(h),'low':finite_num(l),'close':finite_num(c),'quote_vol':finite_num(vol)}
    if x['minute_ms']<0 or x['minute_ms']%MINUTE_MS!=0 or x['open']<=0 or x['close']<=0 or x['low']<=0 or x['high']<max(x['open'],x['close'],x['low']) or x['low']>min(x['open'],x['close']) or x['quote_vol']<0:
        raise ValueError('INVALID_OHLCV_BAR')
    return x

def parse_candles(venue,resp):
    if venue=='gate':return [_bar(int(r[0])*1000,r[5],r[3],r[4],r[2],r[1]) for r in resp]
    if venue=='bitget':
        if str(resp.get('code'))!='00000':raise RuntimeError('BITGET_BAD_STATUS '+str(resp.get('code')))
        return [_bar(r[0],r[1],r[2],r[3],r[4],r[6]) for r in resp.get('data',[])]
    if venue=='mexc':return [_bar(r[0],r[1],r[2],r[3],r[4],r[7]) for r in resp]
    if venue=='kucoin':
        if str(resp.get('code'))!='200000':raise RuntimeError('KUCOIN_BAD_STATUS')
        return [_bar(int(r[0])*1000,r[1],r[3],r[4],r[2],r[6]) for r in resp.get('data',[])]
    if venue=='bybit':
        if resp.get('retCode')!=0:raise RuntimeError('BYBIT_BAD_STATUS '+str(resp.get('retMsg')))
        return [_bar(r[0],r[1],r[2],r[3],r[4],r[6]) for r in resp.get('result',{}).get('list',[])]
    if venue=='okx':
        if str(resp.get('code'))!='0':raise RuntimeError('OKX_BAD_STATUS')
        return [_bar(r[0],r[1],r[2],r[3],r[4],r[7]) for r in resp.get('data',[]) if len(r)>=9 and r[8]=='1']
    if venue=='binance_alpha':
        if str(resp.get('code'))!='000000' or resp.get('success') is False:raise RuntimeError('ALPHA_BAD_STATUS')
        return [_bar(r[0],r[1],r[2],r[3],r[4],r[7]) for r in resp.get('data',[])]
    if venue=='geckoterminal':
        return [_bar(int(r[0])*1000,r[1],r[2],r[3],r[4],r[5]) for r in resp.get('data',{}).get('attributes',{}).get('ohlcv_list',[])]
    raise RuntimeError('UNSUPPORTED_MARKET_ADAPTER '+venue)

def get_market_spec(v,t0,t1):
    venue=v['venue']; pair=v['pair'];start=t0//1000;end=t1//1000
    if venue=='gate':return 'https://api.gateio.ws/api/v4/spot/candlesticks',{'currency_pair':pair,'interval':'1m','from':start,'to':end}
    if venue=='bitget':return 'https://api.bitget.com/api/v3/market/candles',{'category':'SPOT','symbol':pair,'interval':'1m','startTime':t0,'endTime':t1,'limit':1000}
    if venue=='mexc':return 'https://api.mexc.com/api/v3/klines',{'symbol':pair,'interval':'1m','startTime':t0,'endTime':t1,'limit':1000}
    if venue=='kucoin':return 'https://api.kucoin.com/api/v1/market/candles',{'type':'1min','symbol':pair,'startAt':start,'endAt':end}
    if venue=='bybit':return 'https://api.bybit.com/v5/market/kline',{'category':'spot','symbol':pair,'interval':'1','start':t0,'end':t1,'limit':1000}
    if venue=='okx':return 'https://www.okx.com/api/v5/market/history-candles',{'instId':pair,'bar':'1m','limit':100,'after':(t1+MINUTE_MS) if (t1-t0)<=100*MINUTE_MS else None}
    if venue=='binance_alpha':return 'https://www.binance.com/bapi/defi/v1/public/alpha-trade/klines',{'symbol':pair,'interval':'1m','limit':1000,'startTime':t0,'endTime':t1}
    if venue=='geckoterminal':return f'https://api.geckoterminal.com/api/v2/networks/{v["chain"]}/pools/{v["pool"]}/ohlcv/minute',{'aggregate':1,'limit':1000,'before_timestamp':end+60}
    raise ValueError('UNKNOWN_VENUE')


def market_ingest(store,rid,v,http,ts,lookback=70,cap=1000):
    closed_to=(ts//MINUTE_MS)*MINUTE_MS
    old=store.last_market_end(v['coin'],v['venue']);initial=store.market_initial_start(v['coin'],v['venue'])
    t0=(initial if initial is not None else closed_to-lookback*MINUTE_MS) if old is None else old
    if closed_to<=t0:t0=closed_to-MINUTE_MS
    page_caps={'okx':90,'gate':900,'kucoin':1200,'bitget':900,'mexc':900,'bybit':900,'binance_alpha':900,'geckoterminal':900}
    venue_cap=min(cap,page_caps.get(v['venue'],900))
    t1=min(closed_to,t0+venue_cap*MINUTE_MS)
    if not v.get('pair') or v.get('identity_status')=='ALPHA_PAIR_NOT_TRADING_AS_OF_PREVIOUS_SCAN':
        return store.market_save(rid,v['coin'],v['venue'],v.get('pair'),'UNSUPPORTED',t0,t1,[],detail='PAIR_UNVERIFIED_OR_NOT_TRADING',observed_ms=ts)
    try:
        url,args=get_market_spec(v,t0,t1)
        response=http.get(url,args)
        bars=parse_candles(v['venue'],response)
        observed={b['minute_ms']:b for b in bars if t0<=b['minute_ms']<t1};valid=list(observed.values())
        expected=(t1-t0)//MINUTE_MS
        if not valid:status='UNKNOWN';msg='EMPTY_CANDLE_RESULT; NO_TRADE_NOT_PROVEN'
        elif len(valid)!=expected:status='PARTIAL';msg=f'MISSING_OR_ZERO_TRADE_CANDLES:{expected-len(valid)}; NOT_VERIFIED_TAKER_TAPE'
        else:status='OK';msg='COMPLETE_1M_CANDLE_GRID; NOT_VERIFIED_TAKER_TAPE'
        if t1<closed_to:
            if status=='OK':status='BACKLOG'
            msg+='; BACKLOG_PENDING'
        return store.market_save(rid,v['coin'],v['venue'],v['pair'],status,t0,t1,valid,detail=msg,observed_ms=ts,backlog_to_ms=closed_to if t1<closed_to else None)
    except Exception as e:
        return store.market_save(rid,v['coin'],v['venue'],v['pair'],'ERROR',t0,t1,[],detail=f'{type(e).__name__}:{str(e)[:180]}',observed_ms=ts)

def rpc_first(http,chain,method,params):
    errors=[]
    for u in CHAINS[chain]['rpc']:
        try:
            val=http.rpc(u,method,params)
            return u,val
        except Exception as e:errors.append(f'{u}:{str(e)[:100]}')
    raise RuntimeError(';'.join(errors))

def confirmed_head(http,chain):
    expected=CHAINS[chain]['id'];errors=[]
    for url in CHAINS[chain]['rpc']:
        try:
            actual=int(http.rpc(url,'eth_chainId',[]),16)
            if actual!=expected:raise ValueError('WRONG_CHAIN')
            head=int(http.rpc(url,'eth_blockNumber',[]),16)
            safe=max(0,head-(15 if chain=='bsc' else 10))
            return url,safe
        except Exception as e:errors.append(f'{url}:{str(e)[:120]}')
    raise RuntimeError('NO_VERIFIED_CHAIN_RPC '+ ';'.join(errors))

def wallet_ingest(store,rid,config,http,ts,max_workers=6):
    bycoin={x['symbol']:x for x in config['coins']}
    # Mark each source independently; registry label is not proof of entity ownership.
    check=[];ready={};
    for chain in CHAINS:
        try:ready[chain]=confirmed_head(http,chain)
        except Exception as e:ready[chain]=(None,None,str(e))
    def query(w):
        c=bycoin[w['coin']];chain=c['chain'];chain_ref=ready[chain]
        if chain_ref[0] is None:return (w,c,None,None,chain_ref[2])
        url,block=chain_ref[:2]
        data='0x70a08231'+w['address'][2:].zfill(64)
        try:
            raw=http.rpc(url,'eth_call',[{'to':c['token'],'data':data},hex(block)])
            if not isinstance(raw,str) or not raw.startswith('0x'):raise ValueError('BAD_BALANCE_RESPONSE')
            return (w,c,block,int(raw,16),None)
        except Exception as e:return (w,c,block,None,str(e))
    # SQLite writes stay single-threaded, while network queries may be parallel.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        for w,c,block,raw,error in pool.map(query,config['wallets']):
            status=store.wallet_save(rid,c,w['address'],block,raw,error,ts)
            check.append({'coin':c['symbol'],'address':w['address'],'status':status})
    return check


def decode_transfer(log,token_to_coin):
    topic0='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    if str(log.get('topics',[''])[0]).lower()!=topic0 or len(log.get('topics',[]))<3:raise ValueError('NOT_TRANSFER')
    token=log['address'].lower()
    if token not in token_to_coin:raise ValueError('OTHER_TOKEN')
    return {'token':token,'coin':token_to_coin[token], 'tx':log['transactionHash'].lower(),'log_index':int(log['logIndex'],16),
            'block':int(log['blockNumber'],16),'from':'0x'+log['topics'][1][-40:].lower(),'to':'0x'+log['topics'][2][-40:].lower(), 'raw':str(int(log['data'],16))}


def chain_ingest(store,rid,chain,config,http,ts,max_span=5000,chunk_blocks=200):
    tokens=[c for c in config['coins'] if c['chain']==chain]
    token_to_coin={x['token'].lower():x['symbol'] for x in tokens}
    try:
        url,head=confirmed_head(http,chain)
    except Exception as e:
        # Without chain head, no fabricated block range or zero observations.
        store.chain_save(rid,chain,None,None,None,[],'ERROR','NO_VERIFIED_RPC:'+str(e)[:150],ts)
        return {'chain':chain,'status':'ERROR','detail':'NO_VERIFIED_RPC'}
    last=store.last_chain_block(chain)
    if last is None:
        from_b=max(1,head-int(3600/CHAINS[chain]['block_seconds'])+1)
        bootstrap=True
    else:
        from_b=max(1,last-3);bootstrap=False # small finalized overlap; dedup by (chain,tx,logIndex,token)
    if from_b>head:
        store.chain_save(rid,chain,head,from_b,None,[],'OK','NO_NEW_CONFIRMED_BLOCKS',ts)
        return {'chain':chain,'status':'OK','detail':'NO_NEW_CONFIRMED_BLOCKS'}
    limit=min(head,from_b+max_span-1);observed=[];end=from_b-1;failure=None
    for a in range(from_b,limit+1,chunk_blocks):
        b=min(a+chunk_blocks-1,limit)
        try:
            logs=http.rpc(url,'eth_getLogs',[{'fromBlock':hex(a),'toBlock':hex(b),'address':list(token_to_coin),'topics':['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef']}])
            if not isinstance(logs,list):raise ValueError('LOGS_NOT_LIST')
            staged=[decode_transfer(z,token_to_coin) for z in logs]
            if len(staged)!=len(logs):raise ValueError('LOG_COUNT_MISMATCH')
            observed.extend(staged)
            end=b
        except Exception as e:failure=f'{type(e).__name__}:{str(e)[:160]}';break
    if failure:status='PARTIAL' if end>=from_b else 'ERROR'
    elif limit<head:status='PARTIAL'
    else:status='OK'
    detail=';'.join(x for x in [('BOOTSTRAP_NO_PREEXISTING_HISTORY' if bootstrap else None),failure,('BACKLOG_PENDING' if limit<head else None)] if x)
    store.chain_save(rid,chain,head,from_b,end if end>=from_b else None,observed,status,detail,ts)
    return {'chain':chain,'status':status,'from':from_b,'last_complete':end,'head':head,'observed_events':len(observed),'detail':detail}


def collect_all(config,store,http,ts=None,market=True,wallet=True,chain=True,dex=True):
    lease=store.acquire_lease()
    rid=None
    try:
        ts=ts if ts is not None else now_ms();validate_config(config);rid=store.begin_run(ts)
        summary={'started_utc':utc_ms(ts),'source_origin':'LIVE_CONNECTORS_NOT_YET_VERIFIED_IN_THIS_ENVIRONMENT','run_id':rid,'spot':[],'onchain':[]}
        if market:
            for v in config['venues']:
                summary['spot'].append(market_ingest(store,rid,v,http,ts,config['limits']['initial_lookback_minutes'],config['limits']['max_backfill_minutes_per_run']))
        if dex:
            for x in config['dex_pools']:
                if 'pool' not in x:
                    store.market_save(rid,x['coin'],'geckoterminal',None,'SKIPPED_UNMAPPED',ts-70*MINUTE_MS,ts,[],detail=x.get('status','POOL_UNMAPPED'),observed_ms=ts)
                    continue
                v={'coin':x['coin'],'chain':x['chain'],'venue':'geckoterminal','pool':x['pool'],'pair':f'{x["pool"]}/USDT','identity_status':'POOL_IDENTITY_UNVERIFIED'}
                summary['spot'].append(market_ingest(store,rid,v,http,ts,config['limits']['initial_lookback_minutes'],config['limits']['max_backfill_minutes_per_run']))
        if wallet:summary['wallet_results']=wallet_ingest(store,rid,config,http,ts)
        if chain:
            for c in CHAINS:summary['onchain'].append(chain_ingest(store,rid,c,config,http,ts))
        store.finish_run(rid,ts)
        summary['health']=store.report(rid)
        return summary
    except BaseException:
        if rid is not None:store.fail_run(rid)
        raise
    finally:
        store.release_lease(lease)

def main():
    parser=argparse.ArgumentParser(description='15 coin Radar V2 / STAGE 1 data ingestion only. No orders, alarms, publishing.')
    parser.add_argument('--db',default=str(ROOT/'radar_v2_local.sqlite3'))
    parser.add_argument('--config',default=str(ROOT/'config.json'))
    parser.add_argument('--market-only',action='store_true',help='Only public market data; skip wallets and chain logs')
    parser.add_argument('--no-network',action='store_true',help='Only validate config and SQLite storage')
    parser.add_argument('--report-path',default=str(ROOT/'results'/'last_run.json'))
    args=parser.parse_args()
    config=json_file(args.config);validate_config(config)
    if args.no_network:
        s=Store(args.db);print(json.dumps({'status':'CONFIG_AND_DATABASE_READY','coins':len(config['coins']),'markets':len(config['venues']),'wallets':len(config['wallets']),'dex':len(config['dex_pools'])},indent=2));s.close();return
    store=Store(args.db)
    try:
        result=collect_all(config,store,HTTPClient(config['limits']['request_timeout_seconds']),market=True,chain=not args.market_only,wallet=not args.market_only)
        p=pathlib.Path(args.report_path);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n');print(json.dumps({'run_id':result['run_id'],'market_counts':result['health']['market_counts'],'gap_records':result['health']['gap_records'],'chain_checks':result['health']['chain_checks']},ensure_ascii=False,indent=2))
    finally:store.close()

if __name__=='__main__':main()