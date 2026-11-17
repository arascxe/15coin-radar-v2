#!/usr/bin/env python3
"""Follow *discovered* ERC20 recipients across runs using existing SQLite history.

No schema migration, new account, paid API, address ownership claim, buy order,
or alarm. Source: persisted transfer logs; observations: same-block EOA code
and ERC20 balanceOf via validated free chain RPC. Fail closed on RPC errors.
"""
import argparse
import collections
import concurrent.futures
import datetime
import json
import pathlib
import sqlite3
import time

from radar_stage1 import Store, HTTPClient, CHAINS, validate_config
from flow_discovery import final_contiguous_interval

GOOD = 'DYNAMIC_EOA_CODE_VERIFIED_OWNER_UNKNOWN'
FAIL = 'DYNAMIC_RPC_ERROR'
CONTRACT = 'DYNAMIC_CONTRACT_CODE_SKIP'
ZERO = '0x' + '0'*40
RECENT_BLOCKS = 6000
ACTIVE_BLOCKS = 1800
HISTORY_MS = 7 * 86400000
RECENT_RETRY_MS = 3600000
MAX_PER_COIN = 3

def propose(conn, config, ts_ms=None, per_coin=MAX_PER_COIN):
    """Rank for follow-up, *not* economic signals or whale identities."""
    validate_config(config)
    if ts_ms is None:
        latest=conn.execute('SELECT started_ms FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
        ts_ms=latest[0] if latest else int(time.time()*1000)
    positions={chain:row for chain,row in conn.execute(
        'SELECT chain,last_complete_block FROM chain_cursor')}
    candidates=[]
    for coin in config['coins']:
        sym=coin['symbol'];chain=coin['chain'];head=positions.get(chain)
        base={'coin':sym,'chain':chain,'cursor_block':head,
              'coverage_status':'UNKNOWN_NO_CONTIGUOUS_CHAIN',
              'address_candidates':[],'count':0}
        interval=final_contiguous_interval(conn,chain,head) if head is not None else None
        if not interval:
            candidates.append(base);continue
        begin,end=interval
        begin=max(begin,end-RECENT_BLOCKS+1)
        activity_from=max(begin,end-ACTIVE_BLOCKS+1)
        base['coverage_status']='CONTIGUOUS_STORED_TOKEN_LOGS'
        base['interval_from']=begin
        base['interval_to']=end
        registered={w['address'].lower():w for w in config['wallets']
                    if w['coin']==sym and w['chain']==chain}
        infra=set(registered)
        infra.update(x.get('pool','').lower() for x in config.get('dex_pools',[])
                     if x['coin']==sym and x['chain']==chain and x.get('pool'))
        infra.discard('')
        flows=collections.defaultdict(lambda:{'in':0,'out':0,'recent_in':0,'recent_out':0,
                 'transactions':set(),'from_addresses':set(),'dex_tx':set(),'all_from':set()})
        for tx,block,frm,to,raw in conn.execute('''SELECT tx_hash,block,from_address,to_address,amount_raw
             FROM token_transfers WHERE coin=? AND chain=? AND block BETWEEN ? AND ?''',
             (sym,chain,begin,end)):
            if frm==to or int(raw)<=0:continue
            amount=int(raw)
            if to!=ZERO:
                v=flows[to];v['in']+=amount;v['transactions'].add(tx)
                v['all_from'].add(frm)
                if frm!=ZERO:v['from_addresses'].add(frm)
                if block>=activity_from:v['recent_in']+=amount
                if (frm in registered and registered[frm].get('role')
                        in ('dex_pool','dex_vault','pool_address_unverified')) or (
                            frm in infra and any(x.get('pool','').lower()==frm for x in config.get('dex_pools',[])
                                                  if x['coin']==sym and x['chain']==chain)):
                    v['dex_tx'].add(tx)
            if frm!=ZERO:
                flows[frm]['out']+=amount
                if block>=activity_from:flows[frm]['recent_out']+=amount
        history={}
        for address,last_seen in conn.execute('''SELECT w.address,MAX(b.seen_ms)
             FROM wallet_checks w JOIN wallet_balances b
              ON w.coin=b.coin AND w.chain=b.chain AND w.address=b.address
             WHERE w.coin=? AND w.chain=? AND w.detail=? AND w.status='OK'
             GROUP BY w.address''',(sym,chain,GOOD)):
            if address not in infra and ts_ms-last_seen<=HISTORY_MS:
                history[address]=last_seen
        cooldown=set()
        for address,attempt_ms in conn.execute('''SELECT w.address,MAX(r.started_ms)
              FROM wallet_checks w JOIN runs r ON w.run_id=r.run_id
              WHERE w.coin=? AND w.chain=? AND w.detail LIKE 'DYNAMIC_%'
              GROUP BY w.address''',(sym,chain)):
            if ts_ms-attempt_ms<RECENT_RETRY_MS:
                cooldown.add(address)
        # Once eth_getCode proved a candidate was a contract at a confirmed
        # block, never waste the constrained EOA-monitor budget on it again.
        # The evidence remains in wallet_checks rather than a new table.
        known_contracts={r[0] for r in conn.execute(
            """SELECT DISTINCT address FROM wallet_checks
               WHERE coin=? AND chain=? AND status='SKIPPED_CONTRACT' AND detail=?""",
            (sym,chain,CONTRACT))}
        eligible=[]
        for address,v in flows.items():
            if address in infra or address in known_contracts or address==ZERO or v['in']<=v['out'] or v['in']==0:continue
            if len(v['transactions'])<2 or v['recent_in']<=v['recent_out']:continue
            if ZERO in v['all_from']:continue
            net=v['in']-v['out']
            eligible.append({'address':address,'net_raw':str(net),
                  'recent_net_raw':str(v['recent_in']-v['recent_out']),
                  'inflow_transactions':len(v['transactions']),
                  'independent_sources_not_proven':len(v['from_addresses']),
                  'known_dex_inflow_transactions':len(v['dex_tx']),
                  'already_monitored':address in history,
                  'reason':'BALANCE_AND_EOA_OWNER_NOT_YET_VERIFIED'})
        eligible.sort(key=lambda x:(int(x['recent_net_raw']),int(x['net_raw'])),reverse=True)
        sticky=[{'address':a,'already_monitored':True,'previous_seen_ms':seen,
                 'reason':'FOLLOW_UP_PREVIOUS_EOA_BALANCE_OBSERVATION'}
                for a,seen in sorted(history.items(),key=lambda x:x[1])
                if a not in cooldown and a not in known_contracts]
        selected=sticky[:min(2,per_coin)]
        seen={x['address'] for x in selected}
        for x in eligible:
            if len(selected)>=per_coin:break
            if x['address'] in seen or x['address'] in cooldown or x['address'] in history:continue
            selected.append(x);seen.add(x['address'])
        if len(selected)<per_coin:
            for x in sticky[min(2,per_coin):]:
                if len(selected)>=per_coin:break
                if x['address'] not in seen:selected.append(x);seen.add(x['address'])
        base['address_candidates']=selected
        base['count']=len(selected)
        base['eligible_unlabeled_receivers']=len(eligible)
        base['historically_rejected_contract_addresses']=len(known_contracts)
        candidates.append(base)
    return {'schema':'radar-v2-dynamic-wallet-followup-1',
        'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'source':'PERSISTED_CHAIN_LOGS_AND_PREVIOUS_BALANCE_OBSERVATIONS',
        'strong_alert':False,'verified_beneficial_owner_count':0,
        'latest_chain_heads_not_guaranteed_current':True,
        'coins':candidates}

def read_same_block(http, coin, address, block):
    errors=[]
    chain=coin['chain']
    for url in CHAINS[chain]['rpc']:
        try:
            actual=http.rpc(url,'eth_chainId',[])
            if int(actual,16)!=CHAINS[chain]['id']:
                raise ValueError('WRONG_CHAIN_ID')
            provider_block=int(http.rpc(url,'eth_blockNumber',[]),16)
            if provider_block<block:raise ValueError('PROVIDER_BEHIND_TARGET_BLOCK')
            bytecode=http.rpc(url,'eth_getCode',[address,hex(block)])
            if not isinstance(bytecode,str) or not bytecode.startswith('0x'):
                raise ValueError('INVALID_CODE_RESPONSE')
            if bytecode not in ('0x','0x0'):
                return {'status':'SKIPPED_CONTRACT','provider':url,
                        'detail':CONTRACT,'raw':None,'block':block}
            data='0x70a08231'+address[2:].zfill(64)
            value=http.rpc(url,'eth_call',[{'to':coin['token'],'data':data},hex(block)])
            if not isinstance(value,str) or not value.startswith('0x') or len(value)<=2:
                raise ValueError('INVALID_BALANCE_RESPONSE')
            raw=int(value,16)
            if raw<0:raise ValueError('NEGATIVE_BALANCE')
            return {'status':'OK','provider':url,'detail':GOOD,
                    'raw':raw,'block':block}
        except Exception as exc:
            errors.append(type(exc).__name__+':'+str(exc)[:65])
    return {'status':'ERROR','detail':FAIL+':'+('|'.join(errors))[:155],
            'block':block,'raw':None,'provider':None}

def monitor(store,config,http,ts_ms=None,max_workers=5,per_coin=MAX_PER_COIN):
    if ts_ms is None:ts_ms=int(time.time()*1000)
    plan=propose(store.db,config,ts_ms,per_coin)
    last=store.db.execute('SELECT run_id FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
    if last is None:
        return {'schema':plan['schema'],'status':'NO_COLLECTOR_RUN',
                'coins':plan['coins'],'results':[],'strong_alert':False}
    run_id=last[0]
    config_by_symbol={c['symbol']:c for c in config['coins']}
    jobs=[(item,coin_config,addr)
          for item in plan['coins']
          for coin_config in [config_by_symbol[item['coin']]]
          if item['cursor_block'] is not None
          for addr in item['address_candidates']]
    results=[]
    def query(job):
        row,coin,a=job
        return row,coin,a,read_same_block(http,coin,a['address'],row['cursor_block'])
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        for row,coin,addr,reading in pool.map(query,jobs):
            address=addr['address'];block=reading['block']
            exists=store.db.execute('SELECT 1 FROM wallet_checks WHERE run_id=? AND coin=? AND chain=? AND address=?',
                                     (run_id,coin['symbol'],coin['chain'],address)).fetchone()
            if exists:
                results.append({'coin':coin['symbol'],'address':address,'status':'ALREADY_CHECKED_SAME_RUN'})
                continue
            if reading['status']=='OK':
                store.wallet_save(run_id,coin,address,block,reading['raw'],observed_ms=ts_ms)
                with store.db:
                    store.db.execute('''UPDATE wallet_checks SET detail=? WHERE run_id=? AND coin=? AND chain=? AND address=?''',
                         (GOOD,run_id,coin['symbol'],coin['chain'],address))
            elif reading['status']=='SKIPPED_CONTRACT':
                with store.db:
                    store.db.execute('INSERT INTO wallet_checks VALUES(?,?,?,?,?,?,?)',
                                     (run_id,coin['symbol'],address,coin['chain'],
                                      'SKIPPED_CONTRACT',block,CONTRACT))
            else:
                store.wallet_save(run_id,coin,address,block,None,err=reading['detail'],observed_ms=ts_ms)
            results.append({'coin':coin['symbol'],'address':address,'block':block,
                'status':reading['status'],'rpc_provider':reading['provider'],
                'owner_status':'EOA_CODE_ONLY_OWNER_NOT_PROVEN' if reading['status']=='OK' else 'NOT_VERIFIED',
                'snapshot_amount_raw':str(reading['raw']) if reading['raw'] is not None else None,
                'detail':reading['detail'],'strong_alert':False})
    states=dict(collections.Counter(x['status'] for x in results))
    return {'schema':'radar-v2-dynamic-wallet-followup-1',
       'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
       'run_id':run_id,'source':'FREE_EXISTING_CHAIN_RPCS_AND_SQLITE',
       'owner_attribution_verified':False,'strong_alert':False,'buy_signal':False,
       'purpose':'Discover new wallet balance time series for later corroboration; no immediate trade inference',
       'planned':plan['coins'],'check_status_counts':states,'results':results}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--config',required=True)
    p.add_argument('--out',required=True);p.add_argument('--propose-only',action='store_true')
    p.add_argument('--per-coin',type=int,default=MAX_PER_COIN)
    a=p.parse_args()
    if not 1<=a.per_coin<=6:raise ValueError('PER_COIN_OUT_OF_BOUNDS')
    config=json.loads(pathlib.Path(a.config).read_text())
    if a.propose_only:
        conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
        try:
            if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('INVALID_DB')
            result=propose(conn,config,per_coin=a.per_coin)
        finally:conn.close()
    else:
        store=Store(a.db)
        try:result=monitor(store,config,HTTPClient(config['limits']['request_timeout_seconds']),per_coin=a.per_coin)
        finally:store.close()
    dest=pathlib.Path(a.out);dest.parent.mkdir(parents=True,exist_ok=True)
    temporary=dest.with_suffix(dest.suffix+'.tmp')
    temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    temporary.replace(dest)
    print(json.dumps({'coins':len(result['coins']) if 'coins' in result else len(result['planned']),
       'selected':sum(x['count'] for x in result.get('coins',result.get('planned',[]))),
       'states':result.get('check_status_counts'),'strong_alert':False},ensure_ascii=False))
if __name__=='__main__':main()
