#!/usr/bin/env python3
"""Discover previously untracked token net receivers, never infer a buy signal.

Read-only analysis of *persisted, contiguous* ERC-20 Transfer logs. A receiver
might be a contract, AMM user, market maker, CEX, or arbitrage router. Detection
is a research queue only; address/code identity and actual buys are unproven.
"""
import argparse,collections,datetime,decimal,hashlib,json,pathlib,sqlite3
from radar_stage1 import validate_config

D=decimal.Decimal
ZERO='0x'+'0'*40
BLOCK_LIMIT=30000
RECENT_BLOCKS=3500
EXCLUDE_ROLES={'exchange_custody','base_attribution_unverified','dex_pool',
               'dex_vault','pool_address_unverified','platform_proxy',
               'large_contract_unclassified'}
decimal.getcontext().prec=65

def token_string(raw,decimals):
    return format((D(int(raw))/(D(10)**decimals)).normalize(),'f')

def final_contiguous_interval(conn,chain,cursor):
    """Do not bridge missing or not-yet-confirmed chain sections."""
    if cursor is None:return None
    pieces=conn.execute('''SELECT from_block,to_block FROM chain_checks
        WHERE chain=? AND from_block IS NOT NULL AND to_block IS NOT NULL
        AND to_block>=from_block AND from_block<=? ORDER BY to_block DESC,from_block DESC''',
        (chain,cursor)).fetchall()
    current=cursor
    start=cursor+1
    for frm,to in pieces:
        if frm>current+1:continue
        if to<current:break
        start=min(start,frm)
        current=start-1
        if start<=max(0,cursor-BLOCK_LIMIT+1):break
    return (max(start,cursor-BLOCK_LIMIT+1),cursor) if start<=cursor else None

def discover(conn,config):
    validate_config(config)
    last=conn.execute('SELECT run_id,status FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
    checks={x[0]:(x[1],x[2],x[3]) for x in conn.execute(
        'SELECT chain,status,to_block,head_block FROM chain_checks WHERE run_id=?',
        (last[0] if last else '',))}
    cursor=dict(conn.execute('SELECT chain,last_complete_block FROM chain_cursor'))
    out=[]
    for coin in config['coins']:
        symbol,chain,decimals=coin['symbol'],coin['chain'],coin['decimals']
        end=cursor.get(chain)
        interval=final_contiguous_interval(conn,chain,end)
        status,verified_end,head=checks.get(chain,('UNKNOWN',None,None))
        complete=(end is not None and status=='OK' and head is not None and end>=head)
        known={w['address'].lower():w for w in config['wallets']
               if w['coin']==symbol and w['chain']==chain}
        excluded=collections.Counter()
        flows=collections.defaultdict(lambda:{'in':0,'out':0,'recent_in':0,'recent_out':0,
            'in_txs':set(),'recent_txs':set(),'sources':set(),
            'noninfra_sources':set(),'noninfra_in_txs':set(),
            'infra_in_raw':0,'dex_in_raw':0,'infra_in_txs':set(),'dex_in_txs':set(),
            'source_types':collections.Counter(),'blocks':set(),'examples':[],'mint':0})
        event_count=0
        if interval:
            begin,finish=interval
            boundary=max(begin,finish-RECENT_BLOCKS+1)
            for tx,idx,block,frm,to,raw in conn.execute(
                '''SELECT tx_hash,log_index,block,from_address,to_address,amount_raw
                FROM token_transfers WHERE coin=? AND chain=? AND block BETWEEN ? AND ?
                ORDER BY block,log_index''',(symbol,chain,begin,finish)):
                raw=int(raw)
                if raw<=0 or frm==to:continue
                event_count+=1
                recent=block>=boundary
                if to!=ZERO:
                    item=flows[to]
                    item['in']+=raw
                    if recent:item['recent_in']+=raw;item['recent_txs'].add(tx)
                    item['in_txs'].add(tx);item['blocks'].add(block)
                    item['sources'].add(frm)
                    sender=known.get(frm)
                    if frm==ZERO:item['mint']+=raw
                    if sender and sender.get('role') in EXCLUDE_ROLES:
                        item['source_types']['KNOWN_INFRASTRUCTURE_TAG_UNVERIFIED']+=1
                        item['infra_in_raw']+=raw
                        item['infra_in_txs'].add(tx)
                        if sender.get('role') in {'dex_pool','dex_vault','pool_address_unverified'}:
                            item['dex_in_raw']+=raw
                            item['dex_in_txs'].add(tx)
                    else:
                        item['source_types']['OWNER_OR_SOURCE_UNKNOWN']+=1
                        if frm!=ZERO:
                            item['noninfra_sources'].add(frm)
                            item['noninfra_in_txs'].add(tx)
                    if len(item['examples'])<6:
                        item['examples'].append({'tx':tx,'block':block,
                                                 'source':frm,'amount_raw':str(raw)})
                if frm!=ZERO:
                    item=flows[frm]
                    item['out']+=raw
                    if recent:item['recent_out']+=raw
        else:begin=finish=boundary=None
        records=[]
        for address,v in flows.items():
            if address==ZERO:continue
            w=known.get(address)
            if w and w.get('role') in EXCLUDE_ROLES:
                excluded[w['role']]+=1
                continue
            net=v['in']-v['out']
            if net<=0:continue
            records.append((address,net,v,w))
        positive_nets=sorted(x[1] for x in records)
        threshold=positive_nets[(len(positive_nets)*9)//10] if positive_nets else None
        picks=[]
        dex_dominated=[]
        for address,net,v,w in records:
            recent_net=v['recent_in']-v['recent_out']
            if v['in'] and len(v['in_txs'])>=2 and (
                D(v['dex_in_raw'])/D(v['in'])>=D('0.5') or
                D(len(v['dex_in_txs']))/D(len(v['in_txs']))>=D('0.5')):
                dex_dominated.append({'address':address,
                    'known_dex_share_of_inflow_amount':str((D(v['dex_in_raw'])/D(v['in'])).quantize(D('0.0001'))),
                    'known_dex_share_of_inflow_transactions':str((D(len(v['dex_in_txs']))/D(len(v['in_txs']))).quantize(D('0.0001'))),
                    'inflow_transactions':len(v['in_txs']),
                    'net_observed':token_string(net,decimals),
                    'classification':'DEX_FLOW_NOT_INDEPENDENT_HOLDER_ACCUMULATION'})
            valid=(threshold is not None and net>=threshold and
                   v['in']>0 and D(net)/D(v['in'])>=D('0.7') and
                   len(v['in_txs'])>=2 and len(v['noninfra_sources'])>=2 and
                   len(v['noninfra_in_txs'])>=2 and
                   D(v['infra_in_raw'])/D(v['in'])<=D('0.5') and
                   D(len(v['infra_in_txs']))/D(len(v['in_txs']))<=D('0.5') and
                   len(v['blocks'])>=2 and v['mint']==0 and
                   len(v['recent_txs'])>=2 and recent_net>0)
            if not valid:continue
            p={'address':address,
               'address_role':w.get('role','UNSEEN_ADDRESS') if w else 'UNSEEN_ADDRESS',
               'ownership_status':'UNKNOWN_NOT_PROVEN_EOA_OR_WHALE',
               'exchange_withdrawal_verified':False,'market_purchase_verified':False,
               'net_received':token_string(net,decimals),'net_received_raw':str(net),
               'received_raw':str(v['in']),'sent_raw':str(v['out']),
               'retained_fraction_of_received':str((D(net)/D(v['in'])).quantize(D('0.0001'))),
               'recent_net_received':token_string(recent_net,decimals),
               'recent_inflow_transactions':len(v['recent_txs']),
               'source_address_count_unverified':len(v['sources']-{ZERO}),
               'distinct_inflow_transactions':len(v['in_txs']),
               'source_types':dict(v['source_types']),
               'independent_non_infrastructure_sources_unverified':len(v['noninfra_sources']),
               'infrastructure_share_of_inflow':str((D(v['infra_in_raw'])/D(v['in'])).quantize(D('0.0001'))),
               'sample_incoming_transactions':v['examples'][:4],
               'status':'DISCOVERY_RESEARCH_CANDIDATE_NOT_SIGNAL',
               'limitations':['OWNER_NOT_VERIFIED','EOA_OR_CONTRACT_NOT_VERIFIED',
                              'TRANSFER_IS_NOT_PROOF_OF_PURCHASE',
                              'BLOCK_WINDOW_NOT_TIME_WINDOW']}
            p['fingerprint']=hashlib.sha256(
                (symbol+chain+address+str(begin)+str(finish)).encode()).hexdigest()[:20]
            picks.append(p)
        picks.sort(key=lambda x:int(x['net_received_raw']),reverse=True)
        out.append({'coin':symbol,'chain':chain,'token':coin['token'],
            'coverage':{'contiguous_scanned_from':begin,'to':finish,
                'latest_head':head,'chain_complete_at_last_run':complete,
                'recent_block_boundary':boundary,
                'time_unit':'BLOCK_NOT_24H',
                'note':'Only the persisted contiguous chain interval'},
            'observed_event_count':event_count,
            'observed_net_positive_addresses':len(records),
            'materiality_threshold_raw':str(threshold) if threshold is not None else None,
            'excluded_infrastructure_addresses':dict(excluded),
            'dex_dominated_receivers':sorted(dex_dominated,key=lambda x:x['inflow_transactions'],reverse=True)[:5],
            'watch_candidates':picks[:15],
            'candidate_count':len(picks),
            'strong_alert':False,
            'interpretation':'UNVERIFIED_NET_RECEIPTS_NEED_OWNER_AND_BALANCE_CHECKS'})
    return {'schema':'radar-v2-flow-discovery-1',
        'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'last_run_id':last[0] if last else None,
        'source':'PERSISTED_CONTIGUOUS_TOKEN_TRANSFER_LOGS_ONLY',
        'buy_signal':False,'strong_alert':False,
        'note':'A transfer-log net receipt is not a verified stockpiling, market purchase or CEX withdrawal.',
        'coins':out}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--config',required=True);p.add_argument('--out',required=True)
    a=p.parse_args()
    cfg=json.loads(pathlib.Path(a.config).read_text())
    validate_config(cfg)
    conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('CORRUPT_SQLITE')
        res=discover(conn,cfg)
    finally:conn.close()
    path=pathlib.Path(a.out);path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(res,indent=2,ensure_ascii=False)+'\n')
    tmp.replace(path)
    print(json.dumps({'coins':len(res['coins']),
      'watch_coins':sum(bool(c['watch_candidates']) for c in res['coins']),
      'watch_addresses':sum(c['candidate_count'] for c in res['coins']),
      'strong_alert':False}))

if __name__=='__main__':main()
