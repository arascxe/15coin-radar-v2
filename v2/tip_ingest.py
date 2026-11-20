#!/usr/bin/env python3
"""Near-head BSC/Base Transfer audit, separate from lagging archive cursor.
Only a completely verified block prefix advances. NO trading, ownership inference,
or predictive 10x signals. Reuses existing V2 checked RPC parsers.
"""
import argparse, datetime, json, pathlib, time
from chain_v2 import select_provider, scan_logs, scan_receipts, _probe_receipt
from radar_stage1 import CHAINS, HTTPClient, Store, now_ms, validate_config

DDL = """
CREATE TABLE IF NOT EXISTS chain_tip_cursor(
  chain TEXT PRIMARY KEY, last_complete_block INTEGER NOT NULL, updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS chain_tip_checks(
  run_id TEXT NOT NULL,chain TEXT NOT NULL,from_block INTEGER,to_block INTEGER,
  safe_head INTEGER,status TEXT NOT NULL,detail TEXT,PRIMARY KEY(run_id,chain));
"""

def save(store,rid,chain,ts,start,last,head,events,status,note):
    if last<start and events:
        raise ValueError('EVENTS_WITHOUT_VERIFIED_BLOCKS')
    for x in events:
        if x['block']<start or x['block']>last:
            raise ValueError('TRANSFER_OUTSIDE_VERIFIED_INTERVAL')
    store.db.executescript(DDL)
    with store.db:
        for e in events:
            key=(CHAINS[chain]['id'],e['tx'],e['log_index'],e['token'])
            old=store.db.execute(
                """SELECT from_address,to_address,amount_raw,block FROM token_transfers
                WHERE chain_id=? AND tx_hash=? AND log_index=? AND token=?""",key).fetchone()
            fields=(e['from'],e['to'],str(e['raw']),e['block'])
            if old is not None and old!=fields:
                raise ValueError('CONFLICTING_TOKEN_TRANSFER')
            store.db.execute('INSERT OR IGNORE INTO token_transfers VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (chain,CHAINS[chain]['id'],e['token'],e['coin'],e['tx'],
                 e['log_index'],e['block'],e['from'],e['to'],str(e['raw']),ts))
        store.db.execute("""INSERT OR REPLACE INTO chain_tip_checks
             VALUES(?,?,?,?,?,?,?)""",(rid,chain,start,last if last>=start else None,
                                      head,status,note))
        if last>=start:
            store.db.execute("""INSERT INTO chain_tip_cursor VALUES(?,?,?)
                ON CONFLICT(chain) DO UPDATE SET
                last_complete_block=excluded.last_complete_block,
                updated_ms=excluded.updated_ms
                WHERE excluded.last_complete_block>chain_tip_cursor.last_complete_block""",
                (chain,last,ts))

def ingest(store,rid,config,chain,http=None,ts=None,initial_blocks=600,
           max_blocks=4000,deadline_seconds=420):
    http=http or HTTPClient(12)
    ts=now_ms() if ts is None else ts
    contracts={c['token'].lower():c['symbol'] for c in config['coins'] if c['chain']==chain}
    if not contracts:return {'chain':chain,'status':'NOT_CONFIGURED'}
    store.db.executescript(DDL)
    row=store.db.execute('SELECT last_complete_block FROM chain_tip_cursor WHERE chain=?',
                         (chain,)).fetchone()
    prior=row[0] if row else None
    try:
        url,head,method=select_provider(http,chain,next(iter(contracts)))
    except Exception as exc:
        save(store,rid,chain,ts,0,-1,None,[],'ERROR_PROVIDER',str(exc)[:300])
        return {'chain':chain,'status':'ERROR_PROVIDER','reason':str(exc)[:300]}
    start=prior+1 if prior is not None else max(1,head-initial_blocks+1)
    if start>head+1:
        save(store,rid,chain,ts,start,start-1,head,[],'ERROR_CURSOR_AHEAD','possible reorg')
        return {'chain':chain,'status':'ERROR_CURSOR_AHEAD','head':head,'prior':prior}
    if start==head+1:
        save(store,rid,chain,ts,start,start-1,head,[],'OK','NO_NEW_FINALIZED_BLOCKS')
        return {'chain':chain,'status':'OK','from':start,'verified_to':head,
                'head':head,'lag_blocks':0,'events':0,'bootstrap':False}
    stop=min(head,start+max_blocks-1)
    verified=start-1
    found=[]
    deadline=time.monotonic()+deadline_seconds
    def on_complete(end,batch):
        nonlocal verified
        if end<=verified or any(x['block']<=verified or x['block']>end for x in batch):
            raise ValueError('MISORDERED_OR_OUT_OF_PAGE_TRANSFER')
        found.extend(batch);verified=end
    failure=None
    try:
        if method=='logs':
            try:scan_logs(http,url,start,stop,contracts,deadline,on_complete)
            except Exception as exc:
                failure='logs:'+str(exc)[:120]
                if verified<stop:
                    _probe_receipt(http,url,verified+1)
                    scan_receipts(http,url,verified+1,stop,contracts,deadline,on_complete)
                    failure=None
        else:
            scan_receipts(http,url,start,stop,contracts,deadline,on_complete)
    except Exception as exc:
        failure=(failure or '')+';'+type(exc).__name__+':'+str(exc)[:180]
    status=('ERROR_EMPTY' if verified<start else 'PARTIAL' if verified<head else 'OK')
    if failure and status=='OK':status='PARTIAL'
    note=f'provider={url};method={method};failure={failure or "none"};initial={prior is None}'
    save(store,rid,chain,ts,start,verified,head,found,status,note)
    return {'chain':chain,'status':status,'from':start,
            'verified_to':verified if verified>=start else None,'head':head,
            'lag_blocks':head-verified,'events':len(found),'bootstrap':prior is None,
            'source':url,'method':method,'note':failure,
            'important':'Verified tip is not proof of CEX ownership or sellable supply'}

def restore_prior_cursor(store,path):
    """Tip cursors live in a tiny JSON file, not the immutable archive DB schema."""
    if not path or not pathlib.Path(path).exists():return
    d=json.loads(pathlib.Path(path).read_text())
    if d.get('schema')!='v2-verified-independent-chain-tip-1':
        raise RuntimeError('UNTRUSTED_TIP_CURSOR_REPORT')
    store.db.executescript(DDL)
    with store.db:
        for x in d.get('chains',[]):
            ch=x.get('chain');last=x.get('verified_to')
            if ch not in ('bsc','base') or not isinstance(last,int) or last<=0:continue
            store.db.execute("""INSERT INTO chain_tip_cursor VALUES(?,?,?)
                 ON CONFLICT(chain) DO UPDATE SET
                 last_complete_block=MAX(last_complete_block,excluded.last_complete_block)""",
                 (ch,last,now_ms()))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--db',required=True)
    ap.add_argument('--config',required=True)
    ap.add_argument('--out',required=True)
    ap.add_argument('--prior',default=None,
                    help='Previous independent tip JSON report, not archive SQLite')
    ap.add_argument('--deadline-seconds',type=int,default=150)
    ap.add_argument('--head-only',action='store_true',
                    help='Separate recent head window; DO NOT resume or replace historical tip cursor')
    ap.add_argument('--initial-blocks',type=int,default=600)
    a=ap.parse_args()
    cfg=json.loads(pathlib.Path(a.config).read_text())
    validate_config(cfg)
    store=Store(a.db)
    report=None
    try:
        row=store.db.execute('SELECT run_id FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()
        if not row:raise RuntimeError('NO_COLLECTOR_RUN_TO_ATTACH')
        if not a.head_only:
            restore_prior_cursor(store,a.prior)
        if not 1<=a.initial_blocks<=4000:
            raise ValueError('FRESH_HEAD_WINDOW_BOUNDS')
        report={'schema':'v2-verified-independent-chain-tip-1',
                'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'run_id':row[0],'signals':False,'buy_signal':False,
                'head_only_window':bool(a.head_only),
                'archive_history_not_claimed_complete_by_head_window':True,
                'chains':[ingest(store,row[0],cfg,chain,initial_blocks=a.initial_blocks,
                                 deadline_seconds=a.deadline_seconds) for chain in ('bsc','base')]}
        # The archive is an immutable-schema incremental SQLite ledger.
        # These two scratch tip tables would break its lossless restore.
        # Retain verified transfers in the original token_transfers table
        # and the cursors/proof ranges in the independent tip JSON report.
        with store.db:
            store.db.execute('DROP TABLE IF EXISTS chain_tip_checks')
            store.db.execute('DROP TABLE IF EXISTS chain_tip_cursor')
        if report is not None:
            report['cursor_persisted_in']='OUT_OF_BAND_JSON_ONLY'
            report['archive_schema_unchanged']=True
    finally:
        store.close()
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_name(target.name+'.tmp')
    tmp.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    tmp.replace(target)
    print(json.dumps(report,ensure_ascii=False))

if __name__=='__main__':main()
