#!/usr/bin/env python3
"""Trace exact-amount token relay hypotheses; no ownership or withdrawal claims.

Same-sized subsequent transfers may be unrelated because tokens are fungible.
This is a lead for human review, not a determination of beneficial ownership.
"""
import argparse,collections,datetime,json,pathlib,sqlite3
from radar_stage1 import validate_config
from flow_discovery import final_contiguous_interval

ZERO='0x'+'0'*40
HORIZON_BLOCKS=500
INFRA={'platform_proxy','dex_pool','dex_vault','exchange_custody','pool_address_unverified'}

def trace_coin(conn,coin,config):
    sym,chain=coin['symbol'],coin['chain']
    cur=conn.execute('SELECT last_complete_block FROM chain_cursor WHERE chain=?',(chain,)).fetchone()
    if cur is None:return {'coin':sym,'chain':chain,'status':'NO_CHAIN_COVERAGE','traces':[]}
    interval=final_contiguous_interval(conn,chain,cur[0])
    if not interval:return {'coin':sym,'chain':chain,'status':'NO_CONTIGUOUS_COVERAGE','traces':[]}
    start,end=interval
    wallets={w['address'].lower():w for w in config['wallets']
             if w['coin']==sym and w['chain']==chain}
    labeled_cex={x for x,w in wallets.items() if w.get('role')=='exchange_custody'}
    if not labeled_cex:return {'coin':sym,'chain':chain,'status':'NO_LABELED_CEX_CANDIDATES','traces':[]}
    items=conn.execute('''SELECT tx_hash,log_index,block,from_address,to_address,amount_raw
       FROM token_transfers WHERE coin=? AND chain=? AND block BETWEEN ? AND ?
       ORDER BY block,log_index''',(sym,chain,start,end)).fetchall()
    by_sender=collections.defaultdict(list)
    for tx,idx,block,frm,to,raw in items:
        if frm==to or frm==ZERO or to==ZERO or int(raw)<=0:continue
        by_sender[(frm,str(raw))].append((tx,idx,block,frm,to,str(raw)))
    traces=[]
    for frm in sorted(labeled_cex):
        for row in [x for x in items if x[3]==frm and x[3]!=x[4] and x[4]!=ZERO and int(x[5])>0]:
            chain_rows=[tuple(row)];used={(row[0],row[1])};latest=row
            for _ in range(3):
                dest=latest[4];amt=str(latest[5]);t,idx,blk=latest[0],latest[1],latest[2]
                options=by_sender.get((dest,amt),[])
                valid=[x for x in options if (x[2]>blk or (x[2]==blk and (x[0]!=t or x[1]>idx)))
                      and x[2]-blk<=HORIZON_BLOCKS and (x[0],x[1]) not in used]
                if not valid:break
                nxt=min(valid,key=lambda x:(x[2],x[1]))
                used.add((nxt[0],nxt[1]));chain_rows.append(nxt);latest=nxt
            last_addr=chain_rows[-1][4]
            role=wallets.get(last_addr,{}).get('role','UNCLASSIFIED')
            if len(chain_rows)>1:
                state=('EXACT_AMOUNT_RELAY_TO_KNOWN_INFRASTRUCTURE_TAG'
                       if role in INFRA else 'EXACT_AMOUNT_RELAY_TO_UNCLASSIFIED_ADDRESS')
            else:state='NO_EXACT_RELAY_OBSERVED_NOT_PROOF_OF_HOLDING'
            traces.append({'from_labeled_cex_address':frm,
                'cex_label_only':wallets[frm].get('label'),
                'independent_exchange_ownership_verified':False,
                'coin_amount_raw':str(row[5]),
                'path':[{'tx':x[0],'block':x[2],'from':x[3],'to':x[4]} for x in chain_rows],
                'final_address_role_unverified':role,'classification':state,
                'warning':'Same-amount relay is a path hypothesis, not proof of common owner.'})
    traces.sort(key=lambda x:int(x['coin_amount_raw']),reverse=True)
    return {'coin':sym,'chain':chain,'status':'EXPLORATORY_UNVERIFIED_CEX_FLOW_TRACING',
      'chain_scanned_from':start,'chain_scanned_to':end,
      'observed_from_candidate_cex':len(traces),
      'exact_relays':sum(len(x['path'])>1 for x in traces),
      'traces':traces[:10]}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--db',required=True);p.add_argument('--config',required=True);p.add_argument('--out',required=True)
    a=p.parse_args()
    config=json.loads(pathlib.Path(a.config).read_text())
    validate_config(config)
    conn=sqlite3.connect('file:'+pathlib.Path(a.db).resolve().as_posix()+'?mode=ro',uri=True)
    try:
        if conn.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('CORRUPT_SQLITE')
        out={'schema':'radar-v2-route-tracing-1',
             'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
             'signal':False,'no_verified_cex_withdrawal_claim':True,
             'coins':[trace_coin(conn,coin,config) for coin in config['coins']]}
    finally:conn.close()
    dest=pathlib.Path(a.out);dest.parent.mkdir(parents=True,exist_ok=True)
    tmp=dest.with_suffix(dest.suffix+'.tmp')
    tmp.write_text(json.dumps(out,indent=2)+'\n');tmp.replace(dest)
    print(json.dumps({'coin_count':len(out['coins']),
        'relay_count':sum(x.get('exact_relays',0) for x in out['coins']),
        'signal':False}))
if __name__=='__main__':main()
