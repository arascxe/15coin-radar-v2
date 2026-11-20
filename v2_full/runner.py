"""Standalone local/repo entrypoint. --live required for external read-only calls.
Example (inside repository):
PYTHONPATH=.:v2 python -m v2_full.runner --config v2/config.json --live-seconds 1200 --chain --dex --output v2_runtime/live_economic_evidence.json
No existing V2 collector or GitHub workflows are changed by this command.
"""
from __future__ import annotations
import argparse, concurrent.futures, json, pathlib, time, sys
from .core import COINS, stamp
from .streams import StreamCapture
from .inventory import analyze
from .decision import verdict
from .chain_tail import scan_tip
from .dex import probe_v3
from .identity import verify_all
from .proofs import verify_transfers


def atomic_json(path,obj):
    p=pathlib.Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    q=p.with_name(p.name+'.tmp');q.write_text(json.dumps(obj,ensure_ascii=False,indent=2)+'\n')
    q.replace(p)

def evaluate(config,market,chain,identity=None,codeproofs=None,balanceproofs=None,dex=None):
    inv=analyze(config,chain,identity,codeproofs,balanceproofs)
    d=verdict(config,market,inv,dex)
    d['asof_ms']=market.get('asof_ms',stamp())
    d['chain_health']=chain.get('chains',{})
    return d

def live_run(config,seconds,chain_enabled,dex_enabled,output,verified_pair_ids,verified_cex_ids):
    if not verified_pair_ids:
        verified_pair_ids=verify_all(config)['statuses']
    capture=StreamCapture(config,verified_pair_ids)
    outbox=pathlib.Path(str(output)+'.outbox.json')
    prior=json.loads(outbox.read_text()) if outbox.exists() else {'events':[],'last_state':{}}
    prev=dict(prior.get('last_state',{}))
    events=list(prior.get('events',[]))
    chain_live={'chains':{},'events':[]}
    def checkpoint(m):
        nonlocal chain_live
        if future is not None and future.done():
            try:chain_live=future.result()
            except Exception as e:chain_live={'chains':{},'events':[],'error':str(e)[:250]}
        result=evaluate(config,m,chain_live,verified_cex_ids)
        atomic_json(str(output)+'.market.partial',m)
        atomic_json(str(output)+'.decision.partial',result)
        for row in result['coins']:
            sym,state=row['coin'],row['state']
            previous=prev.get(sym,'EVIDENCE_INSUFFICIENT_UNKNOWN')
            if state not in ('EVIDENCE_INSUFFICIENT_UNKNOWN',) and state!=previous:
                event={'coin':sym,'state':state,'first_observed_ms':m['asof_ms'],
                       'markets':row['market_evidence'],'chain_status':chain_live.get('chains',{}).get(row['coin']=='TRUST' and 'base' or 'bsc',{}).get('status'),
                       'predictive_10x_validation':False}
                events.append(event)
                print('RADAR_EVIDENCE_EVENT '+json.dumps(event,ensure_ascii=False),flush=True)
            prev[sym]=state
        atomic_json(outbox,{'schema':'radar-v2-evidence-events-1','events':events[-500:],
                            'last_state':prev,'asof_ms':m['asof_ms'],
                            'note':'Events are research notifications, not verified investment signals.'})
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        future=pool.submit(scan_tip,config,1800,max(600,seconds+120)) if chain_enabled else None
        market=capture.live(seconds=seconds,checkpoint=checkpoint)
        try:
            chain=future.result(timeout=60) if future else {'chains':{},'events':[]}
        except Exception as e:chain={'chains':{},'events':[],'error':str(e)[:300]}
    dex={}
    if dex_enabled:
        # Only if a public RPC has already been verified by a successful chain scan.
        try:
            from radar_stage1 import HTTPClient
            http=HTTPClient(12)
            for p in config.get('dex_pools',[]):
                if p.get('kind')!='v3':
                    dex[p['coin']]={'status':'UNSUPPORTED_POOL_TYPE_FAIL_CLOSED'};continue
                c=next(x for x in config['coins'] if x['symbol']==p['coin'])
                ch=chain.get('chains',{}).get(p['chain'],{})
                if ch.get('status')!='FRESH_CONTIGUOUS_TAIL':
                    dex[p['coin']]={'status':'NO_HEALTHY_POOL_RPC'};continue
                dex[p['coin']]=probe_v3(http,ch['provider'],p['pool'],c['token'],ch['safe_head'])
        except Exception as e:dex={'_error':str(e)[:200]}
    proofs=verify_transfers(config,chain,verified_cex_ids,str(output)+'.balance_ledger.json') if verified_cex_ids else {}
    res=evaluate(config,market,chain,verified_cex_ids,
                 proofs.get('codeproofs'),proofs.get('balanceproofs'),dex=dex)
    res['balance_reconciliation']=dict((k,v) for k,v in proofs.items() if k!='codeproofs' and k!='balanceproofs')
    res['exchange_pair_identity_states']=verified_pair_ids
    atomic_json(output,res)
    return res

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--config',required=True)
    p.add_argument('--output',default='v2_runtime/live_economic_evidence.json')
    p.add_argument('--live-seconds',type=int,default=0)
    p.add_argument('--chain',action='store_true')
    p.add_argument('--dex',action='store_true')
    p.add_argument('--verified-pairs',default=None)
    p.add_argument('--verified-cex',default=None)
    p.add_argument('--replay-market');p.add_argument('--replay-chain');p.add_argument('--replay-dex')
    a=p.parse_args(argv)
    config=json.loads(pathlib.Path(a.config).read_text())
    if tuple(x['symbol'] for x in config['coins'])!=COINS:raise SystemExit('FAIL_CLOSED_WRONG_COIN_UNIVERSE')
    pairs=json.loads(pathlib.Path(a.verified_pairs).read_text()) if a.verified_pairs else {}
    cex=json.loads(pathlib.Path(a.verified_cex).read_text()) if a.verified_cex else {}
    if a.live_seconds>0:
        if a.verified_pairs:
            raise SystemExit('MANUAL_PAIR_IDENTITY_OVERRIDE_DISALLOWED_IN_LIVE_MODE: use official public exchange metadata')
        res=live_run(config,a.live_seconds,a.chain,a.dex,a.output,pairs,cex)
    else:
        if not (a.replay_market and a.replay_chain):
            raise SystemExit('NO_NETWORK_BY_DEFAULT: add --live-seconds 1200 OR --replay-market and --replay-chain')
        market=json.loads(pathlib.Path(a.replay_market).read_text())
        chain=json.loads(pathlib.Path(a.replay_chain).read_text())
        dex=json.loads(pathlib.Path(a.replay_dex).read_text()) if a.replay_dex else {}
        res=evaluate(config,market,chain,cex,dex=dex)
        atomic_json(a.output,res)
    print(json.dumps({'counts':{s:sum(c['state']==s for c in res['coins']) for s in sorted({c['state'] for c in res['coins']})},
                      'strong_buy_signals':0,'total_coins':15},ensure_ascii=False))
    return 0
if __name__=='__main__':raise SystemExit(main())
