"""Separate near-tip chain scan; does NOT reuse lagging archive block cursor.
Runs inside cloned V2 repo with v2/ on PYTHONPATH, reusing audited chain_v2.
Append-only chain proofs are separate from backfill/history; no advancement on failure.
"""
from __future__ import annotations
import datetime, json, time
from collections import defaultdict
from .core import stamp


def scan_tip(config,blocks=700,seconds=750,prior_cursors=None):
    try:
        from chain_v2 import select_provider,scan_logs,scan_receipts,_probe_receipt,provider_head
        from radar_stage1 import HTTPClient
    except ImportError as e:
        return {'status':'MISSING_REPO_IMPORT','error':str(e),'chains':{},'events':[]}
    http=HTTPClient(13)
    findings=[];chains={};dead=time.monotonic()+seconds
    prior_cursors=prior_cursors or {}
    for chain in ('bsc','base'):
        mapping={c['token'].lower():c['symbol'] for c in config['coins'] if c['chain']==chain}
        if not mapping:continue
        if time.monotonic()>dead:
            chains[chain]={'status':'NO_TIME_BUDGET'};continue
        try:
            url,head,mode=select_provider(http,chain,next(iter(mapping)))
            prior=prior_cursors.get(chain)
            start=max(1,int(prior)+1) if prior is not None else max(1,head-blocks+1)
            last=start-1
            seen=[]
            def accept(b,batch):
                nonlocal last
                seen.extend(batch);last=b
            try:
                if start<=head:
                    if mode=='logs':scan_logs(http,url,start,head,mapping,dead,accept)
                    else:scan_receipts(http,url,start,head,mapping,dead,accept)
                elif start==head+1:
                    last=head  # no new finalized block; preserve verified continuity
                else:
                    raise ValueError('CURSOR_AHEAD_OF_CHAIN_REORG_OR_BAD_STATE')
            except Exception as e:
                failure=str(e)[:180]
                chains[chain]={'status':'PARTIAL_OR_ERROR','from_block':start,'to_block':last,
                               'safe_head':head,'provider':url,'error':failure}
                # Only the contiguous successfully verified prefix is usable.
                continue
            header=http.rpc(url,'eth_getBlockByNumber',[hex(head),False])
            tip_ms=int(header['timestamp'],16)*1000
            status='FRESH_CONTIGUOUS_TAIL' if stamp()-tip_ms<=300000 else 'STALE_TIP'
            # Only timestamps for source-attributed candidate CEX transfers; hundreds
            # of DEX trades would otherwise waste public RPC capacity on headers.
            watched={w['address'].lower() for w in config.get('wallets',[])
                     if w.get('role')=='exchange_custody' and w.get('chain')==chain}
            block_times={}
            for row in seen:
                if row['from'] not in watched:continue
                height=row['block']
                if height not in block_times:
                    h=http.rpc(url,'eth_getBlockByNumber',[hex(height),False])
                    block_times[height]=int(h['timestamp'],16)*1000
                row['block_ms']=block_times[height]
            findings+=seen
            chains[chain]={'status':status,'from_block':start,'to_block':head,
                'safe_head':head,'safe_head_ms':tip_ms,'event_count':len(seen),
                'cursor_used':prior,'incremental_verified_to':head,
                'provider':url,'method':mode,'complete':True,
                'scope_note':'Only the verified recent block interval; older gaps are NOT proved resolved'}
        except Exception as e:
            chains[chain]={'status':'ERROR_NO_VERIFIED_COVERAGE','error':str(e)[:350],'complete':False}
    return {'schema':'radar-v2-recent-chain-1','observed_ms':stamp(),
        'chains':chains,'events':findings,'status':'OBSERVED_NOT_EXCHANGE_OWNERSHIP_VERIFIED'}
