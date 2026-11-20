"""Public-RPC balance/EOA reconciliation of identified outbound exchange transfers.
Runs only after a *fresh contiguous* token Transfer scan. Stores prior observed
balances; verifies delta exactly against observed transfer inflows - outflows.
This cannot reveal unobserved off-chain CEX inventory.
"""
from __future__ import annotations
from collections import defaultdict
import json, pathlib
from .core import stamp

def _call(http,url,address,data,block):
    return http.rpc(url,'eth_call',[{'to':address,'data':data},hex(block)])

def _bal(http,url,token,addr,block):
    data='0x70a08231'+'0'*24+addr[2:].lower()
    return int(_call(http,url,token,data,block),16)

def verify_transfers(config,chain,verified_entities,ledger_file,cap=50):
    try:from radar_stage1 import HTTPClient
    except ImportError as e:return {'codeproofs':{},'balanceproofs':{},'error':'MISSING_REPO_MODULE:'+str(e)}
    f=pathlib.Path(ledger_file)
    old=json.loads(f.read_text()) if f.exists() else {}
    http=HTTPClient(12)
    codeproofs={};balanceproofs={};new=dict(old)
    tokens={x['symbol']:x['token'].lower() for x in config['coins']}
    tracked=defaultdict(list)
    for x in chain.get('events',[]):
        sym=x['coin'];ch='base' if sym=='TRUST' else 'bsc'
        src=verified_entities.get(ch+'|'+x['from'].lower())
        if not src or src.get('proof')!='OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC':continue
        dst=verified_entities.get(ch+'|'+x['to'].lower())
        if dst:continue
        tracked[(sym,ch,x['to'].lower())].append(x)
    items=sorted(tracked.items(),key=lambda z:sum(int(e['raw']) for e in z[1]),reverse=True)[:cap]
    for (sym,ch,addr),incoming in items:
        state=chain.get('chains',{}).get(ch,{})
        if state.get('status')!='FRESH_CONTIGUOUS_TAIL' or not state.get('complete'):continue
        provider=state['provider'];block=state['safe_head'];token=tokens[sym]
        try:
            code=http.rpc(provider,'eth_getCode',[addr,hex(block)])
            if code!='0x':continue
            codeproofs[ch+'|'+addr]='EOA_ETH_GETCODE_EMPTY_AT_CONFIRMED_BLOCK'
            current=_bal(http,provider,token,addr,block)
            key=sym+'|'+addr
            prior=old.get(key)
            if prior and prior.get('block')==block:continue
            new[key]={'block':block,'balance_raw':str(current),'observed_ms':stamp()}
            if not prior:continue
            start=state.get('from_block')
            prevblock=int(prior['block'])
            # Token transfer list covers (prevblock, block] without missing blocks.
            if not isinstance(start,int) or start>prevblock+1:continue
            inraw=sum(int(e['raw']) for e in chain['events'] if e['coin']==sym and e['to'].lower()==addr and prevblock<e['block']<=block)
            outraw=sum(int(e['raw']) for e in chain['events'] if e['coin']==sym and e['from'].lower()==addr and prevblock<e['block']<=block)
            delta=current-int(prior['balance_raw'])
            if delta!=inraw-outraw:continue  # rebasing/fee token or incomplete coverage
            # At least two separate snapshots and two exchange transactions in the interval.
            source_txs=[e for e in incoming if prevblock<e['block']<=block]
            if len(set(e['tx'] for e in source_txs))<2:continue
            emitted=sum(int(e['raw']) for e in source_txs)
            if emitted<=0 or delta<emitted*7//10:continue
            balanceproofs[key]={'reconciled':True,
                'two_distinct_finalized_block_snapshots':True,
                'retained_raw':str(delta),
                'prevblock':prevblock,'end_block':block,'increase_raw':str(delta),
                'exchange_txs':[e['tx'] for e in source_txs],
                'note':'Balance delta equals all known token transfers in contiguous block interval; owner independence beyond EOA not proved.'}
        except Exception:
            continue
    f.parent.mkdir(parents=True,exist_ok=True)
    tmp=f.with_suffix('.tmp');tmp.write_text(json.dumps(new,sort_keys=True,indent=2)+'\n');tmp.replace(f)
    return {'codeproofs':codeproofs,'balanceproofs':balanceproofs,
        'tracked_candidates':len(items),'reconciled_destinations':len(balanceproofs)}
