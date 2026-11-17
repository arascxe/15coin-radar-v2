"""V2 read-only onchain coverage: BSC receipt fallback; Base multi-provider logs.
Never advance a confirmed block cursor past an unscanned block. No paid endpoints.
"""
import concurrent.futures
import math
import time
from radar_stage1 import CHAINS, now_ms, decode_transfer, MINUTE_MS

TRANSFER_TOPIC='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
ENDPOINTS={
 'bsc':('https://bsc.publicnode.com','https://bsc-dataseed.bnbchain.org',
        'https://bsc-dataseed-public.bnbchain.org'),
 'base':('https://api.zan.top/base-mainnet','https://rpc.swiftnodes.io/rpc/base',
         'https://base-rpc.blockreq.com/v1/rpc/public',
         'https://base-mainnet.public.blastapi.io','https://mainnet.base.org',
         'https://base.publicnode.com')
}
def nhex(x):
    if not isinstance(x,str) or not x.startswith('0x'):raise ValueError('INVALID_HEX')
    return int(x,16)

def provider_head(http,chain,url):
    got=nhex(http.rpc(url,'eth_chainId',[]))
    if got!=CHAINS[chain]['id']:raise ValueError('WRONG_CHAIN')
    return nhex(http.rpc(url,'eth_blockNumber',[]))-(15 if chain=='bsc' else 10)

def receipt_logs(receipts,token_to_coin):
    if not isinstance(receipts,list):raise ValueError('INVALID_BLOCK_RECEIPTS_RESPONSE')
    result=[]
    for rec in receipts:
        if not isinstance(rec,dict) or not isinstance(rec.get('logs'),list):raise ValueError('INVALID_RECEIPT_ENTRY')
        for lg in rec['logs']:
            if not isinstance(lg,dict):raise ValueError('INVALID_LOG_ENTRY')
            if str(lg.get('address','')).lower() in token_to_coin and str((lg.get('topics') or [''])[0]).lower()==TRANSFER_TOPIC:
                result.append(decode_transfer(lg,token_to_coin))
    return result

def _probe_receipt(http,url,block):
    receipts=http.rpc(url,'eth_getBlockReceipts',[hex(block)])
    if not isinstance(receipts,list):raise ValueError('BLOCK_RECEIPTS_UNSUPPORTED')
    # Cross-check a populated block against its transaction count (empty is also legitimate).
    header=http.rpc(url,'eth_getBlockByNumber',[hex(block),False])
    if not isinstance(header,dict) or not isinstance(header.get('transactions'),list):
        raise ValueError('NO_BLOCK_TO_VALIDATE_RECEIPTS')
    if len(header['transactions'])!=len(receipts):
        raise ValueError('INCOMPLETE_BLOCK_RECEIPTS')
    return True

def select_provider(http,chain,token_contract):
    errors=[]
    for url in ENDPOINTS[chain]:
        try:
            head=provider_head(http,chain,url)
            if head<=0:raise ValueError('INVALID_SAFE_HEAD')
            # Prefer inexpensive filtered log ranges, but official BSC disables eth_getLogs.
            if not ('bsc-dataseed' in url):
                try:
                    probe=http.rpc(url,'eth_getLogs',[{'fromBlock':hex(head-2),'toBlock':hex(head-2),'address':token_contract, 'topics':[TRANSFER_TOPIC]}])
                    if not isinstance(probe,list):raise ValueError('LOGS_PROBE_NOT_ARRAY')
                    return url,head,'logs'
                except Exception as e:errors.append('logs:'+url+':'+str(e)[:85])
            try:
                _probe_receipt(http,url,head-2)
                return url,head,'receipts'
            except Exception as e:errors.append('receipts:'+url+':'+str(e)[:85])
        except Exception as e:errors.append('head:'+url+':'+str(e)[:85])
    raise RuntimeError('NO_COVERAGE_CAPABLE_RPC '+ ';'.join(errors)[:820])

def scan_logs(http,url,start,end,token_to_coin,deadline):
    observations=[];last=start-1;span=120
    a=start
    while a<=end:
        if time.monotonic()>deadline:raise TimeoutError('BLOCK_SCAN_TIME_BUDGET')
        b=min(end,a+span-1)
        try:
            data=http.rpc(url,'eth_getLogs',[{'fromBlock':hex(a),'toBlock':hex(b),'address':list(token_to_coin),'topics':[TRANSFER_TOPIC]}])
            if not isinstance(data,list):raise ValueError('LOG_RESPONSE_NOT_ARRAY')
            observations.extend(decode_transfer(x,token_to_coin) for x in data)
            last=b;a=b+1
            if span<120:span=min(120,span*2)
        except Exception:
            if span<=1:raise
            span=max(1,span//2)
    return last,observations

def scan_receipts(http,url,start,end,token_to_coin,deadline):
    events=[];last=start-1
    # Ordered futures: a timeout or failed block stops the verified contiguous watermark.
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for a in range(start,end+1,24):
            if time.monotonic()>deadline:raise TimeoutError('BLOCK_RECEIPTS_TIME_BUDGET')
            b=min(end,a+23)
            replies=list(pool.map(lambda n:http.rpc(url,'eth_getBlockReceipts',[hex(n)]),range(a,b+1)))
            for num,receipts in zip(range(a,b+1),replies):
                events.extend(receipt_logs(receipts,token_to_coin))
                last=num
    return last,events

def chain_ingest(store,rid,chain,config,http,ts=None,max_span=4500,deadline_seconds=750):
    ts=now_ms() if ts is None else ts
    token_to_coin={x['token'].lower():x['symbol'] for x in config['coins'] if x['chain']==chain}
    try:
        url,head,method=select_provider(http,chain,next(iter(token_to_coin)))
    except Exception as e:
        store.chain_save(rid,chain,None,None,None,[],'ERROR','RPC_NO_VERIFIED_TRANSFER_READ:'+str(e)[:700],ts)
        return {'chain':chain,'status':'ERROR','detail':str(e)[:700]}
    last_cursor=store.last_chain_block(chain)
    if last_cursor is not None:
        start=max(1,last_cursor+1)
    else:
        first=store.db.execute('SELECT MIN(started_ms) FROM runs').fetchone()[0] or ts
        # Recover from the first radar run, even if earlier providers failed.
        elapsed=max(0,(ts-first)/1000)
        start=max(1,head-math.ceil((3600+elapsed)/CHAINS[chain]['block_seconds'])+1)
    if start>head:
        store.chain_save(rid,chain,head,start,None,[],'OK','NO_NEW_FINALIZED_BLOCKS',ts)
        return {'chain':chain,'status':'OK','head':head,'detail':'NO_NEW_FINALIZED_BLOCKS'}
    end=min(head,start+max_span-1)
    observed=[];last=start-1;err=None;used=method;deadline=time.monotonic()+deadline_seconds
    # If provider can't serve filtered logs despite a successful probe, retry
    # with full block receipts from the same checked provider, then report gaps.
    if method=='logs':
        try:last,observed=scan_logs(http,url,start,end,token_to_coin,deadline)
        except Exception as e:
            err='LOGS_FAILED:'+str(e)[:150]
            # Do not publish a partial, possibly duplicated scan; retry this window by receipts.
            last=start-1;observed=[]
            try:
                _probe_receipt(http,url,start)
                used='receipts'
                last,observed=scan_receipts(http,url,start,end,token_to_coin,deadline)
                err=None
            except Exception as e2:err+=';RECEIPTS_FAILED:'+str(e2)[:150]
    else:
        try:last,observed=scan_receipts(http,url,start,end,token_to_coin,deadline)
        except Exception as e:err='RECEIPTS_FAILED:'+str(e)[:180]
    # Important: no partial receipt batch is committed on failure (fail closed).
    if err and last<start:
        state='ERROR'
    elif err or last<head:
        state='PARTIAL'
    else:
        state='OK'
    detail=f'provider={url};method={used};verified_to={last};head={head}'
    if err:detail+=';error='+err
    if last<head:detail+=';BACKLOG_PENDING'
    store.chain_save(rid,chain,head,start,last if last>=start else None,observed,state,detail,ts)
    return {'chain':chain,'status':state,'provider':url,'method':used,'from':start,'last_complete':last,'head':head,
            'observed_events':len(observed),'backlog_blocks':max(0,head-last),'detail':detail}
