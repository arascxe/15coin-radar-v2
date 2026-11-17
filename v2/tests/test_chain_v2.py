"""Deterministic chain and spot controls: every covered block must have actual RPC evidence."""
import json,pathlib,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store,MINUTE_MS,market_ingest,CHAINS
from chain_v2 import chain_ingest,select_provider,TRANSFER_TOPIC
C=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
T=1800000000000
BSC_HEAD=100000;BSC_SAFE=BSC_HEAD-15
BSC_FROM=BSC_SAFE-int(3600/CHAINS['bsc']['block_seconds'])+1
class MockRPC:
    def __init__(self,fail_at=None):
        self.fail_at=fail_at;self.calls=[];self.base_log_calls=0
    def rpc(self,url,method,params):
        self.calls.append((url,method,params))
        bsc='bsc' in url
        if method=='eth_chainId':return hex(56 if bsc else 8453)
        if method=='eth_blockNumber':return hex(BSC_HEAD if bsc else 110000)
        if method=='eth_getLogs':
            if bsc:raise RuntimeError('eth_getLogs disabled on public BSC')
            self.base_log_calls+=1
            a=int(params[0]['fromBlock'],16);b=int(params[0]['toBlock'],16)
            token=next(x['token'] for x in C['coins'] if x['chain']=='base')
            if b-a>5 and a+2<=b:
                return [{'address':token,'transactionHash':'0x'+'9'*64,'logIndex':'0x0','blockNumber':hex(a+2),
                         'topics':[TRANSFER_TOPIC,'0x'+'0'*24+'1'*40,'0x'+'0'*24+'2'*40],
                         'data':hex(1234)}]
            return []
        if method=='eth_getBlockByNumber':return {'transactions':[]}
        if method=='eth_getBlockReceipts':
            block=int(params[0],16)
            if self.fail_at is not None and block==self.fail_at:raise RuntimeError('RPC_THROTTLED_BLOCK')
            return []
        raise AssertionError(method)

class SingleTokenBscRPC(MockRPC):
    def rpc(self,url,method,params):
        bsc='bsc' in url
        if bsc and method=='eth_getLogs':
            self.calls.append((url,method,params))
            p=params[0]
            if isinstance(p.get('address'),list):raise RuntimeError('HTTP_403_MULTI_ADDRESS')
            token=next(x['token'] for x in C['coins'] if x['chain']=='bsc')
            a=int(p['fromBlock'],16);b=int(p['toBlock'],16)
            if p['address']==token and a<=BSC_FROM+1<=b:
                return [{'address':token,'transactionHash':'0x'+'a'*64,'logIndex':'0x0','blockNumber':hex(BSC_FROM+1),
                         'topics':[TRANSFER_TOPIC,'0x'+'0'*24+'1'*40,'0x'+'0'*24+'2'*40],
                         'data':hex(12345)}]
            return []
        return super().rpc(url,method,params)

class FakeSpot:
    def __init__(self):self.calls=[]
    def get(self,url,params):
        self.calls.append(params)
        start=int(params['from'])*1000;end=int(params['to'])*1000
        return [[str(x//1000),'1','1','1','1','1'] for x in range(start,end,MINUTE_MS)]

class ChainChecks(unittest.TestCase):
    def test_bsc_fallback_real_receipt_method(self):
        r=MockRPC();url,head,method=select_provider(r,'bsc',next(x['token'] for x in C['coins'] if x['chain']=='bsc'))
        self.assertEqual(method,'receipts');self.assertEqual(head,BSC_SAFE)
        self.assertTrue(any(x[1]=='eth_getBlockReceipts' for x in r.calls))
    def test_bsc_cursor_advances_only_contiguous_complete_receipts(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'db')
            r=MockRPC(fail_at=BSC_FROM+27)
            event=store.begin_run(T)
            out=chain_ingest(store,event,'bsc',C,r,T,max_span=72)
            self.assertEqual(out['status'],'PARTIAL',out)
            self.assertEqual(out['last_complete'],BSC_FROM+23)
            self.assertEqual(store.last_chain_block('bsc'),BSC_FROM+23)
            self.assertGreater(out['backlog_blocks'],0)
            store.close()
    def test_bsc_resume_from_previous_watermark(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'db')
            r=MockRPC();first=chain_ingest(store,store.begin_run(T),'bsc',C,r,T,max_span=24)
            second=chain_ingest(store,store.begin_run(T+MINUTE_MS),'bsc',C,r,T+MINUTE_MS,max_span=24)
            self.assertEqual(second['from'],first['last_complete']+1)
            self.assertEqual(store.db.execute("select count(*) from chain_checks where chain='bsc'").fetchone()[0],2)
            store.close()
    def test_bsc_one_address_per_call_and_atomic_page(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'db');rpc=SingleTokenBscRPC()
            out=chain_ingest(store,store.begin_run(T),'bsc',C,rpc,T,max_span=20)
            self.assertEqual(out['method'],'token_separated_logs')
            self.assertEqual(out['observed_events'],1)
            self.assertEqual(store.db.execute('select count(*) from token_transfers').fetchone()[0],1)
            self.assertTrue(all(isinstance(x[2][0]['address'],str) for x in rpc.calls if x[1]=='eth_getLogs'))
            self.assertEqual(store.last_chain_block('bsc'),out['last_complete'])
            store.close()
    def test_base_chain_log_scanner_accepts_only_monitored_token(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'db');r=MockRPC()
            x=chain_ingest(store,store.begin_run(T),'base',C,r,T,max_span=120)
            self.assertEqual(x['method'],'logs')
            self.assertEqual(x['observed_events'],1)
            self.assertEqual(store.db.execute('select count(*) from token_transfers').fetchone()[0],1)
            store.close()
    def test_delayed_candle_and_overlap_repair(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'db');r=FakeSpot()
            v={'coin':'ON','venue':'gate','pair':'ON_USDT'}
            first=market_ingest(store,store.begin_run(T),v,r,T,lookback=4)
            self.assertEqual(first['bars'],4)
            self.assertEqual(first['to_utc'],__import__('radar_stage1').utc_ms(T-2*MINUTE_MS))
            second=market_ingest(store,store.begin_run(T+5*MINUTE_MS),v,r,T+5*MINUTE_MS,lookback=4)
            self.assertEqual(second['bars'],5)
            self.assertEqual(store.db.execute('select count(*) from market_candles').fetchone()[0],9)
            # Next request explicitly overlaps prior 3 mins for gap repair.
            self.assertEqual(int(r.calls[1]['from']),int(r.calls[0]['to'])-3*60)
            store.close()

if __name__=='__main__':unittest.main()
