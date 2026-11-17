"""Real-market wallet source continuity controls. Synthetic mock RPC only."""
import pathlib,sys,tempfile,unittest,json
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store,wallet_ingest,CHAINS
C=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
TRUST=next(x for x in C['wallets'] if x['coin']=='TRUST')
class RPC:
    def __init__(self,broken_first=False,all_fail=False):
        self.broken_first=broken_first;self.all_fail=all_fail;self.calls=[]
    def rpc(self,url,method,args):
        self.calls.append((url,method))
        chain='base' if ('base' in url or 'swiftnodes' in url or 'zan.top' in url or 'blockreq' in url) else 'bsc'
        if method=='eth_chainId':return hex(8453 if chain=='base' else 56)
        if method=='eth_blockNumber':return hex(10_000)
        if method=='eth_call':
            if self.all_fail or (self.broken_first and url==CHAINS[chain]['rpc'][0]):raise RuntimeError('RPC_THROTTLE_429')
            return hex(123456789)
        raise ValueError(method)
class WalletContinuity(unittest.TestCase):
    def test_failed_primary_recovers_same_block(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'x.db')
            rid=store.begin_run(1800000000000)
            rpc=RPC(broken_first=True)
            cfg={'coins':C['coins'],'wallets':[TRUST]}
            r=wallet_ingest(store,rid,cfg,rpc,1800000000000,max_workers=1)
            self.assertEqual(r[0]['status'],'OK')
            row=store.db.execute('SELECT block,raw_amount FROM wallet_balances WHERE coin=?',('TRUST',)).fetchone()
            self.assertEqual(row[1],'123456789')
            self.assertTrue(any(url==CHAINS['base']['rpc'][1] and method=='eth_call' for url,method in rpc.calls))
            store.close()
    def test_all_rpc_failure_kept_as_error_not_zero(self):
        with tempfile.TemporaryDirectory() as d:
            store=Store(pathlib.Path(d)/'x.db')
            rid=store.begin_run(1800000000000)
            cfg={'coins':C['coins'],'wallets':[TRUST]}
            r=wallet_ingest(store,rid,cfg,RPC(all_fail=True),1800000000000,max_workers=1)
            self.assertEqual(r[0]['status'],'ERROR')
            self.assertEqual(store.db.execute('SELECT COUNT(*) FROM wallet_balances').fetchone()[0],0)
            store.close()
if __name__=='__main__':unittest.main()
