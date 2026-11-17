"""Dynamic discovered wallet snapshots: no owner/buy claim, no lost history."""
import json,pathlib,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store, CHAINS
from dynamic_wallet_monitor import propose,read_same_block,monitor,GOOD
from economic_brain import build
CONFIG=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
DOS=next(x for x in CONFIG['coins'] if x['symbol']=='DOS')
A='0x'+'a'*40
B='0x'+'b'*40
DEST='0x'+'d'*40
NOW=1800000000000
def xfer(i,b,src,to,amt):
    return {'coin':'DOS','token':DOS['token'],'tx':'0x'+str(i).zfill(64),
      'log_index':0,'block':b,'from':src,'to':to,'raw':str(amt)}
class FakeHTTP:
    def __init__(self,contract=False,fail=False,amount=1800):
        self.contract=contract;self.fail=fail;self.amount=amount;self.calls=[]
    def rpc(self,url,method,args):
        self.calls.append((method,args))
        if self.fail:raise RuntimeError('PROVIDER_OFFLINE')
        if method=='eth_chainId':return hex(8453 if 'base' in url else 56)
        if method=='eth_blockNumber':return hex(3000)
        if method=='eth_getCode':return '0x60016000' if self.contract else '0x'
        if method=='eth_call':return hex(self.amount)
        raise AssertionError(method)
def setup_db(path):
    st=Store(path);rid=st.begin_run(NOW)
    st.chain_save(rid,'bsc',1100,1000,1100,
        [xfer(1,1051,A,DEST,1000),xfer(2,1062,B,DEST,800)],'OK',observed_ms=NOW)
    st.finish_run(rid,NOW)
    return st
class DynamicMonitorTests(unittest.TestCase):
    def test_select_new_receivers_no_owner_claim(self):
        with tempfile.TemporaryDirectory() as d:
            st=setup_db(pathlib.Path(d)/'s.db')
            p=propose(st.db,CONFIG,NOW)
            dos=next(z for z in p['coins'] if z['coin']=='DOS')
            self.assertIn(DEST,[x['address'] for x in dos['address_candidates']])
            self.assertFalse(p['strong_alert'])
            st.close()
    def test_same_block_balance_success_persists_and_becomes_dynamic_eoa(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'s.db';st=setup_db(path)
            rpc=FakeHTTP()
            outcome=monitor(st,CONFIG,rpc,NOW,max_workers=1)
            self.assertTrue(any(x['coin']=='DOS' and x['address']==DEST and x['status']=='OK'
                                for x in outcome['results']))
            check=st.db.execute('SELECT status,detail,block FROM wallet_checks WHERE coin=? AND address=?',
                ('DOS',DEST)).fetchone()
            self.assertEqual(check,('OK',GOOD,1100))
            balance=st.db.execute('SELECT raw_amount,block FROM wallet_balances WHERE coin=? AND address=?',
                  ('DOS',DEST)).fetchone()
            self.assertEqual(balance,('1800',1100))
            self.assertIn(('eth_getCode',[DEST,hex(1100)]),rpc.calls)
            st.close()
            brain=build(path,CONFIG)
            dos=next(z for z in brain['coins'] if z['coin']=='DOS')
            self.assertFalse(dos['strong_signal'])
            self.assertFalse(dos['buy_signal'])
    def test_contract_is_not_added_as_holder(self):
        with tempfile.TemporaryDirectory() as d:
            st=setup_db(pathlib.Path(d)/'s.db')
            r=monitor(st,CONFIG,FakeHTTP(contract=True),NOW,max_workers=1)
            self.assertTrue(any(x['coin']=='DOS' and x['address']==DEST and x['status']=='SKIPPED_CONTRACT'
                                for x in r['results']))
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM wallet_balances WHERE coin=? AND address=?',
                ('DOS',DEST)).fetchone()[0],0)
            st.close()
    def test_rejected_contract_not_reselected_after_retry_cooldown(self):
        with tempfile.TemporaryDirectory() as d:
            st=setup_db(pathlib.Path(d)/'s.db')
            monitor(st,CONFIG,FakeHTTP(contract=True),NOW,max_workers=1)
            rid=st.begin_run(NOW+3700000)
            st.chain_save(rid,'bsc',1200,1101,1200,[],
                'OK',observed_ms=NOW+3700000)
            st.finish_run(rid,NOW+3700000)
            p=propose(st.db,CONFIG,NOW+3700000)
            dos=next(x for x in p['coins'] if x['coin']=='DOS')
            self.assertNotIn(DEST,[a['address'] for a in dos['address_candidates']])
            self.assertGreaterEqual(dos['historically_rejected_contract_addresses'],1)
            st.close()
    def test_failed_rpc_does_not_forge_zero_balance(self):
        with tempfile.TemporaryDirectory() as d:
            st=setup_db(pathlib.Path(d)/'s.db')
            r=monitor(st,CONFIG,FakeHTTP(fail=True),NOW,max_workers=1)
            self.assertTrue(any(x['coin']=='DOS' and x['status']=='ERROR' for x in r['results']))
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM wallet_balances WHERE coin=? AND address=?',
                ('DOS',DEST)).fetchone()[0],0)
            st.close()
    def test_followup_same_address_next_run_remeasured_and_balance_history_grows(self):
        with tempfile.TemporaryDirectory() as d:
            st=setup_db(pathlib.Path(d)/'s.db')
            monitor(st,CONFIG,FakeHTTP(amount=1800),NOW,max_workers=1)
            rid=st.begin_run(NOW+3700000)
            st.chain_save(rid,'bsc',1200,1101,1200,
                [xfer(3,1170,A,DEST,300),xfer(4,1180,B,DEST,200)],'OK',
                observed_ms=NOW+3700000)
            st.finish_run(rid,NOW+3700000)
            next_result=monitor(st,CONFIG,FakeHTTP(amount=2300),NOW+3700000,max_workers=1)
            self.assertTrue(any(z['coin']=='DOS' and z['address']==DEST and z['status']=='OK'
                for z in next_result['results']))
            count=st.db.execute('SELECT COUNT(*) FROM wallet_balances WHERE coin=? AND address=?',
                              ('DOS',DEST)).fetchone()[0]
            self.assertEqual(count,2)
            st.close()
            brain=build(pathlib.Path(d)/'s.db',CONFIG)
            coin=next(z for z in brain['coins'] if z['coin']=='DOS')
            self.assertTrue(coin['wallet_accumulation_candidates'])
            self.assertFalse(coin['strong_signal'])
    def test_no_new_run_is_safely_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'s.db')
            r=monitor(st,CONFIG,FakeHTTP(),NOW,max_workers=1)
            self.assertEqual(r['status'],'NO_COLLECTOR_RUN')
            st.close()
if __name__=='__main__':unittest.main()
