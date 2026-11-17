"""No false preparation alert from unverified exchange tags or OHLCV."""
import json,pathlib,sqlite3,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store
from preparation_research import analyze,classify_transfers,role
C=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
class AnalysisChecks(unittest.TestCase):
    def test_exchange_candidate_is_not_verified(self):
        tagged=next(x for x in C['wallets'] if x.get('role')=='exchange_custody' and x.get('identity_confidence') not in ('official_exchange_verified','confirmed_onchain_proof'))
        self.assertEqual(role(tagged),'CEX_CANDIDATE_UNVERIFIED')
        tx=[('ON','0x'+'1'*64,0,150,tagged['address'].lower(),'0x'+'2'*40,'100000000')]
        flows=classify_transfers(tx,{tagged['address'].lower():tagged})
        self.assertEqual(flows['verified_cex_outflow_raw'],'0')
        self.assertEqual(flows['candidate_cex_outflow_raw_unverified'],'100000000')
    def test_all_coins_have_research_not_signal(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'db');st.close()
            report=analyze(pathlib.Path(d)/'db',C)
            self.assertEqual(len(report['coins']),15)
            self.assertEqual(set(x['coin'] for x in report['coins']),set(x['symbol'] for x in C['coins']))
            self.assertTrue(all(x['classification']=='RESEARCH_ONLY_NOT_BUY_SIGNAL' for x in report['coins']))
            self.assertTrue(all('TAKER_SIDE_SPOT_FLOW_NOT_CAPTURED' in x['missing_evidence'] for x in report['coins']))
    def test_unknown_chain_is_not_declared_zero_transfer_activity(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'db');st.close()
            x=next(z for z in analyze(pathlib.Path(d)/'db',C)['coins'] if z['coin']=='ON')
            self.assertFalse(x['coverage']['chain_cursor_known'])
            self.assertIn('NO_CONFIRMED_CHAIN_CURSOR',x['missing_evidence'])
if __name__=='__main__':unittest.main()
