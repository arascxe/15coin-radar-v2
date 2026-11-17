"""Synthetic economic flow and alert-safety controls. No network, no market orders."""
import json,pathlib,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store,MINUTE_MS
from economic_brain import build,covered,transfer_kind,kinds
ROOT=pathlib.Path(__file__).resolve().parents[1]
C=json.loads((ROOT/'config.json').read_text())
BSC=next(x for x in C['coins'] if x['symbol']=='DOS')
W=next(w for w in C['wallets'] if w['coin']=='DOS' and w['role']=='eoa_unverified')
T=1800000000000
def transfer(tx,block,frm,to,raw):
    return {'token':BSC['token'],'coin':'DOS','tx':'0x'+tx*64,'log_index':0,'block':block,'from':frm,'to':to,'raw':str(raw)}
class EconomicBrainTests(unittest.TestCase):
    def test_unverified_cex_label_not_exchange_exit(self):
        w=next(w for w in C['wallets'] if w['role']=='exchange_custody')
        self.assertEqual(kinds(w),'EXCHANGE_TAG_UNVERIFIED')
        self.assertEqual(transfer_kind(w['address'].lower(),'0x'+'3'*40,{w['address'].lower():w}),
                         'UNVERIFIED_EXCHANGE_TAG_MOVEMENT')
    def test_dex_and_mint_not_whale_accumulation(self):
        pool='0x'+'4'*40
        self.assertEqual(transfer_kind(pool,'0x'+'5'*40,{pool:{'role':'dex_pool'}}),
                         'DEX_SWAP_OR_LP_UNRESOLVED')
        self.assertEqual(transfer_kind('0x'+'0'*40,pool,{}),'MINT_OR_ZERO_ADDRESS_TRANSFER')
    def test_chain_gaps_are_not_accepted_as_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'db')
            rid=st.begin_run(T)
            st.chain_save(rid,'bsc',250,200,205,[],'PARTIAL',observed_ms=T)
            st.chain_save(st.begin_run(T+1),'bsc',250,210,220,[],'PARTIAL',observed_ms=T)
            self.assertFalse(covered(st.db,'bsc',202,215))
            self.assertTrue(covered(st.db,'bsc',210,220))
            st.close()
    def make_db(self,path,correct=True,complete=True):
        st=Store(path)
        old=W['address'].lower()
        src='0x'+'7'*40
        src2='0x'+'8'*40
        st.wallet_save(st.begin_run(T),BSC,old,200,1000,observed_ms=T)
        st.wallet_save(st.begin_run(T+120000),BSC,old,210,1800,observed_ms=T+120000)
        trans=[transfer('a',204,src,old,400),transfer('b',208,src2,old,400 if correct else 300)]
        st.chain_save(st.begin_run(T+1),'bsc',210,201,210 if complete else 206,trans if complete else trans[:1],
                      'OK' if complete else 'PARTIAL',observed_ms=T+120000)
        st.finish_run(st.db.execute('SELECT run_id FROM runs ORDER BY started_ms DESC LIMIT 1').fetchone()[0],T+120000)
        st.close()
    def test_correlated_two_transaction_increase_becomes_unverified_watch(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';self.make_db(path)
            r=build(path,C)
            dos=next(x for x in r['coins'] if x['coin']=='DOS')
            self.assertEqual(r['candidate_coins'],1)
            self.assertEqual(dos['classification'],'PREPARATION_WATCH_HYPOTHESIS_ONLY')
            self.assertEqual(dos['wallet_accumulation_candidates'][0]['incoming_transactions'],2)
            self.assertFalse(dos['buy_signal'])
            self.assertFalse(dos['wallet_accumulation_candidates'][0]['full_24h_baseline'])
            self.assertIn('NO_VERIFIED_EXCHANGE_OWNERSHIP',dos['missing_evidence'])
    def test_dex_pool_source_two_transactions_not_prep(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db'
            cfg=json.loads(json.dumps(C))
            pool1='0x'+'7'*40
            pool2='0x'+'8'*40
            cfg['wallets']+=[
                {'coin':'DOS','chain':'bsc','address':pool1,'role':'dex_pool'},
                {'coin':'DOS','chain':'bsc','address':pool2,'role':'dex_vault'}]
            self.make_db(path)
            result=build(path,cfg)
            self.assertEqual(result['candidate_coins'],0)
            w=next(z for z in result['coins'] if z['coin']=='DOS')
            self.assertTrue(any(x.get('inbound_provenance',{}).get('dex_dominated')
                for x in w['wallet_observations_not_corrobated']))
    def test_same_unverified_source_not_prep(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';self.make_db(path)
            # Replace the second transfer source to make both incoming from one.
            import sqlite3
            c=sqlite3.connect(path)
            c.execute("UPDATE token_transfers SET from_address=? WHERE coin='DOS' AND block=208",
                      ('0x'+'7'*40,))
            c.commit();c.close()
            result=build(path,C)
            self.assertEqual(result['candidate_coins'],0)
    def test_same_block_multi_source_not_prep(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';self.make_db(path)
            import sqlite3
            c=sqlite3.connect(path)
            c.execute("UPDATE token_transfers SET block=204 WHERE coin='DOS' AND block=208")
            c.commit();c.close()
            self.assertEqual(build(path,C)['candidate_coins'],0)
    def test_transfer_balance_disagreement_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';self.make_db(path,correct=False)
            r=build(path,C)
            self.assertEqual(r['candidate_coins'],0)
    def test_gap_or_missing_latest_proof_suppresses_candidate(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';self.make_db(path,complete=False)
            r=build(path,C)
            self.assertEqual(r['candidate_coins'],0)
    def test_no_live_db_no_false_no_preparation(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'db';st=Store(path);st.close()
            r=build(path,C)
            self.assertEqual(r['candidate_coins'],0)
            self.assertTrue(all(x['classification']=='INSUFFICIENT_EVIDENCE_NOT_NO_PREPARATION' for x in r['coins']))
if __name__=='__main__':unittest.main()
