"""Synthetic acceptance + negative controls; never label synthetic data as live."""
import collections
import hashlib
import json
import pathlib
import tempfile
import threading
import unittest
from types import SimpleNamespace

from v2_full.core import COINS,VenueState
from v2_full.evidence_packet import build,write_immutable,packet_id
from v2_full.delivery_index import build as delivery
from v2_full.packet_publish import stage

T=1800000000000
ADDR='0x'+'a'*40
SRC='0x'+'b'*40
TOKEN='0x'+'c'*40
TX='0x'+'1'*64

def setup():
    st=VenueState('ON','gate','ON_USDT','VERIFIED_SAME_CONTRACT_NETWORK')
    st.book.snapshot([['0.99','100']],[['1.01','100'],['1.02','200'],['1.04','300']],100,T-95000)
    st.book.update([], [['1.01','50']],101,101,T-1000,'gate')
    st.tape.connected_at(T-105000)
    st.tape.add('first',T-80000,'buy','1.01','20')
    st.tape.add('second',T-20000,'buy','1.01','40')
    b=st.book.compare(T,90000)
    t=st.tape.window(T,90000)
    assert b['status']=='VERIFIED_SEQUENCED_BOOK_INTERVAL'
    assert t['status']=='LIVE_TRANSPORT_CONTINUOUS_NOT_EXCHANGE_AUDITED'
    e={'coin':'ON','evidence_state':'FUSION_PREPARATION_THEN_SPOT_IGNITION_RESEARCH',
       'first_seen_ms':T,'preparation_proved':False,
       'spot_markets':[{'venue':'gate','pair':'ON_USDT','phase':'early_90s',
         'identity_verified':True,
         'window_start_ms':t['window_start_ms'],'window_end_ms':t['window_end_ms'],
         'buy_quote':t['buy_quote'],'sell_quote':t['sell_quote'],
         'book_observed':b,'tape_observed':t}],
       'unified_economic_evidence':{'phase':'PREPARATION_THEN_SPOT_IGNITION_RESEARCH',
          'multiday_wallet_evidence':[{
            'address':ADDR,'observed_days':3,
            'net_retained_increase_token':'1000',
            'start_block':100,'end_block':200,
            'start_balance_token':'0','end_balance_token':'1000',
            'transfer_reconciled':True,
            'transaction_proof_sample':{'window_days':3,'entries':[{
                 'chain':'bsc','tx':TX,'block':150,
                 'from':SRC,'to':ADDR,'amount_raw':'1000000000000000000000'}]}}],
          'spot_ignition_observed':True,'orders_cancelled_vs_consumed_exactly_known':False},
       'onchain_economics':{'recipient_evidence':[{'address':ADDR,'evidence_txs':[TX]}]},
       'dex_active_quote':{'status':'NO_FRESH_SUPPORTED_V3_POOL'}}
    chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL',
                    'complete':True,'from_block':100,'safe_head':200}},
        'events':[{'coin':'ON','tx':TX,'block':150,'log_index':0,
                   'block_ms':T-30000,'from':SRC,'to':ADDR,'raw':'1000'}]}
    capture=SimpleNamespace(markets={('gate','ON_USDT'):st},
                 locks=collections.defaultdict(threading.RLock))
    config={'coins':[{'symbol':'ON','chain':'bsc','token':TOKEN}]}
    return e,chain,capture,config

class AlarmPacketTests(unittest.TestCase):
    def test_freeze_actual_ws_ids_sequenced_asks_transfer_and_replay(self):
        e,chain,cap,config=setup()
        packet=build(e,cap,chain,config)
        self.assertEqual(packet['event_id'],packet_id(e))
        self.assertEqual(packet['token_contract'],TOKEN)
        self.assertEqual(packet['chain_transactions'][0]['explorer'],
                         'https://bscscan.com/tx/'+TX)
        r=packet['spot_decision_inputs_and_bounded_replays'][0]
        self.assertTrue(r['market_proof_replayable'])
        self.assertEqual(r['book_replay']['from_seq'],100)
        self.assertEqual(r['book_replay']['to_seq'],101)
        self.assertEqual(r['book_replay']['recomputed_before_quote'], '305.00')
        self.assertEqual(r['book_replay']['recomputed_after_quote'], '254.50')
        self.assertEqual([p['trade_id'] for p in r['trade_replay']['trade_receipts_latest']],['first','second'])
        self.assertTrue(packet['proof_health']['L2_cancellation_vs_execution_unknown'])
        self.assertTrue(packet['not_buy_advice'])

    def test_hash_stays_identical_and_previous_evidence_cannot_be_overwritten(self):
        e,chain,cap,config=setup()
        with tempfile.TemporaryDirectory() as d:
            a=write_immutable(d,e,cap,chain,config)
            b=write_immutable(d,e,cap,chain,config)
            self.assertEqual(a,b)
            path=pathlib.Path(d)/(a['evidence_packet_id']+'.json')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),
                             a['evidence_packet_sha256'])
            e['unified_economic_evidence']['phase']='CHANGED_AFTER_EVENT'
            with self.assertRaisesRegex(ValueError,'IMMUTABLE'):
                write_immutable(d,e,cap,chain,config)

    def test_delivery_must_check_packet_identity_and_sha_before_link(self):
        e,chain,cap,config=setup()
        with tempfile.TemporaryDirectory() as d:
            ref=write_immutable(d,e,cap,chain,config)
            e.update(ref)
            snapshot={'asof_ms':T,'coins':[{'coin':x,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'} for x in COINS]}
            inbox=delivery(snapshot,{'events':[e]},packet_dir=d)
            item=inbox['research_events_72h'][0]['evidence_packet']
            self.assertEqual(item['status'],'VERIFIED_LOCAL_PACKET_READY_FOR_GITHUB_PUBLISH')
            self.assertIsNone(item['url'])
            raw=pathlib.Path(d)/(ref['evidence_packet_id']+'.json')
            raw.write_text(raw.read_text()+' ')
            other=delivery(snapshot,{'events':[e]},packet_dir=d)
            self.assertEqual(other['research_events_72h'][0]['evidence_packet']['status'],
                             'PACKET_SHA_OR_EVENT_IDENTITY_MISMATCH')
            self.assertIsNone(other['research_events_72h'][0]['evidence_packet']['url'])

    def test_missing_and_legacy_packet_never_linked(self):
        e,chain,cap,config=setup()
        snapshot={'asof_ms':T,'coins':[{'coin':x,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'} for x in COINS]}
        old=delivery(snapshot,{'events':[e]})
        self.assertEqual(old['research_events_72h'][0]['evidence_packet']['status'],
                         'NOT_PRODUCED_IN_LEGACY_EVENT')
        e.update({'evidence_packet_id':packet_id(e),'evidence_packet_sha256':'a'*64})
        with tempfile.TemporaryDirectory() as d:
            new=delivery(snapshot,{'events':[e]},packet_dir=d)
            self.assertEqual(new['research_events_72h'][0]['evidence_packet']['status'],
                             'LOCAL_PACKET_NOT_AVAILABLE_MAY_BE_PREVIOUS_RUN')

    def test_cancel_only_and_unverified_markets_cannot_prove_l2(self):
        e,chain,cap,config=setup()
        st=cap.markets[('gate','ON_USDT')]
        e['spot_markets'][0]['book_observed']['from_seq']+=10
        p=build(e,cap,chain,config)
        self.assertFalse(p['proof_health']['all_reported_market_windows_replayable'])
        self.assertEqual(p['market_windows_replayable_count'],0)

    def test_atomic_packet_stage_preserves_old_links_and_blocks_bad_hash(self):
        e,chain,cap,config=setup()
        with tempfile.TemporaryDirectory() as td:
            root=pathlib.Path(td);fresh=root/'fresh';main=root/'main'
            e.update(write_immutable(fresh,e,cap,chain,config))
            snapshot={'asof_ms':T,'coins':[{'coin':x,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'} for x in COINS],
                'scope':'ALL_15_MONITORED_NOT_ALL_15_VERIFIED'}
            inbox=delivery(snapshot,{'events':[e]},packet_dir=fresh)
            ip=root/'live_alert_inbox.json';ip.write_text(json.dumps(inbox))
            result=stage(ip,fresh,main)
            self.assertEqual(result['new_packets_staged'],1)
            ref=json.loads(ip.read_text())['research_events_72h'][0]['evidence_packet']
            self.assertEqual(ref['status'],'PACKET_VERIFIED_READY_FOR_ATOMIC_GITHUB_PUBLISH')
            self.assertIn('/blob/main/v2_state/evidence_packets/',ref['url'])
            src=fresh/(packet_id(e)+'.json')
            dst=main/(packet_id(e)+'.json')
            self.assertEqual(src.read_bytes(),dst.read_bytes())
            src.unlink()
            result=stage(ip,fresh,main)
            self.assertEqual(result['new_packets_staged'],0)
            dst.write_text(dst.read_text()+'\\n')
            with self.assertRaisesRegex(ValueError,'SHA'):
                stage(ip,fresh,main)

    def test_missing_packet_stage_publishes_no_fake_url(self):
        e,chain,cap,config=setup()
        with tempfile.TemporaryDirectory() as td:
            root=pathlib.Path(td)
            e.update({'evidence_packet_id':packet_id(e),'evidence_packet_sha256':'a'*64})
            snapshot={'asof_ms':T,'coins':[{'coin':x,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'} for x in COINS],
                'scope':'ALL_15_MONITORED_NOT_ALL_15_VERIFIED'}
            inbox=delivery(snapshot,{'events':[e]})
            ip=root/'inbox.json';ip.write_text(json.dumps(inbox))
            result=stage(ip,root/'no_source',root/'published')
            self.assertEqual(result['older_packets_missing'],1)
            packet=json.loads(ip.read_text())['research_events_72h'][0]['evidence_packet']
            self.assertEqual(packet['status'],'NOT_PUBLISHED_PACKET_MISSING')
            self.assertIsNone(packet['url'])

if __name__=='__main__':
    unittest.main()
