"""Offline negative controls: immutable V2 alarm proof is independently replayed."""
import copy
import hashlib
import json
import pathlib
import tempfile
import unittest
from v2_full.evidence_packet import build,canon
from v2_full.packet_verify import validate,validate_file,STATUS_COMPLETE,STATUS_PARTIAL,STATUS_NONE
from v2_full.packet_publish import stage

T=1800000000000
def sample(with_market=True):
    event={'coin':'ON','evidence_state':'FUSION_PREPARATION_THEN_SPOT_IGNITION_RESEARCH',
           'first_seen_ms':T}
    config={'coins':[{'symbol':'ON','chain':'bsc','token':'0x'+'a'*40}]}
    p=build(event,config=config)
    if with_market:
        p['spot_decision_inputs_and_bounded_replays']=[{
            'venue':'gate','pair':'ON_USDT','phase':'early_90s',
            'market_token_identity_verified_at_observation':True,
            'decision_window_start_ms':T-90000,
            'decision_window_end_ms':T,
            'decision_buy_quote':'80','decision_sell_quote':'20',
            'decision_book':{'status':'VERIFIED_SEQUENCED_BOOK_INTERVAL',
                'reference_price':'1','from_seq':100,'to_seq':110,
                'ask_before_quote':'100','ask_after_quote':'60'},
            'decision_tape':{'trades':2},
            'book_replay':{'status':'BOOK_ASK_LEVELS_REPLAYABLE',
                'from_seq':100,'to_seq':110,'fixed_price':'1',
                'before_ask_price_token_qty_pairs':[['1','100']],
                'after_ask_price_token_qty_pairs':[['1','60']],
                'before_levels_total':1,'after_levels_total':1,
                'all_necessary_levels_included':True,
                'recomputed_before_quote':'100','recomputed_after_quote':'60'},
            'trade_replay':{'status':'RECEIVED_PUBLIC_TRADES_AGGREGATE_RECONCILED',
                'start_ms':T-90000,'end_ms':T,
                'received_count':2,'aggregate_record_count':2,
                'recomputed_buy_quote':'80','recomputed_sell_quote':'20',
                'all_received_trade_ids_in_packet':True,'unshown_trades':0,
                'trade_receipts_latest':[{'trade_id':'1','trade_ms':T-50000,'side':'buy',
                    'price':'1','token_qty':'80','quote_notional':'80'},
                    {'trade_id':'2','trade_ms':T-45000,'side':'sell',
                    'price':'1','token_qty':'20','quote_notional':'20'}]},
            'market_proof_replayable':True
        }]
        p['market_window_count']=1
        p['market_windows_replayable_count']=1
    return p

class ImmutablePacketAudit(unittest.TestCase):
    def test_original_market_book_trade_arithmetic_replays(self):
        p=sample()
        s=validate(p)
        self.assertEqual(s['status'],STATUS_COMPLETE)
        self.assertEqual(s['market_windows_fully_independently_replayed'],1)
        self.assertFalse(s['L2_cancellation_attribution_proven'])
        self.assertFalse(s['economic_outcome_or_accuracy_validated'])

    def test_tampering_fails_closed(self):
        p=sample()
        for change in ('book_qty','trade_price','trade_id','event','chain'):
            x=copy.deepcopy(p)
            m=x['spot_decision_inputs_and_bounded_replays'][0]
            if change=='book_qty':m['book_replay']['after_ask_price_token_qty_pairs'][0][1]='59'
            if change=='trade_price':m['trade_replay']['trade_receipts_latest'][0]['price']='2'
            if change=='trade_id':m['trade_replay']['trade_receipts_latest'][1]['trade_id']='1'
            if change=='event':x['event_id']='0'*22
            if change=='chain':x['chain']='unknown'
            with self.subTest(change=change):
                with self.assertRaises(ValueError):validate(x)

    def test_bounded_tape_does_not_claim_complete_replay(self):
        p=sample()
        tr=p['spot_decision_inputs_and_bounded_replays'][0]['trade_replay']
        tr['all_received_trade_ids_in_packet']=False
        tr['unshown_trades']=1
        tr['trade_receipts_latest']=tr['trade_receipts_latest'][1:]
        check=validate(p)
        self.assertEqual(check['status'],STATUS_PARTIAL)
        self.assertEqual(check['market_windows_fully_independently_replayed'],0)

    def test_packet_with_no_market_not_false_pump_proof(self):
        s=validate(sample(False))
        self.assertEqual(s['status'],STATUS_NONE)
        self.assertEqual(s['multiday_wallet_count'],0)

    def test_atomic_stage_sha_and_replay_guard(self):
        with tempfile.TemporaryDirectory() as root:
            d=pathlib.Path(root)
            src=d/'source';src.mkdir()
            dest=d/'dest'
            p=sample()
            raw=canon(p)
            ident=p['event_id']
            (src/(ident+'.json')).write_bytes(raw)
            inbox={'scope':'ALL_15_MONITORED_NOT_ALL_15_VERIFIED',
                   'coin_states':{x:x for x in 'ON DOS HANA COLLECT LIGHT APR IN TRUST B2 CYS STBL BSB AIO STAR LYN'.split()},
                   'research_events_72h':[{
                       'event_id':ident,'coin':p['coin'],'state':p['state'],
                       'first_observed_ms':T,
                       'evidence_packet':{'id':ident,
                          'path':'v2_state/evidence_packets/'+ident+'.json',
                          'sha256':hashlib.sha256(raw).hexdigest()}}]}
            f=d/'inbox.json';f.write_text(json.dumps(inbox))
            out=stage(f,src,dest)
            self.assertEqual(out['new_packets_staged'],1)
            updated=json.loads(f.read_text())['research_events_72h'][0]['evidence_packet']
            self.assertEqual(updated['independent_replay_verification']['status'],STATUS_COMPLETE)
            self.assertEqual(validate_file(dest/(ident+'.json'),ident,updated['sha256'])['status'],STATUS_COMPLETE)
            # Different bytes with correctly updated SHA still cannot bypass
            # a full independent replay failure.
            p['spot_decision_inputs_and_bounded_replays'][0]['trade_replay']['trade_receipts_latest'][0]['price']='999'
            tampered=canon(p)
            (src/(ident+'.json')).write_bytes(tampered)
            inbox['research_events_72h'][0]['evidence_packet']['sha256']=hashlib.sha256(tampered).hexdigest()
            f.write_text(json.dumps(inbox))
            with self.assertRaises(ValueError):stage(f,src,d/'new_dest')

if __name__=='__main__':unittest.main()
