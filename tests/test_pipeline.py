import unittest
from decimal import Decimal
from v2_full.core import COINS,L2,Tape,VenueState,Gap
from v2_full.streams import StreamCapture
from v2_full.inventory import analyze
from v2_full.decision import verdict
from v2_full.dex import v3_raw_ask_quote, v3_raw_ask_token_quantity, tick_sqrt
from v2_full.runner import evaluate

T=1_800_000_000_000
CONFIG={'coins':[{'symbol':s,'chain':'base' if s=='TRUST' else 'bsc','token':'0x'+'1'*40} for s in COINS],
        'wallets':[{'coin':'ON','chain':'bsc','address':'0x'+'a'*40,'role':'exchange_custody','label':'Gate candidate'},
                   {'coin':'ON','chain':'bsc','address':'0x'+'b'*40,'role':'exchange_custody','label':'Gate internal'}],
        'venues':[{'coin':'ON','venue':'gate','pair':'ON_USDT'},{'coin':'DOS','venue':'bitget','pair':'DOSUSDT'}]}

def sample_good_market(identity='VERIFIED_SAME_CONTRACT_NETWORK'):
    s=VenueState('ON','gate','ON_USDT',identity)
    s.tape.connected_at(T-350_000)
    s.book.snapshot([['99','100']],[['100','20'],['101','10'],['103','20']],100,T-320_000)
    s.book.update([], [['100','6'],['101','5']],101,101,T-5_000,'gate')
    s.tape.add('1000',T-10_000,'buy','100','30')
    s.tape.add('1001',T-9_000,'sell','100','1')
    return s.evaluate(T)

class CoreTests(unittest.TestCase):
    def test_positive_live_buy_and_ask_drop(self):
        result=sample_good_market();self.assertTrue(result['evidence_ready']);self.assertFalse(result['blockers'])
    def test_symbol_collision_blocks_positive(self):
        result=sample_good_market('SYMBOL_ONLY');self.assertFalse(result['evidence_ready']);self.assertTrue(result['candidate'])
    def test_fixed_price_anchor(self):
        x=sample_good_market()['book']
        self.assertAlmostEqual(float(x['ask_before_quote']),3010.0)
        self.assertAlmostEqual(float(x['ask_after_quote']),1105.0)
        self.assertGreater(float(x['fraction_depleted']),.6)
    def test_gate_sequence_gap_blocks(self):
        o=L2();o.snapshot([['99','2']],[['100','2'],['103','1']],10,T-300000)
        with self.assertRaises(Gap):o.update([],[],13,14,T,'gate')
        self.assertFalse(o.synced)
    def test_gate_overlap_accepted(self):
        o=L2();o.snapshot([['99','2']],[['100','2'],['103','1']],10,T-300000)
        self.assertTrue(o.update([],[],10,12,T,'gate'));self.assertEqual(o.seq,12)
    def test_bitget_pseq_gap_blocks(self):
        o=L2();o.snapshot([['99','2']],[['100','2'],['103','1']],20,T-300000)
        with self.assertRaises(Gap):o.update([],[],18,21,T,'bitget')
    def test_orderbook_fails_when_outside_truncated(self):
        o=L2();o.snapshot([['99','2']],[['100','2'],['101','1']],10,T-300000)
        o.update([],[],11,11,T,'gate')
        self.assertEqual(o.compare(T)['status'],'TRUNCATED_2PCT_ASK_RANGE')
    def test_missing_trades_blocks(self):
        s=VenueState('ON','gate','ON_USDT','VERIFIED_SAME_CONTRACT_NETWORK');s.tape.connected_at(T-400000)
        s.book.snapshot([['99','20']],[['100','20'],['103','20']],1,T-350000)
        s.book.update([],[['100','6']],2,2,T,'gate')
        self.assertFalse(s.evaluate(T)['evidence_ready'])
    def test_tape_disconnection_blocks(self):
        t=Tape();t.connected_at(T-350000);t.add('1',T-10,'buy','10','400');t.disconnected(T-5,'CONNECT_LOSS')
        self.assertNotEqual(t.window(T)['status'],'LIVE_TRANSPORT_CONTINUOUS_NOT_EXCHANGE_AUDITED')
    def test_tape_numeric_id_gap_blocks(self):
        t=Tape();t.connected_at(T-350000);t.add('3',T-20,'buy','100','10',True)
        with self.assertRaises(Gap):t.add('5',T-10,'buy','100','10',True)
        self.assertFalse(t.connected)
    def test_denies_10x_forecast(self):
        market={'asof_ms':T,'markets':[sample_good_market()]}
        chain={'chains':{'bsc':{'status':'ERROR'}},'events':[]}
        d=evaluate(CONFIG,market,chain)
        self.assertEqual(len(d['coins']),15)
        self.assertIsNone(d['verified_predictive_10x_performance'])
        self.assertEqual(d['strong_buy_signals'],0)
        self.assertEqual(d['coins'][0]['state'],'INDEPENDENT_SPOT_IGNITION_OBSERVED')
    def test_inventory_tag_only_blocks(self):
        e={'coin':'ON','from':'0x'+'a'*40,'to':'0x'+'f'*40,'raw':'100','tx':'0x00','block_ms':T-400000}
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},'events':[e]}
        v=analyze(CONFIG,chain)['coins'][0]
        self.assertEqual(v['status'],'UNVERIFIED_EXCHANGE_MOVEMENTS')
        self.assertEqual(v['verified_retained_eoa_count'],0)
    def test_internal_cex_transfer_excluded(self):
        address='0x'+'a'*40;another='0x'+'b'*40
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},
               'events':[{'coin':'ON','from':address,'to':another,'raw':'500','tx':'tx1','block_ms':T-600000}]}
        entities={'bsc|'+address:{'entity':'Gate','proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC'},
                  'bsc|'+another:{'entity':'Gate','proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC'}}
        v=analyze(CONFIG,chain,entities)['coins'][0]
        self.assertEqual(v['verified_cex_external_net_raw'],'0')
        self.assertEqual(v['verified_retained_eoa_count'],0)
    def test_verified_retained_two_transfers(self):
        src='0x'+'a'*40;dst='0x'+'f'*40
        events=[{'coin':'ON','from':src,'to':dst,'raw':'500','tx':f'tx{i}','block_ms':T-480000+i*60000} for i in range(2)]
        ch={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},'events':events}
        entities={'bsc|'+src:{'entity':'Gate','proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC'}}
        codes={'bsc|'+dst:'EOA_ETH_GETCODE_EMPTY_AT_CONFIRMED_BLOCK'}
        balance={'ON|'+dst:{'reconciled':True,'two_distinct_finalized_block_snapshots':True,'source_balance_reconciled':True,'recipient_baseline_raw':'0','source_net_decrease_raw':{'0x'+'a'*40:'1000'},'retained_raw':'1000'}}
        v=analyze(CONFIG,ch,entities,codes,balance)['coins'][0]
        self.assertEqual(v['status'],'VERIFIED_RETENTION_PREPARATION')
        self.assertEqual(v['verified_cex_external_net_raw'],'1000')
        result=evaluate(CONFIG,{'asof_ms':T,'markets':[sample_good_market()]},ch,entities,codes,balance)
        self.assertEqual(result['coins'][0]['state'],'COMBINED_PREP_THEN_SPOT_OBSERVED')
        self.assertTrue(result['coins'][0]['temporal_sequence_proved'])
    def test_timestamp_reverse_blocks_combination(self):
        src='0x'+'a'*40;dst='0x'+'f'*40
        events=[{'coin':'ON','from':src,'to':dst,'raw':'500','tx':f'tx{i}','block_ms':T+i} for i in range(2)]
        ch={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},'events':events}
        entities={'bsc|'+src:{'entity':'Gate','proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC'}}
        codes={'bsc|'+dst:'EOA_ETH_GETCODE_EMPTY_AT_CONFIRMED_BLOCK'}
        balance={'ON|'+dst:{'reconciled':True,'two_distinct_finalized_block_snapshots':True,'source_balance_reconciled':True,'recipient_baseline_raw':'0','source_net_decrease_raw':{'0x'+'a'*40:'1000'},'retained_raw':'1000'}}
        result=evaluate(CONFIG,{'asof_ms':T,'markets':[sample_good_market()]},ch,entities,codes,balance)
        self.assertEqual(result['coins'][0]['state'],'INDEPENDENT_SPOT_IGNITION_OBSERVED')
    def test_v3_ask_band_returns_token_quantity_not_only_quote(self):
        # At parity, buying token0 or token1 must consume a positive and
        # finite target amount before a +2% price impact.
        for is_token0 in (True,False):
            quote=v3_raw_ask_quote(2**96,1000000,{},is_token0)
            token=v3_raw_ask_token_quantity(2**96,1000000,{},is_token0)
            self.assertGreater(quote,0)
            self.assertGreater(token,0)
            self.assertLess(float(token),30000)

    def test_v3_direction_both(self):
        a=v3_raw_ask_quote(2**96,1000000,{},True)
        b=v3_raw_ask_quote(2**96,1000000,{},False)
        self.assertGreater(a,0);self.assertGreater(b,0)
    def test_conflicting_trade_id_invalidates_tape(self):
        t=Tape();t.connected_at(T-90_000)
        t.add('same',T-10_000,'buy','100','2')
        with self.assertRaises(Gap):t.add('same',T-10_000,'sell','100','2')
        self.assertFalse(t.connected)
    def test_identical_trade_id_deduplicates(self):
        t=Tape();t.connected_at(T-90_000)
        t.add('same',T-10_000,'buy','100','2')
        t.add('same',T-10_000,'buy','100','2')
        self.assertEqual(len(t.recent),1)
    def test_early_90s_proof_not_overwritten_by_5m(self):
        s=VenueState('ON','gate','ON_USDT','VERIFIED_SAME_CONTRACT_NETWORK')
        s.tape.connected_at(T-130_000)
        s.book.snapshot([['99','200']],[['100','20'],['101','10'],['103','20']],100,T-110_000)
        s.book.update([],[['100','6'],['101','5']],101,101,T-5_000,'gate')
        s.tape.add('a',T-10_000,'buy','100','30')
        s.tape.add('b',T-9_000,'sell','100','1')
        m=s.evaluate(T)
        self.assertTrue(m['early_ready']);self.assertFalse(m['confirmed_5m_ready'])
        report=evaluate(CONFIG,{'asof_ms':T,'markets':[m]}, {'chains':{},'events':[]})
        proof=report['coins'][0]['market_evidence'][0]
        self.assertEqual(proof['phase'],'early_90s')
        self.assertEqual(proof['buy_quote'],'3000')
    def test_no_spot_state_when_unverified_symbol_even_when_book_depletes(self):
        m=sample_good_market('UNVERIFIED_SYMBOL_MATCH')
        x=evaluate(CONFIG,{'asof_ms':T,'markets':[m]},{'chains':{},'events':[]})
        self.assertFalse(x['coins'][0]['first_spot_pressure_observed'])
        self.assertEqual(x['coins'][0]['state'],'SPOT_MARKET_IDENTITY_REVIEW_URGENT')
    def test_gate_seed_replay(self):
        c=StreamCapture(CONFIG)
        s=c._get('gate','ON_USDT');s.tape.connected_at(T-300000)
        c.gate_message({'channel':'spot.order_book_update','event':'update','result':{'s':'ON_USDT','U':101,'u':101,'t':T,
                    'a':[['100','2']], 'b':[]}})
        c.seed_gate_book('ON_USDT',{'id':100,'bids':[['99','1']],'asks':[['100','5'],['103','1']]})
        self.assertTrue(s.book.synced);self.assertEqual(s.book.seq,101)
        self.assertEqual(s.book.asks[Decimal('100')],2)
    def test_bitget_parse_snapshot_and_trade(self):
        c=StreamCapture(CONFIG);s=c._get('bitget','DOSUSDT');s.tape.connected_at(T-400000)
        c.bitget_message({'arg':{'instType':'spot','topic':'books50','symbol':'DOSUSDT'},'action':'snapshot',
                          'data':[{'a':[['100','1'],['103','2']],'b':[['99','5']],'seq':30,'pseq':0,'ts':T-300000}]})
        c.bitget_message({'arg':{'instType':'spot','topic':'books50','symbol':'DOSUSDT'},'action':'update',
                          'data':[{'a':[['100','0.5']],'b':[],'seq':31,'pseq':30,'ts':T}]})
        c.bitget_message({'arg':{'instType':'spot','topic':'publicTrade','symbol':'DOSUSDT'},'action':'update',
                          'data':[{'i':'tt1','T':T-1000,'S':'buy','p':'100','v':'1'}]})
        self.assertEqual(s.book.seq,31);self.assertEqual(s.tape.recent[-1][1],'buy')
        self.assertEqual(len(s.book.history),2)

if __name__=='__main__':unittest.main()
