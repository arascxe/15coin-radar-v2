import copy,pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from decision_engine import build,EXPECTED
from economic_fusion import build as fusion_build
def setup():
    now=1800000000000
    records=[{'coin':sym,'chain_currently_complete':True,
              'wallet_accumulation_candidates':[]} for sym in EXPECTED]
    economic={'schema':'radar-v2-economic-brain-1','last_run_id':'TEST',
        'coins':records}
    def venue(coin,name,buy,sell,events=12):
        return {'coin':coin,'venue':name,'trade_status':'RECENT_INCOMPLETE_SAMPLE',
            'trade':{'last_3min_observed':{'count':events,'buy_quote':str(buy),
                'sell_quote':str(sell),'observed_quote_notional':str(buy+sell),
                'buy_side_fraction':str(buy/(buy+sell) if (buy+sell) else 0)},
                'trade_side_semantics':'SAMPLE_NOT_FULL_TAPE'},
            'book_status':'ONE_INSTANTANEOUS_BOOK_SNAPSHOT',
            'book':{'ask_quote_notional_within_2pct':'1000'}}
    spot={'schema':'radar-v2-public-spot-microstructure-1','run_id':'TEST',
          'asof_ms':now,'data':[venue('ON','gate',1000,100),venue('ON','mexc',900,100),
             venue('DOS','gate',200,100)]}
    tech={'schema':'radar-v2-technical-confirmation-1',
          'coins':[{'coin':sym,'markets':[]} for sym in EXPECTED]}
    fusion=fusion_build(spot,economic,None,tech)
    return economic,spot,tech,fusion
class DecisionTests(unittest.TestCase):
    def test_spot_early_path_does_not_require_wallet_or_rvol(self):
        e,s,t,f=setup();out=build(f,s,t)
        on=out['coins'][0]
        self.assertEqual(on['state'],'SPOT_IGNITION_RESEARCH_WATCH')
        self.assertFalse(on['pathways']['preparation']['active_unverified'])
        self.assertFalse(on['automatic_delivery_ready'])
        self.assertFalse(on['strong_alert'])
    def test_preparation_independent_even_without_spot(self):
        e,s,t,f=setup();f['coins'][1]['preparation_path']['wallet_candidate_count']=1
        f['coins'][1]['preparation_path']['chain_currently_complete']=True
        r=build(f,s,t)['coins'][1]
        self.assertEqual(r['state'],'PREPARATION_RESEARCH_WATCH')
    def test_combined_is_not_trade_signal(self):
        e,s,t,f=setup();f['coins'][0]['preparation_path']['wallet_candidate_count']=1
        out=build(f,s,t)['coins'][0]
        self.assertEqual(out['state'],'MULTI_PATH_RESEARCH_WATCH')
        self.assertFalse(out['buy_signal'])
    def test_rvol_cannot_invent_signal(self):
        e,s,t,f=setup();s['data']=[];f=fusion_build(s,e,None,t)
        t['coins'][0]['markets']=[{'venue':'gate','latest_minute_ms':s['asof_ms']-60000,
            'features':{'status':'COMPLETE_120_CONSECUTIVE_1M_BARS','rvol_15m':35,'higher_lows':True}}]
        out=build(f,s,t)['coins'][0]
        self.assertEqual(out['state'],'INSUFFICIENT_EVIDENCE_UNKNOWN')
    def test_low_n_and_notional_ignored(self):
        e,s,t,f=setup();s['data'][0]['trade']['last_3min_observed']['count']=1
        s['data'][1]['trade']['last_3min_observed']['count']=1
        out=build(f,s,t)['coins'][0]
        self.assertFalse(out['watch_research_only'])
    def test_missing_source_is_unknown_not_sell(self):
        e,s,t,f=setup();s['data'][0]['trade_status']='ERROR_UNVERIFIED'
        s['data'][1]['trade_status']='STALE_SAMPLE'
        out=build(f,s,t)['coins'][0]
        self.assertEqual(out['state'],'INSUFFICIENT_EVIDENCE_UNKNOWN')
        self.assertIn('SOME_SPOT_TRADE_SAMPLES_MISSING_OR_FAILED',out['evidence_limitations'])
    def test_no_future_or_stale_tech_as_evidence(self):
        e,s,t,f=setup()
        t['coins'][0]['markets']=[{'venue':'gate','latest_minute_ms':s['asof_ms']+60000,
              'features':{'status':'COMPLETE_120_CONSECUTIVE_1M_BARS','rvol_15m':88}},
              {'venue':'mexc','latest_minute_ms':s['asof_ms']-16*60000,
              'features':{'status':'COMPLETE_120_CONSECUTIVE_1M_BARS','rvol_15m':99}}]
        self.assertEqual(build(f,s,t)['coins'][0]['optional_technical_context'],[])
    def test_bad_schema_or_missing_coin_fails(self):
        e,s,t,f=setup();f['coins'].pop()
        with self.assertRaises(ValueError):build(f,s,t)
    def test_small_two_venue_samples_are_weak_not_watch(self):
        e,s,t,f=setup()
        for m in s['data'][:2]:
            m['trade']['last_3min_observed']['count']=6
            m['trade']['last_3min_observed']['observed_quote_notional']='120'
        r=build(f,s,t)['coins'][0]
        self.assertEqual(r['state'],'INSUFFICIENT_EVIDENCE_UNKNOWN')
        self.assertTrue(r['pathways']['spot_ignition']['weak_sample_research_lead'])
        self.assertFalse(r['strong_alert'])
    def test_single_tiny_sample_plus_thinning_is_weak(self):
        e,s,t,f=setup()
        s['data']=s['data'][:1]
        f['coins'][0]['market_evidence']=[{'venue':'gate','ask_depth_change':'ASK_THINNER_TWO_SNAPSHOTS','ask_depth_change_pct':-42.0}]
        r=build(f,s,t)['coins'][0]
        self.assertFalse(r['watch_research_only'])
        self.assertTrue(r['pathways']['spot_ignition']['weak_sample_research_lead'])
    def test_strong_single_venue_research_does_not_require_wallet_or_rvol(self):
        e,s,t,f=setup()
        s['data']=s['data'][:1]
        m=s['data'][0];m['trade']['last_3min_observed']['count']=28
        f['coins'][0]['market_evidence']=[{'venue':'gate','ask_depth_change':'ASK_THINNER_TWO_SNAPSHOTS','ask_depth_change_pct':-35.0}]
        r=build(f,s,t)['coins'][0]
        self.assertEqual(r['state'],'SPOT_IGNITION_RESEARCH_WATCH')
        self.assertFalse(r['strong_alert'])
    def test_two_medium_venues_need_aggregate_notional(self):
        e,s,t,f=setup()
        s['data'][0]['trade']['last_3min_observed']['observed_quote_notional']='350'
        s['data'][1]['trade']['last_3min_observed']['observed_quote_notional']='350'
        r=build(f,s,t)['coins'][0]
        self.assertEqual(r['state'],'INSUFFICIENT_EVIDENCE_UNKNOWN')
        self.assertTrue(r['pathways']['spot_ignition']['weak_sample_research_lead'])
    def test_research_dedupe_stable_for_same_conditions(self):
        e,s,t,f=setup();a=build(f,s,t)
        b=build(f,s,t)
        self.assertEqual(a['coins'][0]['research_dedupe_key'],b['coins'][0]['research_dedupe_key'])
if __name__=='__main__':unittest.main()
