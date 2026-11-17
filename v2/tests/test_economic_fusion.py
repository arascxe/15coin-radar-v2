import copy,json,pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from economic_fusion import build,compare_spot
def item(coin,venue,buy,sell,depth,n=10):
    return {'coin':coin,'venue':venue,'trade_status':'RECENT_INCOMPLETE_SAMPLE',
      'trade':{'last_3min_observed':{'count':n,'buy_quote':str(buy),'sell_quote':str(sell)}},
      'book_status':'ONE_INSTANTANEOUS_BOOK_SNAPSHOT',
      'book':{'ask_quote_notional_within_2pct':str(depth)}}
def setup():
    now=1800000000000
    s={'schema':'radar-v2-public-spot-microstructure-1','run_id':'test-r','asof_ms':now,'data':[item('ON','gate',800,100,200),item('ON','mexc',900,100,300)]}
    old=copy.deepcopy(s);old['run_id']='test-p';old['asof_ms']=now-3600000
    old['data'][0]['book']['ask_quote_notional_within_2pct']='1000'
    old['data'][1]['book']['ask_quote_notional_within_2pct']='800'
    b={'schema':'radar-v2-economic-brain-1','last_run_id':'test-r','coins':[
        {'coin':'ON','chain_currently_complete':True,'wallet_accumulation_candidates':[]},
        {'coin':'DOS','chain_currently_complete':True,'wallet_accumulation_candidates':[{'address':'x'}]}]}
    return s,old,b
class Fusion(unittest.TestCase):
    def test_independent_spot_path_without_wallet_preparation(self):
        s,old,b=setup();r=build(s,b,old)
        on=r['coins'][0]
        self.assertTrue(on['ignition_path']['independent_research_trigger'])
        self.assertEqual(on['preparation_path']['status'],'NOT_CORROBORATED_UNKNOWN')
        self.assertFalse(on['strong_alert'])
        self.assertEqual(r['coin_count'],2)
    def test_preparation_path_without_spot(self):
        s,old,b=setup();s['data']=[]
        r=build(s,b,None)
        self.assertEqual(r['coins'][1]['classification'],'PREPARATION_RESEARCH_WATCH_OWNER_UNVERIFIED')
        self.assertFalse(r['coins'][1]['ignition_path']['independent_research_trigger'])
    def test_stale_or_few_trades_not_claimed_buy(self):
        s,old,b=setup()
        s['data'][0]['trade_status']='STALE_SAMPLE'
        s['data'][1]['trade']['last_3min_observed']['count']=1
        r=build(s,b,old)
        self.assertFalse(r['coins'][0]['ignition_path']['independent_research_trigger'])
    def test_one_venue_only_without_book_comparison_not_flag(self):
        s,old,b=setup()
        s['data']=s['data'][:1]
        r=build(s,b,None)
        self.assertFalse(r['coins'][0]['ignition_path']['independent_research_trigger'])
    def test_no_lookahead_and_bad_interval(self):
        s,old,b=setup();old['asof_ms']=s['asof_ms']+3600000
        r=build(s,b,old)
        self.assertEqual(r['coins'][0]['market_evidence'][0]['ask_depth_change'],'UNKNOWN')
    def test_run_mismatch_fail_closed(self):
        s,old,b=setup();b['last_run_id']='x'
        with self.assertRaises(ValueError):build(s,b,old)
    def test_no_trades_not_equivalent_sell(self):
        s,old,b=setup();s['data'][0]['trade_status']='ERROR_UNVERIFIED'
        s['data'][1]['trade_status']='STALE_SAMPLE'
        v=build(s,b,old)['coins'][0]
        self.assertTrue(all(x['trades_recent']=='UNKNOWN' for x in v['market_evidence']))
        self.assertFalse(v['ignition_path']['independent_research_trigger'])
    def test_no_forced_all_three_components(self):
        s,old,b=setup()
        r=build(s,b,old)
        self.assertTrue(r['coins'][0]['ignition_path']['independent_research_trigger'])
        self.assertFalse(r['coins'][0]['preparation_path']['wallet_candidate_count'])
if __name__=='__main__':unittest.main()
