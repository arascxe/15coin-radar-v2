"""Fail-closed public spot microstructure tests; synthetic HTTP only."""
import json,pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from spot_microstructure import trades,book,sample
CONF=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
NOW=1800000000000
class FakeHTTP:
    def __init__(self,broken=False):self.broken=broken
    def get(self,url,params):
        if self.broken:raise RuntimeError('NETWORK_DOWN')
        if '/api/v3/market/fills' in url:
            return {'code':'00000','data':[{'execId':'1','ts':str(NOW-30000),'price':'0.50','size':'10','side':'buy'},
                                           {'execId':'2','ts':str(NOW-29000),'price':'0.50','size':'10','side':'sell'}]}
        if '/api/v3/market/orderbook' in url:
            return {'code':'00000','data':{'ts':str(NOW),'a':[['0.51','10'],['0.52','20']],
                                                 'b':[['0.49','10'],['0.48','20']]}}
        if '/api/v3/trades' in url:
            return [{'id':None,'time':NOW-29000,'price':'0.5','qty':'10','isBuyerMaker':False},
                    {'id':None,'time':NOW-20000,'price':'0.5','qty':'10','isBuyerMaker':True}]
        if '/api/v3/depth' in url:
            return {'asks':[['0.51','10']],'bids':[['0.49','10']]}
        if '/api/v4/spot/trades' in url:
            return [{'id':'a','create_time_ms':str(NOW-30000),'price':'0.5','amount':'10','side':'buy'}]
        if '/api/v4/spot/order_book' in url:
            return {'current':NOW,'asks':[['0.51','10']],'bids':[['0.49','10']]}
        raise AssertionError(url)
class MicroTests(unittest.TestCase):
    def test_bitget_side_and_quote_notional_correct(self):
        a=trades(FakeHTTP().get('https://api.bitget.com/api/v3/market/fills',{}),'bitget',NOW)
        self.assertEqual(a['last_3min_observed']['buy_quote'],'5')
        self.assertEqual(a['last_3min_observed']['sell_quote'],'5')
        self.assertEqual(a['last_3min_observed']['buy_side_fraction'],'0.5')
        self.assertEqual(a['status'],'RECENT_INCOMPLETE_SAMPLE')
    def test_duplicate_trades_count_once(self):
        row={'execId':'a','ts':str(NOW),'price':'1','size':'2','side':'buy'}
        a=trades({'code':'00000','data':[row,row]},'bitget',NOW)
        self.assertEqual(a['last_N_trades_observed']['count'],1)
    def test_stale_not_called_no_buy(self):
        a=trades({'code':'00000','data':[{'execId':'x','ts':str(NOW-800000),
              'side':'buy','price':'10','size':'100'}]},'bitget',NOW)
        self.assertEqual(a['status'],'STALE_SAMPLE')
        self.assertEqual(a['last_3min_observed']['count'],0)
        self.assertIsNone(a['last_3min_observed']['buy_side_fraction'])
    def test_unknown_side_is_error_not_guessed(self):
        with self.assertRaises(ValueError):
            trades({'code':'00000','data':[{'execId':'a','ts':str(NOW),'side':'maker',
                'price':'1','size':'2'}]},'bitget',NOW)
    def test_ask_depth_is_one_snapshot_only(self):
        a=book(FakeHTTP().get('https://api.bitget.com/api/v3/market/orderbook',{}),'bitget',NOW)
        self.assertEqual(a['status'],'ONE_INSTANTANEOUS_BOOK_SNAPSHOT')
        self.assertEqual(a['best_ask'],'0.51')
        self.assertNotIn('liquidity_withdrawal_confirmed',a)
    def test_mexc_buyer_maker_direction_with_missing_ids(self):
        a=trades(FakeHTTP().get('https://api.mexc.com/api/v3/trades',{}),'mexc',NOW)
        self.assertEqual(a['last_3min_observed']['buy_quote'],'5')
        self.assertEqual(a['last_3min_observed']['sell_quote'],'5')
        self.assertFalse(a['deduplication_possible'])
        self.assertIn('MEXC_BUYER_MAKER_FLAG',a['trade_side_semantics'])
    def test_crossed_book_is_rejected(self):
        with self.assertRaises(ValueError):
            book({'asks':[['0.4','1']],'bids':[['0.5','1']]},'gate',NOW)
    def test_all_34_markets_cover_15_coins_without_alarm(self):
        a=sample(CONF,FakeHTTP(),'r',asof_ms=NOW)
        self.assertEqual(a['venues_sampled'],34)
        self.assertEqual(a['coins_with_public_market'],15)
        self.assertEqual(a['verified_early_ignition_alerts'],0)
        self.assertTrue(all(not z['market_purchase_early_alert'] for z in a['data']))
        self.assertTrue(all(z['trade_status']=='RECENT_INCOMPLETE_SAMPLE' for z in a['data']))
    def test_network_failure_returns_error_not_false_zero(self):
        a=sample(CONF,FakeHTTP(broken=True),'r',asof_ms=NOW)
        self.assertEqual(a['verified_early_ignition_alerts'],0)
        self.assertTrue(all(z['trade_status']=='ERROR_UNVERIFIED' for z in a['data']))
        self.assertTrue(all('trade' not in z for z in a['data']))
if __name__=='__main__':unittest.main()
