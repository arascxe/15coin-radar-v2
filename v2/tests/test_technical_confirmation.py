import pathlib,sys,unittest,math,sqlite3
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from technical_confirmation import evaluate,MINUTE,cross_market_dispersion
END=1800000000000
def bars():
    base=END-120*MINUTE
    return [(base+i*MINUTE,1+i*.0001,1.01+i*.0001,.99+i*.0001,1+i*.0001,10+i) for i in range(120)]
class TechnicalTests(unittest.TestCase):
    def test_complete_candles_and_consistent_features(self):
        d=evaluate(bars(),END)
        self.assertEqual(d['status'],'COMPLETE_120_CONSECUTIVE_1M_BARS')
        self.assertTrue(d['higher_lows'])
        self.assertGreater(d['rvol_15m'],1)
        self.assertGreater(d['rvol_1h'],1)
        self.assertIsNotNone(d['volume_price_response_pct_per_1000_quote'])
    def test_gap_not_filled_with_zero(self):
        x=bars();x.pop(50)
        d=evaluate(x,END)
        self.assertEqual(d['status'],'INSUFFICIENT_CONTIGUOUS_MINUTE_COVERAGE')
        self.assertIsNone(d['rvol_15m'])
        self.assertIsNone(d['higher_lows'])
    def test_duplicate_minute_does_not_hide_gap(self):
        x=bars();x[55]=x[54]
        self.assertEqual(evaluate(x,END)['status'],'INSUFFICIENT_CONTIGUOUS_MINUTE_COVERAGE')
    def test_invalid_negative_or_inf_quote_volume(self):
        x=bars();x[-1]=(*x[-1][:5],float('inf'))
        self.assertEqual(evaluate(x,END)['status'],'INVALID_CANDLE_VALUES_NO_FEATURES')
    def test_zero_baseline_is_unknown_not_infinite_rvol(self):
        x=bars();x=[(*r[:5],0 if i<105 else 1) for i,r in enumerate(x)]
        d=evaluate(x,END)
        self.assertIsNone(d['rvol_15m'])
        self.assertIsNone(d['rvol_1h'])
    def test_cross_venue_only_same_minute(self):
        c=sqlite3.connect(':memory:')
        c.execute("CREATE TABLE market_candles(coin text,venue text,minute_ms integer,close real)")
        c.executemany("INSERT INTO market_candles VALUES(?,?,?,?)",[
            ('ON','gate',END,1.),('ON','mexc',END-60000,3.)])
        cfg={'venues':[{'coin':'ON','venue':'gate','pair':'ON_USDT'},
                       {'coin':'ON','venue':'mexc','pair':'ONUSDT'}]}
        self.assertEqual(cross_market_dispersion(c,'ON',cfg)['status'],'NO_MATCHED_MINUTE_WITHIN_LAST_4M')
        c.execute("INSERT INTO market_candles VALUES(?,?,?,?)",('ON','mexc',END,1.1))
        got=cross_market_dispersion(c,'ON',cfg)
        self.assertEqual(got['status'],'MATCHED_MINUTE_RESEARCH_ONLY')
        self.assertAlmostEqual(got['max_min_close_difference_pct'],10.)
        c.close()
    def test_high_price_low_violation_rejected(self):
        x=bars();x[-1]=(x[-1][0],1,0.2,0.3,2,10)
        self.assertEqual(evaluate(x,END)['status'],'INVALID_CANDLE_VALUES_NO_FEATURES')
if __name__=='__main__':unittest.main()
