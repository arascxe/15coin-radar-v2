import gzip,json,pathlib,sqlite3,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from prospective_validation import future_outcome,choose_price_venue,review,summarise,read_archive
from decision_engine import EXPECTED
M=60000
NOW=1800000000000
def db():
    x=sqlite3.connect(':memory:')
    x.execute("CREATE TABLE market_candles(coin TEXT,venue TEXT,minute_ms INTEGER,close REAL,high REAL,low REAL)")
    for i in range(-6,65):
        t=(NOW//M)*M+i*M
        x.execute('INSERT INTO market_candles VALUES(?,?,?,?,?,?)',('ON','gate',t,1.,1.01,0.99))
    x.commit()
    return x
def report(run,ts):
    return {'schema':'radar-v2-public-spot-microstructure-1','run_id':run,'asof_ms':ts,
        'data':[{'coin':'ON','venue':v,'trade_status':'RECENT_INCOMPLETE_SAMPLE',
          'trade':{'last_3min_observed':{'count':10,'buy_quote':'900','sell_quote':'100',
                'observed_quote_notional':'1000','buy_side_fraction':'0.9'}},
          'book_status':'ONE_INSTANTANEOUS_BOOK_SNAPSHOT',
          'book':{'ask_quote_notional_within_2pct':'1000'}} for v in ('gate','mexc')]}
class ForwardTests(unittest.TestCase):
    def test_future_only_after_snapshot_matured(self):
        c=db()
        x=future_outcome(c,'ON',NOW,15)
        self.assertEqual(x['status'],'MATURED_COMPLETE_FUTURE_BARS')
        self.assertLess(x['future_max_high_return_pct'],2)
        c.close()
    def test_known_synthetic_post_event_10x_is_detectable_only_as_outcome(self):
        c=db()
        c.execute("UPDATE market_candles SET high=11 WHERE coin='ON' AND minute_ms=?",(NOW+6*M,))
        future=future_outcome(c,'ON',NOW,15)
        self.assertEqual(future['status'],'MATURED_COMPLETE_FUTURE_BARS')
        self.assertGreaterEqual(future['future_max_high_return_pct'],900)
        c.close()
    def test_past_high_not_leaked_into_future_labels(self):
        c=db()
        c.execute("UPDATE market_candles SET high=11 WHERE coin='ON' AND minute_ms=?",(NOW-4*M,))
        future=future_outcome(c,'ON',NOW,15)
        self.assertLess(future['future_max_high_return_pct'],5)
        c.close()
    def test_missing_future_block_returns_unknown_not_negative(self):
        c=db();c.execute('DELETE FROM market_candles WHERE minute_ms=?',(NOW+5*M,))
        x=future_outcome(c,'ON',NOW,15)
        self.assertEqual(x['status'],'UNKNOWN_FUTURE_WINDOW_NOT_COMPLETE')
        c.close()
    def test_no_future_yet_doesnt_count_as_no_pump(self):
        c=db();x=future_outcome(c,'ON',NOW,240)
        self.assertEqual(x['status'],'UNKNOWN_FUTURE_WINDOW_NOT_COMPLETE')
        c.close()
    def test_incomplete_replay_is_not_statistical_false_positive_rate(self):
        c=db()
        q=[report('a',NOW-60*M),report('b',NOW)]
        rows=review(c,q)
        self.assertEqual(len(rows),15)
        s=summarise(rows,2,[])
        self.assertEqual(s['historical_false_alarm_probability_validated'],False)
        self.assertIsNone(s['horizon_metrics_exploratory_only'][0]['statistically_valid_precision'])
        c.close()
    def test_corrupted_archive_discarded_not_miscounted(self):
        with tempfile.TemporaryDirectory() as d:
            p=pathlib.Path(d)
            (p/'bad.json.gz').write_bytes(b'nonsense')
            (p/'a.json.gz').write_bytes(gzip.compress(json.dumps(report('a',NOW)).encode()))
            got,bad=read_archive(d)
            self.assertEqual(len(got),1)
            self.assertEqual(len(bad),1)
if __name__=='__main__':unittest.main()
