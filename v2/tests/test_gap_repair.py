"""No invented zero-volume candles, cursor rewinds, or overwritten live health."""
import pathlib
import sys
import tempfile
import unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store, MINUTE_MS
from gap_repair import repair_gaps

T=1800000000000
BAR={'minute_ms':T,'open':1.0,'high':1.0,'low':1.0,'close':1.0,'quote_vol':3.0}
CONFIG={'venues':[{'coin':'ON','venue':'gate','pair':'ON_USDT'}],'dex_pools':[]}

class HTTP:
    def __init__(self,fail=False,empty=False):self.fail=fail;self.empty=empty;self.calls=[]
    def get(self,url,params):
        self.calls.append((url,params))
        if self.fail:raise ConnectionError('HTTP_503')
        if self.empty:return []
        return [[str((T+i*MINUTE_MS)//1000),'3','1','1','1','1'] for i in (1,2)]

class GapRepairTests(unittest.TestCase):
    def setup_db(self,path):
        st=Store(path);rid=st.begin_run(T)
        st.market_save(rid,'ON','gate','ON_USDT','PARTIAL',
                       T,T+3*MINUTE_MS,[BAR],observed_ms=T)
        self.assertEqual(st.db.execute('SELECT COUNT(*) FROM market_missing_minutes').fetchone()[0],2)
        return st,rid

    def test_older_than_three_minute_overlap_is_restored_without_cursor_rewind(self):
        with tempfile.TemporaryDirectory() as d:
            st,rid=self.setup_db(pathlib.Path(d)/'db.sqlite')
            original_cursor=st.last_market_end('ON','gate')
            http=HTTP()
            result=repair_gaps(st,rid,CONFIG,http,ts=T+15*MINUTE_MS)
            self.assertEqual(result['resolved_this_run'],2)
            self.assertEqual(result['unresolved_spot_minutes'],0)
            self.assertEqual(st.last_market_end('ON','gate'),original_cursor)
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0],3)
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM market_checks').fetchone()[0],1)
            self.assertEqual(st.db.execute('SELECT status FROM spot_gap_repairs').fetchone()[0],'OK')
            self.assertEqual(len(http.calls),1)
            st.close()

    def test_http_error_keeps_missing_history_and_live_rows(self):
        with tempfile.TemporaryDirectory() as d:
            st,rid=self.setup_db(pathlib.Path(d)/'db.sqlite')
            result=repair_gaps(st,rid,CONFIG,HTTP(fail=True),ts=T+15*MINUTE_MS)
            self.assertEqual(result['unresolved_spot_minutes'],2)
            self.assertEqual(result['status_counts'],{'ERROR':1})
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0],1)
            self.assertEqual(st.last_market_end('ON','gate'),T+3*MINUTE_MS)
            st.close()

    def test_empty_response_is_not_fabricated_zero_trades(self):
        with tempfile.TemporaryDirectory() as d:
            st,rid=self.setup_db(pathlib.Path(d)/'db.sqlite')
            result=repair_gaps(st,rid,CONFIG,HTTP(empty=True),ts=T+15*MINUTE_MS)
            self.assertEqual(result['status_counts'],{'UNKNOWN':1})
            self.assertEqual(result['unresolved_spot_minutes'],2)
            self.assertEqual(st.db.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0],1)
            st.close()

if __name__=='__main__':unittest.main()
