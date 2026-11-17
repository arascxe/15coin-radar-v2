"""Deterministic proof of lossless immutable anchor + incremental segment replay."""
import gzip,json,pathlib,sqlite3,sys,tempfile,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from storage_segments import migrate,restore,append,semantic_digest
from backup_restore import create_backup
from radar_stage1 import Store,MINUTE_MS
T=1800000000000
def bar(m):return {'minute_ms':m,'open':1.,'high':2.,'low':.5,'close':1.,'quote_vol':300.}
class SegmentTests(unittest.TestCase):
    def test_migrate_append_replay_exactly_and_tamper_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);original=root/'original.sqlite';st=Store(original)
            rid=st.begin_run(T);st.market_save(rid,'ON','gate','ON_USDT','PARTIAL',T,T+2*MINUTE_MS,[bar(T)],observed_ms=T)
            st.finish_run(rid,T);st.close()
            archive=root/'old.sqlite.gz';create_backup(original,archive)
            store=root/'storage';migrate(archive,store)
            restored=root/'replayed.sqlite';restore(store,restored)
            self.assertEqual(semantic_digest(original),semantic_digest(restored))
            st=Store(original);rid=st.begin_run(T+3*MINUTE_MS)
            st.market_save(rid,'ON','gate','ON_USDT','OK',T+MINUTE_MS,T+2*MINUTE_MS,[bar(T+MINUTE_MS)],observed_ms=T+3*MINUTE_MS)
            st.finish_run(rid,T+3*MINUTE_MS);st.close()
            info=append(store,restored,original)
            self.assertGreater(info['bytes'],0)
            self.assertEqual(len(json.loads((store/'manifest.json').read_text())['segments']),1)
            second=root/'fresh-runner.sqlite';restore(store,second)
            self.assertEqual(semantic_digest(second),semantic_digest(original))
            c=sqlite3.connect(second)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM runs').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM market_missing_minutes').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0],2)
            c.close()
            segment=store/'segments'/'0000001.json.gz'
            segment.write_bytes(segment.read_bytes()+b'corrupt')
            with self.assertRaises(RuntimeError):restore(store,root/'bad.sqlite')
    def test_schema_change_rejected_instead_of_dropping(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);a=root/'a.db';st=Store(a);st.close()
            old=root/'old.gz';create_backup(a,old);folder=root/'storage';migrate(old,folder)
            b=root/'b.db';restore(folder,b)
            c=sqlite3.connect(b);c.execute('CREATE TABLE extra(id TEXT PRIMARY KEY)');c.commit();c.close()
            with self.assertRaises(RuntimeError):append(folder,a,b)
if __name__=='__main__':unittest.main()
