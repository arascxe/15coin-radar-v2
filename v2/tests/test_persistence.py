"""Fail-closed independent tests for Radar V2 data persistence (synthetic data only)."""
import pathlib,sys,tempfile,unittest,json
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store,validate_config,MINUTE_MS
from backup_restore import create_backup,restore_backup

ROOT=pathlib.Path(__file__).resolve().parents[1]
T=1800000000000
BAR={'minute_ms':T,'open':1.0,'high':1.1,'low':0.9,'close':1.0,'quote_vol':100.0}

class TestPersistence(unittest.TestCase):
    def test_exact_config_coverage(self):
        c=json.loads((ROOT/'config.json').read_text())
        self.assertTrue(validate_config(c))
        self.assertEqual(len(c['coins']),15)
        self.assertEqual(len(c['venues']),60)
        self.assertEqual(len(c['wallets']),239)
        self.assertEqual(len(c['dex_pools']),15)
        self.assertTrue(all('pool' in x for x in c['dex_pools']))
        self.assertTrue(all(str(x['pool']).startswith('0x') and len(str(x['pool'])) in (42,66) for x in c['dex_pools']))
        self.assertEqual(len([x for x in c['dex_pools'] if len(x['pool'])==66]),4)

    def test_config_rejects_missing_coin(self):
        c=json.loads((ROOT/'config.json').read_text());c['coins'].pop()
        with self.assertRaises(ValueError):validate_config(c)

    def test_error_does_not_advance_market_cursor(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db')
            r=st.begin_run(T)
            st.market_save(r,'ON','gate','ON_USDT','ERROR',T,T+MINUTE_MS,[],observed_ms=T)
            self.assertIsNone(st.last_market_end('ON','gate'))
            self.assertEqual(st.market_initial_start('ON','gate'),T)
            st.close()

    def test_sparse_minutes_kept_unknown_and_hour_progresses(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db')
            r=st.begin_run(T)
            st.market_save(r,'LIGHT','gate','LIGHT_USDT','PARTIAL',T,T+2*MINUTE_MS,[BAR],observed_ms=T)
            self.assertEqual(st.last_market_end('LIGHT','gate'),T+2*MINUTE_MS)
            self.assertEqual(st.db.execute('select count(*) from market_missing_minutes').fetchone()[0],1)
            r2=st.begin_run(T+3*MINUTE_MS)
            st.market_save(r2,'LIGHT','gate','LIGHT_USDT','OK',T+2*MINUTE_MS,T+3*MINUTE_MS,[{**BAR,'minute_ms':T+2*MINUTE_MS}],observed_ms=T+3*MINUTE_MS)
            self.assertEqual(st.db.execute('select count(*) from market_missing_minutes').fetchone()[0],1)
            st.close()

    def test_backfilled_missing_minute_is_resolved(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db')
            st.market_save(st.begin_run(T),'LIGHT','gate','LIGHT_USDT','PARTIAL',T,T+2*MINUTE_MS,[BAR],observed_ms=T)
            st.market_save(st.begin_run(T+2*MINUTE_MS),'LIGHT','gate','LIGHT_USDT','OK',T+MINUTE_MS,T+2*MINUTE_MS,[{**BAR,'minute_ms':T+MINUTE_MS}],observed_ms=T+2*MINUTE_MS)
            self.assertEqual(st.db.execute('select count(*) from market_missing_minutes').fetchone()[0],0)
            st.close()

    def test_duplicate_transfer_is_deduplicated(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db')
            x={'token':'0x'+'1'*40,'coin':'ON','tx':'0x'+'2'*64,'log_index':0,'block':100,'from':'0x'+'3'*40,'to':'0x'+'4'*40,'raw':'1000'}
            st.chain_save(st.begin_run(T),'bsc',100,100,100,[x,x],'OK',observed_ms=T)
            self.assertEqual(st.db.execute('select count(*) from token_transfers').fetchone()[0],1)
            st.close()

    def test_run_lease_blocks_concurrent_collector(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db');owner=st.acquire_lease()
            with self.assertRaises(RuntimeError):st.acquire_lease()
            st.release_lease(owner);st.close()

    def test_separate_runner_restore_preserves_two_runs(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);src=root/'runner1'/'state.db';st=Store(src)
            r=st.begin_run(T);st.market_save(r,'ON','gate','ON_USDT','OK',T,T+MINUTE_MS,[BAR],observed_ms=T);st.finish_run(r,T);st.close()
            archive=root/'persist'/'checkpoint.gz';create_backup(src,archive)
            new=root/'runner2'/'state.db';res=restore_backup(archive,new)
            self.assertTrue(res['checksum_verified']);st2=Store(new)
            r2=st2.begin_run(T+MINUTE_MS);st2.market_save(r2,'ON','gate','ON_USDT','OK',T+MINUTE_MS,T+2*MINUTE_MS,[{**BAR,'minute_ms':T+MINUTE_MS}],observed_ms=T+MINUTE_MS)
            self.assertEqual(st2.db.execute('select count(*) from runs').fetchone()[0],2)
            self.assertEqual(st2.db.execute('select count(*) from market_candles').fetchone()[0],2)
            st2.close()

    def test_corrupt_archive_is_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d);db=root/'db';st=Store(db);st.close();archive=root/'state.gz';create_backup(db,archive)
            archive.write_bytes(archive.read_bytes()+b'bad')
            with self.assertRaises(ValueError):restore_backup(archive,root/'new-db')
            self.assertFalse((root/'new-db').exists())


    def test_source_failure_never_deletes_existing_market_rows(self):
        with tempfile.TemporaryDirectory() as d:
            st=Store(pathlib.Path(d)/'data.db')
            first=st.begin_run(T)
            st.market_save(first,'ON','gate','ON_USDT','OK',T,T+MINUTE_MS,[BAR],observed_ms=T)
            st.finish_run(first,T)
            second=st.begin_run(T+MINUTE_MS)
            st.market_save(second,'ON','gate','ON_USDT','ERROR',T+MINUTE_MS,T+2*MINUTE_MS,[],detail='HTTP_503',observed_ms=T+MINUTE_MS)
            st.finish_run(second,T+MINUTE_MS)
            self.assertEqual(st.db.execute('select count(*) from market_candles').fetchone()[0],1)
            self.assertEqual(st.db.execute('select count(*) from runs').fetchone()[0],2)
            self.assertEqual(st.last_market_end('ON','gate'),T+MINUTE_MS)
            self.assertEqual(st.db.execute('select count(*) from source_gaps').fetchone()[0],1)
            st.close()

if __name__=='__main__':unittest.main()
