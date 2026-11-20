"""Independent current chain ingestion must never skip old history or promote gaps."""
import pathlib, sqlite3, sys, unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from tip_ingest import ingest

class MemoryStore:
    def __init__(self):
        self.db=sqlite3.connect(':memory:')
        self.db.executescript("""
        CREATE TABLE token_transfers(chain TEXT,chain_id INT,token TEXT,coin TEXT,
          tx_hash TEXT,log_index INT,block INT,from_address TEXT,to_address TEXT,
          amount_raw TEXT,seen_ms INT,PRIMARY KEY(chain_id,tx_hash,log_index,token));
        CREATE TABLE chain_cursor(chain TEXT PRIMARY KEY,last_complete_block INTEGER);
        INSERT INTO chain_cursor VALUES('bsc',500);
        """)

CFG={'coins':[{'symbol':'ON','token':'0x'+'1'*40,'chain':'bsc'}]}
EVENT={'coin':'ON','token':'0x'+'1'*40,'tx':'0x'+'a'*64,'log_index':0,
       'block':1000,'from':'0x'+'2'*40,'to':'0x'+'3'*40,'raw':'15'}

class TipTests(unittest.TestCase):
    def test_independent_cursor(self):
        store=MemoryStore()
        def complete(http,url,start,stop,mapping,deadline,cb):
            self.assertEqual(start,951)
            cb(stop,[EVENT])
        with patch('tip_ingest.select_provider',return_value=('x',1000,'logs')), \
             patch('tip_ingest.scan_logs',side_effect=complete):
            a=ingest(store,'a',CFG,'bsc',http=object(),ts=10,initial_blocks=50)
            b=ingest(store,'b',CFG,'bsc',http=object(),ts=11)
        self.assertEqual(a['status'],'OK')
        self.assertEqual(b['status'],'OK')
        self.assertEqual(store.db.execute('SELECT last_complete_block FROM chain_cursor').fetchone()[0],500)
        self.assertEqual(store.db.execute('SELECT last_complete_block FROM chain_tip_cursor').fetchone()[0],1000)
        self.assertEqual(store.db.execute('SELECT COUNT(*) FROM token_transfers').fetchone()[0],1)

    def test_partial_scan_does_not_jump(self):
        store=MemoryStore()
        def break_scan(http,url,start,stop,mapping,deadline,cb):
            cb(start+10,[])
            raise ValueError('source unavailable')
        with patch('tip_ingest.select_provider',return_value=('x',1000,'logs')), \
             patch('tip_ingest.scan_logs',side_effect=break_scan), \
             patch('tip_ingest._probe_receipt',side_effect=RuntimeError('unsupported')):
            row=ingest(store,'a',CFG,'bsc',http=object(),ts=10,initial_blocks=30)
        self.assertEqual(row['status'],'PARTIAL')
        self.assertEqual(row['verified_to'],981)
        self.assertEqual(store.db.execute('SELECT last_complete_block FROM chain_tip_cursor').fetchone()[0],981)
        self.assertEqual(store.db.execute('SELECT last_complete_block FROM chain_cursor').fetchone()[0],500)

    def test_provider_failure_never_advances(self):
        store=MemoryStore()
        with patch('tip_ingest.select_provider',side_effect=RuntimeError('403')):
            row=ingest(store,'a',CFG,'bsc',http=object(),ts=11)
        self.assertEqual(row['status'],'ERROR_PROVIDER')
        self.assertEqual(store.db.execute('SELECT COUNT(*) FROM chain_tip_cursor').fetchone()[0],0)

    def test_cursor_ahead_fails_closed(self):
        store=MemoryStore()
        store.db.executescript("CREATE TABLE chain_tip_cursor(chain TEXT PRIMARY KEY,last_complete_block INTEGER NOT NULL,updated_ms INTEGER NOT NULL);INSERT INTO chain_tip_cursor VALUES('bsc',1050,1);")
        with patch('tip_ingest.select_provider',return_value=('x',1000,'logs')):
            row=ingest(store,'a',CFG,'bsc',http=object(),ts=11)
        self.assertEqual(row['status'],'ERROR_CURSOR_AHEAD')
        self.assertEqual(store.db.execute('SELECT last_complete_block FROM chain_tip_cursor').fetchone()[0],1050)

    def test_tip_scratch_schema_removed_before_archive(self):
        import json, tempfile, pathlib, sys
        from unittest.mock import patch
        from tip_ingest import main
        from radar_stage1 import Store, now_ms
        repo=pathlib.Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as td:
            base=pathlib.Path(td);db=base/'live.db'
            st=Store(db);st.begin_run(now_ms());st.close()
            prior=base/'prior.json'
            prior.write_text(json.dumps({
                'schema':'v2-verified-independent-chain-tip-1',
                'chains':[{'chain':'bsc','verified_to':1000}]}))
            out=base/'tip.json'
            args=['tip_ingest','--db',str(db),'--config',str(repo/'v2/config.json'),
                  '--prior',str(prior),'--out',str(out)]
            with patch.object(sys,'argv',args), patch('tip_ingest.ingest',
                side_effect=lambda store,rid,cfg,ch,**kw:
                    {'chain':ch,'status':'OK','verified_to':1001 if ch=='bsc' else None}):
                main()
            import sqlite3
            con=sqlite3.connect(db)
            names={r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            con.close()
            self.assertNotIn('chain_tip_checks',names)
            self.assertNotIn('chain_tip_cursor',names)
            self.assertIn('token_transfers',names)
            self.assertTrue(json.loads(out.read_text())['archive_schema_unchanged'])

