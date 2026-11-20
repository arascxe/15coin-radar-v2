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
