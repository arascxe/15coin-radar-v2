"""Economic unknown-wallet flow discovery: safety and positive controls."""
import json,pathlib,sqlite3,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from flow_discovery import discover,final_contiguous_interval
C=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
A='0x'+'a'*40;B='0x'+'b'*40;D='0x'+'d'*40;E='0x'+'e'*40
ZERO='0x'+'0'*40
COIN=next(z for z in C['coins'] if z['symbol']=='DOS')
def db():
    c=sqlite3.connect(':memory:')
    c.executescript('''CREATE TABLE runs(run_id TEXT,started_ms INTEGER,status TEXT);
      CREATE TABLE chain_cursor(chain TEXT,last_complete_block INTEGER);
      CREATE TABLE chain_checks(run_id TEXT,chain TEXT,status TEXT,from_block INTEGER,to_block INTEGER,head_block INTEGER);
      CREATE TABLE token_transfers(tx_hash TEXT,log_index INTEGER,block INTEGER,from_address TEXT,to_address TEXT,amount_raw TEXT,coin TEXT,chain TEXT);
      INSERT INTO runs VALUES('last',20000,'OK');
      INSERT INTO chain_cursor VALUES('bsc',10000);
      INSERT INTO chain_checks VALUES('last','bsc','OK',9000,10000,10000);
      ''')
    return c
def add(c,n,block,frm,to,amt):
    c.execute('INSERT INTO token_transfers VALUES(?,?,?,?,?,?,?,?)',(
      '0x'+str(n).zfill(64),0,block,frm,to,str(amt*10**COIN['decimals']),'DOS','bsc'))
def dos(c):return next(x for x in discover(c,C)['coins'] if x['coin']=='DOS')
class FlowDiscoveryTests(unittest.TestCase):
    def test_distinct_sources_retention_yields_only_research_candidate(self):
        c=db();add(c,1,9300,A,D,100);add(c,2,9600,B,D,250);add(c,3,9700,D,E,30)
        z=dos(c);self.assertEqual(z['candidate_count'],1)
        w=z['watch_candidates'][0]
        self.assertEqual(w['address'],D);self.assertFalse(w['market_purchase_verified'])
        self.assertFalse(w['exchange_withdrawal_verified']);self.assertFalse(z['strong_alert'])
    def test_router_pass_through_is_not_accumulation(self):
        c=db();add(c,1,9500,A,B,100);add(c,2,9600,D,B,100);add(c,3,9650,B,E,200)
        self.assertNotIn(B,[w['address'] for w in dos(c)['watch_candidates']])
    def test_many_pool_swaps_do_not_fake_independent_accumulation(self):
        c=db()
        pool=next(w['address'] for w in C['wallets']
                  if w['coin']=='DOS' and w['chain']=='bsc' and w['role']=='dex_pool')
        for n in range(1,19):
            add(c,n,9500+n,pool,D,100)
        add(c,20,9900,A,D,60000)
        z=dos(c)
        self.assertEqual(z['candidate_count'],0)
        self.assertTrue(any(x['address']==D and x['inflow_transactions']==19
                            for x in z['dex_dominated_receivers']))
    def test_one_sender_two_transfers_is_not_independent(self):
        c=db();add(c,1,9500,A,B,100);add(c,2,9600,A,B,200)
        self.assertEqual(dos(c)['candidate_count'],0)
    def test_mint_is_not_whale(self):
        c=db();add(c,1,9500,ZERO,B,100);add(c,2,9600,A,B,200)
        self.assertEqual(dos(c)['candidate_count'],0)
    def test_unverified_cex_tag_cannot_be_discovered_as_whale(self):
        c=db()
        g=next(w['address'] for w in C['wallets']
               if w['coin']=='DOS' and w['chain']=='bsc' and w['role']=='exchange_custody')
        add(c,1,9500,A,g,100);add(c,2,9600,B,g,200)
        z=dos(c)
        self.assertNotIn(g,[w['address'] for w in z['watch_candidates']])
        self.assertGreater(z['excluded_infrastructure_addresses'].get('exchange_custody',0),0)
    def test_gap_is_not_silently_bridged(self):
        c=db();c.execute('DELETE FROM chain_checks')
        c.execute("INSERT INTO chain_checks VALUES('last','bsc','PARTIAL',9000,9499,10000)")
        c.execute("INSERT INTO chain_checks VALUES('old','bsc','OK',9600,10000,10000)")
        self.assertEqual(final_contiguous_interval(c,'bsc',10000),(9600,10000))
    def test_no_cursor_not_equivalent_to_no_buyers(self):
        c=db();c.execute('DELETE FROM chain_cursor');z=dos(c)
        self.assertIsNone(z['coverage']['contiguous_scanned_from'])
        self.assertEqual(z['candidate_count'],0)
        self.assertFalse(z['strong_alert'])
if __name__=='__main__':unittest.main()
