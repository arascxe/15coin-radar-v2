"""Negative controls for exact-amount pass-through vs fake exchange withdrawals."""
import json,pathlib,sqlite3,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from trace_economic_routes import trace_coin
C=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
COIN=next(x for x in C['coins'] if x['symbol']=='CYS')
G=next(w['address'] for w in C['wallets'] if w['coin']=='CYS' and w['role']=='exchange_custody')
P=next(w['address'] for w in C['wallets'] if w['coin']=='CYS' and w['role']=='platform_proxy')
A='0x'+'2'*40
def db():
    c=sqlite3.connect(':memory:')
    c.executescript('''CREATE TABLE chain_cursor(chain TEXT,last_complete_block INTEGER);
      CREATE TABLE chain_checks(run_id TEXT,chain TEXT,status TEXT,from_block INTEGER,to_block INTEGER,head_block INTEGER);
      CREATE TABLE token_transfers(tx_hash TEXT,log_index INTEGER,block INTEGER,from_address TEXT,to_address TEXT,amount_raw TEXT,coin TEXT,chain TEXT);
      INSERT INTO chain_cursor VALUES('bsc',1000);INSERT INTO chain_checks VALUES('r','bsc','OK',500,1000,1000);''')
    return c
def add(c,id,block,frm,to,amt):
    c.execute('INSERT INTO token_transfers VALUES(?,?,?,?,?,?,?,?)',
              (id,0,block,frm,to,str(amt),'CYS','bsc'))
class TraceControls(unittest.TestCase):
    def test_relay_to_platform_proxy_not_whale_buy(self):
        c=db();add(c,'aa',700,G,A,9000145);add(c,'bb',710,A,P,9000145)
        x=trace_coin(c,COIN,C)
        self.assertEqual(x['exact_relays'],1)
        self.assertEqual(x['traces'][0]['classification'],'EXACT_AMOUNT_RELAY_TO_KNOWN_INFRASTRUCTURE_TAG')
        self.assertFalse(x['traces'][0]['independent_exchange_ownership_verified'])
    def test_different_amount_is_not_exact_relay(self):
        c=db();add(c,'aa',700,G,A,9000145);add(c,'bb',710,A,P,9000000)
        self.assertEqual(trace_coin(c,COIN,C)['exact_relays'],0)
    def test_gapped_chain_not_claimed_complete(self):
        c=db();c.execute('DELETE FROM chain_checks');add(c,'aa',700,G,A,9000145)
        self.assertEqual(trace_coin(c,COIN,C)['status'],'NO_CONTIGUOUS_COVERAGE')
if __name__=='__main__':unittest.main()
