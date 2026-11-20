"""Non-regression tests for distinct economic evidence / no fabricated buy signal."""
import json, tempfile, pathlib, sys, unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'v2'))
from v2_full.onchain_economics import observe,classify
from v2.dynamic_wallet_monitor import fresh_tip_heads,monitor
from v2.radar_stage1 import Store

NOW=1_800_000_000_000
D18=10**18
def addr(x):return '0x'+format(x,'040x')
A,B=addr(11),addr(12)
S1,S2,S3=addr(21),addr(22),addr(23)
POOL=addr(40);HOT=addr(50);COLD=addr(51)
TOKEN=addr(90)
CONFIG={'coins':[{'symbol':'DOS','chain':'bsc','token':TOKEN,'decimals':18}],
        'wallets':[{'coin':'DOS','chain':'bsc','address':HOT,'role':'exchange_custody'},
                   {'coin':'DOS','chain':'bsc','address':COLD,'role':'exchange_custody'},
                   {'coin':'DOS','chain':'bsc','address':POOL,'role':'dex_pool'}],
        'dex_pools':[{'coin':'DOS','chain':'bsc','pool':POOL,'kind':'v3'}],
        'venues':[],'limits':{'request_timeout_seconds':12}}
ENT={'bsc|'+HOT:{'proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC','entity':'Gate'},
     'bsc|'+COLD:{'proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC','entity':'Gate'}}

def evt(i,frm,to,amount,block):
    return {'coin':'DOS','from':frm,'to':to,'raw':str(amount),'block':block,
        'tx':'0x'+format(i,'064x'),'log_index':0}

def ch(events,start=101,head=110,fresh=True):
    return {'observed_ms':NOW,'events':events,
        'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL' if fresh else 'ERROR',
            'from_block':start,'safe_head':head,'safe_head_ms':NOW-5000,
            'provider':'https://fake','complete':fresh}}}

class RPC:
    def __init__(self,balances=None,fail=False):
        self.balances=balances or {}
        self.fail=fail
    def rpc(self,url,method,params):
        if self.fail:raise RuntimeError('RPC_UNAVAILABLE')
        if method=='eth_getCode':return '0x'
        if method=='eth_call':
            data=params[0]['data']
            if data=='0x18160ddd':return hex(1_000_000*D18)
            who='0x'+data[-40:]
            return hex(self.balances.get(who,0))
        raise AssertionError(method)

def baseline():
    return {'snapshots':{
        'DOS|'+A:{'coin':'DOS','chain':'bsc','address':A,'block':100,
                  'raw':'0','observed_ms':NOW-100000},
        'DOS|'+B:{'coin':'DOS','chain':'bsc','address':B,'block':100,
                  'raw':'0','observed_ms':NOW-100000}
    },'episodes':{}}

class TestEconomicDetection(unittest.TestCase):
    def test_exchange_internal_not_external(self):
        t=ch([evt(1,HOT,COLD,100*D18,102)])
        z,_,_=classify(CONFIG,t,ENT)
        self.assertEqual(z['DOS']['classes']['VERIFIED_CEX_INTERNAL_TRANSFER_NOT_OUTFLOW'],1)
        self.assertEqual(z['DOS']['classes']['VERIFIED_CEX_TO_UNKNOWN_DESTINATION_NOT_PROVEN_WITHDRAWAL'],0)

    def test_exchange_to_unknown_is_not_proven_withdrawal(self):
        z,_,_=classify(CONFIG,ch([evt(1,HOT,A,100*D18,102)]),ENT)
        self.assertEqual(z['DOS']['classes']['VERIFIED_CEX_TO_UNKNOWN_DESTINATION_NOT_PROVEN_WITHDRAWAL'],1)

    def test_dex_swap_not_called_beneficial_owner_accumulation(self):
        z,_,_=classify(CONFIG,ch([evt(1,POOL,A,100*D18,102)]),ENT)
        self.assertEqual(z['DOS']['classes']['DEX_SWAP_OR_LP_INTERACTION_UNRESOLVED'],1)
        r,_=observe(CONFIG,ch([evt(1,POOL,A,100*D18,102)]),verified_entities=ENT,
                    prior=baseline(),http=RPC({A:100*D18}),now_ms=NOW)
        self.assertEqual(r['coins'][0]['balance_verified_eoa_receiver_count_unknown_owner'],0)

    def test_multi_hop_not_double_counted(self):
        z,_,_=classify(CONFIG,ch([
            evt(1,S1,A,100*D18,102),evt(2,A,B,70*D18,104)]))
        self.assertEqual(z['DOS']['net_by_address'][A],30*D18)
        self.assertEqual(z['DOS']['net_by_address'][B],70*D18)
        self.assertEqual(z['DOS']['net_by_address'][A]+z['DOS']['net_by_address'][B],100*D18)

    def test_new_eoa_cluster_requires_two_snapshots_and_exact_flows(self):
        tx=[evt(1,S1,A,600*D18,102),evt(2,S2,A,600*D18,105),
            evt(3,S2,B,800*D18,103),evt(4,S3,B,800*D18,107)]
        r,state=observe(CONFIG,ch(tx),verified_entities=ENT,prior=baseline(),
            http=RPC({A:1200*D18,B:1600*D18}),now_ms=NOW)
        row=r['coins'][0]
        self.assertTrue(row['multi_receiver_unknown_owner_cluster'])
        self.assertEqual(row['research_state'],'CLUSTER_ACCUMULATION_RESEARCH')
        self.assertEqual(row['balance_verified_eoa_receiver_count_unknown_owner'],2)
        self.assertFalse(row['buy_signal'])
        self.assertFalse(r['strong_buy_signal'])
        self.assertTrue(all(e['owner_verified'] is False for e in row['recipient_evidence']))

    def test_no_previous_snapshot_cannot_claim_preparation(self):
        tx=[evt(1,S1,A,600*D18,102),evt(2,S2,A,600*D18,105),
            evt(3,S2,B,800*D18,103),evt(4,S3,B,800*D18,107)]
        r,state=observe(CONFIG,ch(tx),prior={},http=RPC({A:1200*D18,B:1600*D18}),now_ms=NOW)
        self.assertEqual(r['coins'][0]['balance_verified_eoa_receiver_count_unknown_owner'],0)
        self.assertTrue(state['snapshots'])

    def test_false_cluster_if_chain_interval_not_contiguous(self):
        tx=[evt(1,S1,A,600*D18,102),evt(2,S2,A,600*D18,105),
            evt(3,S2,B,800*D18,103),evt(4,S3,B,800*D18,107)]
        r,_=observe(CONFIG,ch(tx,start=106),prior=baseline(),
            http=RPC({A:1200*D18,B:1600*D18}),now_ms=NOW)
        self.assertEqual(r['coins'][0]['balance_verified_eoa_receiver_count_unknown_owner'],0)

    def test_balance_delta_mismatch_blocks_candidate(self):
        tx=[evt(1,S1,A,600*D18,102),evt(2,S2,A,600*D18,105),
            evt(3,S2,B,800*D18,103),evt(4,S3,B,800*D18,107)]
        r,_=observe(CONFIG,ch(tx),prior=baseline(),
            http=RPC({A:10*D18,B:1600*D18}),now_ms=NOW)
        self.assertEqual(r['coins'][0]['multi_receiver_unknown_owner_cluster'],False)

    def test_single_whale_transfer_not_preparation(self):
        r,_=observe(CONFIG,ch([evt(1,S1,A,2000*D18,102)]),http=RPC({A:2000*D18}),now_ms=NOW)
        self.assertEqual(r['coins'][0]['research_state'],'UNEXPLAINED_LARGE_TRANSFER_REVIEW')
        self.assertEqual(r['coins'][0]['balance_verified_eoa_receiver_count_unknown_owner'],0)

    def test_rpc_error_never_promotes_old_episode(self):
        episodes={'DOS|'+A:{'coin':'DOS','chain':'bsc','address':A,'retained_post_raw':str(1200*D18),
                    'last_observed_ms':NOW-10000,'source_addresses':[S1,S2],
                    'delta_raw':str(1200*D18)},
                  'DOS|'+B:{'coin':'DOS','chain':'bsc','address':B,'retained_post_raw':str(1600*D18),
                    'last_observed_ms':NOW-10000,'source_addresses':[S2,S3],
                    'delta_raw':str(1600*D18)}}
        prior={'snapshots':baseline()['snapshots'],'episodes':episodes}
        r,_=observe(CONFIG,ch([],start=101),prior=prior,http=RPC(fail=True),now_ms=NOW)
        self.assertEqual(r['coins'][0]['balance_verified_eoa_receiver_count_unknown_owner'],0)

    def test_capacity_ratio_is_only_partial_market_depth(self):
        tx=[evt(1,S1,A,600*D18,102),evt(2,S2,A,600*D18,105),
            evt(3,S2,B,800*D18,103),evt(4,S3,B,800*D18,107)]
        book={'markets':[{'coin':'DOS','venue':'gate','pair':'DOS_USDT',
                'book':{'status':'VERIFIED_SEQUENCED_BOOK_INTERVAL',
                        'reference_price':'0.5','ask_after_quote':'500',
                        'fraction_depleted':'0.4'}}]}
        r,_=observe(CONFIG,ch(tx),market=book,prior=baseline(),
            http=RPC({A:1200*D18,B:1600*D18}),now_ms=NOW)
        row=r['coins'][0]
        self.assertEqual(float(row['accumulation_to_visible_ask_ratio']),2.8)
        self.assertIn('excludes hidden orders',row['visible_spot_2pct_ask_token_capacity']['scope'])
        
class TipFreshness(unittest.TestCase):
    def test_recent_verified_tip_admitted(self):
        import datetime
        iso=datetime.datetime.fromtimestamp(NOW/1000,datetime.timezone.utc).isoformat()
        doc={'schema':'v2-verified-independent-chain-tip-1','created_utc':iso,
             'chains':[{'chain':'bsc','status':'OK','verified_to':2048,'head':2048,'lag_blocks':0}]}
        self.assertEqual(fresh_tip_heads(doc,NOW),{'bsc':2048})
        self.assertEqual(fresh_tip_heads(doc,NOW+22*60*1000),{})
        doc['chains'][0]['status']='PARTIAL'
        self.assertEqual(fresh_tip_heads(doc,NOW),{})

if __name__=='__main__':unittest.main()
