import json, tempfile, pathlib, unittest
from unittest.mock import patch
from v2_full.entities import load_entities
from v2_full.proofs import verify_transfers
from v2_full.prep_memory import revalidate
from v2_full.decision import verdict
from v2_full.inventory import analyze
from v2_full.core import COINS

SRC='0x'+'a'*40; DST='0x'+'b'*40; TOKEN='0x'+'c'*40
T=1800000000000
CONFIG={'coins':[{'symbol':'ON','token':TOKEN,'chain':'bsc'}],
        'wallets':[{'coin':'ON','chain':'bsc','address':SRC,'role':'exchange_custody'}]}
ENTITY={'bsc|'+SRC:{'entity':'Gate','proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC'}}
E1={'coin':'ON','token':TOKEN,'from':SRC,'to':DST,'raw':'100',
    'block':102,'tx':'0x'+'1'*64,'log_index':0,'block_ms':T-700000}
E2={**E1,'raw':'200','block':106,'tx':'0x'+'2'*64,'block_ms':T-600000}

class RPC:
    bad_src=False
    def rpc(self,url,method,params):
        if method=='eth_getCode':return '0x'
        if method=='eth_call':
            who='0x'+params[0]['data'][-40:]
            block=int(params[1],16)
            amount= (10 if block==101 else 310) if who==DST else \
                    (1000 if block==101 else (1300 if self.bad_src else 700))
            return hex(amount)
        raise RuntimeError('Unexpected RPC '+method)

class Tests(unittest.TestCase):
    def test_two_source_provenance_required(self):
        with tempfile.TemporaryDirectory() as td:
            path=pathlib.Path(td)/'source.json'
            entry={'entity':'Gate','chain':'bsc','address':SRC,
                   'proof':'OFFICIAL_OR_TWO_INDEPENDENT_CHAIN_SPECIFIC',
                   'source_count':2,'withdrawal_verified':False,
                   'recipient_beneficial_owner_verified':False,
                   'sources':[{'publisher':'Arkham','url':'https://arkm.com/explorer/address/'+SRC},
                              {'publisher':'BscScan','url':'https://bscscan.com/tx/abc'}]}
            d={'schema':'radar-v2-cex-entity-attribution-1','entities':{'bsc|'+SRC:entry}}
            path.write_text(json.dumps(d))
            self.assertEqual(len(load_entities(path)),1)
            entry['sources'][1]['publisher']='Arkham'
            entry['sources'][1]['url']='https://arkm.com/explorer/address/'+SRC
            path.write_text(json.dumps(d))
            with self.assertRaises(ValueError):load_entities(path)

    def test_two_sided_historical_balance_proof(self):
        chain={'events':[E1,E2],
            'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL',
                'complete':True,'provider':'https://test','from_block':100,'safe_head':112,
                'safe_head_ms':T-10000}}}
        with tempfile.TemporaryDirectory() as td:
            with patch('radar_stage1.HTTPClient',return_value=RPC()):
                proof=verify_transfers(CONFIG,chain,ENTITY,pathlib.Path(td)/'proofs.json')
            got=proof['balanceproofs']['ON|'+DST]
            self.assertTrue(got['source_balance_reconciled'])
            self.assertEqual(got['retained_raw'],'300')
            self.assertEqual(got['recipient_baseline_raw'],'10')
            self.assertEqual(got['source_net_decrease_raw'][SRC],'300')
            self.assertEqual(got['snapshot_method'],
                             'HISTORICAL_FINALIZED_BALANCE_AT_FIRST_CEX_OUTFLOW_MINUS_ONE')
            inv=analyze(CONFIG,chain,ENTITY,proof['codeproofs'],proof['balanceproofs'])
            self.assertEqual(inv['coins'][0]['status'],'VERIFIED_RETENTION_PREPARATION')
            self.assertEqual(inv['coins'][0]['verified_retained_eoa_count'],1)

    def test_source_wallet_net_increase_fails_closed(self):
        chain={'events':[E1,E2],
            'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL',
                'complete':True,'provider':'https://test','from_block':100,'safe_head':112}}}
        rpc=RPC();rpc.bad_src=True
        with tempfile.TemporaryDirectory() as td, patch('radar_stage1.HTTPClient',return_value=rpc):
            proof=verify_transfers(CONFIG,chain,ENTITY,pathlib.Path(td)/'proofs.json')
        self.assertEqual(proof['reconciled_destinations'],0)

    def test_retention_cannot_be_faked_by_old_large_recipient_balance(self):
        # Receiver initially held 1000 and received 100; it has since sold all
        # 100 newly received. Absolute balance=1000 would pass a broken
        # "70% of post balance" check but MUST fail original-baseline-relative.
        class RPCAtBaseline:
            def rpc(self,url,method,params):
                if method=='eth_getCode':return '0x'
                if method=='eth_call':return hex(1000)
                raise AssertionError(method)
        coin={'symbol':'ON','chain':'bsc','token':TOKEN}
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL',
                               'complete':True,'safe_head':200,
                               'safe_head_ms':T-10000,'provider':'https://test'}}}
        row={'coins':[{'coin':'ON','state':'EVIDENCE_INSUFFICIENT_UNKNOWN',
                       'preparation_proved':False,'coin_inventory':{},
                       'first_spot_pressure_observed':False,'market_evidence':[]}]}
        old={'episodes':{'ON|'+DST:{
            'coin':'ON','chain':'bsc','recipient':DST,'baseline_raw':'1000',
            'retained_raw':'100','last_block_ms':T-100000,
            'last_reverified_ms':T-90000}}}
        _,new=revalidate({'coins':[coin]},row,chain,old,now=T,http=RPCAtBaseline())
        self.assertEqual(row['coins'][0]['state'],'EVIDENCE_INSUFFICIENT_UNKNOWN')
        self.assertEqual(len(new['episodes']),0)

    def test_memorized_preparation_rechecked_then_combined(self):
        cfg={'coins':[{'symbol':s,'chain':'base' if s=='TRUST' else 'bsc','token':TOKEN} for s in COINS]}
        report={'coins':[{'coin':s,'preparation_proved':s=='ON',
                         'coin_inventory':{'independent_recipients':[{
                             'address':DST,'raw':'300','last_block_ms':T-300000,
                             'source_balance_reconciled':True,
                             'baseline_raw':'0',
                             'source_net_decrease_raw':{SRC:'300'}}] if s=='ON' else []},
                         'first_spot_pressure_observed':False,'market_evidence':[],
                         'state':'VERIFIED_INVENTORY_PREPARATION_OBSERVED' if s=='ON' else 'EVIDENCE_INSUFFICIENT_UNKNOWN'} for s in COINS]}
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL',
            'complete':True,'provider':'https://test','safe_head':112,
            'safe_head_ms':T-10000}}}
        _,mem=revalidate(cfg,report,chain,now=T,http=RPC())
        self.assertEqual(len(mem['episodes']),1)
        # Later runner has fresh chain, ignition and no current transfer;
        # retained EOA must be rechecked from historical evidence.
        current={'coins':[{'coin':s,'preparation_proved':False,
                          'coin_inventory':{},'first_spot_pressure_observed':s=='ON',
                          'market_evidence':[{'window_start_ms':T+100000,'window_end_ms':T+190000}] if s=='ON' else [],
                          'state':'INDEPENDENT_SPOT_IGNITION_OBSERVED' if s=='ON' else 'EVIDENCE_INSUFFICIENT_UNKNOWN'} for s in COINS]}
        later=T+200000;chain['chains']['bsc']['safe_head_ms']=later-10000
        _,nextmem=revalidate(cfg,current,chain,mem,now=later,http=RPC())
        self.assertEqual(current['coins'][0]['state'],'COMBINED_PREP_THEN_SPOT_OBSERVED')
        self.assertTrue(current['coins'][0]['temporal_sequence_proved'])
        # Failed RPC/chain cannot promote cached evidence into an alert.
        other={'coins':[{'coin':'ON','preparation_proved':False,'coin_inventory':{},
                         'first_spot_pressure_observed':False,'market_evidence':[],
                         'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'}]}
        bad={'chains':{'bsc':{'status':'PARTIAL','complete':False}}}
        revalidate({'coins':[cfg['coins'][0]]},other,bad,nextmem,now=later+10000,http=RPC())
        self.assertEqual(other['coins'][0]['state'],'EVIDENCE_INSUFFICIENT_UNKNOWN')

if __name__=='__main__':unittest.main()
