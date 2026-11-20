import unittest
from v2_full.core import COINS
from v2_full.delivery_index import build

T=1800000000000
def report():
    return {'asof_ms':T,'verified_exchange_source_addresses':1,
            'coins':[{'coin':x,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN'} for x in COINS],
            'chain_health':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True,
                                   'safe_head':100},
                            'base':{'status':'ERROR_NO_VERIFIED_COVERAGE','complete':False}},
            'stream_health':[{} for _ in range(34)]}

class DeliveryTests(unittest.TestCase):
    def test_real_15_coin_inbox_with_one_combined_event(self):
        d=report()
        d['coins'][0]['state']='COMBINED_PREP_THEN_SPOT_OBSERVED'
        event={'coin':'ON','evidence_state':'COMBINED_PREP_THEN_SPOT_OBSERVED',
               'first_seen_ms':T-90000}
        inbox=build(d,{'events':[event,event]},workflow_id=123456)
        self.assertEqual(inbox['github_workflow_run_id'],'123456')
        self.assertEqual(inbox['coin_states']['ON'],'COMBINED_PREP_THEN_SPOT_OBSERVED')
        self.assertEqual(len(inbox['research_events_72h']),1)
        self.assertEqual(inbox['research_events_72h'][0]['urgency'],
                         'EARLY_COMBINED_ECONOMIC_EVENT')
        self.assertEqual(inbox['delivery_status'],'NOT_SENT_BY_GITHUB_ACTIONS')
    def test_alert_carries_exact_market_and_onchain_proof(self):
        event={'coin':'ON','evidence_state':'COMBINED_PREP_THEN_SPOT_OBSERVED',
               'first_seen_ms':T-60000,
               'spot_markets':[{'venue':'gate','pair':'ON_USDT','phase':'early_90s',
                                'buy_quote':'4500','sell_quote':'900',
                                'ask_depleted_fraction':'0.45',
                                'identity_verified':True}],
               'inventory_proof':[{'address':'0x'+'a'*40,
                                   'txs':['0x'+'b'*64,'0x'+'c'*64],
                                   'source_balance_reconciled':True,
                                   'source_net_decrease_raw':{'0x'+'d'*40:'30000'},
                                   'last_block_ms':T-180000}]}
        x=build(report(),{'events':[event]})['research_events_72h'][0]
        self.assertEqual(x['spot_proof'][0]['buy_quote'],'4500')
        self.assertEqual(x['inventory_proof'][0]['source_balance_reconciled'],True)
        self.assertEqual(len(x['inventory_proof'][0]['txs']),2)
    def test_unknown_does_not_alert(self):
        inbox=build(report(),{'events':[]})
        self.assertEqual(inbox['research_events_72h'],[])
    def test_stale_event_not_mislabeled_live(self):
        e={'coin':'ON','evidence_state':'INDEPENDENT_SPOT_IGNITION_OBSERVED',
           'first_seen_ms':T-73*3600000}
        self.assertEqual(build(report(),{'events':[e]})['research_events_72h'],[])
    def test_missing_coin_fails_closed(self):
        r=report();r['coins']=r['coins'][:-1]
        with self.assertRaises(ValueError):build(r,{'events':[]})
    def test_no_10x_claim_from_custody_review(self):
        e={'coin':'ON','evidence_state':'CEX_FLOW_ATTRIBUTION_REVIEW',
           'first_seen_ms':T-60000}
        result=build(report(),{'events':[e]})
        self.assertEqual(result['research_events_72h'][0]['urgency'],
                         'UNVERIFIED_CUSTODY_REVIEW')
        self.assertFalse(result['research_events_72h'][0]['verified_combined'])
        self.assertTrue(result['research_events_72h'][0]['evidence_only_not_10x_prediction'])
if __name__=='__main__':unittest.main()
