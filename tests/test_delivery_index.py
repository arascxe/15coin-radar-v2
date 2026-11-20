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
    def test_new_combined_fusion_event_keeps_real_evidence_and_delivery_uncertain(self):
        r=report()
        r['coins'][0]['state']='FUSION_PREPARATION_THEN_SPOT_IGNITION_RESEARCH'
        r['coins'][0]['unified_economic_evidence']={'phase':'PREPARATION_THEN_SPOT_IGNITION_RESEARCH',
            'orders_cancelled_vs_consumed_exactly_known':False,
            'spot_windows':[{'venue':'gate','proved_ask_execution_lower_bound_quote':'0'}]}
        r['unified_economic_fusion']={'schema':'radar-v2-live-economic-fusion-1'}
        ev={'coin':'ON','evidence_state':'FUSION_PREPARATION_THEN_SPOT_IGNITION_RESEARCH',
            'first_seen_ms':T-60000,
            'unified_economic_evidence':r['coins'][0]['unified_economic_evidence']}
        inbox=build(r,{'events':[ev]},workflow_id=123)
        self.assertEqual(len(inbox['research_events_72h']),1)
        item=inbox['research_events_72h'][0]
        self.assertEqual(item['urgency'],'EARLY_PREPARATION_THEN_SPOT_RESEARCH')
        self.assertFalse(item['verified_combined'])
        self.assertEqual(item['unified_economic_evidence']['spot_windows'][0]['venue'],'gate')
        self.assertEqual(inbox['unified_economic_states']['ON'],'PREPARATION_THEN_SPOT_IGNITION_RESEARCH')
        self.assertEqual(inbox['delivery_status'],'NOT_SENT_BY_GITHUB_ACTIONS')

if __name__=='__main__':unittest.main()
