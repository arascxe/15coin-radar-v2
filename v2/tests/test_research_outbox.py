import copy,pathlib,sys,unittest
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from research_outbox import produce
from decision_engine import EXPECTED,SCHEMA
def fake(research=True):
    rows=[{'coin':coin,'state':'SPOT_IGNITION_RESEARCH_WATCH' if coin=='ON' and research
           else 'INSUFFICIENT_EVIDENCE_UNKNOWN',
           'watch_research_only':coin=='ON' and research,
           'research_dedupe_key':'same',
           'pathways':{'preparation':{},'spot_ignition':{}},
           'evidence_limitations':['UNPROVEN']} for coin in EXPECTED]
    return {'schema':SCHEMA,'coin_count':15,'asof_ms':1800000000000,
            'run_id':'TEST','coins':rows}
class OutboxTests(unittest.TestCase):
    def test_initial_single_research_event_not_buy(self):
        v=produce(fake())
        self.assertEqual(v['new_research_event_count'],1)
        self.assertEqual(v['chatgpt_delivery_status'],'NOT_CONFIGURED_NO_DELIVERY_TEST')
        self.assertEqual(v['automatic_notifications_sent'],0)
    def test_same_episode_is_deduplicated(self):
        a=produce(fake());b=produce(fake(),a)
        self.assertEqual(b['new_research_event_count'],0)
        self.assertEqual(b['deduplicated_repeats'],1)
    def test_disappearance_then_reappearance_is_new_episode(self):
        a=produce(fake());gone=produce(fake(False),a)
        self.assertFalse(gone['active'])
        self.assertEqual(gone['resolved_event_count'],1)
        b=produce(fake(),gone)
        self.assertEqual(b['new_research_event_count'],1)
    def test_escalation_creates_new_review_event(self):
        a=produce(fake());n=fake()
        n['coins'][0]['state']='MULTI_PATH_RESEARCH_WATCH'
        n['coins'][0]['research_dedupe_key']='other'
        b=produce(n,a)
        self.assertEqual(b['new_research_event_count'],1)
        self.assertEqual(b['resolved_event_count'],1)
    def test_backwards_time_rejected(self):
        a=produce(fake());n=fake();n['asof_ms']=a['asof_ms']-1
        with self.assertRaises(ValueError):produce(n,a)
    def test_old_wrong_schema_rejected(self):
        with self.assertRaises(ValueError):produce(fake(),{'schema':'UNKNOWN','active':{}})
if __name__=='__main__':unittest.main()
