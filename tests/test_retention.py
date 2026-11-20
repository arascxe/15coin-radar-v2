import unittest
from v2_full.monitor import merge_verified_chain
from v2_full.core import Tape,Gap

class TestLedger(unittest.TestCase):
    def test_sequential_scans_retained_across_runs(self):
        old={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':100,'safe_head':110}},
             'events':[{'coin':'ON','block':105,'tx':'a','log_index':0}]}
        new={'observed_ms':1800000000000,'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':111,'safe_head':120}},
             'events':[{'coin':'ON','block':114,'tx':'b','log_index':0}]}
        merged=merge_verified_chain(old,new)
        self.assertEqual(merged['chains']['bsc']['from_block'],100)
        self.assertEqual(merged['chains']['bsc']['safe_head'],120)
        self.assertEqual(len(merged['events']),2)
    def test_disjoint_scans_reset_prior_evidence(self):
        old={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':100,'safe_head':110}},
             'events':[{'coin':'ON','block':105,'tx':'a','log_index':0}]}
        new={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':115,'safe_head':120}},
             'events':[{'coin':'ON','block':119,'tx':'b','log_index':0}]}
        merged=merge_verified_chain(old,new)
        self.assertEqual(merged['chains']['bsc']['from_block'],115)
        self.assertEqual(len(merged['events']),1)
    def test_failed_refresh_cannot_be_considered_fresh(self):
        old={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':100,'safe_head':110}},
             'events':[{'coin':'ON','block':105,'tx':'a','log_index':0}]}
        new={'chains':{'bsc':{'status':'ERROR_NO_VERIFIED_COVERAGE'}},'events':[]}
        merged=merge_verified_chain(old,new)
        self.assertEqual(merged['chains']['bsc']['status'],'UNVERIFIED_LATEST_REFRESH')
        self.assertEqual(len(merged['events']),1)
    def test_no_new_finalized_blocks_keeps_contiguity(self):
        old={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':100,'safe_head':110}},'events':[]}
        new={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','from_block':111,'safe_head':110}},'events':[]}
        self.assertEqual(merge_verified_chain(old,new)['chains']['bsc']['from_block'],100)

if __name__=='__main__':unittest.main()
