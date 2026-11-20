import pathlib, unittest
from v2_full.entities import load_entities
from v2_full.inventory import analyze

ROOT=pathlib.Path(__file__).resolve().parents[1]
GATE_HOT='0x0d0707963952f2fba59dd06f2b425ace40b492fe'
GATE_COLD='0xc882b111a75c0c657fc507c04fbfcd2cc984f071'
BITGET_HOT='0x0639556f03714a74a5feeaf5736a4a64ff70d206'
CFG={'coins':[{'symbol':'ON','chain':'bsc','token':'0x'+'1'*40}], 'wallets':[]}
class ClusterTests(unittest.TestCase):
    def test_three_independently_labeled_wallets_available(self):
        entities=load_entities(ROOT/'v2/verified_cex_entities.json')
        self.assertEqual(len(entities),3)
        self.assertEqual(entities['bsc|'+GATE_HOT]['entity'],'Gate')
        self.assertEqual(entities['bsc|'+GATE_COLD]['entity'],'Gate')
        self.assertEqual(entities['bsc|'+BITGET_HOT]['entity'],'Bitget')
    def test_hot_to_cold_is_not_off_exchange_inventory(self):
        entities=load_entities(ROOT/'v2/verified_cex_entities.json')
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},
               'events':[{'coin':'ON','from':GATE_HOT,'to':GATE_COLD,'raw':'99999','tx':'0x'+'a'*64,
                          'block_ms':1800000000000}]}
        inv=analyze(CFG,chain,entities)
        self.assertEqual(inv['coins'][0]['status'],'NO_POSITIVE_EVIDENCE_OBSERVED')
        self.assertEqual(inv['coins'][0]['verified_retained_eoa_count'],0)
        self.assertEqual(inv['coins'][0]['verified_cex_external_net_raw'],'0')
    def test_gate_to_bitget_is_exchange_transfer_not_removed_supply(self):
        entities=load_entities(ROOT/'v2/verified_cex_entities.json')
        chain={'chains':{'bsc':{'status':'FRESH_CONTIGUOUS_TAIL','complete':True}},
               'events':[{'coin':'ON','from':GATE_HOT,'to':BITGET_HOT,'raw':'99999','tx':'0x'+'a'*64,
                          'block_ms':1800000000000}]}
        inv=analyze(CFG,chain,entities)
        self.assertEqual(inv['coins'][0]['status'],'NO_POSITIVE_EVIDENCE_OBSERVED')
if __name__=='__main__': unittest.main()
