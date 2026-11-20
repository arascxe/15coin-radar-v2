import unittest
from v2_full.mexc_pb import decode, DecodeError
from v2_full.identity import gate_match, bitget_match,mexc_match,matching_chain
from v2_full.core import VenueState
from v2_full.dex import selector_int, v3_raw_ask_quote

def varint(n):
    b=bytearray()
    while n>127:b.append((n&127)|128);n>>=7
    b.append(n);return bytes(b)
def wrap(f,v):
    if isinstance(v,int):return varint(f<<3)+varint(v)
    if isinstance(v,str):v=v.encode()
    return varint(f<<3|2)+varint(len(v))+v

class TestExchangeAdapters(unittest.TestCase):
    def test_mexc_protobuf_trade_1buy_2sell(self):
        tr=wrap(1,'100.5')+wrap(2,'2')+wrap(3,1)+wrap(4,1800000000000)+wrap(5,'id1')
        msg=wrap(1,tr)
        pb=(wrap(1,'spot@public.aggre.deals.v3.api.pb@100ms@STARUSDT')+
            wrap(3,'STARUSDT')+wrap(6,1800000000001)+wrap(314,msg))
        got=decode(pb)
        self.assertEqual(got['type'],'trade');self.assertEqual(got['deals'][0]['side'],'buy')
        self.assertEqual(got['deals'][0]['quantity'],'2')
    def test_mexc_protobuf_depth_sequence(self):
        ask=wrap(1,'100')+wrap(2,'12')
        bid=wrap(1,'99')+wrap(2,'18')
        payload=wrap(1,ask)+wrap(2,bid)+wrap(4,'201')+wrap(5,'205')
        pb=(wrap(1,'spot@public.aggre.depth.v3.api.pb@100ms@LYNUSDT')+
            wrap(3,'LYNUSDT')+wrap(6,1800000000001)+wrap(313,payload))
        x=decode(pb)
        self.assertEqual(x['fromVersion'],201);self.assertEqual(x['toVersion'],205)
        self.assertEqual(x['asks'],[['100','12']]);self.assertEqual(x['bids'],[['99','18']])
    def test_mexc_truncation_fails(self):
        with self.assertRaises(DecodeError):decode(b'\x0a\x90\xff')
    def test_mexc_nonspot_channel_fails(self):
        with self.assertRaises(DecodeError):decode(wrap(1,'spot@private.account@abc')+wrap(3,'ABC'))
    def test_mexc_book_gap_fails(self):
        s=VenueState('LYN','mexc','LYNUSDT')
        s.book.snapshot([['99','1']],[['100','1'],['103','1']],100,1800000000000)
        with self.assertRaises(Exception):s.book.update([],[],103,104,1800000001000,'mexc')
        self.assertFalse(s.book.synced)
    def test_gate_contract_and_pair_verification(self):
        c={'symbol':'ON','chain':'bsc','token':'0x'+'c'*40}
        def get(url,params=None):
            if '/currency_pairs/' in url:return {'base':'ON','quote':'USDT'}
            return {'currency':'ON','chains':[{'name':'BSC','addr':c['token']},{'name':'ETH','addr':'0x'+'d'*40}]}
        self.assertEqual(gate_match(c,'ON_USDT',get)['status'],'VERIFIED_SAME_CONTRACT_NETWORK')
    def test_gate_wrong_contract_fails(self):
        c={'symbol':'ON','chain':'bsc','token':'0x'+'c'*40}
        def get(url,params=None):
            if '/currency_pairs/' in url:return {'base':'ON','quote':'USDT'}
            return {'currency':'ON','chains':[{'name':'BSC','addr':'0x'+'d'*40}]}
        self.assertEqual(gate_match(c,'ON_USDT',get)['status'],'CONTRACT_NOT_MATCHED_OR_UNPUBLISHED')
    def test_bitget_contract_check(self):
        c={'symbol':'DOS','chain':'bsc','token':'0x'+'c'*40}
        def get(url,params=None):
            if '/instruments' in url:return {'code':'00000','data':[{'symbol':'DOSUSDT','baseCoin':'DOS'}]}
            return {'code':'00000','data':[{'coin':'DOS','chains':[{'chain':'BEP20','contractAddress':c['token']}]}]}
        self.assertEqual(bitget_match(c,'DOSUSDT',get)['status'],'VERIFIED_SAME_CONTRACT_NETWORK')
    def test_official_mexc_listing_plus_live_pair(self):
        c={'symbol':'DOS','chain':'bsc','token':'0xb0f09ea9ae0515c3551080d4a745c8115aa30e37'}
        def get(url,params=None):
            self.assertEqual(params['symbol'],'DOSUSDT')
            return {'symbols':[{'symbol':'DOSUSDT','baseAsset':'DOS','quoteAsset':'USDT','status':'1','isSpotTradingAllowed':True}]}
        self.assertEqual(mexc_match(c,'DOSUSDT',get)['status'],
           'VERIFIED_OFFICIAL_HISTORICAL_MEXC_SPOT_LISTING_WITH_ACTIVE_PAIR')

    def test_official_mexc_listing_reject_wrong_contract_and_inactive_pair(self):
        c={'symbol':'DOS','chain':'bsc','token':'0x'+'1'*40}
        self.assertEqual(mexc_match(c,'DOSUSDT',lambda *a: (_ for _ in ()).throw(AssertionError()))['status'],
                         'NO_EXACT_OFFICIAL_SPOT_LISTING_PROOF')
        c['token']='0xb0f09ea9ae0515c3551080d4a745c8115aa30e37'
        self.assertEqual(mexc_match(c,'DOSUSDT',lambda *a: {'symbols':[{'symbol':'DOSUSDT','baseAsset':'DOS','quoteAsset':'USDT','status':'BREAK'}]})['status'],
                         'OFFICIAL_LISTING_BUT_PAIR_INACTIVE_OR_UNVERIFIED')

    def test_nonmatching_chain_rejected(self):
        self.assertFalse(matching_chain('ETH','bsc'))
        self.assertTrue(matching_chain('BASE','base'))
    def test_v3_must_pin_block(self):
        class H:
            def rpc(self,*args):return '0x'+'0'*64
        with self.assertRaises(ValueError):selector_int(H(),'u','a','token0')
    def test_v3_liquidity_change_measured(self):
        # For token0 being purchased, price rising crosses an upward tick with +L.
        a=v3_raw_ask_quote(2**96,1000000,{100:500000},True)
        b=v3_raw_ask_quote(2**96,1000000,{},True)
        self.assertGreater(a,b)
if __name__=='__main__':unittest.main()
