"""No fabricated month-long history, owner proof, or transfer reconciliation."""
import copy
import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from radar_stage1 import Store
from accumulation_history import build, validated_snapshots

CONFIG=json.loads((pathlib.Path(__file__).resolve().parents[1]/'config.json').read_text())
COIN=next(x for x in CONFIG['coins'] if x['symbol']=='DOS')
EOAS=[w['address'].lower() for w in CONFIG['wallets']
      if w['coin']=='DOS' and w['role']=='eoa_unverified']
DAY=86400000
BASE=1800000000000
UNIT=10**COIN['decimals']

def populate(db,days,addresses,stale=False,flat=False):
    s=Store(db)
    for day in days:
        ts=BASE+day*DAY
        rid=s.begin_run(ts)
        head=5000+100*day
        s.chain_save(rid,'bsc',head,head,head,[],'OK',observed_ms=ts)
        for addr in addresses:
            block=head-4000 if stale else head
            amount=100 if flat else 100+10*day
            s.wallet_save(rid,COIN,addr,block,amount*UNIT,observed_ms=ts)
        s.finish_run(rid,ts)
    s.close()

class AccumulationHistoryTests(unittest.TestCase):
    def test_all_fifteen_coins_reported_and_month_not_invented(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'t.sqlite'
            populate(path,[0,8],EOAS[:1])
            report=build(path,CONFIG,asof_ms=BASE+8*DAY+1000)
            self.assertEqual(len(report['coins']),15)
            dos=next(x for x in report['coins'] if x['coin']=='DOS')
            a=next(x for x in dos['wallets'] if x['address']==EOAS[0])
            self.assertFalse(a['periods']['30d']['regular_multi_day_increase_observed'])
            self.assertEqual(a['periods']['7d']['state'],'PARTIAL_OBSERVED_WINDOW')
            self.assertEqual(dos['long_term_status'],'NO_VERIFIED_MULTIDAY_ACCUMULATION_OR_DATA_INSUFFICIENT')
            self.assertFalse(report['buy_signal'])

    def test_regular_weekly_balances_two_wallets_without_owner_or_transfer_claim(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'t.sqlite'
            populate(path,list(range(9)),EOAS)
            report=build(path,CONFIG,asof_ms=BASE+8*DAY+1000)
            dos=next(x for x in report['coins'] if x['coin']=='DOS')
            self.assertEqual(dos['recent_regular_7d_wallets_unknown_owners'],2)
            self.assertEqual(dos['long_term_status'],'MULTI_WALLET_BALANCE_ACCUMULATION_RESEARCH')
            for w in dos['wallets']:
                if w['address'] not in EOAS:continue
                p=w['periods']['7d']
                self.assertTrue(p['regular_multi_day_increase_observed'])
                self.assertFalse(p['transfer_flow_reconciled_across_full_period'])
                self.assertEqual(p['transfer_proof_status'],'CHAIN_INTERVAL_NOT_COMPLETE')
                self.assertFalse(w['beneficial_owner_verified'])
                self.assertFalse(w['periods']['30d']['regular_multi_day_increase_observed'])
            self.assertEqual(dos['relative_to_total_sellable_supply'],
                             'UNKNOWN_NO_COMPLETE_SELLABLE_SUPPLY')

    def test_old_block_queried_today_is_rejected_as_historic_not_live(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'t.sqlite'
            populate(path,list(range(9)),EOAS[:1],stale=True)
            report=build(path,CONFIG,asof_ms=BASE+8*DAY+1000)
            dos=next(x for x in report['coins'] if x['coin']=='DOS')
            self.assertEqual(dos['observed_non_infrastructure_wallet_count'],0)
            self.assertEqual(dos['regular_30d_wallets_unknown_owners'],0)

    def test_three_day_regular_increase_detected_before_seven_day_window(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'t.sqlite'
            populate(path,[0,1,2,3],EOAS[:1])
            report=build(path,CONFIG,asof_ms=BASE+3*DAY+1000)
            dos=next(x for x in report['coins'] if x['coin']=='DOS')
            self.assertEqual(dos['recent_regular_3d_wallets_unknown_owners'],1)
            self.assertEqual(dos['recent_regular_7d_wallets_unknown_owners'],0)
            self.assertEqual(dos['long_term_status'],'EARLY_3D_BALANCE_ACCUMULATION_RESEARCH')
            a=next(x for x in dos['wallets'] if x['address']==EOAS[0])
            self.assertTrue(a['periods']['3d']['regular_multi_day_increase_observed'])
            self.assertFalse(a['periods']['7d']['regular_multi_day_increase_observed'])
            self.assertFalse(a['periods']['3d']['transfer_flow_reconciled_across_full_period'])

    def test_flat_balance_not_called_accumulation_and_stale_live_is_unknown(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'t.sqlite'
            populate(path,list(range(9)),EOAS[:1],flat=True)
            report=build(path,CONFIG,asof_ms=BASE+8*DAY+1000)
            dos=next(x for x in report['coins'] if x['coin']=='DOS')
            a=next(x for x in dos['wallets'] if x['address']==EOAS[0])
            self.assertFalse(a['periods']['7d']['regular_multi_day_increase_observed'])
            old=build(path,CONFIG,asof_ms=BASE+12*DAY)
            dos2=next(x for x in old['coins'] if x['coin']=='DOS')
            self.assertEqual(dos2['long_term_status'],
                             'NO_VERIFIED_MULTIDAY_ACCUMULATION_OR_DATA_INSUFFICIENT')

if __name__=='__main__':
    unittest.main()
