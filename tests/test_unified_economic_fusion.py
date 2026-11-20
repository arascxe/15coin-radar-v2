"""Counterfactual/negative-control tests for 15-coin early research fusion.
Every value is synthetic unless a separate live data acceptance is run.
"""
import copy
import unittest
from v2_full.core import COINS,L2
from v2_full.economic_fusion_live import enrich,SCHEMA,HISTORY_SCHEMA
from v2_full.monitor import add_dex_cap_change

NOW=1800000000000
PREP_ADDRESS='0x'+'a'*40

def row(c):
    return {'coin':c,'state':'EVIDENCE_INSUFFICIENT_UNKNOWN',
        'preparation_proved':False,'coin_inventory':{},
        'market_evidence':[],'dex_pool':{'status':'NOT_OBSERVED'}}

def report():
    return {'asof_ms':NOW,'coins':[row(c) for c in COINS],
            'onchain_economic_checked_ms':NOW}

def wallet(complete=True,reconciled=True,period=7,end=None):
    end=NOW-140000 if end is None else end
    data={'state':'COMPLETE_OBSERVED_WINDOW' if complete else 'PARTIAL_OBSERVED_WINDOW',
        'regular_multi_day_increase_observed':True,'current_snapshot_fresh':True,
        'end_observed_ms':end,'start_observed_ms':end-period*86400000,
        'net_delta':'3000000','positive_observed_daily_intervals':5,
        'transfer_flow_reconciled_across_full_period':reconciled,
        'transfer_proof_status':'FULL_INTERVAL_TOKEN_TRANSFERS_RECONCILED' if reconciled else 'CHAIN_INTERVAL_NOT_COMPLETE'}
    return {'address':PREP_ADDRESS,'role':'dynamic_eoa_unverified',
            'beneficial_owner_verified':False,'validated_snapshot_count':9,
            'periods':{str(period)+'d':data},
            'cex_sources_completeness':'PARTIAL_NOT_ALL_CEX_ADDRESSES'}

def history(w=None,asof=None):
    return {'schema':HISTORY_SCHEMA,'coin_count':15,
        'asof_ms':NOW-200000 if asof is None else asof,
        'coins':[{'coin':c,'wallets':[w] if c=='ON' and w is not None else []} for c in COINS]}

def market(valid=True,buy=800,sell=120,depth_after=600,reference=1):
    lo=NOW-95000;hi=NOW-1000
    return {'venue':'gate','pair':'ON_USDT','phase':'early_90s',
        'identity_verified':valid,'blockers':[] if valid else ['SYMBOL_TO_CONTRACT_IDENTITY_UNVERIFIED'],
        'window_start_ms':lo,'window_end_ms':hi,
        'buy_quote':str(buy),'sell_quote':str(sell),
        'book':{'status':'VERIFIED_SEQUENCED_BOOK_INTERVAL',
                'from_ms':lo-4000,'to_ms':hi,
                'ask_before_quote':'1000','ask_after_quote':str(depth_after),
                'ask_after_token_qty':'580','reference_price':str(reference)}}

class UnifiedFusionTest(unittest.TestCase):
    def test_positive_spot_does_not_assert_cancellations_were_consumed(self):
        r=report()
        r['coins'][0]['market_evidence']=[market()]
        d=enrich(r,None,NOW)
        x=d['coins'][0]['unified_economic_evidence']
        self.assertEqual(x['phase'],'SPOT_IGNITION_RESEARCH')
        self.assertTrue(x['spot_ignition_observed'])
        self.assertEqual(x['spot_windows'][0]['observed_ask_drop_quote'],'400')
        self.assertEqual(x['spot_windows'][0]['possible_ask_execution_upper_bound_quote'],'400')
        self.assertEqual(x['spot_windows'][0]['proved_ask_execution_lower_bound_quote'],'0')
        self.assertFalse(x['orders_cancelled_vs_consumed_exactly_known'])
        self.assertFalse(x['sellable_supply_denominator_complete'])
        self.assertFalse(d['unified_economic_fusion']['auto_trade'])
        self.assertEqual(len(d['coins']),15)

    def test_reconciled_historical_prep_precedes_spot(self):
        r=report()
        w=wallet(end=NOW-200000)
        r['coins'][0]['market_evidence']=[market()]
        d=enrich(r,history(w),NOW)
        x=d['coins'][0]['unified_economic_evidence']
        self.assertEqual(x['phase'],'PREPARATION_THEN_SPOT_IGNITION_RESEARCH')
        self.assertTrue(x['preparation_before_spot_proved_in_observation_time'])
        self.assertEqual(x['full_period_transfer_reconciled_wallets'],1)
        self.assertEqual(x['wallet_7d_or_30d_net_retained_delta_token'],'3000000')
        self.assertIsNotNone(x['relative_retained_balance_change_to_partial_observed_ask_depth'])

    def test_no_hindsight_preparation_historical_time_after_trade_window(self):
        r=report()
        r['coins'][0]['market_evidence']=[market()]
        w=wallet(end=NOW-30000)
        d=enrich(r,history(w,asof=NOW-5000),NOW)
        x=d['coins'][0]['unified_economic_evidence']
        self.assertEqual(x['phase'],'SPOT_IGNITION_WITH_PREPARATION_TIMING_UNKNOWN')
        self.assertFalse(x['preparation_before_spot_proved_in_observation_time'])

    def test_stale_or_partial_history_does_not_promote(self):
        for h in (history(wallet(),asof=NOW-4*3600000),history(wallet(complete=False)),
                  {'schema':'FAKE','coins':[]}):
            r=enrich(report(),h,NOW)
            self.assertFalse(r['coins'][0]['unified_economic_evidence']['preparation_present'])
            self.assertEqual(r['coins'][0]['state'],'EVIDENCE_INSUFFICIENT_UNKNOWN')

    def test_cancel_only_drop_without_buyer_trades_not_spot(self):
        r=report()
        r['coins'][0]['market_evidence']=[market(buy=10,sell=500)]
        x=enrich(r,None,NOW)['coins'][0]['unified_economic_evidence']
        self.assertFalse(x['spot_ignition_observed'])
        self.assertEqual(x['phase'],'DATA_INSUFFICIENT_UNKNOWN')

    def test_unverified_identity_or_uncaptured_book_no_spot(self):
        for change in ('identity','book','range','stale','token'):
            r=report();m=market()
            if change=='identity':m=market(valid=False)
            if change=='book':m['book']['status']='TRUNCATED_2PCT_ASK_RANGE'
            if change=='range':m['book']['from_ms']=NOW-200000
            if change=='stale':m['window_end_ms']=NOW-400000
            if change=='token':m['book'].pop('ask_after_token_qty')
            r['coins'][0]['market_evidence']=[m]
            self.assertFalse(enrich(r,None,NOW)['coins'][0]['unified_economic_evidence']['spot_ignition_observed'],change)

    def test_double_wallet_or_multiple_markets_not_double_counted(self):
        r=report();r['coins'][0]['market_evidence']=[market(),market()]
        w=wallet(end=NOW-200000)
        h=history(w);h['coins'][0]['wallets'].append(copy.deepcopy(w))
        x=enrich(r,h,NOW)['coins'][0]['unified_economic_evidence']
        self.assertEqual(x['observed_multiday_wallet_count_unknown_owners'],1)
        self.assertEqual(x['wallet_7d_or_30d_net_retained_delta_token'],'3000000')
        self.assertEqual(x['visible_ask_token_qty_across_observed_independent_venues_and_one_pool'],'580')

    def test_dex_same_tick_liquidity_drop_not_proven_lp_withdrawal(self):
        previous={'ON':{'status':'V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED',
                     'quote_token':'0x'+'2'*40,'pool':'0x'+'b'*40,'pinned_finalized_block':100,
                     'collected_ms':NOW-10000,'active_tick':250,'onchain_liquidity_raw':'1000',
                     'quote_needed_for_2pct_price_impact':'100'}}
        now={'ON':{**previous['ON'],'collected_ms':NOW,'pinned_finalized_block':110,
                   'onchain_liquidity_raw':'500','quote_needed_for_2pct_price_impact':'50',
                   'token_qty_out_before_2pct_price_impact':'200'}}
        add_dex_cap_change(previous,now)
        self.assertEqual(now['ON']['active_liquidity_delta_same_tick'],'-500')
        r=report();r['coins'][0]['dex_pool']=now['ON']
        x=enrich(r,None,NOW)['coins'][0]['unified_economic_evidence']
        self.assertEqual(x['phase'],'DEX_ACTIVE_LIQUIDITY_REDUCTION_REVIEW')
        self.assertFalse(x['dex_scope']['LP_burn_event_verified'])

    def test_L2_reports_anchored_token_ask_quantity(self):
        book=L2()
        book.snapshot([['0.99','100']],[['1.01','100'],['1.02','200'],['1.04','400']],10,NOW-100000)
        book.update([], [['1.01','50']],11,11,NOW-1000,'gate')
        result=book.compare(NOW,window_ms=90000)
        self.assertEqual(result['status'],'VERIFIED_SEQUENCED_BOOK_INTERVAL')
        self.assertEqual(result['ask_before_token_qty'],'300')
        self.assertEqual(result['ask_after_token_qty'],'250')

    def test_fail_closed_for_wrong_coin_universe_and_future_market(self):
        r=report()
        r['coins'].pop()
        with self.assertRaises(ValueError):enrich(r,None,NOW)
        r=report();r['asof_ms']=NOW+200000
        with self.assertRaises(ValueError):enrich(r,None,NOW)

if __name__=='__main__':unittest.main()
