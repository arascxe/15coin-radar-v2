"""15-coin live convergence of prior wallet preparation, sequenced CEX asks,
aggressor-side trade tape and scoped DEX observations.

Pure function; research messages only. A shrink of aggregate L2 asks CANNOT be
assigned uniquely to buyer executions versus cancels/changes. An order-by-order
causal decomposition needs a trustworthy L3 feed, which these sources lack.
"""
from __future__ import annotations
from collections import Counter
from decimal import Decimal as D, InvalidOperation
from .core import COINS

SCHEMA='radar-v2-live-economic-fusion-1'
HISTORY_SCHEMA='radar-v2-observed-multiday-accumulation-1'
MAX_HISTORY_AGE_MS=3*3600*1000
MAX_LIVE_AGE_MS=120000
MAX_DEX_AGE_MS=20*60000

def positive(v):
    try:
        n=D(str(v))
        return n if n.is_finite() and n>0 else None
    except (InvalidOperation,TypeError,ValueError):return None

def _recent(now,then,limit):
    return isinstance(then,int) and 0<=now-then<=limit

def prior_history(history,now):
    """Keep every missing or stale historical proof explicitly UNKNOWN."""
    if not isinstance(history,dict):
        return {},'NO_PUBLISHED_MULTI_DAY_REPORT'
    if (history.get('schema')!=HISTORY_SCHEMA or history.get('coin_count')!=15
        or tuple(x.get('coin') for x in history.get('coins',[]))!=COINS):
        return {},'INVALID_MULTI_DAY_SCHEMA_OR_UNIVERSE'
    if not _recent(now,history.get('asof_ms'),MAX_HISTORY_AGE_MS):
        return {},'STALE_OR_FUTURE_MULTI_DAY_REPORT'
    return {x['coin']:x for x in history['coins']},'FRESH_PUBLISHED_MULTI_DAY_REPORT'

def qualified_prep(src,now,history_asof):
    """Observable retained balance change, not owner, intention or trade proof."""
    out=[]
    for w in src.get('wallets',[]):
        if w.get('role') not in ('eoa_unverified','dynamic_eoa_unverified'):
            continue
        if w.get('beneficial_owner_verified') is not False:continue
        addr=w.get('address','').lower()
        if not (addr.startswith('0x') and len(addr)==42):continue
        for days in (30,7):
            x=w.get('periods',{}).get(str(days)+'d',{})
            if (x.get('state')!='COMPLETE_OBSERVED_WINDOW'
                or x.get('regular_multi_day_increase_observed') is not True
                or x.get('current_snapshot_fresh') is not True
                or not _recent(now,x.get('end_observed_ms'),MAX_HISTORY_AGE_MS)):
                continue
            increase=positive(x.get('net_delta'))
            if increase is None:continue
            end=x.get('end_observed_ms')
            if not isinstance(end,int) or end>history_asof+60000:continue
            out.append({'address':addr,'observed_days':days,
              'net_retained_increase_token':str(increase),
              'positive_observed_daily_intervals':x.get('positive_observed_daily_intervals'),
              'history_asof_ms':end,
              'period_begin_ms':x.get('start_observed_ms'),
              'transfer_reconciled':x.get('transfer_flow_reconciled_across_full_period') is True,
              'transfer_proof_status':x.get('transfer_proof_status','UNKNOWN'),
              'source_exchange_scope':w.get('cex_sources_completeness','UNKNOWN'),
              'owner_verified':False})
            break
    dedup={}
    for x in out:
        if x['address'] not in dedup:dedup[x['address']]=x
    return list(dedup.values())

def spot_interval(row,now):
    """Only verified identity + continuous 90s/5m trade/book evidence qualifies."""
    res=[]
    for m in row.get('market_evidence',[]):
        if m.get('identity_verified') is not True or m.get('blockers'):
            continue
        book=m.get('book',{})
        if book.get('status')!='VERIFIED_SEQUENCED_BOOK_INTERVAL':continue
        lo=m.get('window_start_ms');hi=m.get('window_end_ms')
        before=book.get('from_ms');after=book.get('to_ms')
        if not (isinstance(lo,int) and isinstance(hi,int)
                and isinstance(before,int) and isinstance(after,int)
                and lo-10000<=before<=lo+5000 and before<after<=hi+5000
                and _recent(now,hi,MAX_LIVE_AGE_MS)):
            continue
        old=positive(book.get('ask_before_quote'))
        new=positive(book.get('ask_after_quote'))
        buy=positive(m.get('buy_quote'))
        if old is None or new is None or buy is None or old<=new:continue
        sell=D(str(m.get('sell_quote','0')))
        if not sell.is_finite() or sell<0:continue
        if buy/(buy+sell)<D('.65'):continue
        token=positive(book.get('ask_after_token_qty'))
        if token is None:continue
        vanished=old-new
        # Critical: no causal matching between aggregate L2 levels and individual fills.
        # Even the min(buy_quote, vanished) amount is an UPPER bound on how much
        # buying *might* explain. The proven execution-to-depth lower bound is ZERO.
        res.append({'venue':m['venue'],'pair':m['pair'],'phase':m.get('phase'),
            'window_start_ms':lo,'window_end_ms':hi,
            'reference_price':book.get('reference_price'),
            'fixed_2pct_ask_before_quote':str(old),
            'fixed_2pct_ask_after_quote':str(new),
            'fixed_2pct_ask_after_token_qty':str(token),
            'observed_ask_drop_quote':str(vanished),
            'observed_ask_drop_fraction':str(vanished/old),
            'buy_aggressor_quote_observed':str(buy),
            'sell_aggressor_quote_observed':str(sell),
            'possible_ask_execution_upper_bound_quote':str(min(buy,vanished)),
            'proved_ask_execution_lower_bound_quote':'0',
            'cancellation_vs_execution':'UNRESOLVABLE_WITH_AGGREGATE_L2_AND_TRADE_TAPE',
            'identity_verified':True,
            'continuous_book_and_tape_within_window':True,
            'not_proof_of_market_wide_sellable_supply':True})
    return sorted(res,key=lambda x:(x['window_start_ms'],x['venue']))

def dex_scope(row,now):
    src=row.get('dex_pool',{})
    if (src.get('status')!='V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED'
        or not _recent(now,src.get('collected_ms'),MAX_DEX_AGE_MS)):
        return {'status':'NO_FRESH_SUPPORTED_V3_POOL'}
    cap=positive(src.get('token_qty_out_before_2pct_price_impact'))
    if cap is None:return {'status':'INVALID_DEX_ASK_CAPACITY'}
    liq=src.get('active_liquidity_delta_same_tick')
    change=src.get('relative_2pct_quote_capacity_change')
    drop=(positive(str(-D(str(liq)))) is not None if liq is not None else False)
    return {'status':'PINNED_V3_ACTIVE_TICK_SCOPE','pool':src.get('pool'),
        'block':src.get('pinned_finalized_block'),
        'ask_token_qty_within_pool_2pct_curve':str(cap),
        'pool_quote_token':src.get('quote_token'),
        'relative_2pct_quote_capacity_change':change,
        'active_liquidity_decrease_same_tick':drop,
        'active_liquidity_delta_same_tick':liq,
        'LP_burn_event_verified':False,
        'LP_withdrawal_causality':'NOT_PROVEN_WITHOUT_V3_BURN_LOG_EVIDENCE',
        'not_all_dex_or_cex_sellable_supply':True}

def enrich(report,history=None,now=None):
    """Mutate only derived research classifications; preserve old event inputs."""
    now=int(report.get('asof_ms') if now is None else now)
    if not _recent(now,report.get('asof_ms'),MAX_LIVE_AGE_MS):
        raise ValueError('STALE_OR_FUTURE_LIVE_MARKET_REPORT')
    rows=report.get('coins',[])
    if tuple(x.get('coin') for x in rows)!=COINS:raise ValueError('NOT_EXACT_15_COIN_UNIVERSE')
    bycoin,hist_status=prior_history(history,now)
    history_ms=history['asof_ms'] if hist_status=='FRESH_PUBLISHED_MULTI_DAY_REPORT' else None
    states=Counter()
    for row in rows:
        sym=row['coin'];src=bycoin.get(sym,{})
        wallets=qualified_prep(src,now,history_ms) if history_ms else []
        existing_verified_preparation=row.get('preparation_proved') is True
        # On-chain cluster reports are strictly current and explicitly not owner proof.
        econ=row.get('onchain_economics',{})
        cluster=bool(econ.get('research_state')=='CLUSTER_ACCUMULATION_RESEARCH'
                     and econ.get('chain_fresh_contiguous') is True
                     and _recent(now,report.get('onchain_economic_checked_ms'),300000))
        prep=bool(wallets or existing_verified_preparation or cluster)
        spot=spot_interval(row,now)
        earliest=min((x['window_start_ms'] for x in spot),default=None)
        historical_before=bool(wallets and earliest is not None and
           all(w['history_asof_ms']<=earliest for w in wallets))
        cex_before=False
        if existing_verified_preparation and earliest is not None:
            recipients=row.get('coin_inventory',{}).get('independent_recipients',[])
            recipients+=row.get('coin_inventory',{}).get('revalidated_receivers',[])
            cex_before=any(isinstance(x.get('last_block_ms'),int)
                           and x['last_block_ms']<=earliest
                           and earliest-x['last_block_ms']<=72*3600000
                           for x in recipients)
        temporal=bool(spot and (historical_before or cex_before))
        dex=dex_scope(row,now)
        visible_token_by_venue={x['venue']:D(x['fixed_2pct_ask_after_token_qty']) for x in spot}
        dex_capacity=positive(dex.get('ask_token_qty_within_pool_2pct_curve'))
        if dex_capacity is not None:
            visible_token_by_venue['dex:'+str(dex.get('pool'))]=dex_capacity
        scope_qty=sum(visible_token_by_venue.values(),D(0))
        retained=sum((D(w['net_retained_increase_token']) for w in wallets),D(0))
        observable_ratio=str(retained/scope_qty) if scope_qty>0 and retained>0 else None
        # Multiple venue ask books are not a global executable supply inventory,
        # and the same legal owner may control several wallets.
        phase=('PREPARATION_THEN_SPOT_IGNITION_RESEARCH' if temporal else
               'SPOT_IGNITION_WITH_PREPARATION_TIMING_UNKNOWN' if spot and prep else
               'SPOT_IGNITION_RESEARCH' if spot else
               'PREPARATION_RESEARCH' if prep else
               'DEX_ACTIVE_LIQUIDITY_REDUCTION_REVIEW' if dex.get('active_liquidity_decrease_same_tick') else
               'DATA_INSUFFICIENT_UNKNOWN')
        detail={'phase':phase,'preparation_present':prep,
          'historical_wallet_report_status':hist_status,
          'observed_multiday_wallet_count_unknown_owners':len(wallets),
          'multiday_wallet_evidence':wallets[:8],
          'wallet_7d_or_30d_net_retained_delta_token':str(retained),
          'full_period_transfer_reconciled_wallets':sum(w['transfer_reconciled'] for w in wallets),
          'existing_verified_cex_inventory_preparation':existing_verified_preparation,
          'onchain_short_window_cluster_unknown_owner':cluster,
          'spot_ignition_observed':bool(spot),'spot_windows':spot,
          'preparation_before_spot_proved_in_observation_time':temporal,
          'dex_scope':dex,
          'visible_ask_token_qty_across_observed_independent_venues_and_one_pool':str(scope_qty) if visible_token_by_venue else None,
          'relative_retained_balance_change_to_partial_observed_ask_depth':observable_ratio,
          'sellable_supply_denominator_complete':False,
          'orders_cancelled_vs_consumed_exactly_known':False,
          'price_pump_predicted_or_buy_recommended':False,
          'evidence_uncertainty':['NO_FULL_CEX_OR_DEX_GLOBAL_SELLABLE_SUPPLY',
            'UNKNOWN_BENEFICIAL_OWNER_AND_POSSIBLE_INTERNAL_HOPS',
            'L2_ASK_REMOVAL_MAY_BE_CANCELS_OR_TAKER_CONSUMPTION',
            'NO_BACKTESTED_FALSE_ALARM_RATE']}
        row['unified_economic_evidence']=detail
        row['pre_fusion_state']=row['state']
        if phase!='DATA_INSUFFICIENT_UNKNOWN':
            row['state']='FUSION_'+phase
        states[phase]+=1
    report['unified_economic_fusion']={'schema':SCHEMA,'coin_count':len(rows),
        'states':dict(states),'multiday_history_status':hist_status,
        'spot_feed':'CONTINUOUS_SEQUENCED_L2_AND_TRANSPORT_OBSERVED_TAKER_TRADES',
        'causality':'CANNOT_SEPARATE_CANCELS_FROM_EXECUTIONS_WITH_L2',
        'predictive_precision':None,'auto_trade':False,'notifications_sent':False}
    return report
