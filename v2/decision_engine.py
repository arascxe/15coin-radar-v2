#!/usr/bin/env python3
"""V2 research decision states; not an investment recommendation or buy alert.

Two INDEPENDENT research pathways: observed wallet accumulation OR sampled spot
ignition. Price/volume is optional context, NEVER a hard gate. No fabricated
probabilities. Every result explicitly records missing evidence, sample quality,
and the actual supporting venues. Only validated research notifications may
be considered later, separately from this pure decision code.
"""
import argparse, datetime, hashlib, json, pathlib
from collections import Counter

EXPECTED=('ON','DOS','HANA','COLLECT','LIGHT','APR','IN','TRUST','B2','CYS','STBL','BSB','AIO','STAR','LYN')
SCHEMA='radar-v2-decision-research-1'
# Observational thresholds for RESEARCH ONLY, not tuned/validated as prediction.
# Material research-watch thresholds, NOT calibrated predictive cutoffs.
# Tiny public last-100 samples remain visible as weak leads but never promote
# themselves to a watch merely because two sparse samples overlap.
MIN_TRADES=5
MIN_QUOTE=100.0
BUY_FRACTION=0.65
MATERIAL_TRADES=10
MATERIAL_QUOTE=300.0
MIN_CROSS_VENUE_QUOTE=1000.0
STRONG_SINGLE_VENUE_TRADES=25
STRONG_SINGLE_VENUE_QUOTE=1000.0
MAX_TECH_STALENESS_MS=15*60*1000

def _positive(value):
    try:
        x=float(value)
        return x if 0<x<1e30 else None
    except (ValueError,TypeError,OverflowError):
        return None

def evidence_for_coin(fusion,spot,technical,asof_ms):
    f=fusion
    sym=f['coin']
    known={v['venue']:v for v in spot if v.get('coin')==sym}
    spot_details={v['venue']:v for v in f.get('market_evidence',[])}
    qualified=[]
    material=[]
    bad=[]
    for venue,market in sorted(known.items()):
        t=market.get('trade',{})
        obs=t.get('last_3min_observed',{}) if isinstance(t,dict) else {}
        n=obs.get('count',0)
        frac=_positive(obs.get('buy_side_fraction'))
        notional=_positive(obs.get('observed_quote_notional'))
        enough=(market.get('trade_status')=='RECENT_INCOMPLETE_SAMPLE'
            and isinstance(n,int) and n>=MIN_TRADES
            and frac is not None and frac>=BUY_FRACTION
            and notional is not None and notional>=MIN_QUOTE)
        if enough:
            entry={'venue':venue,'observed_trades':n,
                'sample_buy_fraction':round(frac,4),'observed_quote_notional':round(notional,2),
                'direction_semantics':t.get('trade_side_semantics','UNVERIFIED')}
            qualified.append(entry)
            if n>=MATERIAL_TRADES and notional>=MATERIAL_QUOTE:
                material.append(entry)
        elif market.get('trade_status') not in ('RECENT_INCOMPLETE_SAMPLE','STALE_SAMPLE'):
            bad.append({'venue':venue,'status':market.get('trade_status','UNKNOWN')})
    thin=[]
    for venue,detail in sorted(spot_details.items()):
        if detail.get('ask_depth_change')=='ASK_THINNER_TWO_SNAPSHOTS':
            thin.append({'venue':venue,'depth_change_pct':detail.get('ask_depth_change_pct'),
                'withdrawal_proven':False})
    qualified_venues={a['venue'] for a in qualified}
    material_venues={a['venue'] for a in material}
    thin_venues={a['venue'] for a in thin}
    same_venue=sorted(qualified_venues & thin_venues)
    material_same_venue=sorted(material_venues & thin_venues)
    # Do not let a tiny sample and two noncontinuous depth readings imply
    # convincing ignition. Two material independent spot venues can suffice
    # without wallet or price indicators; alternatively require a more
    # substantial single-venue sample plus observable ask thinning.
    cross_material=(len(material_venues)>=2 and
        sum(x['observed_quote_notional'] for x in material)>=MIN_CROSS_VENUE_QUOTE)
    single_material=any(x['venue'] in thin_venues and
        x['observed_trades']>=STRONG_SINGLE_VENUE_TRADES and
        x['observed_quote_notional']>=STRONG_SINGLE_VENUE_QUOTE for x in material)
    weak_spot_sample=bool(len(qualified_venues)>=2 or same_venue)
    spot_watch=bool(cross_material or single_material)
    prep=f.get('preparation_path',{})
    prep_count=int(prep.get('wallet_candidate_count',0) or 0)
    prep_watch=prep_count>0 and prep.get('chain_currently_complete') is True
    # Secondary price context must have a fresh, contiguous 120m history.
    features=[]
    for m in technical.get('markets',[]) if isinstance(technical,dict) else []:
        latest=m.get('latest_minute_ms')
        z=m.get('features',{})
        if (isinstance(latest,int) and 0<=asof_ms-latest<=MAX_TECH_STALENESS_MS
            and isinstance(z,dict)
            and z.get('status')=='COMPLETE_120_CONSECUTIVE_1M_BARS'):
            features.append({'venue':m['venue'],
                'rvol_15m':z.get('rvol_15m'),
                'rvol_1h':z.get('rvol_1h'),
                'higher_lows':z.get('higher_lows'),
                'compression_ratio':z.get('range_compression'),
                'return_15m_pct':z.get('return_15m_pct'),
                'volume_price_response':z.get('volume_price_response_pct_per_1000_quote'),
                'volatility_expansion_ratio':z.get('volatility_expansion_ratio')})
    if prep_watch and spot_watch:
        level='MULTI_PATH_RESEARCH_WATCH'
    elif prep_watch:
        level='PREPARATION_RESEARCH_WATCH'
    elif spot_watch:
        level='SPOT_IGNITION_RESEARCH_WATCH'
    else:
        level='INSUFFICIENT_EVIDENCE_UNKNOWN'
    evidence_flags=[]
    if not prep_watch:evidence_flags.append('INDEPENDENT_EOA_PREPARATION_NOT_VERIFIED')
    if not spot_watch:evidence_flags.append('INDEPENDENT_SPOT_IGNITION_NOT_CORROBORATED')
    if weak_spot_sample and not spot_watch:
        evidence_flags.append('WEAK_SPOT_SAMPLE_BELOW_MATERIAL_RESEARCH_THRESHOLD')
    if not features:evidence_flags.append('NO_FRESH_COMPLETE_PRICE_VOLUME_CONFIRMATION')
    if len(known)<2:evidence_flags.append('FEWER_THAN_TWO_SPOT_VENUES_OBSERVED')
    if bad:evidence_flags.append('SOME_SPOT_TRADE_SAMPLES_MISSING_OR_FAILED')
    evidence_flags+=['PUBLIC_LAST_100_TRADES_NOT_CONTINUOUS_TAPE',
        'BOOK_DEPTH_CHANGE_NOT_PROOF_OF_CANCELLED_LIQUIDITY',
        'VENUE_TOKEN_PAIR_IDENTITY_NOT_INDEPENDENTLY_VERIFIED',
        'NO_HISTORICAL_PRECISION_RECALL_VALIDATION']
    # Exactly reproducible within the same run, without pretending repeated
    # hourly snapshots are distinct economic events.
    dedupe=hashlib.sha256((sym+'|'+level+'|'+','.join(sorted(material_venues))+
        '|'+','.join(sorted(thin_venues))).encode()).hexdigest()[:20]
    return {'coin':sym,'state':level,
        'watch_research_only':spot_watch or prep_watch,
        'strong_alert':False,'buy_signal':False,'automatic_delivery_ready':False,
        'pathways':{'preparation':{'active_unverified':prep_watch,
                    'candidate_count':prep_count,'chain_scan_complete':prep.get('chain_currently_complete')},
                    'spot_ignition':{'active_sample_based':spot_watch,
                    'weak_sample_research_lead':weak_spot_sample and not spot_watch,
                    'qualified_buy_sample_venues':qualified,
                    'material_buy_sample_venues':material,
                    'ask_thinner_snapshot_venues':thin,
                    'both_buy_and_thinner_in_same_venue':same_venue,
                    'material_both_in_same_venue':material_same_venue,
                    'cross_venue_material':bool(cross_material),
                    'single_venue_material':bool(single_material),
                    'sampled_markets':len(known)}},
        'optional_technical_context':features,
        'cross_venue_dispersion_research_only':technical.get('cross_venue_close_dispersion') if isinstance(technical,dict) else None,
        'evidence_limitations':evidence_flags,'bad_trade_sources':bad,
        'research_dedupe_key':dedupe,'precision_validated':False}

def build(fusion,spot,tech):
    if fusion.get('schema')!='radar-v2-economic-fusion-research-1':
        raise ValueError('INVALID_FUSION_SCHEMA')
    if spot.get('schema')!='radar-v2-public-spot-microstructure-1':
        raise ValueError('INVALID_SPOT_SCHEMA')
    if tech.get('schema')!='radar-v2-technical-confirmation-1':
        raise ValueError('INVALID_TECH_SCHEMA')
    if fusion.get('run_id')!=spot.get('run_id'):
        raise ValueError('FAIL_CLOSED_DIFFERENT_RUNS')
    fs={c['coin']:c for c in fusion.get('coins',[])}
    ts={c['coin']:c for c in tech.get('coins',[])}
    if set(fs)!=set(EXPECTED) or len(fusion.get('coins',[]))!=15:
        raise ValueError('FAIL_CLOSED_NOT_EXACTLY_15_COIN_FUSION')
    if set(ts)!=set(EXPECTED) or len(tech.get('coins',[]))!=15:
        raise ValueError('FAIL_CLOSED_NOT_EXACTLY_15_COIN_TECH')
    asof=spot.get('asof_ms')
    if not isinstance(asof,int) or asof<=0:raise ValueError('INVALID_SAMPLE_TIME')
    report=[evidence_for_coin(fs[sym],spot.get('data',[]),ts[sym],asof)
            for sym in EXPECTED]
    counts=Counter(x['state'] for x in report)
    return {'schema':SCHEMA,'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'run_id':fusion['run_id'],'asof_ms':asof,
        'method':'OBSERVATIONAL_UNCALIBRATED_RESEARCH_CLASSIFIER',
        'historical_detection_rate':None,'false_alert_rate':None,
        'strong_alerts':0,'buy_signal':False,'notification_delivery_validated':False,
        'coin_count':15,'state_counts':dict(counts),
        'unverified_watch_count':sum(x['watch_research_only'] for x in report),
        'important':'Research candidate is not a proven early pump signal or buy recommendation.',
        'coins':report}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--fusion',required=True);ap.add_argument('--spot',required=True)
    ap.add_argument('--technical',required=True);ap.add_argument('--out',required=True)
    a=ap.parse_args()
    def load(s):return json.loads(pathlib.Path(s).read_text())
    d=build(load(a.fusion),load(a.spot),load(a.technical))
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_suffix(target.suffix+'.tmp')
    tmp.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');tmp.replace(target)
    print(json.dumps({'coins':d['coin_count'],'states':d['state_counts'],
          'strong_alerts':0,'unverified_watches':d['unverified_watch_count']}))
if __name__=='__main__':main()
