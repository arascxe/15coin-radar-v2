#!/usr/bin/env python3
"""Economic fusion research: preparation and ignition are INDEPENDENT pathways.

Uses already-persisted wallet/economic reports plus public spot trade samples.
Missing/stale/ambiguous evidence cannot count as a failed condition OR a trade
signal. No parameter here has been backtested on BTW/AKE/IRYS or negatives.
"""
import argparse, collections, datetime, decimal, gzip, json, pathlib
D=decimal.Decimal
decimal.getcontext().prec=40
MAX_PRIOR_MS=6*3600000
MIN_INTERVAL_MS=20*60000
MAX_INTERVAL_MS=150*60000

def positive(v):
    try:
        z=D(str(v))
        return z if z.is_finite() and z>0 else None
    except (TypeError,decimal.InvalidOperation,ValueError):return None

def prior_spot_reports(dirpath,current):
    """Load only honest time-ordered historical observations; no future leak."""
    folder=pathlib.Path(dirpath)
    now=int(current['asof_ms'])
    found=[]
    if not folder.is_dir():return found
    for file in sorted(folder.glob('*.json.gz')):
        try:
            if file.stat().st_size>250000:continue
            d=json.loads(gzip.decompress(file.read_bytes()))
            then=int(d['asof_ms'])
            if (d.get('schema')=='radar-v2-public-spot-microstructure-1' and
                0<now-then<=MAX_PRIOR_MS and d.get('run_id')!=current.get('run_id')):
                found.append(d)
        except (OSError,ValueError,KeyError,UnicodeError,gzip.BadGzipFile):continue
    return sorted(found,key=lambda x:x['asof_ms'])

def by_market(report):
    return {(r['coin'],r['venue']):r for r in report.get('data',[])}

def compare_spot(current,previous):
    """Conservative sample comparison; no assumption complete trade tape."""
    now=int(current['asof_ms'])
    older=by_market(previous) if previous is not None else {}
    minutes=None if previous is None else (now-int(previous['asof_ms']))/60000
    interval_ok=minutes is not None and MIN_INTERVAL_MS<=now-int(previous['asof_ms'])<=MAX_INTERVAL_MS
    details=[]
    for item in current.get('data',[]):
        old=older.get((item['coin'],item['venue']))
        row={'coin':item['coin'],'venue':item['venue'],
             'trades_recent':'UNKNOWN',
             'ask_depth_change':'UNKNOWN',
             'liquidity_not_proven_withdrawn':True,
             'trade_tape_incomplete':True,
             'book_snapshot_interval_minutes':round(minutes,2) if minutes is not None else None}
        t=item.get('trade')
        if item.get('trade_status')=='RECENT_INCOMPLETE_SAMPLE' and isinstance(t,dict):
            s=t.get('last_3min_observed',{})
            n=s.get('count',0)
            buy=positive(s.get('buy_quote'))
            sell=positive(s.get('sell_quote'))
            if n>=3 and buy is not None and sell is not None:
                total=buy+sell
                row['observed_trades']=n
                row['observed_buy_fraction']=format((buy/total).quantize(D('0.0001')),'f')
                # Research hypothesis ONLY: this is a small recent sample, not full tape.
                row['trades_recent']='BUY_DOMINANT_SAMPLE' if buy/total>=D('0.65') else (
                    'SELL_DOMINANT_SAMPLE' if sell/total>=D('0.65') else 'MIXED_SAMPLE')
            elif n>=3 and (buy is not None or sell is not None):
                # One-sided samples still are valid if zero is explicitly reported.
                b=D(str(s.get('buy_quote',0)))
                sell0=D(str(s.get('sell_quote',0)))
                if b>=0 and sell0>=0 and b+sell0>0:
                    row['observed_trades']=n
                    row['observed_buy_fraction']=format((b/(b+sell0)).quantize(D('0.0001')),'f')
                    row['trades_recent']='BUY_DOMINANT_SAMPLE' if b/(b+sell0)>=D('0.65') else (
                        'SELL_DOMINANT_SAMPLE' if sell0/(b+sell0)>=D('0.65') else 'MIXED_SAMPLE')
        if interval_ok and old is not None and item.get('book_status')=='ONE_INSTANTANEOUS_BOOK_SNAPSHOT' and old.get('book_status')=='ONE_INSTANTANEOUS_BOOK_SNAPSHOT':
            curr_book=item['book'];prior_book=old['book']
            a=positive(curr_book.get('ask_quote_notional_within_2pct'))
            b=positive(prior_book.get('ask_quote_notional_within_2pct'))
            if a is not None and b is not None:
                row['ask_depth_change_pct']=float(round(D(100)*(a/b-1),2))
                row['ask_depth_change']='ASK_THINNER_TWO_SNAPSHOTS' if a/b<=D('0.7') else (
                    'ASK_THICKER_TWO_SNAPSHOTS' if a/b>=D('1.3') else 'ASK_LITTLE_CHANGE_TWO_SNAPSHOTS')
                row['liquidity_not_proven_withdrawn']=True
        details.append(row)
    return details

def build(spot,brain,previous=None,technical=None):
    if spot.get('schema')!='radar-v2-public-spot-microstructure-1':
        raise ValueError('WRONG_SPOT_REPORT')
    if brain.get('schema')!='radar-v2-economic-brain-1':
        raise ValueError('WRONG_ECONOMIC_BRAIN')
    if spot.get('run_id')!=brain.get('last_run_id'):
        raise ValueError('FAIL_CLOSED_SAME_RUN_ID_MISMATCH')
    if technical is not None and technical.get('schema')!='radar-v2-technical-confirmation-1':
        raise ValueError('WRONG_TECHNICAL_REPORT')
    tech_by_coin={z['coin']:z for z in (technical or {}).get('coins',[])}
    evidence=compare_spot(spot,previous)
    grouped=collections.defaultdict(list)
    for item in evidence:grouped[item['coin']].append(item)
    coins=[]
    for b in brain['coins']:
        sym=b['coin'];observed=grouped[sym]
        sample_buy=[v for v in observed if v['trades_recent']=='BUY_DOMINANT_SAMPLE']
        thin=[v for v in observed if v['ask_depth_change']=='ASK_THINNER_TWO_SNAPSHOTS']
        matching=[v['venue'] for v in observed if v['trades_recent']=='BUY_DOMINANT_SAMPLE' and v['ask_depth_change']=='ASK_THINNER_TWO_SNAPSHOTS']
        prep=b.get('wallet_accumulation_candidates',[])
        confirmed_prep=len(prep)>0 and b.get('chain_currently_complete') is True
        # Broad event candidate, not alert: neither all components nor even
        # genuine market-wide taker activity and liquidity withdrawal proven.
        tentative_ignition=bool(matching or len({r['venue'] for r in sample_buy})>=2)
        if confirmed_prep:
            status='PREPARATION_RESEARCH_WATCH_OWNER_UNVERIFIED'
        elif tentative_ignition:
            status='SPOT_MICROSTRUCTURE_RESEARCH_WATCH_NOT_BUY'
        else:
            status='INSUFFICIENT_EVIDENCE_UNKNOWN_NOT_NEGATIVE'
        coins.append({'coin':sym,'preparation_path':{
                'wallet_candidate_count':len(prep),'chain_currently_complete':b.get('chain_currently_complete'),
                'status':'PREPARATION_WATCH_UNVERIFIED_OWNER' if confirmed_prep else 'NOT_CORROBORATED_UNKNOWN'},
            'ignition_path':{'sample_buy_dominant_venues':[r['venue'] for r in sample_buy],
                'snapshot_ask_thinner_venues':[r['venue'] for r in thin],
                'both_in_same_venue':matching,
                'independent_research_trigger':tentative_ignition,
                'status':'EARLY_RESEARCH_ONLY' if tentative_ignition else 'NOT_CORROBORATED_UNKNOWN'},
            'market_evidence':observed,
            'price_volume_confirmation':tech_by_coin.get(sym,{'status':'NOT_COMPUTED_UNKNOWN'}),
            'classification':status,
            'strong_alert':False,'buy_signal':False,
            'missing_evidence':['NO_VALIDATED_HISTORICAL_FALSE_POSITIVE_RATE',
                'PUBLIC_TRADE_SAMPLE_IS_NOT_COMPLETE_TAKER_TAPE',
                'ASK_THINNING_NOT_PROVEN_ORDER_CANCELLATION',
                'PRICE_VOLUME_FEATURES_MAY_BE_UNKNOWN_OR_UNCALIBRATED']})
    return {'schema':'radar-v2-economic-fusion-research-1',
        'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'run_id':spot['run_id'],'asof_ms':spot['asof_ms'],
        'previous_sample_ms':previous['asof_ms'] if previous else None,
        'coin_count':len(coins),'strong_alert_count':0,'buy_signal':False,
        'decision_model_backtested':False,'automatic_notification_authorized':False,
        'limitation':'Independent preparation and spot ignition research paths; not calibrated alerts or buying guidance.',
        'coins':coins}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--spot',required=True);p.add_argument('--economic',required=True)
    p.add_argument('--history',required=True);p.add_argument('--out',required=True)
    p.add_argument('--technical')
    args=p.parse_args()
    spot=json.loads(pathlib.Path(args.spot).read_text())
    brain=json.loads(pathlib.Path(args.economic).read_text())
    hist=prior_spot_reports(args.history,spot)
    # Prefer a prior report sufficiently separated to compare approximate
    # hourly book-depth change rather than seconds-apart screenshots.
    usable=[x for x in hist if MIN_INTERVAL_MS<=int(spot['asof_ms'])-int(x['asof_ms'])<=MAX_INTERVAL_MS]
    prev=usable[-1] if usable else None
    technical=json.loads(pathlib.Path(args.technical).read_text()) if args.technical else None
    d=build(spot,brain,prev,technical)
    dest=pathlib.Path(args.out);dest.parent.mkdir(parents=True,exist_ok=True)
    tmp=dest.with_suffix(dest.suffix+'.tmp')
    tmp.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');tmp.replace(dest)
    print(json.dumps({'coins':d['coin_count'],'research_watches':sum(x['ignition_path']['independent_research_trigger'] for x in d['coins']),
        'has_prior':prev is not None,'strong_alerts':d['strong_alert_count']}))
if __name__=='__main__':main()
