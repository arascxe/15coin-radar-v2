"""Persistent local all-15-coin read-only monitor.

Run on an always-on Windows/Linux PC using the original v2/config.json; unlike
hourly GitHub jobs, this process retains continuous WS sessions and can emit
research events as soon as an observed 90 s event satisfies the evidence checks.
Never submits trades, never uses API keys, never modifies original V2 artifacts.
"""
from __future__ import annotations
import argparse, concurrent.futures, datetime, gzip, json, pathlib, sys, time
from .core import COINS, stamp, dec, D
from .streams import StreamCapture
from .chain_tail import scan_tip
from .dex import probe_v3
from .runner import evaluate, atomic_json
from .identity import verify_all
from .proofs import verify_transfers
from .entities import load_entities
from .prep_memory import revalidate
from .onchain_economics import observe as observe_economics
from .economic_fusion_live import enrich as unify_economics

def fresh_chain(src,now):
    if not src:return {'chains':{},'events':[]}
    if now-int(src.get('observed_ms',0))<300_000:return src
    return {'chains':{chain:{**info,'status':'STALE_CACHED_CHAIN_RESULT'}
                      for chain,info in src.get('chains',{}).items()},
            'events':[], 'observed_ms':src.get('observed_ms')}

def dex_snapshot(config,chain):
    try:from radar_stage1 import HTTPClient
    except ImportError:return {'_error':'REPO_V2_PACKAGE_NOT_FOUND'}
    http=HTTPClient(12);out={}
    for p in config.get('dex_pools',[]):
        sym=p['coin'];ch=chain.get('chains',{}).get(p['chain'],{})
        if p.get('kind')!='v3':
            out[sym]={'status':'POOL_TYPE_NOT_YET_SUPPORTED'};continue
        if ch.get('status')!='FRESH_CONTIGUOUS_TAIL':
            out[sym]={'status':'POOL_RPC_NOT_VERIFIED'};continue
        coin=next(x for x in config['coins'] if x['symbol']==sym)
        out[sym]=probe_v3(http,ch['provider'],p['pool'],coin['token'],ch['safe_head'])
    return out

def add_dex_cap_change(old,new):
    for sym,x in new.items():
        prior=old.get(sym,{})
        if x.get('status')!='V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED' or prior.get('status')!='V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED':continue
        if (x['quote_token']!=prior['quote_token']
            or x.get('pool')!=prior.get('pool')
            or not isinstance(prior.get('pinned_finalized_block'),int)
            or not isinstance(x.get('pinned_finalized_block'),int)
            or x['pinned_finalized_block']<=prior['pinned_finalized_block']
            or not isinstance(prior.get('collected_ms'),int)
            or not isinstance(x.get('collected_ms'),int)
            or x['collected_ms']-prior['collected_ms']>20*60000
            or x['collected_ms']<prior['collected_ms']):continue
        oldv=dec(prior['quote_needed_for_2pct_price_impact']);newv=dec(x['quote_needed_for_2pct_price_impact'])
        if oldv<=0:continue
        x['relative_2pct_quote_capacity_change']=str((newv-oldv)/oldv)
        x['less_liquid_in_native_quote']=newv<oldv*D('.7')
        if x.get('active_tick')==prior.get('active_tick'):
            before=dec(prior['onchain_liquidity_raw'])
            after=dec(x['onchain_liquidity_raw'])
            x['active_liquidity_delta_same_tick']=str(after-before)
            x['active_liquidity_fraction_change_same_tick']=str((after-before)/before) if before>0 else None
        x['causality_unresolved']='Capacity may change from pool price/ticks and LP actions; no decoded burn/mint logs, so LP withdrawal is NOT proven'

def load_chain_ledger(path):
    import gzip
    try:
        with gzip.open(path,'rt',encoding='utf-8') as f:return json.load(f)
    except (OSError,ValueError):return {'chains':{},'events':[],'observed_ms':0}

def merge_verified_chain(old,new,limit=80000):
    """Only advance a chain cursor after full contiguous verification.
    Persist past fills for retention proofs across repeated polling loops.
    A failed update invalidates freshness, but does not erase previously proven events.
    """
    merged={'schema':'radar-v2-contiguous-near-tip-ledger-1',
            'observed_ms':new.get('observed_ms',stamp()),
            'chains':dict(old.get('chains',{})),
            'events':list(old.get('events',[]))}
    new_by={x:[] for x in ('bsc','base')}
    for row in new.get('events',[]):
        ch='base' if row.get('coin')=='TRUST' else 'bsc'
        new_by[ch].append(row)
    for ch,meta in new.get('chains',{}).items():
        former=merged['chains'].get(ch,{})
        if meta.get('status')!='FRESH_CONTIGUOUS_TAIL':
            merged['chains'][ch]={**former,'status':'UNVERIFIED_LATEST_REFRESH',
                    'refresh_failure':meta.get('status'),'last_attempt_ms':new.get('observed_ms')}
            continue
        start=meta.get('from_block');last=former.get('safe_head')
        if (last is not None and start!=last+1):
            # A restart/reset must not concatenate disjoint histories.
            merged['events']=[e for e in merged['events']
                              if ('base' if e.get('coin')=='TRUST' else 'bsc')!=ch]
            former={}
        others=[e for e in merged['events'] if ('base' if e.get('coin')=='TRUST' else 'bsc')!=ch]
        curr=[e for e in merged['events'] if ('base' if e.get('coin')=='TRUST' else 'bsc')==ch]
        seen={(e.get('tx'),e.get('log_index'),e.get('coin')) for e in curr}
        for e in new_by[ch]:
            k=(e.get('tx'),e.get('log_index'),e.get('coin'))
            if k not in seen:curr.append(e);seen.add(k)
        if len(curr)>limit:
            curr=sorted(curr,key=lambda r:(r['block'],r['tx'],r['log_index']))[-limit:]
            # Ensure we do not claim coverage for the discarded boundary block.
            new_start=min(r['block'] for r in curr)
            curr=[r for r in curr if r['block']>new_start]
            cutoff=new_start+1
        else:cutoff=former.get('from_block',start)
        merged['events']=others+curr
        merged['chains'][ch]={**meta,'from_block':int(cutoff),
                             'complete':True}
    return merged

def save_chain_ledger(path,ledger):
    import gzip
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_name(path.name+'.tmp')
    with gzip.open(temp,'wt',encoding='utf-8',compresslevel=6) as f:
        json.dump(ledger,f,separators=(',',':'))
    temp.replace(path)

def maybe_notify(msg,enabled):
    print('EARLY_RADAR_RESEARCH_EVENT '+json.dumps(msg,ensure_ascii=False),flush=True)
    if enabled and sys.platform=='win32':
        try:
            import winsound
            winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        except Exception:pass

def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--config',default='v2/config.json')
    p.add_argument('--output-dir',default='v2_local_live')
    p.add_argument('--verified-cex')
    p.add_argument('--accumulation-report',
                   help='Latest published hourly 1d/7d/30d report (read-only; freshness checked)')
    p.add_argument('--notify-local',action='store_true')
    p.add_argument('--interval-sec',type=int,default=10)
    p.add_argument('--max-seconds',type=int,default=0,help='0 = continuous until Ctrl+C')
    a=p.parse_args(argv)
    config=json.loads(pathlib.Path(a.config).read_text())
    if tuple(c['symbol'] for c in config['coins'])!=COINS:raise SystemExit('WRONG_COIN_UNIVERSE')
    out=pathlib.Path(a.output_dir);out.mkdir(parents=True,exist_ok=True)
    entity=load_entities(a.verified_cex) if a.verified_cex else {}
    # Official metadata verified on startup. Failed metadata fetches remain unverified.
    status=verify_all(config)
    atomic_json(out/'official_contract_identity.json',status)
    cap=StreamCapture(config,status['statuses'])
    cap.start_gate();cap.start_bitget();cap.start_mexc()
    chain=load_chain_ledger(out/'verified_chain_recent.json.gz')
    # Cache persists only last observed V3 state; no claim of uninterrupted LP event history.
    dexfile=out/'dex_active_tick_quote.json'
    try:
        dex=json.loads(dexfile.read_text()) if dexfile.exists() else {}
        if not isinstance(dex,dict):dex={}
    except (OSError,ValueError):dex={}
    proven={};newchain_at=0;newdex_at=0
    hourly_history=None;history_mtime=None
    economicfile=out/'onchain_snapshots.json'
    economicprior=json.loads(economicfile.read_text()) if economicfile.exists() else {'snapshots':{},'episodes':{}}
    economic_live=None;economic_error=None;economic_future=None
    prepfile=out/'preparation_memory.json'
    prep_cache=json.loads(prepfile.read_text()) if prepfile.exists() else {'episodes':{}}
    eventfile=out/'economic_event_outbox.json'
    old=json.loads(eventfile.read_text()) if eventfile.exists() else {'events':[],'last_state':{}}
    events=old.get('events',[]);previous=old.get('last_state',{})
    started=time.monotonic();threadpool=concurrent.futures.ThreadPoolExecutor(max_workers=3)
    chain_future=None;dex_future=None;last_chain_snapshot_id=None
    print('RADAR_15COIN_LIVE_MONITOR_ACTIVE_NO_TRADE_EXECUTION',flush=True)
    try:
        while a.max_seconds==0 or time.monotonic()-started<a.max_seconds:
            now=stamp()
            if chain_future is None and (not newchain_at or now-newchain_at>180_000):
                cursors={ch:int(meta['safe_head']) for ch,meta in chain.get('chains',{}).items()
                         if isinstance(meta.get('safe_head'),int)}
                chain_future=threadpool.submit(scan_tip,config,700,300,cursors)
                newchain_at=now
            if chain_future is not None and chain_future.done():
                try:
                    recent=chain_future.result()
                    chain=merge_verified_chain(chain,recent)
                    save_chain_ledger(out/'verified_chain_recent.json.gz',chain)
                    atomic_json(out/'latest_chain_tail.json',
                                {'schema':chain['schema'],'observed_ms':chain['observed_ms'],
                                 'chains':chain['chains'],'total_retained_events':len(chain['events'])})
                    # Persist exact balance proof ledger; query only if entity attribution provided.
                    if entity:
                        proven=verify_transfers(config,chain,entity,out/'balance_proof_ledger.json')
                except Exception as e:chain={'chains':{},'events':[],'error':str(e)[:200]}
                chain_future=None
            # On-chain snapshots are pinned to a *fresh confirmed tip* and
            # obtained outside the fast spot-loop; only completed results can
            # affect current research alert states.
            if economic_future is None and chain_future is None:
                heads=tuple((ch,meta.get('safe_head')) for ch,meta in sorted(chain.get('chains',{}).items()))
                if any(h is not None for _,h in heads) and heads!=last_chain_snapshot_id:
                    try:
                        from radar_stage1 import HTTPClient
                        econmarket=cap.report()
                        economic_future=threadpool.submit(
                            observe_economics,config,chain,econmarket,entity,economicprior,
                            HTTPClient(12),now)
                        last_chain_snapshot_id=heads
                    except Exception as exc:
                        economic_error='ECONOMIC_START_FAILED:'+type(exc).__name__
            if economic_future is not None and economic_future.done():
                try:
                    economic_live,economicprior=economic_future.result()
                    atomic_json(economicfile,economicprior)
                    atomic_json(out/'onchain_economic_evidence.json',economic_live)
                    economic_error=None
                except Exception as exc:
                    economic_error='ECONOMIC_PROCESSING_FAILED:'+type(exc).__name__+':'+str(exc)[:120]
                economic_future=None
            active_chain=fresh_chain(chain,now)
            if dex_future is None and any(x.get('status')=='FRESH_CONTIGUOUS_TAIL' for x in active_chain.get('chains',{}).values()) and (not newdex_at or now-newdex_at>600_000):
                dex_future=threadpool.submit(dex_snapshot,config,active_chain)
                newdex_at=now
            if dex_future is not None and dex_future.done():
                try:
                    update=dex_future.result();add_dex_cap_change(dex,update);dex=update
                    atomic_json(out/'dex_active_tick_quote.json',dex)
                except Exception as e:dex={'_error':str(e)[:200]}
                dex_future=None
            # A single lost depth sequence must not permanently disable a market.
            # Recovery demands a fresh exchange-aligned book and a new trade window.
            cap.recover_unsynced_books()
            market=cap.report()
            result=evaluate(config,market,active_chain,entity,
                            proven.get('codeproofs'),proven.get('balanceproofs'),dex=dex)
            if economic_live and 0<=now-economic_live.get('asof_ms',0)<=300000:
                economics={c['coin']:c for c in economic_live.get('coins',[])}
                for row in result['coins']:
                    obs=economics.get(row['coin'])
                    if obs is None:continue
                    live_chain=active_chain.get('chains',{}).get(obs['chain'],{})
                    if live_chain.get('status')!='FRESH_CONTIGUOUS_TAIL' or not obs.get('chain_fresh_contiguous'):
                        row['onchain_economics']={'coin':row['coin'],'research_state':'DATA_GAP_UNKNOWN',
                            'status':'CHAIN_NOT_CURRENT_NO_ECONOMIC_ALERT'}
                        continue
                    row['onchain_economics']=obs
                    dex_row=row.get('dex_pool',{})
                    if dex_row.get('status')=='V3_ACTIVE_TICK_2PCT_RAW_QUOTE_ESTIMATED':
                        try:
                            tip_time=int(dex_row.get('collected_ms',0))
                            capacity=dec(dex_row['token_qty_out_before_2pct_price_impact'])
                            accumulation=dec(obs.get('accumulation_token_quantity','0'))
                            if 0<=now-tip_time<=900000 and capacity>0:
                                obs['observed_dex_2pct_token_ask_capacity']={
                                    'pool':dex_row['pool'],
                                    'pinned_block':dex_row['pinned_finalized_block'],
                                    'available_token_quantity':str(capacity),
                                    'ratio_of_verified_eoa_delta_to_pool_ask_capacity':str(accumulation/capacity),
                                    'scope':'ONE_VERIFIED_V3_POOL_ONLY_NOT_ALL_SPOT_SUPPLY'}
                                if (obs.get('multi_receiver_unknown_owner_cluster') and
                                    accumulation/capacity>=D('.5') and
                                    obs.get('research_state')=='CLUSTER_ACCUMULATION_SMALL_OR_MATERIALITY_UNKNOWN'):
                                    obs['research_state']='CLUSTER_ACCUMULATION_RESEARCH'
                            if dex_row.get('less_liquid_in_native_quote'):
                                obs['dex_quote_capacity_decline']='OBSERVED_QUOTE_CAPACITY_THINNER_CAUSE_UNKNOWN_NOT_PROVEN_LP_WITHDRAWAL'
                        except (KeyError,ValueError,TypeError,ZeroDivisionError):
                            obs['dex_capacity']={'status':'UNCOMPARABLE_POOL_QUOTE_OR_STALE'}

                    # Independent unknown-owner path is research ONLY, even when
                    # a real spot ignition is also observed. No confirmed
                    # ownership or causal market-making inference is made.
                    research=obs.get('research_state')
                    if research=='CLUSTER_ACCUMULATION_RESEARCH':
                        if row['state']=='EVIDENCE_INSUFFICIENT_UNKNOWN':
                            row['state']='UNVERIFIED_CLUSTER_ACCUMULATION_RESEARCH'
                        elif row['state']=='INDEPENDENT_SPOT_IGNITION_OBSERVED':
                            row['state']='UNVERIFIED_CLUSTER_WITH_SPOT_RESEARCH'
                    elif research=='UNEXPLAINED_LARGE_TRANSFER_REVIEW':
                        if row['state']=='EVIDENCE_INSUFFICIENT_UNKNOWN':
                            row['state']='UNEXPLAINED_LARGE_TRANSFER_REVIEW'
                result['onchain_economic_checked_ms']=economic_live['asof_ms']
            else:
                result['onchain_economic_error']=economic_error or 'NO_RECENT_COMPLETED_ONCHAIN_ECONOMIC_SNAPSHOT'
            # Preserve proven retained inventory over independently verified
            # runner windows; check latest EOA balances before reuse.
            if entity:
                try:
                    from radar_stage1 import HTTPClient
                    result,prep_cache=revalidate(
                        config,result,active_chain,prep_cache,now,HTTPClient(12))
                    atomic_json(prepfile,prep_cache)
                except Exception as exc:
                    result['preparation_memory_error']=type(exc).__name__+':'+str(exc)[:150]
            # Reload hourly proof only when the user-owned V2 report actually exists.
            # GitHub five-minute runners read the last published main snapshot.
            if a.accumulation_report:
                try:
                    path=pathlib.Path(a.accumulation_report)
                    mtime=path.stat().st_mtime_ns
                    if mtime!=history_mtime:
                        hourly_history=json.loads(path.read_text())
                        history_mtime=mtime
                except (OSError,ValueError,TypeError) as exc:
                    hourly_history=None
                    result['multiday_history_read_error']=type(exc).__name__
            try:
                result=unify_economics(result,hourly_history,now)
            except Exception as exc:
                result['unified_economic_fusion_error']=type(exc).__name__+':'+str(exc)[:140]
            result['onchain_economic_engine_status']='COMPLETE_FRESH' if economic_live and 0<=now-economic_live.get('asof_ms',0)<=600000 else 'STALE_OR_UNAVAILABLE'
            result['stream_health']=[{
                'coin':x['coin'],'venue':x['venue'],'pair':x['pair'],
                'evidence_ready':x.get('evidence_ready',False),
                'early_90s_ready':x.get('early_ready',False),
                'book_status':x.get('book',{}).get('status'),
                'tape_status':x.get('tape',{}).get('status'),
                'early_book_status':x.get('layers',{}).get('early_90s',{}).get('book',{}).get('status'),
                'early_tape_status':x.get('layers',{}).get('early_90s',{}).get('tape',{}).get('status'),
                'early_blockers':x.get('layers',{}).get('early_90s',{}).get('blockers',[]),
                'early_buy_quote':x.get('layers',{}).get('early_90s',{}).get('tape',{}).get('buy_quote'),
                'early_sell_quote':x.get('layers',{}).get('early_90s',{}).get('tape',{}).get('sell_quote'),
                'early_depletion':x.get('layers',{}).get('early_90s',{}).get('book',{}).get('fraction_depleted'),
                'blockers':x.get('blockers',[])
                } for x in market.get('markets',[])]
            result['exchange_pair_identity']=status['statuses']
            result['balance_proof_health']={k:v for k,v in proven.items() if k not in ('codeproofs','balanceproofs')}
            result['verified_exchange_source_addresses']=len(entity)
            result['verified_exchange_entity_scope']='ONLY_AUDITED_PUBLIC_WALLET_ADDRESSES_NOT_COMPLETE_CEX_INVENTORY'
            atomic_json(out/'live_15_coin_economic_state.json',result)
            # Durable source/coverage observations for later out-of-sample test.
            day=datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            with gzip.open(out/f'evidence_{day}.jsonl.gz','at',encoding='utf-8') as f:
                f.write(json.dumps({'ms':now,'coins':[{'coin':c['coin'],'state':c['state'],
                    'markets':c['market_evidence'],'prep':c['preparation_proved'],
                    'dex':c['dex_pool']} for c in result['coins']]},separators=(',',':'))+'\n')
            for row in result['coins']:
                sym,st=row['coin'],row['state']
                if st!='EVIDENCE_INSUFFICIENT_UNKNOWN' and previous.get(sym)!=st:
                    # Restart/reconnect flaps must not send the same research
                    # episode repeatedly. Material changes can be evaluated in
                    # the hourly history, but not re-alerted every 10 seconds.
                    recent_same=any(e.get('coin')==sym and e.get('evidence_state')==st
                        and isinstance(e.get('first_seen_ms'),int)
                        and 0<=now-e['first_seen_ms']<30*60000
                        for e in events[-300:])
                    if recent_same:
                        previous[sym]=st
                        continue
                    event={'coin':sym,'first_seen_ms':now,'evidence_state':st,
                           'spot_markets':row['market_evidence'],'preparation_proved':row['preparation_proved'],
                           'dex_active_quote':row['dex_pool'],
                           'onchain_economics':row.get('onchain_economics',{}),
                           'unified_economic_evidence':row.get('unified_economic_evidence',{}),
                           'buy_signal':False,'validated_future_10x':False}
                    events.append(event)
                    verified_event=st in ('INDEPENDENT_SPOT_IGNITION_OBSERVED',
                        'VERIFIED_INVENTORY_PREPARATION_OBSERVED','COMBINED_PREP_THEN_SPOT_OBSERVED')
                    event['evidence_only_not_future_10x_prediction']=True
                    event['only_review_candidate']=not verified_event
                    maybe_notify(event,a.notify_local and verified_event)
                previous[sym]=st
            atomic_json(eventfile,{'schema':'radar-v2-live-evidence-events-1',
                                   'asof_ms':now,'events':events[-1000:],'last_state':previous})
            time.sleep(max(1,a.interval_sec))
    except KeyboardInterrupt:
        print('RADAR_MONITOR_STOPPED_BY_USER',flush=True)
    finally:
        threadpool.shutdown(wait=False,cancel_futures=True)
    return 0
if __name__=='__main__':raise SystemExit(main())
