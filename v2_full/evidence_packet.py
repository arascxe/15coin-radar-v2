"""Immutable, bounded, replayable provenance packet for each 15-coin live research event.

Records the *decision-time observed evidence*, not a reconstruction from newer
blocks. Visible L2 asks cannot distinguish cancellations from executions. Public
WebSocket trade IDs are a sampled transport feed, not exchange-audited fills.
"""
from __future__ import annotations
import datetime
import hashlib
import json
import os
import pathlib
import re
from .core import COINS

SCHEMA='radar-v2-alarm-evidence-packet-1'
HEX_TX=re.compile(r'^0x[0-9a-fA-F]{64}$')
HEX_WALLET=re.compile(r'^0x[0-9a-fA-F]{40}$')
MAX_CHAIN_TX=32
MAX_MARKETS=8
MAX_WALLETS=10

def packet_id(event):
    sym=event.get('coin')
    state=event.get('evidence_state')
    ms=event.get('first_seen_ms')
    if sym not in COINS or not isinstance(state,str) or not state or not isinstance(ms,int) or ms<=0:
        raise ValueError('INVALID_15_COIN_EVENT_IDENTITY')
    return hashlib.sha256((sym+'|'+state+'|'+str(ms)).encode()).hexdigest()[:22]

def chain_link(chain,tx):
    if not isinstance(tx,str) or not HEX_TX.fullmatch(tx):return None
    host={'bsc':'https://bscscan.com/tx/','base':'https://basescan.org/tx/'}.get(chain)
    return host+tx if host else None

def canon(obj):
    return (json.dumps(obj,ensure_ascii=False,sort_keys=True,
                       separators=(',',':'),allow_nan=False)+'\n').encode('utf8')

def build(event,capture=None,chain=None,config=None):
    ident=packet_id(event)
    cfg={r['symbol']:r for r in (config or {}).get('coins',[])}
    sym=event['coin']
    tok=cfg.get(sym,{})
    network=tok.get('chain')
    token=tok.get('token')
    now=event['first_seen_ms']
    evidence=event.get('unified_economic_evidence')
    evidence=evidence if isinstance(evidence,dict) else {}
    sourcewallet=evidence.get('multiday_wallet_evidence',[])
    wallets=[]
    transfers={}
    for w in sourcewallet[:MAX_WALLETS]:
        if not isinstance(w,dict) or not HEX_WALLET.fullmatch(str(w.get('address',''))):continue
        row={k:w.get(k) for k in ('address','observed_days','net_retained_increase_token',
             'start_balance_token','end_balance_token','start_block','end_block',
             'recorded_snapshots','period_begin_ms','history_asof_ms',
             'positive_observed_daily_intervals','transfer_reconciled',
             'transfer_proof_status','source_exchange_scope','owner_verified')}
        sample=w.get('transaction_proof_sample')
        if isinstance(sample,dict):
            row['transaction_proof_sample']={k:sample.get(k) for k in
                ('window_days','from_block_exclusive','to_block_inclusive','selected_event_count',
                 'more_events_may_exist','covered_every_intervening_block',
                 'balance_delta_reconciled_to_complete_transfers','scope')}
            rows=[]
            for tr in sample.get('entries',[])[:8]:
                if not isinstance(tr,dict):continue
                if tr.get('chain')!=network or not HEX_TX.fullmatch(str(tr.get('tx',''))):continue
                record={k:tr.get(k) for k in ('tx','chain','block','from','to','amount_raw',
                                                'direction_relative_to_wallet')}
                record['explorer']=chain_link(network,tr['tx'])
                rows.append(record)
                transfers[(network,tr['tx'])]=record
            row['transaction_proof_sample']['entries']=rows
        row['direct_verified_cex_address_sources']=w.get('direct_verified_cex_address_sources',[])[:5]
        row['wallet_ownership_not_proven']=True
        wallets.append(row)

    observed=event.get('onchain_economics') or {}
    for receiver in observed.get('recipient_evidence',[])[:MAX_WALLETS]:
        if not isinstance(receiver,dict):continue
        for tx in receiver.get('evidence_txs',[])[:8]:
            if HEX_TX.fullmatch(str(tx)):
                transfers.setdefault((network,tx),{'chain':network,'tx':tx,
                    'explorer':chain_link(network,tx),
                    'source':'RECONCILED_EOA_RECEIVER_EVENT_IDS'})
    unexplained=observed.get('unexplained_transfer')
    if isinstance(unexplained,dict):
        tx=unexplained.get('tx')
        if HEX_TX.fullmatch(str(tx)):
            transfers.setdefault((network,tx),{'chain':network,'tx':tx,'block':unexplained.get('block'),
                'amount_raw':unexplained.get('amount_raw'),
                'explorer':chain_link(network,tx),
                'source':'UNEXPLAINED_TRANSFER_NOT_BUY_OR_CEX_PROOF'})

    # Include relevant recent chain events, not all high-volume DEX traffic.
    watch={w['address'].lower() for w in wallets}
    for recipient in observed.get('recipient_evidence',[])[:MAX_WALLETS]:
        if isinstance(recipient,dict) and HEX_WALLET.fullmatch(str(recipient.get('address',''))):
            watch.add(recipient['address'].lower())
    for e in reversed((chain or {}).get('events',[])):
        if len(transfers)>=MAX_CHAIN_TX:break
        if e.get('coin')!=sym or not HEX_TX.fullmatch(str(e.get('tx',''))):continue
        if e.get('to','').lower() not in watch and e.get('from','').lower() not in watch:
            continue
        if (not isinstance(e.get('block_ms'),int)
             or e['block_ms']>now+60000
             or now-e['block_ms']>72*3600000):
            continue
        tx=e['tx'];key=(network,tx)
        if key not in transfers:
            transfers[key]={'chain':network,'tx':tx,'block':e.get('block'),
                'from':e.get('from'),'to':e.get('to'),'raw':str(e.get('raw')),
                'log_index':e.get('log_index'),'block_ms':e.get('block_ms'),
                'explorer':chain_link(network,tx),
                'source':'VERIFIED_RECENT_CHAIN_TAIL_PARTIAL_SCOPE'}
    chain_meta=(chain or {}).get('chains',{}).get(network,{})
    chain_health={k:chain_meta.get(k) for k in ('status','complete','from_block',
        'safe_head','safe_head_ms')}

    market_receipts=[]
    for m in event.get('spot_markets',[])[:MAX_MARKETS]:
        if not isinstance(m,dict):continue
        venue=m.get('venue');pair=m.get('pair')
        row={'venue':venue,'pair':pair,'phase':m.get('phase'),
             'market_token_identity_verified_at_observation':m.get('identity_verified') is True,
             'decision_window_start_ms':m.get('window_start_ms'),
             'decision_window_end_ms':m.get('window_end_ms'),
             'decision_buy_quote':m.get('buy_quote'),
             'decision_sell_quote':m.get('sell_quote'),
             'decision_book':m.get('book_observed'),
             'decision_tape':m.get('tape_observed'),
             'book_replay':{'status':'NO_CAPTURE_AVAILABLE'},
             'trade_replay':{'status':'NO_CAPTURE_AVAILABLE'}}
        state=(capture.markets.get((venue,pair)) if capture is not None else None)
        if state is not None and m.get('identity_verified') is True and isinstance(m.get('book_observed'),dict):
            with capture.locks[(venue,pair)]:
                row['book_replay']=state.book.audit_book_levels(m['book_observed'])
                tape=m.get('tape_observed') or {}
                if (isinstance(m.get('window_start_ms'),int)
                    and isinstance(m.get('window_end_ms'),int)):
                    row['trade_replay']=state.tape.audit_trades(
                        m['window_start_ms'],m['window_end_ms'],
                        count=tape.get('trades'),buy=tape.get('buy_quote'),
                        sell=tape.get('sell_quote'))
        row['market_proof_replayable']=bool(row['book_replay'].get('status')=='BOOK_ASK_LEVELS_REPLAYABLE'
            and row['trade_replay'].get('status')=='RECEIVED_PUBLIC_TRADES_AGGREGATE_RECONCILED')
        market_receipts.append(row)

    replayable=sum(x['market_proof_replayable'] for x in market_receipts)
    dex=event.get('dex_active_quote',{})
    if not isinstance(dex,dict):dex={}
    fusion=event.get('unified_economic_evidence')
    fusion=fusion if isinstance(fusion,dict) else {}
    packet={'schema':SCHEMA,'event_id':ident,'coin':sym,'state':event['evidence_state'],
            'event_first_observed_ms':now,
            'event_first_observed_utc':datetime.datetime.fromtimestamp(now/1000,datetime.timezone.utc).isoformat(),
            'token_contract':token,'chain':network,
            'source_capture_kind':'GITHUB_ACTIONS_RESEARCH_WINDOW_NOT_24_7_CONTINUOUS',
            'source_chain_health':chain_health,'multiday_wallet_evidence':wallets,
            'chain_transactions':list(transfers.values())[:MAX_CHAIN_TX],
            'spot_decision_inputs_and_bounded_replays':market_receipts,
            'market_window_count':len(market_receipts),
            'market_windows_replayable_count':replayable,
            'dex_pool_evidence_as_observed':dex,
            'unified_decision_as_observed':fusion,
            'onchain_economic_research_as_observed':{
              'research_state':observed.get('research_state'),
              'recipient_evidence':observed.get('recipient_evidence',[])[:MAX_WALLETS],
              'unexplained_transfer':unexplained},
            'proof_health':{
              'wallets_with_transaction_proof_samples':sum(bool(w.get('transaction_proof_sample')) for w in wallets),
              'windows_replayable_from_bounded_inputs':replayable,
              'all_reported_market_windows_replayable':replayable==len(market_receipts) and bool(market_receipts),
              'no_complete_global_ask_inventory':True,
              'L2_cancellation_vs_execution_unknown':True,
              'historical_wallet_source_may_be_partial':True,
              'trade_ids_not_full_exchange_audited_tape':True,
              'historical_predictive_precision_not_validated':True,
              'wallet_beneficial_owner_unknown':True,
              'GitHub_to_ChatGPT_push_delivery_unverified':True},
            'not_buy_advice':True}
    return packet

def write_immutable(directory,event,capture=None,chain=None,config=None):
    packet=build(event,capture=capture,chain=chain,config=config)
    ident=packet['event_id']
    raw=canon(packet)
    sha=hashlib.sha256(raw).hexdigest()
    folder=pathlib.Path(directory)
    folder.mkdir(parents=True,exist_ok=True)
    dest=folder/(ident+'.json')
    if dest.exists():
        if dest.read_bytes()!=raw:raise ValueError('IMMUTABLE_EVIDENCE_PACKET_EVENT_ID_COLLISION')
    else:
        tmp=folder/(ident+'.tmp')
        tmp.write_bytes(raw)
        os.replace(tmp,dest)
    return {'evidence_packet_id':ident,
            'evidence_packet_sha256':sha,
            'evidence_packet_schema':SCHEMA,
            'evidence_packet_scope':'BOUND_REAL_OBSERVATIONS_MAY_BE_PARTIAL',
            'packet_path':'v2_state/evidence_packets/'+ident+'.json'}
