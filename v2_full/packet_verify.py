"""Independent, offline verification of frozen V2 event evidence packets.

No RPC, lookahead, exchange API, trade execution or GitHub write.
Recompute numeric book/tape aggregates from exactly the frozen decision-time
sources. Verification of an L2 window is NOT causal attribution of cancellations.
A receipt list with withheld entries is never called fully replayed.
"""
from __future__ import annotations
from decimal import Decimal as D, InvalidOperation
import hashlib
import json
import pathlib
import re
from .core import COINS
from .evidence_packet import packet_id, SCHEMA

SHA=re.compile(r'^[0-9a-f]{64}$')
HEX=re.compile(r'^0x[0-9a-fA-F]{64}$')
URLS={'bsc':'https://bscscan.com/tx/','base':'https://basescan.org/tx/'}
STATUS_COMPLETE='ALL_RECORDED_CEX_MARKET_WINDOWS_REPLAYED'
STATUS_PARTIAL='PARTIAL_PACKET_PROVENANCE'
STATUS_NONE='NO_REPLAYABLE_MARKET_WINDOWS'
MAX_BYTES=250000

def decimal(v,nonnegative=True):
    try:
        z=D(str(v))
        if not z.is_finite() or (nonnegative and z<0):raise ValueError('NONFINITE_OR_NEGATIVE')
        return z
    except (InvalidOperation,TypeError,ValueError):
        raise ValueError('INVALID_DECIMAL')

def validate(packet,expected_event=None,expected_sha=None,raw=None):
    if not isinstance(packet,dict) or packet.get('schema')!=SCHEMA:
        raise ValueError('INVALID_PACKET_SCHEMA')
    if packet.get('coin') not in COINS:raise ValueError('INVALID_COIN')
    event={'coin':packet.get('coin'),'evidence_state':packet.get('state'),
           'first_seen_ms':packet.get('event_first_observed_ms')}
    pid=packet_id(event)
    if packet.get('event_id')!=pid or (expected_event is not None and pid!=expected_event):
        raise ValueError('EVENT_IDENTITY_NOT_REPRODUCIBLE')
    if raw is not None and expected_sha is not None:
        if not isinstance(expected_sha,str) or not SHA.fullmatch(expected_sha):
            raise ValueError('UNSAFE_SHA')
        if hashlib.sha256(raw).hexdigest()!=expected_sha:
            raise ValueError('PACKET_CONTENT_HASH_MISMATCH')
    network=packet.get('chain')
    if network not in URLS:raise ValueError('UNSUPPORTED_PACKET_CHAIN')
    if not isinstance(packet.get('multiday_wallet_evidence'),list):
        raise ValueError('INVALID_WALLET_EVIDENCE')
    if not isinstance(packet.get('spot_decision_inputs_and_bounded_replays'),list):
        raise ValueError('INVALID_MARKET_EVIDENCE')
    if not isinstance(packet.get('chain_transactions'),list):
        raise ValueError('INVALID_CHAIN_EVIDENCE')
    for tx in packet['chain_transactions']:
        if not isinstance(tx,dict) or not isinstance(tx.get('tx'),str) or not HEX.fullmatch(tx['tx']):
            raise ValueError('INVALID_CHAIN_TX')
        if tx.get('chain')!=network or tx.get('explorer')!=URLS[network]+tx['tx']:
            raise ValueError('INVALID_EXPLORER_OR_CHAIN_PROVENANCE')
    verified=0;incomplete=[];marked=[]
    for i,item in enumerate(packet['spot_decision_inputs_and_bounded_replays']):
        if not isinstance(item,dict):raise ValueError('INVALID_MARKET_ROW')
        b=item.get('book_replay',{})
        t=item.get('trade_replay',{})
        if not isinstance(b,dict) or not isinstance(t,dict):
            raise ValueError('INVALID_BOOK_OR_TAPE_ROW')
        breplay=b.get('status')=='BOOK_ASK_LEVELS_REPLAYABLE'
        treplay=t.get('status')=='RECEIVED_PUBLIC_TRADES_AGGREGATE_RECONCILED'
        all_ids=t.get('all_received_trade_ids_in_packet') is True
        if breplay:
            reference=decimal(b.get('fixed_price'))
            if reference<=0:raise ValueError('INVALID_FIXED_PRICE')
            cap=reference*D('1.02')
            before=b.get('before_ask_price_token_qty_pairs',[])
            after=b.get('after_ask_price_token_qty_pairs',[])
            if not isinstance(before,list) or not isinstance(after,list):
                raise ValueError('INVALID_BOOK_LEVELS')
            if (len(before)!=b.get('before_levels_total')
                or len(after)!=b.get('after_levels_total')
                or b.get('all_necessary_levels_included') is not True):
                raise ValueError('BOOK_PROVENANCE_TRUNCATED_DESPITE_REPLAYABLE')
            def recompute(rows):
                prices=[];value=D(0)
                for level in rows:
                    if not isinstance(level,(tuple,list)) or len(level)!=2:
                        raise ValueError('BAD_BOOK_LEVEL')
                    p=decimal(level[0]);q=decimal(level[1])
                    if p<=0 or q<=0 or p>cap:raise ValueError('BAD_LEVEL_OR_WRONG_BAND')
                    prices.append(p);value+=p*q
                if prices!=sorted(prices) or len(set(prices))!=len(prices):
                    raise ValueError('DUPLICATED_OR_UNSORTED_ASK_PRICE')
                return value
            old=recompute(before);new=recompute(after)
            if old!=decimal(b.get('recomputed_before_quote')) or new!=decimal(b.get('recomputed_after_quote')):
                raise ValueError('BOOK_REPLAY_TOTAL_MISMATCH')
            decision=item.get('decision_book',{})
            if (not isinstance(decision,dict)
                or old!=decimal(decision.get('ask_before_quote'))
                or new!=decimal(decision.get('ask_after_quote'))
                or b.get('from_seq')!=decision.get('from_seq')
                or b.get('to_seq')!=decision.get('to_seq')):
                raise ValueError('BOOK_DECISION_SOURCE_MISMATCH')
        if treplay and all_ids:
            receipts=t.get('trade_receipts_latest',[])
            if (not isinstance(receipts,list)
                or len(receipts)!=t.get('received_count')
                or t.get('unshown_trades')!=0
                or len(receipts)!=t.get('aggregate_record_count')):
                raise ValueError('TRADE_IDS_INCOMPLETE_DESPITE_FULL_REPLAY')
            seen=set();buy=D(0);sell=D(0)
            for tx in receipts:
                if not isinstance(tx,dict):raise ValueError('INVALID_TRADE_RECEIPT')
                tid=str(tx.get('trade_id',''))
                if not tid or tid in seen:raise ValueError('MISSING_OR_DUPLICATE_TRADE_ID')
                seen.add(tid)
                ms=tx.get('trade_ms')
                if (not isinstance(ms,int)
                    or ms<t.get('start_ms') or ms>t.get('end_ms')):
                    raise ValueError('TRADE_OUTSIDE_OBSERVED_WINDOW')
                if tx.get('side') not in ('buy','sell'):
                    raise ValueError('INVALID_TRADE_DIRECTION')
                px=decimal(tx.get('price'));qty=decimal(tx.get('token_qty'))
                q=decimal(tx.get('quote_notional'))
                if px<=0 or qty<=0 or px*qty!=q:
                    raise ValueError('TRADE_PRICE_QTY_NOTIONAL_MISMATCH')
                if tx['side']=='buy':buy+=q
                else:sell+=q
            if (buy!=decimal(t.get('recomputed_buy_quote'))
                or sell!=decimal(t.get('recomputed_sell_quote'))
                or buy!=decimal(item.get('decision_buy_quote'))
                or sell!=decimal(item.get('decision_sell_quote'))):
                raise ValueError('TRADE_REPLAY_TOTAL_MISMATCH')
        # "market_proof_replayable" in the old packet only means aggregate
        # receipts existed at freeze-time; the bounded packet could omit IDs.
        # Do not propagate that label as independent full replay.
        full=breplay and treplay and all_ids
        if full:verified+=1
        else:
            incomplete.append({'venue':item.get('venue'),'pair':item.get('pair'),
                'reason':('RECEIPT_LIST_TRUNCATED' if treplay and not all_ids
                          else 'BOOK_OR_TAPE_NOT_REPLAYABLE')})
        marked.append(bool(item.get('market_proof_replayable')))
    declared=packet.get('market_windows_replayable_count')
    # Former aggregate "replayable" count is not an assertion of complete IDs.
    if not isinstance(declared,int) or declared!=sum(marked):
        raise ValueError('PACKET_MARKET_COUNT_DISAGREEMENT')
    total=len(packet['spot_decision_inputs_and_bounded_replays'])
    status=(STATUS_COMPLETE if total>0 and verified==total else
            STATUS_PARTIAL if verified or total else STATUS_NONE)
    return {'event_id':pid,'coin':packet['coin'],'status':status,
        'market_windows_in_packet':total,
        'market_windows_fully_independently_replayed':verified,
        'missing_or_partial':incomplete,
        'chain_transactions_count':len(packet['chain_transactions']),
        'multiday_wallet_count':len(packet['multiday_wallet_evidence']),
        'packet_digest_verified':raw is not None and expected_sha is not None,
        'economic_outcome_or_accuracy_validated':False,
        'ChatGPT_push_verified':False,
        'no_complete_global_sellable_supply':True,
        'L2_cancellation_attribution_proven':False}

def validate_file(path,expected_event=None,sha=None):
    path=pathlib.Path(path)
    raw=path.read_bytes()
    if len(raw)>MAX_BYTES:raise ValueError('PACKET_TOO_LARGE')
    return validate(json.loads(raw),expected_event=expected_event,expected_sha=sha,raw=raw)
