"""Offline consistency check of a frozen alarm packet.

SHA mismatch => fail. Book/WS replays check inputs in packet, NOT independent
exchange authenticity. Blockchain links must be inspected against live explorers
for independent settlement confirmation; this tool makes no network requests.
"""
from __future__ import annotations
import argparse
from decimal import Decimal as D, InvalidOperation
import hashlib
import json
import pathlib
from .evidence_packet import SCHEMA,packet_id

def verify(raw,expected_sha):
    if hashlib.sha256(raw).hexdigest()!=expected_sha:
        raise ValueError('SHA256_PACKET_CONTENT_MISMATCH')
    p=json.loads(raw)
    if p.get('schema')!=SCHEMA:
        raise ValueError('WRONG_PACKET_SCHEMA')
    source={'coin':p.get('coin'),'evidence_state':p.get('state'),
            'first_seen_ms':p.get('event_first_observed_ms')}
    if p.get('event_id')!=packet_id(source):
        raise ValueError('PACKET_EVENT_ID_CANNOT_BE_REPRODUCED')
    states=[]
    for row in p.get('spot_decision_inputs_and_bounded_replays',[]):
        book=row.get('book_replay',{})
        trade=row.get('trade_replay',{})
        findings={'venue':row.get('venue'),'pair':row.get('pair'),
                  'book':'PARTIAL_OR_NOT_REPLAYABLE','trades':'PARTIAL_OR_NOT_REPLAYABLE'}
        if book.get('status')=='BOOK_ASK_LEVELS_REPLAYABLE':
            before=book['before_ask_price_token_qty_pairs']
            after=book['after_ask_price_token_qty_pairs']
            if not book.get('all_necessary_levels_included'):
                raise ValueError('BOOK_REPLAY_FALSE_COMPLETENESS')
            def total(levels):
                return sum((D(str(price))*D(str(qty)) for price,qty in levels),D(0))
            if (total(before)!=D(book['recomputed_before_quote'])
                or total(after)!=D(book['recomputed_after_quote'])):
                raise ValueError('BOOK_LEVEL_SUM_CONFLICT')
            dec=row.get('decision_book',{})
            if (D(dec['ask_before_quote'])!=total(before) or
                D(dec['ask_after_quote'])!=total(after)):
                raise ValueError('BOOK_SNAPSHOT_DECISION_MISMATCH')
            findings['book']='INTERNAL_LEVEL_REPLAY_CONSISTENT'
        if trade.get('status')=='RECEIVED_PUBLIC_TRADES_AGGREGATE_RECONCILED':
            if trade.get('all_received_trade_ids_in_packet'):
                tr=trade['trade_receipts_latest']
                buy=sum((D(x['quote_notional']) for x in tr if x['side']=='buy'),D(0))
                sell=sum((D(x['quote_notional']) for x in tr if x['side']=='sell'),D(0))
                if (buy!=D(trade['recomputed_buy_quote']) or
                    sell!=D(trade['recomputed_sell_quote']) or
                    len(tr)!=trade['received_count']):
                    raise ValueError('PUBLIC_TRADE_REPLAY_MISMATCH')
                findings['trades']='INTERNAL_PUBLIC_WS_TRADE_SAMPLE_REPLAY_CONSISTENT'
            else:
                findings['trades']='TRUNCATED_RECEIPT_SAMPLE_NOT_ALL_TRADE_IDS'
        states.append(findings)
    wallets=[]
    for w in p.get('multiday_wallet_evidence',[]):
        start=w.get('start_balance_token')
        end=w.get('end_balance_token')
        delta=w.get('net_retained_increase_token')
        if start is not None and end is not None and delta is not None:
            try:
                if D(str(end))-D(str(start))!=D(str(delta)):
                    raise ValueError('WALLET_NET_RETAINED_BALANCE_CONFLICT')
            except InvalidOperation as exc:
                raise ValueError('INVALID_WALLET_QUANTITY') from exc
        wallets.append({'address':w.get('address'),
            'snapshot_net_balance_math':'CONSISTENT' if start is not None and end is not None else 'NO_COMPLETE_BALANCE_PAIR',
            'transfers_independently_rechecked':False})
    return {'event_id':p['event_id'],'coin':p['coin'],
        'content_sha256_verified':True,
        'markets':states,'wallets':wallets,
        'scope':'INTERNAL_REPLAY_AND_HASH_ONLY_NOT_INDEPENDENT_EXTERNAL_VALIDATION',
        'DEX_pool_state_independently_rechecked':False,
        'CEX_trade_authenticity_independently_rechecked':False,
        'chain_transactions_independently_rechecked':False,
        'predictive_reliability_or_buy_signal_established':False}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--packet',required=True)
    ap.add_argument('--sha256',required=True)
    args=ap.parse_args()
    print(json.dumps(verify(pathlib.Path(args.packet).read_bytes(),args.sha256),
                     ensure_ascii=False,indent=2))

if __name__=='__main__':main()
