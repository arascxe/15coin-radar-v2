"""Integrate spot, CEX inventory, DEX quote, temporal ordering; independent paths.
The outputs are evidence/urgency alerts, never proven profitable or executable trades.
"""
from __future__ import annotations
from .core import COINS, dec, D


def verdict(config,market,inventory,dex=None):
    dex=dex or {}
    m_by={sym:[] for sym in COINS}
    for x in market.get('markets',[]):m_by[x['coin']].append(x)
    inv_by={x['coin']:x for x in inventory.get('coins',[])}
    results=[]
    for sym in COINS:
        inv=inv_by.get(sym,{'status':'UNKNOWN','blocked':['MISSING_CHAIN_DATA']})
        v=m_by[sym]
        ready=[x for x in v if x.get('evidence_ready')]
        identity_pending=[x for x in v if x.get('candidate')]
        candidate_flow=inv.get('candidate_cex_out_raw','0')!='0'
        prep=inv.get('status')=='VERIFIED_RETENTION_PREPARATION'
        timing=False
        if prep and ready:
            last_prep_ms=max((y['last_block_ms'] for y in inv.get('independent_recipients',[])),default=None)
            # Preserve the actual 90-second evidence when it is the earliest phase.
            for x in ready:
                layer = x.get('layers',{}).get('early_90s',{})
                if not layer.get('ready'):
                    layer=x.get('layers',{}).get('confirmed_5m',{})
                t=layer.get('tape',x.get('tape',{}))
                start=t.get('window_start_ms');end=t.get('window_end_ms')
                if (isinstance(start,int) and isinstance(end,int) and isinstance(last_prep_ms,int) and
                    last_prep_ms<=start and end-last_prep_ms<=72*3600000):
                    timing=True
        if timing:
            state='COMBINED_PREP_THEN_SPOT_OBSERVED'
        elif ready:
            state='INDEPENDENT_SPOT_IGNITION_OBSERVED'
        elif prep:
            state='VERIFIED_INVENTORY_PREPARATION_OBSERVED'
        elif identity_pending:
            state='SPOT_MARKET_IDENTITY_REVIEW_URGENT'
        elif candidate_flow:
            state='CEX_FLOW_ATTRIBUTION_REVIEW'
        else:
            state='EVIDENCE_INSUFFICIENT_UNKNOWN'
        proof=[]
        for x in ready+identity_pending:
            layers=x.get('layers',{})
            phase='early_90s' if layers.get('early_90s',{}).get('ready') or layers.get('early_90s',{}).get('candidate') else 'confirmed_5m'
            source=layers.get(phase,x)
            tape=source.get('tape',{})
            book=source.get('book',{})
            proof.append({'venue':x['venue'],'pair':x['pair'], 'phase':phase,
                'buy_quote':tape.get('buy_quote'), 'sell_quote':tape.get('sell_quote'),
                'window_start_ms':tape.get('window_start_ms'),
                'window_end_ms':tape.get('window_end_ms'),
                'ask_depleted_fraction':book.get('fraction_depleted'),
                'book_from_ms':book.get('from_ms'), 'book_to_ms':book.get('to_ms'),
                'book_from_seq':book.get('from_seq'),'book_to_seq':book.get('to_seq'),
                'book_observed':book,'tape_observed':tape,
                'identity_verified':x['evidence_ready'],
                'blockers':source.get('blockers',[])})
        results.append({'coin':sym,'state':state,'actual_positive_evidence':bool(ready or prep),
            'preparation_proved':prep,'first_spot_pressure_observed':bool(ready),
            'temporal_sequence_proved':timing,
            'markets_evaluated':len(v),'material_spot_markets':len(ready),
            'coin_inventory':inv,'market_evidence':proof,
            'dex_pool':dex.get(sym,{'status':'NOT_OBSERVED'}),
            'predicts_10x_with_validated_precision':False,'recommend_buy':False,
            'unknown_not_negative':state=='EVIDENCE_INSUFFICIENT_UNKNOWN',
            'warning':'Observable spot depth is not total global sellable supply; off-book CEX stock is invisible.'})
    return {'schema':'radar-v2-integrated-evidence-1',
        'universe_exactly_15':tuple(x['coin'] for x in results)==COINS,
        'version':'0.9_LOCAL_UNDEPLOYED',
        'verified_predictive_10x_performance':None,'strong_buy_signals':0,
        'coins':results}
