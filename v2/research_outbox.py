#!/usr/bin/env python3
"""Deduplicated research inbox only; NO automatic notification or trade action.

A new watch becomes a candidate research event once per active episode. It is
NOT a buy alert. The persistent state is small and uses no additional service.
If ChatGPT delivery is later explicitly authorized, a separate consumer must
recheck data, permissions and delivery acknowledgements before claiming success.
"""
import argparse,datetime,json,pathlib
from decision_engine import EXPECTED,SCHEMA as DECISION_SCHEMA
MAX_HISTORY=120
VERSION='radar-v2-research-inbox-1'

def produce(decision,prior=None):
    if decision.get('schema')!=DECISION_SCHEMA or decision.get('coin_count')!=15:
        raise ValueError('WRONG_15_COIN_DECISIONS')
    bycoin={x['coin']:x for x in decision['coins']}
    if set(bycoin)!=set(EXPECTED):raise ValueError('MISSING_OR_DUPLICATE_COIN')
    if prior is not None and prior.get('schema')!=VERSION:
        raise ValueError('UNSAFE_INCOMPATIBLE_PREVIOUS_INBOX')
    if prior and prior.get('asof_ms',0)>decision['asof_ms']:
        raise ValueError('FAIL_CLOSED_BACKWARDS_TIME')
    previous=prior.get('active',{}) if prior else {}
    hist=list(prior.get('history',[])) if prior else []
    events=[];active={};repeats=0
    for symbol in EXPECTED:
        row=bycoin[symbol]
        if row['watch_research_only'] is not True:continue
        key=row['research_dedupe_key']
        state=row['state']
        if previous.get(symbol,{}).get('research_dedupe_key')==key:
            active[symbol]=previous[symbol]
            repeats+=1
            continue
        new={'coin':symbol,'state':state,'research_dedupe_key':key,
             'first_observed_asof_ms':decision['asof_ms'],
             'first_observed_run_id':decision['run_id'],
             'not_a_trade_signal':True,
             'evidence':{'preparation':row['pathways']['preparation'],
                'spot_ignition':row['pathways']['spot_ignition']},
             'limitations':row['evidence_limitations']}
        events.append(new)
        active[symbol]=new
        hist.append(new)
    return {'schema':VERSION,
       'generated_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
       'run_id':decision['run_id'],'asof_ms':decision['asof_ms'],
       'active':active,'new_research_events':events,
       'new_research_event_count':len(events),
       'deduplicated_repeats':repeats,'history':hist[-MAX_HISTORY:],
       'strong_alert_count':0,'buy_signal':False,
       'chatgpt_delivery_status':'NOT_CONFIGURED_NO_DELIVERY_TEST',
       'automatic_notifications_sent':0,
       'meaning':'Events are for research review only, never buy recommendation or proof of delivery.'}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--decision',required=True);p.add_argument('--prior',required=True)
    p.add_argument('--out',required=True)
    a=p.parse_args();d=json.loads(pathlib.Path(a.decision).read_text())
    previous=json.loads(pathlib.Path(a.prior).read_text()) if pathlib.Path(a.prior).exists() else None
    report=produce(d,previous)
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    tmp=target.with_suffix(target.suffix+'.tmp')
    tmp.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n');tmp.replace(target)
    print(json.dumps({'new_research_events':report['new_research_event_count'],
        'active':len(report['active']),'deduplicated':report['deduplicated_repeats'],
        'notifications_sent':0}))
if __name__=='__main__':main()
