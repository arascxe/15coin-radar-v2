"""Publish a small, auditable V2 alert inbox for ChatGPT/Telegram delivery.

GitHub Actions cannot call ChatGPT push directly. This JSON is a durable
read-only handoff to the existing authorized hourly ChatGPT monitor.
"""
from __future__ import annotations
import argparse, datetime, hashlib, json, pathlib

SEVERITY={
    'COMBINED_PREP_THEN_SPOT_OBSERVED':'EARLY_COMBINED_ECONOMIC_EVENT',
    'INDEPENDENT_SPOT_IGNITION_OBSERVED':'EARLY_INDEPENDENT_SPOT_EVENT',
    'VERIFIED_INVENTORY_PREPARATION_OBSERVED':'VERIFIED_CEX_INVENTORY_WATCH',
    'SPOT_MARKET_IDENTITY_REVIEW_URGENT':'UNVERIFIED_IDENTITY_REVIEW',
    'CEX_FLOW_ATTRIBUTION_REVIEW':'UNVERIFIED_CUSTODY_REVIEW',
}

def build(report,outbox,workflow_id=None,now=None):
    now=int(now if now is not None else report.get('asof_ms',0))
    if not isinstance(now,int) or now<=0:raise ValueError('INVALID_OBSERVATION_TIME')
    coins=report.get('coins',[])
    if len(coins)!=15 or len({c['coin'] for c in coins})!=15:
        raise ValueError('INCOMPLETE_15_COIN_REPORT')
    current={c['coin']:c for c in coins}
    entries=[]
    events=outbox.get('events',[])
    for event in events[-500:]:
        coin=event.get('coin');state=event.get('evidence_state')
        observed=event.get('first_seen_ms')
        if coin not in current or state not in SEVERITY or not isinstance(observed,int):
            continue
        if observed>now+60000 or now-observed>72*3600000:continue
        ident=hashlib.sha256(
            (coin+'|'+state+'|'+str(observed)).encode()).hexdigest()[:22]
        entries.append({
            'event_id':ident,'coin':coin,'state':state,'urgency':SEVERITY[state],
            'first_observed_ms':observed,
            'age_minutes_at_publication':round((now-observed)/60000,1),
            'evidence_only_not_10x_prediction':True,
            'verified_combined':state=='COMBINED_PREP_THEN_SPOT_OBSERVED',
            'verification_limitations':[
                'Past performance uncalibrated; not a buy instruction.',
                'Observed ask depth is not total CEX/DEX sellable inventory.']})
    entries=sorted({e['event_id']:e for e in entries}.values(),
                   key=lambda e:(e['first_observed_ms'],e['event_id']))[-100:]
    health=report.get('chain_health',{})
    fresh=[]
    for ch in ('bsc','base'):
        h=health.get(ch,{})
        fresh.append({'chain':ch,'status':h.get('status','MISSING'),
                      'safe_head':h.get('safe_head'),
                      'complete':h.get('complete',False)})
    return {
        'schema':'radar-v2-telegram-chatgpt-readonly-inbox-1',
        'published_event_unix_ms':now,
        'generated_utc':datetime.datetime.fromtimestamp(now/1000,datetime.timezone.utc).isoformat(),
        'github_workflow_run_id':str(workflow_id) if workflow_id is not None else None,
        'source_kind':'LIVE_READ_ONLY_RUNNER_SNAPSHOT',
        'delivery_status':'NOT_SENT_BY_GITHUB_ACTIONS',
        'scope':'ALL_15_MONITORED_NOT_ALL_15_VERIFIED',
        'verified_exchange_source_address_count':report.get('verified_exchange_source_addresses',0),
        'chain_health':fresh,
        'coin_states':{c['coin']:c['state'] for c in coins},
        'stream_markets':len(report.get('stream_health',[])),
        'research_events_72h':entries,
        'note':'A ChatGPT task must check freshness, compare event_id to prior deliveries, and send via its connected notification tool.'}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--report',required=True);p.add_argument('--outbox',required=True)
    p.add_argument('--out',required=True);p.add_argument('--run-id',default=None)
    a=p.parse_args()
    src=json.loads(pathlib.Path(a.report).read_text())
    box=json.loads(pathlib.Path(a.outbox).read_text())
    result=build(src,box,a.run_id)
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    temp=target.with_name(target.name+'.tmp')
    temp.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
    temp.replace(target)
    print(json.dumps({'coins':len(result['coin_states']),
                      'research_episodes':len(result['research_events_72h']),
                      'delivery_status':result['delivery_status'],
                      'chain_health':result['chain_health']}))

if __name__=='__main__':main()
