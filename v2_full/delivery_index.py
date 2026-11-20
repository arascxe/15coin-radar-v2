"""Publish a small, auditable V2 alert inbox for ChatGPT-only delivery.

GitHub Actions cannot call ChatGPT push directly. This JSON is a durable
read-only handoff to the existing authorized hourly ChatGPT monitor.
"""
from __future__ import annotations
import argparse, datetime, hashlib, json, pathlib

SEVERITY={
    'FUSION_PREPARATION_THEN_SPOT_IGNITION_RESEARCH':'EARLY_PREPARATION_THEN_SPOT_RESEARCH',
    'FUSION_SPOT_IGNITION_WITH_PREPARATION_TIMING_UNKNOWN':'EARLY_SPOT_AND_PREPARATION_TIMING_UNKNOWN',
    'FUSION_SPOT_IGNITION_RESEARCH':'EARLY_SPOT_IGNITION_RESEARCH',
    'FUSION_PREPARATION_RESEARCH':'OBSERVED_ACCUMULATION_PREPARATION_RESEARCH',
    'FUSION_DEX_ACTIVE_LIQUIDITY_REDUCTION_REVIEW':'DEX_POOL_LIQUIDITY_SCOPE_REVIEW',
    'COMBINED_PREP_THEN_SPOT_OBSERVED':'EARLY_COMBINED_ECONOMIC_EVENT',
    'INDEPENDENT_SPOT_IGNITION_OBSERVED':'EARLY_INDEPENDENT_SPOT_EVENT',
    'VERIFIED_INVENTORY_PREPARATION_OBSERVED':'VERIFIED_CEX_INVENTORY_WATCH',
    'SPOT_MARKET_IDENTITY_REVIEW_URGENT':'UNVERIFIED_IDENTITY_REVIEW',
    'CEX_FLOW_ATTRIBUTION_REVIEW':'UNVERIFIED_CUSTODY_REVIEW',
    'UNVERIFIED_CLUSTER_ACCUMULATION_RESEARCH':'UNVERIFIED_ONCHAIN_CLUSTER_REVIEW',
    'UNVERIFIED_CLUSTER_WITH_SPOT_RESEARCH':'UNVERIFIED_ONCHAIN_CLUSTER_AND_SPOT_REVIEW',
    'UNEXPLAINED_LARGE_TRANSFER_REVIEW':'UNEXPLAINED_TRANSFER_REVIEW',
}

def verify_packet(event,ident,packet_dir):
    """Verify immutable local packet SHA and identity; never claim GitHub push yet."""
    pid=event.get('evidence_packet_id')
    sha=event.get('evidence_packet_sha256')
    item={'id':ident,'status':'NOT_PRODUCED_IN_LEGACY_EVENT',
          'sha256':None,'url':None,'path':None}
    if pid is None:return item
    if pid!=ident or not isinstance(sha,str) or len(sha)!=64:
        item['status']='INVALID_ID_OR_DIGEST'
        return item
    path='v2_state/evidence_packets/'+ident+'.json'
    item.update({'sha256':sha,'path':path,
                 'status':'PACKET_STATED_BUT_NOT_RECHECKED'})
    if packet_dir is None:return item
    target=pathlib.Path(packet_dir)/(ident+'.json')
    if not target.is_file():
        item['status']='LOCAL_PACKET_NOT_AVAILABLE_MAY_BE_PREVIOUS_RUN'
        return item
    try:
        raw=target.read_bytes()
        packet=json.loads(raw)
        consistent=(hashlib.sha256(raw).hexdigest()==sha and
           packet.get('schema')=='radar-v2-alarm-evidence-packet-1' and
           packet.get('event_id')==ident and
           packet.get('coin')==event.get('coin') and
           packet.get('state')==event.get('evidence_state') and
           packet.get('event_first_observed_ms')==event.get('first_seen_ms'))
        item['status']='VERIFIED_LOCAL_PACKET_READY_FOR_GITHUB_PUBLISH' if consistent else 'PACKET_SHA_OR_EVENT_IDENTITY_MISMATCH'
    except (OSError,ValueError,TypeError):
        item['status']='UNREADABLE_PACKET'
    return item

def build(report,outbox,workflow_id=None,now=None,packet_dir=None):
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
        onchain=event.get('onchain_economics',{})
        if not isinstance(onchain,dict):onchain={}
        fusion=event.get('unified_economic_evidence',{})
        if not isinstance(fusion,dict):fusion={}
        packet=verify_packet(event,ident,packet_dir)
        entries.append({
            'event_id':ident,'evidence_packet':packet,'coin':coin,'state':state,'urgency':SEVERITY[state],
            'first_observed_ms':observed,
            'age_minutes_at_publication':round((now-observed)/60000,1),
            'evidence_only_not_10x_prediction':True,
            'verified_combined':state=='COMBINED_PREP_THEN_SPOT_OBSERVED',
            'unified_economic_evidence':fusion,
            'evidence_summary':{
                'economic_classification':onchain.get('classified_transfer_counts',{}),
                'onchain_research_state':onchain.get('research_state'),
                'balance_verified_eoa_receivers_unknown_owner':onchain.get('balance_verified_eoa_receiver_count_unknown_owner',0),
                'recent_confirmed_token_accumulation':onchain.get('accumulation_token_quantity'),
                'fraction_of_total_supply_not_circulating':onchain.get('fraction_of_total_onchain_supply'),
                'fraction_of_visible_ask_depth_proxy':onchain.get('accumulation_to_visible_ask_ratio'),
                'unexplained_transfer':onchain.get('unexplained_transfer'),
                'recipient_evidence':onchain.get('recipient_evidence',[])[:3],
            },
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
        'schema':'radar-v2-chatgpt-readonly-inbox-2',
        'published_event_unix_ms':now,
        'generated_utc':datetime.datetime.fromtimestamp(now/1000,datetime.timezone.utc).isoformat(),
        'github_workflow_run_id':str(workflow_id) if workflow_id is not None else None,
        'source_kind':'LIVE_READ_ONLY_RUNNER_SNAPSHOT',
        'delivery_status':'NOT_SENT_BY_GITHUB_ACTIONS',
        'scope':'ALL_15_MONITORED_NOT_ALL_15_VERIFIED',
        'delivery_channel':'CHATGPT_TASK_ONLY_NO_EXTERNAL_PUSH',
        'verified_exchange_source_address_count':report.get('verified_exchange_source_addresses',0),
        'chain_health':fresh,
        'coin_states':{c['coin']:c['state'] for c in coins},
        'stream_markets':len(report.get('stream_health',[])),
        'research_events_72h':entries,
        'onchain_engine_health':report.get('onchain_economic_engine_status','UNAVAILABLE'),
        'unified_economic_fusion_status':report.get('unified_economic_fusion',{}),
        'unified_economic_states':{
            c['coin']:c.get('unified_economic_evidence',{}).get('phase','NOT_EVALUATED')
            for c in coins},
        'onchain_research_states':{c['coin']:c.get('onchain_economics',{}).get('research_state','UNKNOWN') for c in coins},
        'note':'Only the pre-existing ChatGPT application task can generate a notification. No Telegram, email, SMS or external message is sent.'}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--report',required=True);p.add_argument('--outbox',required=True)
    p.add_argument('--out',required=True);p.add_argument('--run-id',default=None)
    p.add_argument('--packet-dir',default=None)
    a=p.parse_args()
    src=json.loads(pathlib.Path(a.report).read_text())
    box=json.loads(pathlib.Path(a.outbox).read_text())
    result=build(src,box,a.run_id,packet_dir=a.packet_dir)
    target=pathlib.Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    temp=target.with_name(target.name+'.tmp')
    temp.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
    temp.replace(target)
    print(json.dumps({'coins':len(result['coin_states']),
                      'research_episodes':len(result['research_events_72h']),
                      'delivery_status':result['delivery_status'],
                      'chain_health':result['chain_health']}))

if __name__=='__main__':main()
