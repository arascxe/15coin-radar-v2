"""Stage immutable V2 evidence files with ChatGPT inbox in the SAME Git commit.

Does not push, send messages or execute orders. GitHub workflow commits/pushes
only after every advertised evidence packet has passed file SHA and event checks.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import pathlib
import re
import shutil

SCHEMA='radar-v2-alarm-evidence-packet-1'
ID=re.compile(r'^[a-f0-9]{22}$')
SHA=re.compile(r'^[a-f0-9]{64}$')
MAX_FILE_BYTES=250000
REPO='arascxe/15coin-radar-v2'

def stage(inbox_path,source_dir,dest_dir,repo=REPO):
    inbox_path=pathlib.Path(inbox_path)
    fresh=pathlib.Path(source_dir)
    dest=pathlib.Path(dest_dir)
    dest.mkdir(parents=True,exist_ok=True)
    inbox=json.loads(inbox_path.read_text())
    if inbox.get('scope')!='ALL_15_MONITORED_NOT_ALL_15_VERIFIED':
        raise ValueError('WRONG_INBOX_SCOPE')
    if len(inbox.get('coin_states',{}))!=15:
        raise ValueError('INCOMPLETE_COIN_UNIVERSE')
    written=[]
    missing=[]
    for e in inbox.get('research_events_72h',[]):
        ref=e.get('evidence_packet',{})
        name=e.get('event_id')
        if not isinstance(name,str) or ID.fullmatch(name) is None:
            raise ValueError('UNSAFE_EVIDENCE_EVENT_ID')
        if not isinstance(ref,dict) or not ref.get('sha256'):
            # Historical inbox events may predate evidence packet support.
            continue
        expected='v2_state/evidence_packets/'+name+'.json'
        if ref.get('id')!=name or ref.get('path')!=expected:
            raise ValueError('EVENT_PACKET_PATH_MISMATCH')
        sha=ref.get('sha256')
        if not isinstance(sha,str) or SHA.fullmatch(sha) is None:
            raise ValueError('INVALID_PACKET_SHA')
        incoming=fresh/(name+'.json')
        target=dest/(name+'.json')
        source=incoming if incoming.is_file() else target
        if not source.is_file():
            ref['status']='NOT_PUBLISHED_PACKET_MISSING'
            ref['url']=None
            missing.append(name)
            continue
        raw=source.read_bytes()
        if len(raw)>MAX_FILE_BYTES or hashlib.sha256(raw).hexdigest()!=sha:
            raise ValueError('PACKET_BYTES_OR_SHA_MISMATCH')
        packet=json.loads(raw)
        if (packet.get('schema')!=SCHEMA or packet.get('event_id')!=name
            or packet.get('coin')!=e.get('coin')
            or packet.get('state')!=e.get('state')
            or packet.get('event_first_observed_ms')!=e.get('first_observed_ms')):
            raise ValueError('PACKET_EVENT_IDENTITY_MISMATCH')
        if target.is_file() and target.read_bytes()!=raw:
            raise ValueError('IMMUTABLE_PACKET_CONFLICT_EXISTING_GITHUB_FILE')
        if not target.is_file():
            shutil.copyfile(source,target)
            written.append(name)
        ref['status']='PACKET_VERIFIED_READY_FOR_ATOMIC_GITHUB_PUBLISH'
        ref['url']='https://github.com/'+repo+'/blob/main/'+expected
    tmp=inbox_path.with_name(inbox_path.name+'.tmp')
    tmp.write_text(json.dumps(inbox,indent=2,ensure_ascii=False)+'\n')
    tmp.replace(inbox_path)
    return {'events':len(inbox.get('research_events_72h',[])),
        'new_packets_staged':len(written),'older_packets_missing':len(missing),
        'GitHub_push_or_ChatGPT_delivery_proven':False}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--inbox',required=True)
    ap.add_argument('--source-dir',required=True)
    ap.add_argument('--dest-dir',required=True)
    args=ap.parse_args()
    print(json.dumps(stage(args.inbox,args.source_dir,args.dest_dir)))

if __name__=='__main__':main()
