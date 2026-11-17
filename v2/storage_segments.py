#!/usr/bin/env python3
"""Lossless append-only, checksummed SQLite change segments for Git persistence.

No external service, no secrets, no implicit pruning. A snapshot stays immutable;
only changed/deleted rows are written each run. A complete semantic digest
protects all tables (except transient run_lease).
"""
import argparse,gzip,hashlib,json,pathlib,sqlite3,shutil,sys
from backup_restore import create_backup,restore_backup

EXCLUDED={'run_lease'}
def dumps(x):
    return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)
def tables(c):
    return {name:sql for name,sql in c.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name") if name not in EXCLUDED}
def pk(c,table):
    cols=c.execute('PRAGMA table_info("'+table+'")').fetchall()
    keys=[x[1] for x in sorted(cols,key=lambda x:x[5]) if x[5]]
    if not keys:raise RuntimeError('TABLE_WITHOUT_PRIMARY_KEY:'+table)
    return keys
def rows(c,table):
    keys=pk(c,table);columns=[x[1] for x in c.execute('PRAGMA table_info("'+table+'")')]
    quoted=','.join('"'+x+'"' for x in keys)
    data=c.execute('SELECT * FROM "'+table+'" ORDER BY '+quoted)
    return keys,columns,list(data)
def semantic_digest(path):
    c=sqlite3.connect(path)
    try:
        if c.execute('PRAGMA quick_check').fetchone()[0]!='ok':raise RuntimeError('CORRUPT_DB')
        h=hashlib.sha256();counts={}
        for table,sql in tables(c).items():
            key,cols,records=rows(c,table)
            counts[table]=len(records)
            h.update((dumps([table,sql,cols,key])+'\n').encode())
            for row in records:h.update((dumps(row)+'\n').encode())
        return {'sha256':h.hexdigest(),'counts':counts}
    finally:c.close()
def build_segment(before,after,out):
    a=sqlite3.connect(before);b=sqlite3.connect(after);change={}
    try:
        old=tables(a);new=tables(b)
        for table,sql in new.items():
            if table not in old:raise RuntimeError('SCHEMA_CHANGE_REQUIRES_NEW_ANCHOR:'+table)
            if sql!=old[table]:raise RuntimeError('SCHEMA_CHANGE_REQUIRES_NEW_ANCHOR:'+table)
            keys,cols,arows=rows(a,table)
            keys2,cols2,brows=rows(b,table)
            if keys!=keys2 or cols!=cols2:raise RuntimeError('COLUMN_MISMATCH:'+table)
            indices=[cols.index(x) for x in keys]
            oldmap={tuple(row[i] for i in indices):row for row in arows}
            newmap={tuple(row[i] for i in indices):row for row in brows}
            up=[list(v) for k,v in newmap.items() if k not in oldmap or oldmap[k]!=v]
            delete=[list(k) for k in oldmap if k not in newmap]
            if up or delete:change[table]={'keys':keys,'cols':cols,'up':up,'delete':delete}
        if set(old)!=set(new):raise RuntimeError('SCHEMA_CHANGE_REQUIRES_NEW_ANCHOR')
    finally:a.close();b.close()
    payload={'schema':'radar-v2-segment-1','changes':change,'after':semantic_digest(after)}
    raw=dumps(payload).encode();compressed=gzip.compress(raw,compresslevel=7,mtime=0)
    output=pathlib.Path(out);output.parent.mkdir(parents=True,exist_ok=True)
    output.write_bytes(compressed)
    return {'file':str(output),'sha256':hashlib.sha256(compressed).hexdigest(),'bytes':len(compressed),'changes':{k:{'up':len(v['up']),'delete':len(v['delete'])} for k,v in change.items()},'after':payload['after']}
def replay(db,segment,expected_hash):
    data=pathlib.Path(segment).read_bytes()
    if hashlib.sha256(data).hexdigest()!=expected_hash:raise RuntimeError('SEGMENT_SHA_MISMATCH')
    payload=json.loads(gzip.decompress(data))
    if payload['schema']!='radar-v2-segment-1':raise RuntimeError('SEGMENT_SCHEMA_MISMATCH')
    c=sqlite3.connect(db)
    try:
        c.execute('BEGIN IMMEDIATE')
        for table,op in payload['changes'].items():
            if table not in tables(c):raise RuntimeError('MISSING_REPLAY_TABLE:'+table)
            keys,cols,_=rows(c,table)
            if keys!=op['keys'] or cols!=op['cols']:raise RuntimeError('REPLAY_COLUMN_MISMATCH:'+table)
            where=' AND '.join('"'+x+'"=?' for x in keys)
            c.executemany('DELETE FROM "'+table+'" WHERE '+where,op['delete'])
            placeholders=','.join('?' for _ in cols)
            c.executemany('INSERT OR REPLACE INTO "'+table+'" VALUES('+placeholders+')',op['up'])
        c.commit()
    except BaseException:c.rollback();raise
    finally:c.close()
    result=semantic_digest(db)
    if result!=payload['after']:raise RuntimeError('SEGMENT_RESULT_DIGEST_MISMATCH')
    return result
def restore(root,dest):
    root=pathlib.Path(root);dest=pathlib.Path(dest)
    manifest=json.loads((root/'manifest.json').read_text())
    if manifest['schema']!='radar-v2-storage-1':raise RuntimeError('MANIFEST_SCHEMA_MISMATCH')
    if len(manifest['segments'])!=len(set(s['path'] for s in manifest['segments'])):raise RuntimeError('DUPLICATE_SEGMENT')
    anchor=root/'anchor'/'checkpoint.sqlite.gz'
    restore_backup(anchor,dest)
    state=semantic_digest(dest)
    if state!=manifest['anchor_digest']:raise RuntimeError('ANCHOR_DIGEST_MISMATCH')
    for item in manifest['segments']:
        state=replay(dest,root/item['path'],item['sha256'])
    if state!=manifest['tip_digest']:raise RuntimeError('TIP_DIGEST_MISMATCH')
    return {'segments':len(manifest['segments']),'digest':state,'database':str(dest)}
def migrate(old_archive,root):
    root=pathlib.Path(root);root.mkdir(parents=True,exist_ok=True)
    if (root/'manifest.json').exists():raise RuntimeError('MANIFEST_EXISTS')
    a=root/'anchor'/'checkpoint.sqlite.gz';a.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(old_archive,a);shutil.copy2(str(old_archive)+'.sha256.json',str(a)+'.sha256.json')
    tmp=root/'_validate_anchor.sqlite3'
    try:
        restore_backup(a,tmp)
        dig=semantic_digest(tmp)
    finally:tmp.unlink(missing_ok=True)
    manifest={'schema':'radar-v2-storage-1','anchor_digest':dig,'segments':[],'tip_digest':dig}
    (root/'manifest.json').write_text(dumps(manifest)+'\n')
    return manifest
def append(root,before,after):
    root=pathlib.Path(root);mf=root/'manifest.json'
    manifest=json.loads(mf.read_text())
    bef=semantic_digest(before)
    if bef!=manifest['tip_digest']:raise RuntimeError('PRECOLLECTION_TIP_MISMATCH')
    n=len(manifest['segments'])+1
    name=f'segments/{n:07d}.json.gz'
    info=build_segment(before,after,root/name)
    # Independently reconstruct and compare every table before publishing.
    temp=root/'_validate_full.sqlite3'
    try:
        verified=restore(root,temp)
        existing=semantic_digest(temp)
    finally:temp.unlink(missing_ok=True)
    if existing!=bef:raise RuntimeError('PREAPPEND_REPLAY_MISMATCH')
    replay(temp if False else after,root/name,info['sha256']) if False else None
    # Reconstruct again through the proposed segment by updating an isolated restore.
    temp=root/'_validate_new.sqlite3'
    try:
        restore(root,temp)
        actual=replay(temp,root/name,info['sha256'])
        if actual!=info['after']:raise RuntimeError('PROPOSED_TIP_MISMATCH')
    finally:temp.unlink(missing_ok=True)
    manifest['segments'].append({'path':name,'sha256':info['sha256'],'bytes':info['bytes']})
    manifest['tip_digest']=info['after']
    mf.write_text(dumps(manifest)+'\n')
    return info
def main():
    p=argparse.ArgumentParser();sp=p.add_subparsers(dest='cmd',required=True)
    x=sp.add_parser('migrate');x.add_argument('checkpoint');x.add_argument('root')
    x=sp.add_parser('restore');x.add_argument('root');x.add_argument('database')
    x=sp.add_parser('append');x.add_argument('root');x.add_argument('before');x.add_argument('after')
    args=p.parse_args()
    output=migrate(args.checkpoint,args.root) if args.cmd=='migrate' else restore(args.root,args.database) if args.cmd=='restore' else append(args.root,args.before,args.after)
    print(json.dumps(output,indent=2,ensure_ascii=False))
if __name__=='__main__':main()
