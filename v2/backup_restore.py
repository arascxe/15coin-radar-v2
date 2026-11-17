#!/usr/bin/env python3
"""Transactional SQLite backup, SHA256 verification and fail-closed restore."""
import argparse,gzip,hashlib,json,os,pathlib,sqlite3,tempfile,datetime

def sha256(b):return hashlib.sha256(b).hexdigest()
def check(db_path):
    c=sqlite3.connect(f'file:{pathlib.Path(db_path).resolve().as_posix()}?mode=ro',uri=True)
    try:
        status=c.execute('PRAGMA quick_check').fetchone()[0]
        if status!='ok':raise RuntimeError(f'CORRUPT_DB:{status}')
        return c.execute('SELECT COUNT(*) FROM market_candles').fetchone()[0] if c.execute("SELECT name FROM sqlite_master WHERE name='market_candles'").fetchone() else 0
    finally:c.close()

def create_backup(db_path,out_path):
    db_path=pathlib.Path(db_path);out_path=pathlib.Path(out_path);out_path.parent.mkdir(parents=True,exist_ok=True)
    if not db_path.exists():raise FileNotFoundError(db_path)
    with tempfile.TemporaryDirectory() as d:
        tmp=pathlib.Path(d)/'snapshot.sqlite3';src=sqlite3.connect(f'file:{db_path.resolve().as_posix()}?mode=ro',uri=True)
        target=sqlite3.connect(tmp)
        try:src.backup(target)
        finally:target.close();src.close()
        rows=check(tmp);raw=tmp.read_bytes()
    data=gzip.compress(raw,compresslevel=6,mtime=0)
    st=out_path.with_suffix(out_path.suffix+'.tmp');st.write_bytes(data);os.replace(st,out_path)
    manifest={'schema':'radar-backup-v1','snapshot_bytes':len(raw),'compressed_bytes':len(data),'snapshot_sha256':sha256(raw),
              'compressed_sha256':sha256(data),'snapshot_candle_rows':rows,'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    man=out_path.with_suffix(out_path.suffix+'.sha256.json');man.write_text(json.dumps(manifest,indent=2)+'\n');return manifest

def restore_backup(backup_path,to_path):
    backup_path=pathlib.Path(backup_path);to_path=pathlib.Path(to_path)
    if to_path.exists():raise FileExistsError('REFUSE_OVERWRITE_EXISTING_DB')
    manifest=json.loads(backup_path.with_suffix(backup_path.suffix+'.sha256.json').read_text())
    compressed=backup_path.read_bytes()
    if sha256(compressed)!=manifest['compressed_sha256']:raise ValueError('COMPRESSED_CHECKSUM_MISMATCH')
    raw=gzip.decompress(compressed)
    if sha256(raw)!=manifest['snapshot_sha256']:raise ValueError('SNAPSHOT_CHECKSUM_MISMATCH')
    to_path.parent.mkdir(parents=True,exist_ok=True)
    temp=to_path.with_suffix('.restore-tmp');temp.write_bytes(raw)
    try:
        count=check(temp)
        if count!=manifest['snapshot_candle_rows']:raise ValueError('RESTORED_ROW_COUNT_MISMATCH')
        os.replace(temp,to_path)
    except BaseException:
        temp.unlink(missing_ok=True);raise
    return {'restored':str(to_path),'candle_rows':count,'checksum_verified':True}

if __name__=='__main__':
    p=argparse.ArgumentParser();cmd=p.add_subparsers(dest='command',required=True)
    a=cmd.add_parser('backup');a.add_argument('database');a.add_argument('output')
    b=cmd.add_parser('restore');b.add_argument('archive');b.add_argument('destination')
    args=p.parse_args()
    print(json.dumps(create_backup(args.database,args.output) if args.command=='backup' else restore_backup(args.archive,args.destination),indent=2))
