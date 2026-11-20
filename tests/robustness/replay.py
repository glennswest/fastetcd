#!/usr/bin/env python3
# Replay a failed robustness scenario's client histories (#195): apply
# every acknowledged write (txn branch taken, lease revoke) at its
# revision and check every successful Range against the state at its
# revision. Reads at or past a revision no acknowledged write explains
# (a request whose outcome the client never saw) are skipped.
#
#   python3 tests/robustness/replay.py <report dir with client-*/operations.json>
#   IGNORE_VERSION=1 ... leaves Version out of the comparison.
#
# The report dirs come from run.sh's `==== artifacts` block:
#   sed -n '/^==== artifacts/,/^==== end artifacts/p' build.log | sed '1d;$d' | base64 -d | tar xz -C <dir>
import json,glob,sys,copy,os
d=sys.argv[1]; nov=os.environ.get('IGNORE_VERSION')=='1'
ops=[]
for f in sorted(glob.glob(d+'/client-*/operations.json')):
    for l in open(f):
        o=json.loads(l); o['f']=f.split('/')[-2]; ops.append(o)
ok=[o for o in ops if not o['Output'].get('Error')]
def branch(o):
    t=o['Input']['Txn']; return (t['OperationsOnFailure'] if o['Output']['Txn']['Failure'] else t['OperationsOnSuccess']) or []
def is_write(o):
    i=o['Input']
    if i['Type']=='leaseRevoke': return True
    return i['Type']=='txn' and any(x['Type']!='range-operation' for x in branch(o))
w=sorted([o for o in ok if is_write(o)], key=lambda o:o['Output']['Revision'])
state={}; keylease={}; hist={1:{}}
def strip(kvs): return [{k:v for k,v in x.items() if not(nov and k=='Version')} for x in (kvs or [])]
def rng(st,start,end,limit):
    ks=sorted(k for k in st if start<=k<end) if end else ([start] if start in st else [])
    kvs=[{'Key':k,'Value':st[k]['Value'],'ModRevision':st[k]['ModRevision'],'Version':st[k]['Version']} for k in ks]
    return strip(kvs[:limit] if limit else kvs)
cur=1; bad=0; shown=0
known=set()
for o in w:
    r=o['Output']['Revision']
    for x in range(cur+1,r):
        if x not in hist: hist[x]=None   # unknown
    if o['Input']['Type']=='leaseRevoke':
        lid=o['Input']['LeaseRevoke']['LeaseID']
        dele=[k for k,l in keylease.items() if l==lid and k in state]
        for k in dele: state.pop(k); keylease.pop(k)
        if not dele: continue   # no revision created
    else:
        for op in branch(o):
            if op['Type']=='put-operation':
                k=op['Put']['Key']; e=state.get(k)
                state[k]={'Value':op['Put']['Value'],'ModRevision':r,'Version':(e['Version']+1 if e else 1)}
                keylease[k]=op['Put']['LeaseID']
            elif op['Type']=='delete-operation':
                state.pop(op['Delete']['Key'],None); keylease.pop(op['Delete']['Key'],None)
    known.add(r); hist[r]=copy.deepcopy(state); cur=r
for o in ok:
    i=o['Input']
    if i['Type']!='range': continue
    R=i['Range']; at=R['Revision'] or o['Output']['Revision']
    # skip reads at or after an unknown revision we cannot model
    if any(hist.get(x,0) is None for x in range(2,at+1)): continue
    if at not in hist: continue
    want=rng(hist[at],R['Start'],R['End'],R['Limit'])
    got=strip(o['Output']['Range']['KVs'])
    if got!=want:
        bad+=1
        if shown<6:
            shown+=1
            gm={x['Key']:x for x in got}; wm={x['Key']:x for x in want}
            diff=[(k,gm.get(k),wm.get(k)) for k in sorted(set(gm)|set(wm)) if gm.get(k)!=wm.get(k)]
            print('RANGE MISMATCH',o['f'],'at',at,R['Start'],R['End'],'diff',diff[:4])
first_unknown=min([x for x,v in hist.items() if v is None] or [None]) if any(v is None for v in hist.values()) else None
print('writes',len(w),'first unknown rev',first_unknown,'mismatches',bad)
