import urllib.request, urllib.parse, json, math, statistics, hashlib, random, time
from datetime import datetime, timezone, timedelta

BASE='https://data.binance.vision/data/futures/um'
SYMBOL='ETHUSDT'
NOW=datetime.now(timezone.utc)
NOW_MS=int(NOW.timestamp()*1000)
EVAL_DAYS=365

import io, csv, zipfile, calendar

def _norm_ts(v):
    x=int(v)
    return x//1000 if x > 10**15 else x

def _download(url, retries=5):
    last=None
    for k in range(retries):
        try:
            req=urllib.request.Request(url, headers={'User-Agent':'OpenAI-ETH-Structure-Replay/1.0'})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last=e
        except Exception as e:
            last=e
        time.sleep(1.5*(k+1))
    if last:
        raise last
    return None

def _archive_rows(relpath):
    url=f"{BASE}/{relpath}"
    body=_download(url)
    if body is None:
        return [], False
    checksum_body=_download(url+'.CHECKSUM')
    checksum_ok=False
    if checksum_body:
        expected=checksum_body.decode().strip().split()[0].lower()
        checksum_ok=(hashlib.sha256(body).hexdigest().lower()==expected)
        if not checksum_ok:
            raise RuntimeError(f'Checksum mismatch: {relpath}')
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names=[n for n in z.namelist() if not n.endswith('/')]
        if len(names)!=1:
            raise RuntimeError(f'Unexpected archive members: {relpath}: {names}')
        text=z.read(names[0]).decode('utf-8')
    out=[]
    for row in csv.reader(io.StringIO(text)):
        if not row:
            continue
        try:
            ot=_norm_ts(row[0])
            ct=_norm_ts(row[6])
            float(row[1]); float(row[2]); float(row[3]); float(row[4])
        except Exception:
            continue
        rr=list(row)
        rr[0]=ot; rr[6]=ct
        if ct < NOW_MS:
            out.append(rr)
    return out, checksum_ok

def _month_iter(start_year=2019, start_month=11):
    y,m=start_year,start_month
    cur_first=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    while datetime(y,m,1,tzinfo=timezone.utc) < cur_first:
        yield y,m
        m+=1
        if m==13:
            y+=1; m=1

def fetch_all(interval):
    out=[]
    archives=0
    checksummed=0
    missing_archives=[]
    for y,m in _month_iter():
        rel=f"monthly/klines/{SYMBOL}/{interval}/{SYMBOL}-{interval}-{y:04d}-{m:02d}.zip"
        rows,ck=_archive_rows(rel)
        if rows:
            out.extend(rows); archives+=1; checksummed+=int(ck)
        else:
            missing_archives.append(rel)
    first=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc).date()
    d=first
    today=NOW.date()
    while d < today:
        rel=f"daily/klines/{SYMBOL}/{interval}/{SYMBOL}-{interval}-{d.isoformat()}.zip"
        rows,ck=_archive_rows(rel)
        if rows:
            out.extend(rows); archives+=1; checksummed+=int(ck)
        else:
            missing_archives.append(rel)
        d += timedelta(days=1)
    data={int(r[0]):r for r in out}
    rows=[data[k] for k in sorted(data)]
    if not rows:
        raise RuntimeError(f'No official public archive data for {interval}')
    return rows, {'archives':archives,'checksummed':checksummed,'missing_archive_count':len(missing_archives),
                  'missing_archive_examples':missing_archives[:8]}


def bars(rows):
    return [dict(open_time=int(r[0]), open=float(r[1]), high=float(r[2]), low=float(r[3]), close=float(r[4]), close_time=int(r[6])) for r in rows]


def canonical_atr(bs, n=14):
    tr=[]
    for i,b in enumerate(bs):
        if i==0:
            x=b['high']-b['low']
        else:
            pc=bs[i-1]['close']
            x=max(b['high']-b['low'], abs(b['high']-pc), abs(b['low']-pc))
        tr.append(x)
    atr=[None]*len(bs)
    if len(bs)>=n:
        atr[n-1]=sum(tr[:n])/n
        for i in range(n,len(bs)):
            atr[i]=(atr[i-1]*(n-1)+tr[i])/n
    return atr


def fractal_candidates(bs):
    by_confirm={}
    for i in range(2,len(bs)-2):
        win=range(i-2,i+3)
        maxh=max(bs[j]['high'] for j in win)
        minh=min(bs[j]['low'] for j in win)
        hidx=min(j for j in win if bs[j]['high']==maxh)
        lidx=min(j for j in win if bs[j]['low']==minh)
        isH=(i==hidx)
        isL=(i==lidx)
        if not (isH or isL): continue
        ci=i+2
        arr=by_confirm.setdefault(ci,[])
        if isH:
            arr.append({'type':'H','pivot_i':i,'price':bs[i]['high'],'pivot_time':bs[i]['close_time'],'confirm_i':ci,'confirm_time':bs[ci]['close_time']})
        if isL:
            arr.append({'type':'L','pivot_i':i,'price':bs[i]['low'],'pivot_time':bs[i]['close_time'],'confirm_i':ci,'confirm_time':bs[ci]['close_time']})
    return by_confirm


def more_extreme(c, p):
    if c['type']=='H': return c['price']>p['price'] or (c['price']==p['price'] and c['pivot_time']<p['pivot_time'])
    return c['price']<p['price'] or (c['price']==p['price'] and c['pivot_time']<p['pivot_time'])


def run_engine(bs, timeframe):
    mult=0.75 if timeframe=='4h' else 1.00
    atr=canonical_atr(bs)
    cands=fractal_candidates(bs)
    seq=[]
    states=[]
    bootstrap_skipped_dual=0
    dual_resolved=0
    substructure_rejected=0
    same_dir_updates=0

    for bi,b in enumerate(bs):
        cs=cands.get(bi,[])
        chosen=[]
        if len(cs)==2:
            if not seq:
                bootstrap_skipped_dual+=1
                chosen=[]
            else:
                expected='L' if seq[-1]['type']=='H' else 'H'
                chosen=[c for c in cs if c['type']==expected]
                dual_resolved+=1
        else:
            chosen=cs
        for c in chosen:
            a=atr[bi]
            if a is None: continue
            c=dict(c); c['atr_confirm']=a; c['threshold']=mult*a
            if not seq:
                c['role']='BOOTSTRAP'
                seq.append(c)
                continue
            last=seq[-1]
            if c['type']==last['type']:
                if more_extreme(c,last):
                    c['role']=last.get('role','PIVOT')
                    seq[-1]=c
                    same_dir_updates+=1
                continue
            disp=abs(c['price']-last['price'])
            c['displacement']=disp
            if disp + 1e-12 >= c['threshold']:
                c['role']='PIVOT'
                seq.append(c)
            else:
                substructure_rejected+=1
        lastH=next((p for p in reversed(seq) if p['type']=='H'),None)
        lastL=next((p for p in reversed(seq) if p['type']=='L'),None)
        hs=[p for p in seq if p['type']=='H']
        ls=[p for p in seq if p['type']=='L']
        hlabel=None; llabel=None
        if len(hs)>=2: hlabel='HH' if hs[-1]['price']>hs[-2]['price'] else 'LH' if hs[-1]['price']<hs[-2]['price'] else 'EH'
        if len(ls)>=2: llabel='HL' if ls[-1]['price']>ls[-2]['price'] else 'LL' if ls[-1]['price']<ls[-2]['price'] else 'EL'
        states.append((lastH['price'] if lastH else None,lastL['price'] if lastL else None,hlabel,llabel,atr[bi],len(seq)))
    return {'atr':atr,'seq':seq,'states':states,'stats':{'dual_resolved':dual_resolved,'bootstrap_skipped_dual':bootstrap_skipped_dual,'substructure_rejected':substructure_rejected,'same_dir_updates':same_dir_updates}}


def serializable_result(res):
    return {'seq':[(p['type'],p['pivot_time'],round(p['price'],10),p['confirm_time'],round(p['atr_confirm'],12)) for p in res['seq']],
            'states':[(a,b,c,d,None if e is None else round(e,12),f) for a,b,c,d,e,f in res['states']]}


def hash_result(res):
    return hashlib.sha256(json.dumps(serializable_result(res),separators=(',',':')).encode()).hexdigest()


def validate(bs, tf, res):
    step=4*3600*1000 if tf=='4h' else 24*3600*1000
    gaps=[]
    for i in range(1,len(bs)):
        if bs[i]['open_time']-bs[i-1]['open_time']!=step:
            gaps.append((bs[i-1]['open_time'],bs[i]['open_time']))
    alt=all(res['seq'][i]['type']!=res['seq'][i-1]['type'] for i in range(1,len(res['seq'])))
    causal=all(p['confirm_time']>=p['pivot_time']+2*step for p in res['seq'])
    res2=run_engine(bs,tf)
    deterministic=(hash_result(res)==hash_result(res2))
    latest_close=bs[-1]['close_time']
    start_eval=latest_close-EVAL_DAYS*24*3600*1000
    idx=[i for i,b in enumerate(bs) if b['close_time']>=start_eval]
    valid=sum(1 for i in idx if res['atr'][i] is not None and res['states'][i][0] is not None and res['states'][i][1] is not None)
    coverage=valid/len(idx) if idx else 0
    rng=random.Random(20260915 + (4 if tf=='4h' else 1))
    sample=sorted(rng.sample(idx, min(48,len(idx))))
    prefix_mismatch=[]
    for i in sample:
        pr=run_engine(bs[:i+1],tf)
        fs=res['states'][i]
        ps=pr['states'][-1]
        if ps!=fs:
            prefix_mismatch.append(i)
    ep=[p for p in res['seq'] if p['confirm_time']>=start_eval]
    leg_hours=[]
    for a,b in zip(ep,ep[1:]): leg_hours.append((b['confirm_time']-a['confirm_time'])/3600000)
    diag={
        'eval_boundaries':len(idx),'valid_boundaries':valid,'coverage':coverage,
        'eval_pivots':len(ep),'pivot_rate_per_30d':len(ep)/(EVAL_DAYS/30),
        'median_leg_hours':statistics.median(leg_hours) if leg_hours else None,
        'min_leg_hours':min(leg_hours) if leg_hours else None,
        'max_leg_hours':max(leg_hours) if leg_hours else None,
    }
    return {'gaps':gaps,'alternating':alt,'causal_confirmation':causal,'deterministic':deterministic,'hash':hash_result(res),'prefix_tests':len(sample),'prefix_mismatches':len(prefix_mismatch),'coverage':coverage,'diagnostics':diag}

rows4,src4=fetch_all('4h'); rows1d,src1=fetch_all('1d')
b4=bars(rows4); b1=bars(rows1d)
r4=run_engine(b4,'4h'); r1=run_engine(b1,'1d')
v4=validate(b4,'4h',r4); v1=validate(b1,'1d',r1)
recent4=[p for p in r4['seq'] if p['confirm_time']>=b4[-1]['close_time']-45*24*3600*1000]
recent1=[p for p in r1['seq'] if p['confirm_time']>=b1[-1]['close_time']-180*24*3600*1000]
hard_pre = (
    not v4['gaps'] and not v1['gaps'] and v4['alternating'] and v1['alternating'] and
    v4['causal_confirmation'] and v1['causal_confirmation'] and v4['deterministic'] and v1['deterministic'] and
    v4['prefix_mismatches']==0 and v1['prefix_mismatches']==0 and v4['coverage']>=0.995 and v1['coverage']>=0.995
)

out={
 'engine':'ETH-STRUCT-v0.2-PROVISIONAL',
 'canonical_atr':'Wilder14; TR first bar=H-L; seed=arithmetic mean first 14 TR; recursive ((prev*13)+TR)/14; no intermediate rounding; source history starts pre-listing and resolves to first Binance ETHUSDT perpetual bar',
 'fractal':'2L/2R; equal-price tie within 5-bar window => earliest timestamp; dual-fractal => expected opposite only; dual at bootstrap skipped',
 'data':{
   '4h_source':src4,'1d_source':src1,
   '4h_bars':len(b4),'4h_first_open':b4[0]['open_time'],'4h_last_close':b4[-1]['close_time'],
   '1d_bars':len(b1),'1d_first_open':b1[0]['open_time'],'1d_last_close':b1[-1]['close_time']},
 'validation':{'4h':v4,'1d':v1,'pre_cutover_hard_gates_pass':hard_pre,'live_v02_forward_parity':'PENDING — no pre-existing live v0.2 history'},
 'stats':{'4h':r4['stats'],'1d':r1['stats']},
 'current':{
   '4h_state':r4['states'][-1], '1d_state':r1['states'][-1],
   '4h_atr14':r4['atr'][-1], '1d_atr14':r1['atr'][-1],
   'recent_4h_pivots':[{'type':p['type'],'price':p['price'],'pivot_time':p['pivot_time'],'confirm_time':p['confirm_time'],'atr_confirm':p['atr_confirm']} for p in recent4[-20:]],
   'recent_1d_pivots':[{'type':p['type'],'price':p['price'],'pivot_time':p['pivot_time'],'confirm_time':p['confirm_time'],'atr_confirm':p['atr_confirm']} for p in recent1[-12:]]
 },
 'sample_bars':{
   'first4':rows4[0][:7], 'last4':rows4[-1][:7], 'first1d':rows1d[0][:7], 'last1d':rows1d[-1][:7]
 }
}
print('STRUCT_REPLAY_JSON='+json.dumps(out,separators=(',',':')))
