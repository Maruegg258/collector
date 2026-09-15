import csv, hashlib, io, json, math, statistics, urllib.request, zipfile
from datetime import datetime, timezone, timedelta

SYMBOL='ETHUSDT'
CFG='ETH-SPOT-CLASS-v0.1-PROVISIONAL'
EVAL_DAYS=365
CAL_BARS=180  # rolling 30D at 4H boundaries
NOW=datetime(2026,9,15,tzinfo=timezone.utc)
ARCHIVE_BASE='https://data.binance.vision/data/spot'


def dt_ms(ms):
    if ms > 10**14: ms//=1000  # Binance Spot archive timestamps may be microseconds after 2025-01-01
    return int(ms)


def iso_end(ms):
    return datetime.fromtimestamp(ms/1000,timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')


def get_bytes(url, timeout=60):
    req=urllib.request.Request(url,headers={'User-Agent':'OpenAI-ETH-Spot-Replay/1.0'})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return r.read()


def sha256(b): return hashlib.sha256(b).hexdigest()


def download_zip_checked(url):
    try:
        z=get_bytes(url)
        c=get_bytes(url+'.CHECKSUM').decode().strip().split()[0]
        if sha256(z).lower()!=c.lower():
            raise RuntimeError('checksum mismatch '+url)
        return z, True
    except Exception:
        return None, False


def months_between(a,b):
    y,m=a.year,a.month
    while (y,m) <= (b.year,b.month):
        yield y,m
        m+=1
        if m==13: y,m=y+1,1


def load_spot_archives(interval, start, end):
    rows=[]; meta={'archives':0,'checksummed':0,'missing':[]}
    # Full monthly archives through previous month.
    month_end=datetime(end.year,end.month,1,tzinfo=timezone.utc)-timedelta(days=1)
    for y,m in months_between(datetime(start.year,start.month,1,tzinfo=timezone.utc), month_end):
        fn=f'{SYMBOL}-{interval}-{y:04d}-{m:02d}.zip'
        url=f'{ARCHIVE_BASE}/monthly/klines/{SYMBOL}/{interval}/{fn}'
        z,ok=download_zip_checked(url)
        if not ok:
            meta['missing'].append(url); continue
        meta['archives']+=1; meta['checksummed']+=1
        with zipfile.ZipFile(io.BytesIO(z)) as zz:
            with zz.open(zz.namelist()[0]) as f:
                rows.extend(list(csv.reader(io.TextIOWrapper(f))))
    # Daily archives for current month through yesterday.
    cur=datetime(end.year,end.month,1,tzinfo=timezone.utc)
    d=cur
    while d.date() < end.date():
        fn=f'{SYMBOL}-{interval}-{d:%Y-%m-%d}.zip'
        url=f'{ARCHIVE_BASE}/daily/klines/{SYMBOL}/{interval}/{fn}'
        z,ok=download_zip_checked(url)
        if not ok:
            meta['missing'].append(url); d+=timedelta(days=1); continue
        meta['archives']+=1; meta['checksummed']+=1
        with zipfile.ZipFile(io.BytesIO(z)) as zz:
            with zz.open(zz.namelist()[0]) as f:
                rows.extend(list(csv.reader(io.TextIOWrapper(f))))
        d+=timedelta(days=1)
    out={}
    for r in rows:
        ot=dt_ms(int(r[0])); ct=dt_ms(int(r[6]))
        if ct < int(start.timestamp()*1000) or ot >= int(end.timestamp()*1000): continue
        out[ot]={
            'open_time':ot,'close_time':ct,'open':float(r[1]),'high':float(r[2]),'low':float(r[3]),'close':float(r[4]),
            'volume':float(r[5]),'quote':float(r[7]),'taker_buy_quote':float(r[10])
        }
    return [out[k] for k in sorted(out)], meta


def continuity(bars, step_ms):
    gaps=[]
    for a,b in zip(bars,bars[1:]):
        if b['open_time']-a['open_time']!=step_ms:
            gaps.append([a['open_time'],b['open_time']])
    return gaps


def percentile_inc(vals,p):
    s=sorted(vals); n=len(s)
    if n==0:return None
    if n==1:return s[0]
    h=(n-1)*p; lo=int(math.floor(h)); hi=int(math.ceil(h))
    if lo==hi:return s[lo]
    return s[lo]+(s[hi]-s[lo])*(h-lo)


def classify(r,boundary):
    if r is None or boundary is None:return 'UNKNOWN'
    if r>=boundary:return 'CLEAR POSITIVE'
    if r<=-boundary:return 'CLEAR NEGATIVE'
    return 'MARGINAL / NEUTRAL'


def overall(c4,c24,c3):
    if c24=='CLEAR POSITIVE' and c3!='CLEAR NEGATIVE':
        return 'POSITIVE / SHORT-TERM COOLING' if c4=='CLEAR NEGATIVE' else 'POSITIVE'
    if c24=='CLEAR NEGATIVE' and c3!='CLEAR POSITIVE':
        return 'NEGATIVE / SHORT-TERM BUYING RESPONSE' if c4=='CLEAR POSITIVE' else 'NEGATIVE'
    if c24=='CLEAR POSITIVE' and c3=='CLEAR NEGATIVE':
        return 'MIXED / IMPROVING' if c4=='CLEAR POSITIVE' else 'MIXED / TRANSITION'
    if c24=='CLEAR NEGATIVE' and c3=='CLEAR POSITIVE':
        return 'MIXED / DETERIORATING'
    return 'MIXED / TRANSITION'


def base_overall(s):
    if s.startswith('POSITIVE'): return 'POSITIVE'
    if s.startswith('NEGATIVE'): return 'NEGATIVE'
    return 'MIXED'


def build_series(b4,pct=0.35):
    rec=[]
    d4=[]; q4=[]
    for i,b in enumerate(b4):
        delta=2*b['taker_buy_quote']-b['quote']
        ratio=delta/b['quote'] if b['quote'] else None
        d4.append(delta); q4.append(b['quote'])
        d24=sum(d4[max(0,i-5):i+1]) if i>=5 else None
        q24=sum(q4[max(0,i-5):i+1]) if i>=5 else None
        r24=d24/q24 if i>=5 and q24 else None
        d3=sum(d4[max(0,i-17):i+1]) if i>=17 else None
        q3=sum(q4[max(0,i-17):i+1]) if i>=17 else None
        r3=d3/q3 if i>=17 and q3 else None
        rec.append({'close_time':b['close_time'],'close':b['close'],'d4':delta,'r4':ratio,'d24':d24,'r24':r24,'d3':d3,'r3':r3,'quote4':b['quote'],'tbq4':b['taker_buy_quote']})
    for i,x in enumerate(rec):
        if i < CAL_BARS-1:
            x.update({'q4':None,'q24':None,'q3':None,'c4':'UNKNOWN','c24':'UNKNOWN','c3':'UNKNOWN','overall':'UNKNOWN'})
            continue
        w=rec[i-CAL_BARS+1:i+1]
        vals4=[abs(z['r4']) for z in w if z['r4'] is not None]
        vals24=[abs(z['r24']) for z in w if z['r24'] is not None]
        vals3=[abs(z['r3']) for z in w if z['r3'] is not None]
        q_4=percentile_inc(vals4,pct) if len(vals4)>=120 else None
        q_24=percentile_inc(vals24,pct) if len(vals24)>=120 else None
        q_3=percentile_inc(vals3,pct) if len(vals3)>=120 else None
        c4=classify(x['r4'],q_4); c24=classify(x['r24'],q_24); c3=classify(x['r3'],q_3)
        x.update({'q4':q_4,'q24':q_24,'q3':q_3,'c4':c4,'c24':c24,'c3':c3,'overall':overall(c4,c24,c3)})
    return rec


def episodes(rec,key,eval_start):
    out=[]; prev=None; cur=None
    for i,x in enumerate(rec):
        if x['close_time'] < eval_start: continue
        raw=x[key]
        if key=='overall': state=base_overall(raw)
        else: state=raw
        target = state in ('CLEAR POSITIVE','CLEAR NEGATIVE','POSITIVE','NEGATIVE')
        if state!=prev:
            if cur is not None: cur['end_i']=i-1; out.append(cur); cur=None
            if target: cur={'state':state,'start_i':i,'start_time':x['close_time']}
            prev=state
    if cur is not None: cur['end_i']=len(rec)-1; out.append(cur)
    return out


def outcomes_for_episodes(eps, rec4, b1):
    # 1H bars indexed by open time; start strictly after episode's completed 4H boundary.
    ot=[b['open_time'] for b in b1]
    import bisect
    for e in eps:
        r=rec4[e['start_i']]; ref=r['close']; start=r['close_time']+1
        j=bisect.bisect_left(ot,start)
        e['reference_price']=ref
        for name,n in [('24h',24),('3d',72),('7d',168)]:
            seg=b1[j:j+n]
            if len(seg)<n:
                e[name]=None; continue
            e[name]={
                'return_pct':(seg[-1]['close']/ref-1)*100,
                'mfe_pct':(max(z['high'] for z in seg)/ref-1)*100,
                'mae_pct':(min(z['low'] for z in seg)/ref-1)*100,
            }
    return eps


def summarize_eps(eps):
    states=sorted(set(e['state'] for e in eps))
    out={}
    for s in states:
        es=[e for e in eps if e['state']==s]
        z={'count':len(es)}
        for h in ('24h','3d','7d'):
            hs=[e[h] for e in es if e.get(h)]
            z[h]={'n':len(hs)}
            if hs:
                for k in ('return_pct','mfe_pct','mae_pct'):
                    z[h]['median_'+k]=statistics.median(x[k] for x in hs)
        out[s]=z
    return out


def result_hash(rec):
    slim=[(x['close_time'],round(x['r4'],14),None if x['r24'] is None else round(x['r24'],14),None if x['r3'] is None else round(x['r3'],14),None if x['q4'] is None else round(x['q4'],14),None if x['q24'] is None else round(x['q24'],14),None if x['q3'] is None else round(x['q3'],14),x['c4'],x['c24'],x['c3'],x['overall']) for x in rec]
    return hashlib.sha256(json.dumps(slim,separators=(',',':')).encode()).hexdigest()

# Need 30D warm-up before 365D evaluation, plus a little buffer.
start=NOW-timedelta(days=410)
end=NOW
b4,m4=load_spot_archives('4h',start,end)
b1,m1=load_spot_archives('1h',start,end)
if not b4 or not b1: raise RuntimeError('No Binance Spot archive data')
g4=continuity(b4,4*3600*1000); g1=continuity(b1,3600*1000)
rec35=build_series(b4,0.35); rec35b=build_series(b4,0.35)
rec25=build_series(b4,0.25); rec40=build_series(b4,0.40)
h35=result_hash(rec35); deterministic=(h35==result_hash(rec35b))
latest_ct=min(b4[-1]['close_time'], b1[-1]['close_time'])
eval_start=latest_ct-EVAL_DAYS*86400000
eligible=[x for x in rec35 if x['close_time']>=eval_start and x['q4'] is not None and x['q24'] is not None and x['q3'] is not None]

# Episodes + forward outcome evidence.
eps={}
for key in ('c4','c24','c3','overall'):
    es=episodes(rec35,key,eval_start)
    eps[key]=outcomes_for_episodes(es,rec35,b1)

# q25/q40 diagnostics: episode counts only; q35 remains frozen config.
def ep_counts(rec):
    d={}
    for key in ('c4','c24','c3','overall'):
        es=episodes(rec,key,eval_start)
        counts={}
        for e in es: counts[e['state']]=counts.get(e['state'],0)+1
        d[key]=counts
    return d

# Canonical rows covering current persisted calibration period for repair/replacement.
current_start=int(datetime(2026,7,29,20,0,tzinfo=timezone.utc).timestamp()*1000)
canon=[]
for x in rec35:
    if x['close_time']>=current_start:
        canon.append({
          'completed_4h_time_utc':iso_end(x['close_time']),'close_time_ms':x['close_time'],
          'delta_4h_usdt':x['d4'],'ratio_4h':x['r4'],'delta_24h_usdt':x['d24'],'ratio_24h':x['r24'],
          'delta_3d_usdt':x['d3'],'ratio_3d':x['r3'],'abs_ratio_4h':abs(x['r4']) if x['r4'] is not None else None,
          'abs_ratio_24h':abs(x['r24']) if x['r24'] is not None else None,'abs_ratio_3d':abs(x['r3']) if x['r3'] is not None else None,
          'q35_4h':x['q4'],'q35_24h':x['q24'],'q35_3d':x['q3'],'class_4h':x['c4'],'class_24h':x['c24'],'class_3d':x['c3'],'overall':x['overall'],
          'quote_volume_4h':x['quote4'],'taker_buy_quote_volume_4h':x['tbq4']
        })

# Explicit repair anchors.
anchors={}
for target in (int(datetime(2026,8,29,3,59,59,999000,tzinfo=timezone.utc).timestamp()*1000), int(datetime(2026,9,8,11,59,59,999000,tzinfo=timezone.utc).timestamp()*1000)):
    z=next((x for x in canon if x['close_time_ms']==target),None)
    anchors[str(target)]=z

summary={
 'engine':CFG,
 'source':'Binance official Spot Public Data Archive, checksum-verified',
 'data':{'4h_bars':len(b4),'1h_bars':len(b1),'4h_source':m4,'1h_source':m1,'4h_gaps':g4,'1h_gaps':g1,'latest_close_time':latest_ct},
 'validation':{'deterministic':deterministic,'hash':h35,'eligible_365d_boundaries':len(eligible),'coverage_expected_4h':EVAL_DAYS*6,'coverage_ratio':len(eligible)/(EVAL_DAYS*6),'lookahead_design':'point-in-time only; q35 uses current/prior completed same-window observations only'},
 'latest':eligible[-1] if eligible else None,
 'repair_anchors':anchors,
 'q35_episode_summary':{k:summarize_eps(v) for k,v in eps.items()},
 'q35_episode_counts':ep_counts(rec35),
 'q25_episode_counts_diagnostic_only':ep_counts(rec25),
 'q40_episode_counts_diagnostic_only':ep_counts(rec40),
 'promotion_status_pre_integrated_utility':'PENDING REVIEW',
 'notes':['Integrated Utility Test intentionally deferred until Structure v0.2 forward-parity/cutover review.','No q35 parameter modification is performed by this replay.']
}
print('SPOT_REPLAY_JSON='+json.dumps(summary,separators=(',',':')))
with open('audit/spot_v01_result.json','w') as f: json.dump(summary,f,separators=(',',':'))
with open('audit/spot_v01_result.pretty.json','w') as f: json.dump(summary,f,indent=2)
with open('audit/spot_v01_canonical_current.json','w') as f: json.dump(canon,f,separators=(',',':'))
