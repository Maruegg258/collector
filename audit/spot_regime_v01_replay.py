import bisect, csv, hashlib, io, json, math, statistics, urllib.request, urllib.error, zipfile, time
from datetime import datetime, timezone, timedelta

NOW = datetime(2026,9,15,tzinfo=timezone.utc)
NOW_MS = int(NOW.timestamp()*1000)
EVAL_DAYS = 365
CAL_BARS = 180
SPOT_CFG = 'ETH-SPOT-CLASS-v0.1-PROVISIONAL'
REGIME_CFG = 'ETH-SPOT-REGIME-v0.1-PROVISIONAL'
STRUCT_CFG = 'ETH-STRUCT-v0.2-PROVISIONAL'
SPOT_BASE = 'https://data.binance.vision/data/spot'
FUT_BASE = 'https://data.binance.vision/data/futures/um'

def norm_ts(v):
    x=int(v)
    return x//1000 if x > 10**15 else x

def get_bytes(url, timeout=60, retries=4):
    last=None
    for k in range(retries):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'OpenAI-ETH-Spot-Regime-Replay/1.0'})
            with urllib.request.urlopen(req,timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code==404: return None
            last=e
        except Exception as e:
            last=e
        time.sleep(1.2*(k+1))
    if last: raise last
    return None

def download_zip_checked(url):
    z=get_bytes(url)
    if z is None: return None, False
    c=get_bytes(url+'.CHECKSUM')
    if c is None: raise RuntimeError('Missing checksum '+url)
    expected=c.decode().strip().split()[0].lower()
    actual=hashlib.sha256(z).hexdigest().lower()
    if actual!=expected: raise RuntimeError('Checksum mismatch '+url)
    return z, True

def months_between(a,b):
    y,m=a.year,a.month
    while (y,m) <= (b.year,b.month):
        yield y,m
        m+=1
        if m==13: y,m=y+1,1

def parse_zip_rows(z):
    with zipfile.ZipFile(io.BytesIO(z)) as zz:
        names=[n for n in zz.namelist() if not n.endswith('/')]
        if len(names)!=1: raise RuntimeError('Unexpected zip members')
        with zz.open(names[0]) as f:
            return list(csv.reader(io.TextIOWrapper(f)))

def load_spot_archives(symbol, interval, start, end):
    rows=[]; meta={'archives':0,'checksummed':0,'missing':[]}
    prev_month_end=datetime(end.year,end.month,1,tzinfo=timezone.utc)-timedelta(days=1)
    for y,m in months_between(datetime(start.year,start.month,1,tzinfo=timezone.utc),prev_month_end):
        fn=f'{symbol}-{interval}-{y:04d}-{m:02d}.zip'
        url=f'{SPOT_BASE}/monthly/klines/{symbol}/{interval}/{fn}'
        z,ok=download_zip_checked(url)
        if not ok:
            meta['missing'].append(url); continue
        meta['archives']+=1; meta['checksummed']+=1
        rows.extend(parse_zip_rows(z))
    d=datetime(end.year,end.month,1,tzinfo=timezone.utc)
    while d.date() < end.date():
        fn=f'{symbol}-{interval}-{d:%Y-%m-%d}.zip'
        url=f'{SPOT_BASE}/daily/klines/{symbol}/{interval}/{fn}'
        z,ok=download_zip_checked(url)
        if not ok:
            meta['missing'].append(url); d+=timedelta(days=1); continue
        meta['archives']+=1; meta['checksummed']+=1
        rows.extend(parse_zip_rows(z)); d+=timedelta(days=1)
    out={}
    for r in rows:
        try:
            ot=norm_ts(r[0]); ct=norm_ts(r[6])
            if ct < int(start.timestamp()*1000) or ot >= int(end.timestamp()*1000): continue
            out[ot]={'open_time':ot,'close_time':ct,'open':float(r[1]),'high':float(r[2]),'low':float(r[3]),
                     'close':float(r[4]),'quote':float(r[7]),'taker_buy_quote':float(r[10])}
        except Exception:
            continue
    return [out[k] for k in sorted(out)], meta

def load_futures_all_1d(symbol, start_year, start_month):
    rows=[]; meta={'archives':0,'checksummed':0,'prelisting_missing':0,'postlisting_missing':[]}
    cur_first=datetime(NOW.year,NOW.month,1,tzinfo=timezone.utc)
    y,m=start_year,start_month
    seen=False
    while datetime(y,m,1,tzinfo=timezone.utc) < cur_first:
        fn=f'{symbol}-1d-{y:04d}-{m:02d}.zip'
        url=f'{FUT_BASE}/monthly/klines/{symbol}/1d/{fn}'
        z,ok=download_zip_checked(url)
        if ok:
            seen=True; meta['archives']+=1; meta['checksummed']+=1; rows.extend(parse_zip_rows(z))
        else:
            if not seen: meta['prelisting_missing']+=1
            else: meta['postlisting_missing'].append(url)
        m+=1
        if m==13: y,m=y+1,1
    d=cur_first
    while d.date() < NOW.date():
        fn=f'{symbol}-1d-{d:%Y-%m-%d}.zip'
        url=f'{FUT_BASE}/daily/klines/{symbol}/1d/{fn}'
        z,ok=download_zip_checked(url)
        if ok:
            meta['archives']+=1; meta['checksummed']+=1; rows.extend(parse_zip_rows(z))
        else:
            meta['postlisting_missing'].append(url)
        d+=timedelta(days=1)
    out={}
    for r in rows:
        try:
            ot=norm_ts(r[0]); ct=norm_ts(r[6])
            if ct>=NOW_MS: continue
            out[ot]={'open_time':ot,'close_time':ct,'open':float(r[1]),'high':float(r[2]),'low':float(r[3]),'close':float(r[4])}
        except Exception:
            continue
    bs=[out[k] for k in sorted(out)]
    if not bs: raise RuntimeError('No futures data '+symbol)
    return bs, meta

def continuity(bs, step_ms):
    return [(a['open_time'],b['open_time']) for a,b in zip(bs,bs[1:]) if b['open_time']-a['open_time']!=step_ms]

def percentile_inc(vals,p):
    s=sorted(vals); n=len(s)
    if not n:return None
    if n==1:return s[0]
    h=(n-1)*p; lo=int(math.floor(h)); hi=int(math.ceil(h))
    return s[lo] if lo==hi else s[lo]+(s[hi]-s[lo])*(h-lo)

def spot_classify(r,b):
    if r is None or b is None:return 'UNKNOWN'
    if r>=b:return 'CLEAR POSITIVE'
    if r<=-b:return 'CLEAR NEGATIVE'
    return 'MARGINAL / NEUTRAL'

def spot_overall(c4,c24,c3):
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
    if s.startswith('POSITIVE'):return 'POSITIVE'
    if s.startswith('NEGATIVE'):return 'NEGATIVE'
    return 'MIXED'

def build_spot_series(b4,pct=.35):
    rec=[]; d4=[]; q4=[]
    for i,b in enumerate(b4):
        d=2*b['taker_buy_quote']-b['quote']; r=d/b['quote'] if b['quote'] else None
        d4.append(d); q4.append(b['quote'])
        d24=sum(d4[i-5:i+1]) if i>=5 else None; q24=sum(q4[i-5:i+1]) if i>=5 else None
        d3=sum(d4[i-17:i+1]) if i>=17 else None; q3=sum(q4[i-17:i+1]) if i>=17 else None
        rec.append({'close_time':b['close_time'],'close':b['close'],'r4':r,
                    'r24':d24/q24 if q24 else None,'r3':d3/q3 if q3 else None})
    for i,x in enumerate(rec):
        if i<CAL_BARS-1:
            x.update(q4=None,q24=None,q3=None,c4='UNKNOWN',c24='UNKNOWN',c3='UNKNOWN',overall='UNKNOWN'); continue
        w=rec[i-CAL_BARS+1:i+1]
        v4=[abs(z['r4']) for z in w if z['r4'] is not None]
        v24=[abs(z['r24']) for z in w if z['r24'] is not None]
        v3=[abs(z['r3']) for z in w if z['r3'] is not None]
        q_4=percentile_inc(v4,pct) if len(v4)>=120 else None
        q_24=percentile_inc(v24,pct) if len(v24)>=120 else None
        q_3=percentile_inc(v3,pct) if len(v3)>=120 else None
        c4=spot_classify(x['r4'],q_4); c24=spot_classify(x['r24'],q_24); c3=spot_classify(x['r3'],q_3)
        x.update(q4=q_4,q24=q_24,q3=q_3,c4=c4,c24=c24,c3=c3,overall=spot_overall(c4,c24,c3))
    return rec

def canonical_atr(bs,n=14):
    tr=[]
    for i,b in enumerate(bs):
        if i==0:x=b['high']-b['low']
        else:
            pc=bs[i-1]['close']; x=max(b['high']-b['low'],abs(b['high']-pc),abs(b['low']-pc))
        tr.append(x)
    atr=[None]*len(bs)
    if len(bs)>=n:
        atr[n-1]=sum(tr[:n])/n
        for i in range(n,len(bs)):atr[i]=(atr[i-1]*13+tr[i])/14
    return atr

def ema(bs,n):
    a=2/(n+1); out=[]; v=None
    for b in bs:
        v=b['close'] if v is None else a*b['close']+(1-a)*v
        out.append(v)
    return out

def fractal_candidates(bs):
    by_confirm={}
    for i in range(2,len(bs)-2):
        win=range(i-2,i+3); mh=max(bs[j]['high'] for j in win); ml=min(bs[j]['low'] for j in win)
        hi=min(j for j in win if bs[j]['high']==mh); li=min(j for j in win if bs[j]['low']==ml)
        arr=[]
        if i==hi:arr.append({'type':'H','pivot_i':i,'price':bs[i]['high'],'pivot_time':bs[i]['close_time'],'confirm_i':i+2,'confirm_time':bs[i+2]['close_time']})
        if i==li:arr.append({'type':'L','pivot_i':i,'price':bs[i]['low'],'pivot_time':bs[i]['close_time'],'confirm_i':i+2,'confirm_time':bs[i+2]['close_time']})
        if arr:by_confirm[i+2]=arr
    return by_confirm

def more_extreme(c,p):
    if c['type']=='H':return c['price']>p['price'] or (c['price']==p['price'] and c['pivot_time']<p['pivot_time'])
    return c['price']<p['price'] or (c['price']==p['price'] and c['pivot_time']<p['pivot_time'])

def run_structure_1d(bs):
    atr=canonical_atr(bs); cands=fractal_candidates(bs); seq=[]; states=[]
    for bi,b in enumerate(bs):
        cs=cands.get(bi,[])
        if len(cs)==2:
            if not seq:chosen=[]
            else:
                expected='L' if seq[-1]['type']=='H' else 'H'; chosen=[c for c in cs if c['type']==expected]
        else:chosen=cs
        for c0 in chosen:
            if atr[bi] is None:continue
            c=dict(c0); c['atr_confirm']=atr[bi]; c['threshold']=atr[bi]
            if not seq:
                seq.append(c); continue
            last=seq[-1]
            if c['type']==last['type']:
                if more_extreme(c,last):seq[-1]=c
                continue
            if abs(c['price']-last['price'])+1e-12>=c['threshold']:seq.append(c)
        hs=[p for p in seq if p['type']=='H']; ls=[p for p in seq if p['type']=='L']
        lastH=hs[-1] if hs else None; prevH=hs[-2] if len(hs)>=2 else None
        lastL=ls[-1] if ls else None; prevL=ls[-2] if len(ls)>=2 else None
        hlabel=None if not prevH else ('HH' if lastH['price']>prevH['price'] else 'LH' if lastH['price']<prevH['price'] else 'EH')
        llabel=None if not prevL else ('HL' if lastL['price']>prevL['price'] else 'LL' if lastL['price']<prevL['price'] else 'EL')
        states.append({'lastH':lastH,'prevH':prevH,'lastL':lastL,'prevL':prevL,'hlabel':hlabel,'llabel':llabel,'atr':atr[bi]})
    return states,atr,seq

def downside_break_confirmed(bs, atr, state, i):
    if state['llabel']!='LL' or state['prevL'] is None:return False
    prior=state['prevL']
    for j,b in enumerate(bs):
        if j>i:break
        if b['close_time'] <= prior['confirm_time']:continue
        if atr[j] is None:continue
        if b['close'] < prior['price'] - 0.15*atr[j]:return True
    return False

def classify_regimes(eth,btc):
    es,eatr,eseq=run_structure_1d(eth); bs,batr,bseq=run_structure_1d(btc)
    e20=ema(eth,20); e50=ema(eth,50)
    btc_times=[b['close_time'] for b in btc]
    out=[]
    for i,e in enumerate(eth):
        j=bisect.bisect_right(btc_times,e['close_time'])-1
        if j<0:
            out.append({'close_time':e['close_time'],'regime':'TRANSITION_UNCLASSIFIED','eth_hlabel':None,'eth_llabel':None,'btc_hlabel':None,'btc_llabel':None,'eth_atr14':eatr[i],'ema20':e20[i],'ema50':e50[i]}); continue
        a=eatr[i]; st=es[i]; bst=bs[j]
        if a is None or st['hlabel'] is None or st['llabel'] is None:
            regime='TRANSITION_UNCLASSIFIED'
        else:
            btc_bear=(bst['hlabel']=='LH' and bst['llabel']=='LL')
            hl_intact=st['lastL'] is not None and e['close'] >= st['lastL']['price'] - 0.15*a
            eth_down=(st['hlabel']=='LH' and st['llabel']=='LL' and downside_break_confirmed(eth,eatr,st,i))
            inside=(st['lastH'] is not None and st['lastL'] is not None and
                    e['close'] <= st['lastH']['price'] + 0.15*a and e['close'] >= st['lastL']['price'] - 0.15*a)
            mixed=(st['hlabel'],st['llabel']) in (('HH','LL'),('LH','HL'))
            ema_tight=abs(e20[i]-e50[i]) <= 1.0*a
            if st['hlabel']=='HH' and st['llabel']=='HL' and hl_intact and not btc_bear:
                regime='UPTREND'
            elif eth_down and btc_bear:
                regime='DOWNTREND_RISK_OFF'
            elif mixed and inside and ema_tight and not btc_bear:
                regime='RANGE_CHOP'
            else:
                regime='TRANSITION_UNCLASSIFIED'
        out.append({'close_time':e['close_time'],'regime':regime,'eth_hlabel':st['hlabel'],'eth_llabel':st['llabel'],
                    'btc_hlabel':bst['hlabel'],'btc_llabel':bst['llabel'],'eth_atr14':a,
                    'ema20':e20[i],'ema50':e50[i]})
    return out,es,bs,eseq,bseq

def episodes_overall(rec,eval_start):
    out=[]; prev=None; cur=None
    for i,x in enumerate(rec):
        if x['close_time']<eval_start:continue
        state=base_overall(x['overall'])
        target=state in ('POSITIVE','NEGATIVE')
        if state!=prev:
            if cur is not None:cur['end_i']=i-1; out.append(cur); cur=None
            if target:cur={'state':state,'start_i':i,'start_time':x['close_time']}
            prev=state
    if cur is not None:cur['end_i']=len(rec)-1; out.append(cur)
    return out

def attach_outcomes(eps,rec,b1):
    ots=[b['open_time'] for b in b1]
    for e in eps:
        r=rec[e['start_i']]; ref=r['close']; j=bisect.bisect_left(ots,r['close_time']+1)
        e['reference_price']=ref
        for name,n in [('3d',72),('7d',168)]:
            seg=b1[j:j+n]
            if len(seg)<n:e[name]=None
            else:e[name]={'return_pct':(seg[-1]['close']/ref-1)*100,'mfe_pct':(max(z['high'] for z in seg)/ref-1)*100,'mae_pct':(min(z['low'] for z in seg)/ref-1)*100}
    return eps

def bind_regime(eps,regime_rows):
    times=[x['close_time'] for x in regime_rows]
    violations=0
    for e in eps:
        j=bisect.bisect_right(times,e['start_time'])-1
        if j<0:e['regime']='TRANSITION_UNCLASSIFIED'; e['regime_1d_close_time']=None
        else:
            rr=regime_rows[j]; e['regime']=rr['regime']; e['regime_1d_close_time']=rr['close_time']
            if rr['close_time']>e['start_time']:violations+=1
    return violations

def med(vals):
    return statistics.median(vals) if vals else None

def summarize_regime(eps,regime):
    es=[e for e in eps if e['regime']==regime]
    out={'POSITIVE':{},'NEGATIVE':{}}
    for s in ('POSITIVE','NEGATIVE'):
        xs=[e for e in es if e['state']==s]
        out[s]['count']=len(xs)
        for h in ('3d','7d'):
            ys=[e[h] for e in xs if e.get(h)]
            out[s][h]={'n':len(ys),'median_return_pct':med([y['return_pct'] for y in ys]),
                       'median_mfe_pct':med([y['mfe_pct'] for y in ys]),'median_mae_pct':med([y['mae_pct'] for y in ys])}
    pc=out['POSITIVE']['count']; nc=out['NEGATIVE']['count']
    if pc<30 or nc<30:
        out['formal_status']='INSUFFICIENT REGIME SAMPLE'; out['comparisons']=None; return out
    p=out['POSITIVE']; n=out['NEGATIVE']
    ret3=p['3d']['median_return_pct']>n['3d']['median_return_pct']
    ret7=p['7d']['median_return_pct']>n['7d']['median_return_pct']
    excursion=[
        p['3d']['median_mfe_pct']>=n['3d']['median_mfe_pct'],
        p['3d']['median_mae_pct']>=n['3d']['median_mae_pct'],
        p['7d']['median_mfe_pct']>=n['7d']['median_mfe_pct'],
        p['7d']['median_mae_pct']>=n['7d']['median_mae_pct'],
    ]
    score=sum(excursion)
    comps={'return_3d_positive_gt_negative':ret3,'return_7d_positive_gt_negative':ret7,
           'excursion_better_count':score,'excursion_details':excursion}
    if ret3 and ret7 and score>=3:status='PASS'
    elif (not ret3) and (not ret7) and score<=1:status='FAIL — MATERIAL INVERSION'
    else:status='MIXED / INCONCLUSIVE'
    out['formal_status']=status; out['comparisons']=comps
    return out

def hash_regimes(rows):
    slim=[(x['close_time'],x['regime'],x['eth_hlabel'],x['eth_llabel'],x['btc_hlabel'],x['btc_llabel'],
           None if x['eth_atr14'] is None else round(x['eth_atr14'],12),
           round(x['ema20'],12),round(x['ema50'],12)) for x in rows]
    return hashlib.sha256(json.dumps(slim,separators=(',',':')).encode()).hexdigest()

spot_start=NOW-timedelta(days=410)
b4,m4=load_spot_archives('ETHUSDT','4h',spot_start,NOW)
b1,m1=load_spot_archives('ETHUSDT','1h',spot_start,NOW)
if continuity(b4,4*3600000) or continuity(b1,3600000):raise RuntimeError('Spot continuity gap')
rec=build_spot_series(b4,.35)
latest_ct=min(b4[-1]['close_time'],b1[-1]['close_time'])
eval_start=latest_ct-EVAL_DAYS*86400000

eth1,me=load_futures_all_1d('ETHUSDT',2019,11)
btc1,mb=load_futures_all_1d('BTCUSDT',2019,9)
eth_gaps=continuity(eth1,86400000); btc_gaps=continuity(btc1,86400000)
if eth_gaps or btc_gaps:raise RuntimeError('Futures 1D continuity gap')

reg,eth_states,btc_states,eth_seq,btc_seq=classify_regimes(eth1,btc1)
reg2,_,_,_,_=classify_regimes(eth1,btc1)
reg_hash=hash_regimes(reg); deterministic=(reg_hash==hash_regimes(reg2))

eps=episodes_overall(rec,eval_start)
eps=attach_outcomes(eps,rec,b1)
lookahead_violations=bind_regime(eps,reg)

core=('UPTREND','DOWNTREND_RISK_OFF','RANGE_CHOP')
summary={r:summarize_regime(eps,r) for r in core}
summary['TRANSITION_UNCLASSIFIED']=summarize_regime(eps,'TRANSITION_UNCLASSIFIED')
pass_count=sum(1 for r in core if summary[r]['formal_status']=='PASS')
sufficient_count=sum(1 for r in core if summary[r]['formal_status']!='INSUFFICIENT REGIME SAMPLE')
gate='PASS' if pass_count>=2 else 'HOLD PROVISIONAL'
regime_counts={}
for x in reg:
    if x['close_time']>=eval_start:
        regime_counts[x['regime']]=regime_counts.get(x['regime'],0)+1

out={
 'spot_config':SPOT_CFG,'regime_config':REGIME_CFG,'structure_spec':STRUCT_CFG,
 'source':{'spot':'Binance official Spot Public Data Archive / checksum verified',
           'regime':'Binance official USD-M Perpetual ETHUSDT + BTCUSDT 1D Public Data Archive / checksum verified'},
 'data':{'spot_4h_bars':len(b4),'spot_1h_bars':len(b1),'eth_1d_bars':len(eth1),'btc_1d_bars':len(btc1),
         'spot_4h_meta':m4,'spot_1h_meta':m1,'eth_1d_meta':me,'btc_1d_meta':mb,
         'eth_1d_gaps':eth_gaps,'btc_1d_gaps':btc_gaps,'eval_start':eval_start,'latest_spot_close':latest_ct},
 'validation':{'regime_deterministic':deterministic,'regime_hash':reg_hash,'lookahead_violations':lookahead_violations,
               'binding_rule':'episode start 4H boundary -> latest completed 1D close_time <= start',
               'formal_spot_episode_count':len(eps)},
 'regime_1d_boundary_counts':regime_counts,
 'formal_overall_spot_regime_split':summary,
 'core_regime_sample_sufficient_count':sufficient_count,
 'core_regime_pass_count':pass_count,
 'regime_directional_discrimination_gate':gate,
 'promotion_effect':'No automatic Spot promotion; Integrated Utility Test remains pending Structure v0.2 shadow/cutover review.',
 'notes':['Regime classifier frozen in Protocol v1.2.5 before replay.',
          'TRANSITION_UNCLASSIFIED excluded from 2-of-3 core regime gate.',
          'No q35, Spot aggregation, Structure threshold, EMA threshold, or classifier tuning performed after outcomes.']
}
with open('audit/spot_regime_v01_result.pretty.json','w') as f:json.dump(out,f,indent=2,ensure_ascii=False)
with open('audit/spot_regime_v01_result.json','w') as f:json.dump(out,f,separators=(',',':'),ensure_ascii=False)
print('SPOT_REGIME_REPLAY_JSON='+json.dumps(out,separators=(',',':'),ensure_ascii=False))
