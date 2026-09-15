import csv, json
from pathlib import Path
p=Path('audit/spot_v01_canonical_current.json')
rows=json.loads(p.read_text())
out=Path('audit/spot_v01_canonical_current.csv')
fields=['completed_4h_time_utc','close_time_ms','delta_4h_usdt','ratio_4h','delta_24h_usdt','ratio_24h','delta_3d_usdt','ratio_3d','abs_ratio_4h','abs_ratio_24h','abs_ratio_3d','config_id','source']
with out.open('w',newline='') as f:
    w=csv.writer(f,lineterminator='\n')
    w.writerow(fields)
    for r in rows:
        w.writerow([
            r['completed_4h_time_utc'],r['close_time_ms'],r['delta_4h_usdt'],r['ratio_4h'],r['delta_24h_usdt'],r['ratio_24h'],
            r['delta_3d_usdt'],r['ratio_3d'],r['abs_ratio_4h'],r['abs_ratio_24h'],r['abs_ratio_3d'],
            'ETH-SPOT-CLASS-v0.1-PROVISIONAL','Binance official Spot Public Data Archive / checksum verified'
        ])
print(f'rows={len(rows)}')
