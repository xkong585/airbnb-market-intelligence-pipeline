"""Real, unscaled SF QC snapshots: result-equivalent repeated-report materialization."""
from pathlib import Path
import json,time,math,platform,hashlib
import pandas as pd
import numpy as np
import duckdb
ROOT=Path(__file__).resolve().parents[1]

def compare(con,where=''):
 base=f'''SELECT snapshot_date,neighbourhood_cleansed,room_type,COUNT(*)::BIGINT listing_count,COUNT(price_cents)::BIGINT known_price_count,SUM(price_cents)::HUGEINT price_sum_cents,SUM(availability_365)::BIGINT available_days FROM source_listings {where} GROUP BY 1,2,3 ORDER BY 1,2,3'''
 fast=f'''SELECT snapshot_date,neighbourhood_cleansed,room_type,listing_count,known_price_count,price_sum_cents,available_days FROM report_mart {where} ORDER BY 1,2,3'''
 return base,fast

def materialize(con):
 con.execute('''CREATE OR REPLACE TABLE report_mart AS SELECT snapshot_date,neighbourhood_cleansed,room_type,COUNT(*)::BIGINT listing_count,COUNT(price_cents)::BIGINT known_price_count,SUM(price_cents)::HUGEINT price_sum_cents,SUM(availability_365)::BIGINT available_days FROM source_listings GROUP BY 1,2,3''')

def run():
 paths=sorted((ROOT/'data/processed').rglob('listings.parquet'))
 if len(paths)!=2:raise ValueError('Run the locked two-snapshot SF QC pipeline first')
 df=pd.concat([pd.read_parquet(p,columns=['snapshot_date','neighbourhood_cleansed','room_type','price_cents','availability_365']) for p in paths],ignore_index=True)
 con=duckdb.connect();con.execute('SET threads=1');con.register('source_df',df);con.execute('CREATE TABLE source_listings AS SELECT * FROM source_df');start=time.perf_counter();materialize(con);build_ms=(time.perf_counter()-start)*1000
 cases=[('all',''),('latest',"WHERE snapshot_date='2026-09-14'"),('private_room',"WHERE room_type='Private room'"),('latest_entire_home',"WHERE snapshot_date='2026-09-14' AND room_type='Entire home/apt'")];rows=[];out=ROOT/'evidence';out.mkdir(exist_ok=True)
 for name,where in cases:
  a,b=compare(con,where);left=con.execute(a).fetchall();right=con.execute(b).fetchall();assert left==right
  for _ in range(3):con.execute(a).fetchall();con.execute(b).fetchall()
  timings={'baseline':[],'optimized':[]}
  for i in range(31):
   for label,sql in ([('baseline',a),('optimized',b)] if i%2==0 else [('optimized',b),('baseline',a)]):
    t=time.perf_counter();con.execute(sql).fetchall();timings[label].append((time.perf_counter()-t)*1000)
  p=float(np.median(timings['baseline']));q=float(np.median(timings['optimized']));rows.append({'case':name,'result_rows':len(left),'results_identical':True,'baseline_p50_ms':p,'optimized_p50_ms':q,'baseline_p95_ms':float(np.quantile(timings['baseline'],.95)),'optimized_p95_ms':float(np.quantile(timings['optimized'],.95)),'speedup':p/q,'build_ms':build_ms,'break_even_reuses':math.ceil(build_ms/(p-q)) if p>q else None,'repetitions':31,'samples_ms':timings})
  (out/f'benchmark_{name}_baseline_plan.txt').write_text(con.execute('EXPLAIN ANALYZE '+a).fetchall()[0][1]);(out/f'benchmark_{name}_optimized_plan.txt').write_text(con.execute('EXPLAIN ANALYZE '+b).fetchall()[0][1])
 # A missing-price group remains missing, not a zero mean. Downstream divides SUM(cents)/COUNT(cents).
 reconcile=con.sql('SELECT SUM(listing_count) n,SUM(known_price_count) prices,SUM(price_sum_cents)::HUGEINT cents FROM report_mart').fetchone();assert int(reconcile[0])==len(df) and int(reconcile[1])==int(df.price_cents.notna().sum()) and int(reconcile[2])==int(df.price_cents.sum())
 result={'source_scope':'Two locked, cleaned San Francisco snapshots: 2026-06-14 and 2026-09-14. Not the original four-city/2025 pipeline.','source_rows':len(df),'report_mart_rows':con.sql('SELECT COUNT(*) FROM report_mart').fetchone()[0],'engine':'DuckDB '+duckdb.__version__,'threads':1,'method':'same connection; warm cache; three warmups; 31 interleaved repetitions; fetch included; preprocessing excluded for both; materialization construction separately reported','environment':{'python':platform.python_version(),'platform':platform.platform()},'limits':'Local hardware microbenchmark, not measured Snowflake latency. Materialization trades construction/storage and refresh latency for repeated-report speed. No input replication, query sleeps, artificial index claims or cloud performance claims. Performance is descriptive and varies by host.','cases':rows}
 (out/'sql_benchmark.json').write_text(json.dumps(result,indent=2)+'\n')
 pd.DataFrame([{k:v for k,v in row.items() if k!='samples_ms'} for row in rows]).to_csv(out/'sql_benchmark.csv',index=False)
 body='<h1>Airbnb SQL materialization benchmark</h1><p>'+result['source_scope']+'</p><p>'+result['method']+'</p><p>'+result['limits']+'</p>'+pd.read_csv(out/'sql_benchmark.csv').to_html(index=False,float_format=lambda x:f'{x:.3f}')+'<p>Average prices must be formed from price_sum_cents / known_price_count, not an average of group averages. All four query outputs match exactly, including nulls.</p>'
 (ROOT/'reports/sql_benchmark.html').write_text('<!doctype html><meta charset="utf-8"><style>body{font:16px system-ui;margin:40px;color:#193b42}table{border-collapse:collapse;font-size:12px}td,th{padding:10px;border:1px solid #ccd8d8}p{max-width:1100px;line-height:1.6}</style>'+body)
 ready=ROOT/'reports/READY.json'
 if ready.exists():
  manifest=json.loads(ready.read_text());manifest['sql_benchmark']={'status':'passed','cases':len(rows),'results_identical':all(x['results_identical'] for x in rows)};ready.write_text(json.dumps(manifest,indent=2)+'\n')
 print(json.dumps({'source_rows':result['source_rows'],'mart_rows':result['report_mart_rows'],'speedups':{x['case']:round(x['speedup'],2) for x in rows}},indent=2));return result
if __name__=='__main__':run()
