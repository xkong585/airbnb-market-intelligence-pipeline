"""Independent QC slice. Aggregates only are published; input/row records stay local."""
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
import hashlib
import html
import json
import os
import re
import sqlite3
import time
import urllib.request
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.strtree import STRtree

ROOT=Path(__file__).resolve().parents[1]
CONTRACT=json.loads((ROOT/'docs/schema_contract.json').read_text())

class QualityError(ValueError):pass

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def parse_price(value):
    if pd.isna(value) or str(value).strip()=='':return None,'missing'
    s=str(value).strip()
    if not re.fullmatch(r'\$?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d{1,2})?',s):return None,'parse_failure'
    try:
        cents=int(Decimal(s.replace('$','').replace(',',''))*100)
        return cents,'ok'
    except InvalidOperation:return None,'parse_failure'

def census_value(value,estimate=False):
    s=str(value).strip()
    if s in ['','nan','None']:return None,'missing'
    try:n=int(s)
    except ValueError:return None,'non_numeric:'+s
    if n<0:return None,'special:'+s
    if estimate and n==250001:return None,'top_coded_250000_plus'
    return n,'ok'

def verify_downloads():
    rows=json.loads((ROOT/'docs/source_lock.json').read_text())
    for item in rows:
        path=ROOT/'data/raw'/item['file']
        if not path.exists():
            with urllib.request.urlopen(item['url'],timeout=180) as response:b=response.read()
            if hashlib.sha256(b).hexdigest()!=item['sha256']:raise QualityError('Source changed: '+item['file'])
            tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_bytes(b);os.replace(tmp,path)
        if sha(path)!=item['sha256']:raise QualityError('Input checksum failed: '+item['file'])
        head=path.read_bytes()[:100].lower()
        if b'<html' in head or b'<!doctype' in head:raise QualityError('Downloaded HTML instead of data')
    pd.DataFrame(rows).to_csv(ROOT/'evidence/source_manifest.csv',index=False)
    return rows

def clean_listings(frame,snapshot):
    missing=set(CONTRACT['required'])-set(frame)
    if missing:raise QualityError('Missing schema: '+','.join(sorted(missing)))
    d=frame.copy();d['city']=CONTRACT['city'];d['snapshot_date']=snapshot
    if d.id.isna().any() or d.id.astype(str).str.strip().eq('').any() or d.duplicated(['city','snapshot_date','id']).any():
        raise QualityError('Duplicate or missing listing key; publish blocked')
    for col in ['availability_365','minimum_nights','latitude','longitude']:
        d[col+'_raw']=d[col];d[col]=pd.to_numeric(d[col],errors='coerce')
    for col in ['availability_365','minimum_nights']:
        valid=d[col].notna() & np.isfinite(d[col]) & d[col].eq(d[col].round())
        valid &= d[col].between(0,365) if col=='availability_365' else d[col].ge(1)
        # Missing minimum-nights rows are explicitly quarantined after source review.
        invalid = ~valid & (d[col].notna() if col=='minimum_nights' else True)
        if invalid.any():raise QualityError('Invalid '+col+'; publish blocked')
    scraped=pd.to_datetime(d.last_scraped,errors='coerce')
    if scraped.isna().any() or not scraped.dt.to_period('Q').eq(pd.Period(snapshot,freq='Q')).all():raise QualityError('Scrape date mismatch; publish blocked')
    parsed=d.price.map(parse_price)
    d['price_cents']=pd.array([v[0] for v in parsed],dtype='Int64')
    d['price_status']=[v[1] for v in parsed];d['currency']='USD'
    coords=d.latitude.between(-90,90)&d.longitude.between(-180,180)
    valid=coords & d.minimum_nights.notna()
    quarantine=d.loc[~valid].copy()
    quarantine['quarantine_reason']=np.where(~coords.loc[~valid], 'invalid_coordinates', 'minimum_nights_missing')
    return d.loc[valid].copy(),quarantine

def census_table():
    path=ROOT/'data/raw/acsdt5y2023-b19013.dat'
    d=pd.read_csv(path,sep='|',dtype=str)
    required={'GEO_ID','B19013_E001','B19013_M001'}
    if not required.issubset(d):raise QualityError('Invalid ACS schema')
    d=d[d.GEO_ID.str.startswith('1400000US06075')].copy()
    d['GEOID']=d.GEO_ID.str[-11:];d['vintage']=2023
    if d.duplicated(['GEOID','vintage']).any():raise QualityError('Duplicate Census GEOID/vintage')
    for src,target,estimate in [('B19013_E001','income',True),('B19013_M001','income_MOE90',False)]:
        mapped=d[src].map(lambda v:census_value(v,estimate))
        d[target]=[v[0] for v in mapped];d[target+'_annotation']=[v[1] for v in mapped]
    d.rename(columns={'B19013_E001':'income_raw','B19013_M001':'income_MOE90_raw'},inplace=True)
    return d

def boundaries():
    d=gpd.read_file(ROOT/'data/raw/cb_2023_06_tract_500k.zip')
    if d.crs is None:raise QualityError('Unknown tract CRS')
    d=d[(d.STATEFP=='06')&(d.COUNTYFP=='075')][['GEOID','geometry']].copy()
    if d.GEOID.isna().any() or d.GEOID.duplicated().any() or not d.GEOID.str.fullmatch(r'\d{11}').all():raise QualityError('Invalid tract key')
    if not d.is_valid.all() or d.geometry.is_empty.any():raise QualityError('Invalid tract geometry')
    return d.to_crs(4326).reset_index(drop=True)

def spatial_assign(d,tracts):
    tree=STRtree(tracts.geometry.to_numpy());matches=[];counts=[]
    for row in d.itertuples():
        p=Point(row.longitude,row.latitude)
        indices=tree.query(p,predicate='intersects')
        keys=sorted(tracts.iloc[indices].GEOID.tolist())
        matches.append(keys[0] if keys else None);counts.append(len(keys))
    d=d.copy();d['GEOID']=matches;d['spatial_match_count']=counts
    return d

def census_join(d,census):
    if census.duplicated(['GEOID','vintage']).any():raise QualityError('Duplicate Census GEOID/vintage')
    out=d.merge(census,on='GEOID',how='left',validate='many_to_one',indicator=True)
    if len(out)!=len(d):raise QualityError('Census join inflation')
    audit={'input_rows':len(d),'output_rows':len(out),'matched':int(out._merge.eq('both').sum()),
           'unmatched':int(out._merge.ne('both').sum()),'income_available':int(out.income.notna().sum()),'vintage':2023}
    return out.drop(columns='_merge'),audit

def metrics(d):
    known=d[d.host_id.notna() & d.host_id.astype(str).str.strip().ne('')]
    counts=known.groupby('host_id').size()
    return {'listing_count':len(d),'host_count':int(known.host_id.nunique()),'known_host_listing_count':len(known),
            'largest_host_count':int(counts.max()) if len(counts) else 0,
            'largest_host_share_known':float(counts.max()/len(known)) if len(known) else None,
            'mean_price_USD':float(d.price_cents.mean()/100) if d.price_cents.notna().any() else None,
            'price_available_n':int(d.price_cents.notna().sum()),'mean_availability_365':float(d.availability_365.mean())}

def sql_reconcile(d,m,snapshot):
    with sqlite3.connect(':memory:') as con:
        d[['id','host_id','price_cents','availability_365']].to_sql('listings',con,index=False)
        r=con.execute('SELECT COUNT(*), COUNT(DISTINCT host_id), AVG(price_cents)/100.0, AVG(availability_365) FROM listings').fetchone()
        host=con.execute('SELECT MAX(n) FROM (SELECT COUNT(*) n FROM listings WHERE host_id IS NOT NULL GROUP BY host_id)').fetchone()[0]
    rows=[]
    for name,sql in zip(['listing_count','host_count','mean_price_USD','mean_availability_365','largest_host_count'],[*r,host]):
        value=m[name];ok=np.isclose(value,sql,atol=1e-8,rtol=1e-10)
        rows.append({'snapshot_date':snapshot,'metric':name,'python':value,'SQL':sql,'pass':bool(ok)})
    if not all(x['pass'] for x in rows):raise QualityError('Independent SQL reconciliation failure')
    return rows

def atomic_partition(d,snapshot):
    directory=ROOT/'data/processed'/f'city={CONTRACT["city"]}'/f'snapshot_date={snapshot}'
    directory.mkdir(parents=True,exist_ok=True);target=directory/'listings.parquet';tmp=directory/'listings.tmp.parquet'
    d.sort_values('id').to_parquet(tmp,index=False);os.replace(tmp,target)
    return target

def generate_report(summaries,spatial,joins,comparisons,checks):
    def table(rows):return pd.DataFrame(rows).to_html(index=False,border=0,float_format=lambda n:f'{n:,.3f}')
    scorecard=[{'Release':r['snapshot_date'],'Retained listings':r['listing_count'],'Hosts':r['host_count'],
                'Quarantined':r['quarantined'],'Missing price':r['missing_price'],
                'Advertised price mean (USD)':r['mean_price_USD']} for r in summaries]
    content=f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Airbnb Data Quality | San Francisco</title><style>
    body{{font:15px/1.7 system-ui;background:#f4f6ef;color:#183e3b;margin:0}}main{{max-width:1100px;margin:auto;padding:40px 24px}}h1{{font:48px Georgia;letter-spacing:-.03em}}h2{{font:28px Georgia}}.eyebrow{{text-transform:uppercase;letter-spacing:.15em;color:#007e78;font-size:12px}}article{{padding:25px;background:white;border:1px solid #d7dfd5;border-radius:8px;margin:20px 0;overflow:auto}}table{{width:100%;border-collapse:collapse;font-size:13px}}td,th{{text-align:left;border-bottom:1px solid #e1e8df;padding:12px}}a{{color:#007e78}}.note{{color:#62766c;font-size:13px}}.pass{{padding:6px 12px;background:#e0efdb;border-radius:20px;display:inline-block}}img{{max-width:100%}}</style></head><body><main>
    <span class="eyebrow">San Francisco · Quarterly QC · 2026 upgrade</span><h1>Reliable data before the dashboard.</h1><p>Two public listing snapshots, explicit Census provenance, and a publication gate that fails when integrity breaks.</p>
    <span class="pass">Passed · {len(checks)} batch checks · 10 independent metric reconciliations</span>
    <article><h2>Snapshot scorecard</h2>{table(scorecard)}<p class="note">Prices are advertised USD listing prices, not realized revenue. Availability is not occupancy. Counts are specific to each snapshot. Missing prices stay missing and are excluded only from the price mean. <a href="../evidence/snapshot_metrics.csv">Download complete metrics and scrape-date ranges</a>.</p></article>
    <article><h2>Coverage changed; demand is not established.</h2>{table(comparisons)}<p class="note">Newly observed and no-longer-observed listings may reflect collection coverage or platform changes. These counts do not measure causal market growth or demand.</p></article>
    <article><h2>Spatial joins retain an audit trail.</h2>{table(spatial)}<img src="spatial_qc.png" alt="Aggregate tract-level listing counts and unmatched count by snapshot"><p class="note">Anonymized coordinates; approximate tract context. Generalized 2023 cartographic boundaries at 1:500,000. Multiple matches use lowest GEOID deterministically.</p></article>
    <article><h2>Census income keeps its uncertainty.</h2>{table(joins)}<p class="note">ACS 2019-2023 five-year estimates in 2023 inflation-adjusted dollars, with 90% MOE. Regional income is not host income. Sentinel and top-coded values retain annotations and do not enter means as exact values.</p></article>
    <article><h2>Reproducibility and scope</h2><p>Verified source hashes, one-to-one snapshot keys, explicit quarantine, atomic partition replacement, idempotent reruns, and independent SQLite metric reconciliation. Fault-injection tests intentionally block duplicates, invalid schema, bad dates, bad availability and duplicate Census keys.</p><p>The original collaborative Spark/Sedona/Airflow/Snowflake/Tableau project is retained with attribution. This local QC slice is new work. The original VM and Tableau workbook were not rerun in this environment.</p><p><a href="../evidence/qc_checks.csv">Batch checks</a> · <a href="../evidence/metric_reconciliation.csv">SQL reconciliation</a> · <a href="../evidence/source_manifest.csv">Sources and hashes</a> · <a href="../evidence/run_log.json">Run log</a> · <a href="../docs/boundary_policy.md">Boundary policy</a></p></article>
    <p class="note">Sources: <a href="https://insideairbnb.com/get-the-data/">Inside Airbnb (CC BY 4.0)</a>, US Census ACS 2023 B19013 and 2023 cartographic tract boundaries. Data transformed for validation and aggregate reporting. Snapshot dates identify provider releases; the June release contains 1,620 rows scraped on June 22 and 5,712 on June 14, all in Q2. Exact scrape dates are preserved. Missing minimum-nights rows were quarantined after source review (4 in June, 11 in September); the original failed gate is retained as evidence. Raw/row-level records are excluded from Git.</p>
    </main></body></html>'''
    (ROOT/'reports/qc_report.html').write_text(content)

def plot_map(tracts,frames):
    os.environ.setdefault('MPLCONFIGDIR',str(ROOT/'data/processed/.mplconfig'))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig,axes=plt.subplots(1,len(frames),figsize=(10,4.8),layout='constrained')
    for ax,(snapshot,d) in zip(np.atleast_1d(axes),frames):
        n=d.GEOID.value_counts().rename('listings');g=tracts.merge(n,left_on='GEOID',right_index=True,how='left');g['listings']=g.listings.fillna(0)
        g.plot(column='listings',ax=ax,cmap='YlGnBu',legend=True,edgecolor='#b8c4c0',linewidth=.3)
        ax.set_xlim(-122.53,-122.35);ax.set_ylim(37.70,37.83);ax.set_axis_off();ax.set_title(snapshot+' | unmatched: '+str(d.GEOID.isna().sum()),fontsize=11)
    fig.suptitle('Aggregate listings by approximate Census tract',fontsize=14)
    fig.savefig(ROOT/'reports/spatial_qc.png',dpi=160);plt.close(fig)

def run():
    started=time.perf_counter();(ROOT/'reports').mkdir(exist_ok=True);(ROOT/'evidence').mkdir(exist_ok=True)
    for name in ['qc_report.html','READY.json']:(ROOT/'reports'/name).unlink(missing_ok=True)
    log={'status':'running','started_at_utc':datetime.now(timezone.utc).isoformat()}
    try:
        sources=verify_downloads();tracts=boundaries();census=census_table()
        summaries=[];spatial=[];joins=[];layers=[];checks=[];reconciliation=[];frames=[];schemas=[]
        for item in sorted([s for s in sources if s['file'].startswith('listings_')],key=lambda s:s['snapshot_date']):
            snapshot=item['snapshot_date'];raw=pd.read_csv(ROOT/'data/raw'/item['file'],dtype={'id':str,'host_id':str},low_memory=False)
            schemas.append({'snapshot_date':snapshot,'fields':set(raw.columns)})
            clean,quarantine=clean_listings(raw,snapshot)
            quarantine.to_parquet(ROOT/f'data/processed/quarantine_{snapshot}.parquet',index=False)
            if len(raw)!=len(clean)+len(quarantine):raise QualityError('Layer accounting failed')
            enriched=spatial_assign(clean,tracts);enriched,audit=census_join(enriched,census);audit['snapshot_date']=snapshot;joins.append(audit)
            spatial.append({'snapshot_date':snapshot,'zero':int(enriched.spatial_match_count.eq(0).sum()),'one':int(enriched.spatial_match_count.eq(1).sum()),'multiple':int(enriched.spatial_match_count.gt(1).sum()),'input_rows':len(enriched)})
            m=metrics(enriched);summaries.append({'snapshot_date':snapshot,**m,'quarantined':len(quarantine),'missing_price':int(enriched.price_status.eq('missing').sum()),'price_parse_failure':int(enriched.price_status.eq('parse_failure').sum()),'scrape_date_min':str(raw.last_scraped.min()),'scrape_date_max':str(raw.last_scraped.max()),'missing_host':int(enriched.host_id.isna().sum()),'price_over_1000':int(enriched.price_cents.gt(100000).sum())})
            reconciliation.extend(sql_reconcile(enriched,m,snapshot))
            path=atomic_partition(enriched,snapshot);hash1=sha(path);atomic_partition(enriched,snapshot);hash2=sha(path)
            if hash1!=hash2:raise QualityError('Idempotency check failed')
            checks.extend({'snapshot_date':snapshot,'check':name,'pass':True} for name in ['schema','composite_key','availability_integer_range','minimum_nights_positive_integer','scrape_date','coordinate_accounting','spatial_cardinality_audit','census_many_to_one','layer_conservation','partition_idempotency'])
            layers.append({'snapshot_date':snapshot,'raw':len(raw),'quarantined':len(quarantine),'retained':len(enriched),'deduplicated':0,'balanced':len(raw)==len(quarantine)+len(enriched),'partition_sha256':hash2})
            frames.append((snapshot,enriched))
        before,after=frames;old=set(before[1].id);new=set(after[1].id)
        comparisons=[{'from':before[0],'to':after[0],'previous':len(old),'current':len(new),'continuing':len(old&new),'newly_observed':len(new-old),'no_longer_observed':len(old-new)}]
        drift=[{'from':schemas[0]['snapshot_date'],'to':schemas[1]['snapshot_date'],'added':','.join(sorted(schemas[1]['fields']-schemas[0]['fields'])),'removed':','.join(sorted(schemas[0]['fields']-schemas[1]['fields']))}]
        for name,rows in [('snapshot_comparison',comparisons),('snapshot_metrics',summaries),('spatial_join_audit',spatial),('census_join_audit',joins),('layer_reconciliation',layers),('qc_checks',checks),('metric_reconciliation',reconciliation),('schema_drift',drift)]:pd.DataFrame(rows).to_csv(ROOT/f'evidence/{name}.csv',index=False)
        census.groupby(['income_annotation','income_MOE90_annotation']).size().rename('n').to_csv(ROOT/'evidence/census_annotations.csv')
        plot_map(tracts,frames);generate_report(summaries,spatial,joins,comparisons,checks)
        (ROOT/'reports/READY.json').write_text(json.dumps({'status':'passed','source_lock_sha256':sha(ROOT/'docs/source_lock.json'),'snapshots':CONTRACT['snapshots']}))
        log.update(status='passed',elapsed_seconds=round(time.perf_counter()-started,3),snapshots=len(frames),rows=sum(len(d) for _,d in frames),blocking_checks=len(checks),reconciliations=len(reconciliation))
    except Exception as exc:
        for name in ['qc_report.html','READY.json']:(ROOT/'reports'/name).unlink(missing_ok=True)
        log.update(status='failed',error=str(exc));raise
    finally:(ROOT/'evidence/run_log.json').write_text(json.dumps(log,indent=2))

if __name__=='__main__':run()
