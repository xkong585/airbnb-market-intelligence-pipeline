"""Fail closed before gold/Snowflake. Run with spark-submit, --stage silver|gold.

Requires the original VM paths. Validated syntax here; original cloud execution
is deliberately not claimed. Independent local QC is under qc/.
"""
import argparse
import ast
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

parser=argparse.ArgumentParser()
parser.add_argument('--stage',choices=['silver','gold'],required=True)
args=parser.parse_args()
spark=SparkSession.builder.appName('airbnb-publication-quality-gate').getOrCreate()
base=os.environ.get('AIRBNB_DATA_ROOT','/shared/data')
report=Path(os.environ.get('AIRBNB_QC_LOG_DIR','/shared/data/qc'))
report.mkdir(parents=True,exist_ok=True)
log={'stage':args.stage,'checks':[],'status':'running'}

def require(ok,name):
    log['checks'].append({'name':name,'pass':bool(ok)})
    if not ok:raise ValueError('Publication blocked: '+name)

try:
    silver=spark.read.parquet(f'{base}/silver/listings_enriched')
    keys=['city','snapshot_date','listing_id']
    require(set(keys+['tract_geoid','median_income']).issubset(silver.columns),'silver_required_schema')
    require(not silver.filter(F.col('listing_id').isNull()).limit(1).count(),'nonnull_listing_id')
    require(not silver.groupBy(*keys).count().filter(F.col('count')!=1).limit(1).count(),'snapshot_key_unique')
    require(not silver.filter(F.col('median_income')<0).limit(1).count(),'negative_income_sentinel_excluded')
    # Reuse the actual source script's literal batch configuration without running it.
    module=ast.parse(Path(__file__).with_name('silver_pipeline.py').read_text())
    batches=next(ast.literal_eval(node.value) for node in module.body
                 if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CITY_SNAPSHOTS' for t in node.targets))
    expected={(city,date) for city,dates in batches.items() for date in dates}
    actual={(r.city,str(r.snapshot_date)) for r in silver.select('city','snapshot_date').distinct().collect()}
    require(actual==expected,'complete_expected_batch_set')
    # Independently count raw valid-coordinate rows by source partition.
    # This catches missing/extra batches, spatial multiplication and unrecorded filtering.
    for partition in silver.select('city','snapshot_date').distinct().collect():
        city,date=partition.city,str(partition.snapshot_date)
        raw=spark.read.option('header',True).option('multiLine',True).option('escape','"').csv(
            f'{base}/bronze/city={city}/snapshot_date={date}/listings.csv.gz')
        require('id' in raw.columns,'bronze_required_id_'+city+'_'+date)
        require(not raw.filter(F.col('id').isNull()).limit(1).count(),'bronze_nonnull_key_'+city+'_'+date)
        require(not raw.groupBy('id').count().filter(F.col('count')>1).limit(1).count(),'bronze_unique_key_'+city+'_'+date)
        valid=raw.withColumn('lat',F.expr('try_cast(latitude as double)')).withColumn('lon',F.expr('try_cast(longitude as double)')).filter(
            F.col('lat').between(-90,90)&F.col('lon').between(-180,180))
        output=silver.filter((F.col('city')==city)&(F.col('snapshot_date')==F.lit(date)))
        require(valid.count()==output.count(),'bronze_silver_rows_'+city+'_'+date)
        require(not valid.select(F.col('id').alias('listing_id')).join(output.select('listing_id'),'listing_id','left_anti').limit(1).count(),
                'bronze_silver_membership_'+city+'_'+date)
    if args.stage=='gold':
        listing=spark.read.parquet(f'{base}/golden/fact_listing_summary')
        require(set(keys).issubset(listing.columns),'gold_listing_snapshot_schema')
        require(listing.count()==silver.count(),'silver_gold_listing_count')
        require(not listing.groupBy(*keys).count().filter(F.col('count')!=1).limit(1).count(),'gold_listing_keys')
        require(not silver.select(*keys).join(listing.select(*keys),keys,'left_anti').limit(1).count(),'silver_gold_key_membership')
        host=spark.read.parquet(f'{base}/golden/fact_host_summary')
        require({'snapshot_date','known_host_listing_count','largest_host_share_known'}.issubset(host.columns),'host_snapshot_grain')
        require(not host.filter((F.col('largest_host_share_known')<0)|(F.col('largest_host_share_known')>1)).limit(1).count(),'host_share_range')
    log['status']='passed'
except Exception as exc:
    log.update(status='failed',error=str(exc));raise
finally:
    (report/f'{args.stage}_gate.json').write_text(json.dumps(log,indent=2))
    spark.stop()
