from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
import pandas as pd
import pytest
from shapely.geometry import Polygon
import geopandas as gpd
from pipeline import clean_listings,parse_price,census_value,spatial_assign,census_join,QualityError

@pytest.fixture
def rows():
    return pd.DataFrame({'id':['a','b'],'host_id':['h','h'],'latitude':[37.7,37.8],
        'longitude':[-122.4,-122.4],'room_type':['Entire home/apt']*2,'price':['$1,200.50',None],
        'minimum_nights':[1,30],'availability_365':[0,365],'last_scraped':['2026-09-14']*2})

def test_duplicate_blocks(rows):
    with pytest.raises(QualityError,match='key'):clean_listings(pd.concat([rows,rows]),'2026-09-14')

def test_schema_blocks(rows):
    with pytest.raises(QualityError,match='schema'):clean_listings(rows.drop(columns='host_id'),'2026-09-14')

@pytest.mark.parametrize('col,value',[('availability_365',366),('minimum_nights',0),('minimum_nights',1.5),('last_scraped','2025-01-01')])
def test_bad_values_block(rows,col,value):
    rows[col]=rows[col].astype(object)
    rows.loc[0,col]=value
    with pytest.raises(QualityError):clean_listings(rows,'2026-09-14')

def test_quarantine_conserves(rows):
    rows.loc[0,'latitude']=100
    keep,bad=clean_listings(rows,'2026-09-14')
    assert len(keep)==1 and len(bad)==1 and len(keep)+len(bad)==len(rows)

def test_price_exact_cents():
    assert parse_price('$1,234.56')==(123456,'ok')
    assert parse_price('not a price')==(None,'parse_failure')
    assert parse_price(None)==(None,'missing')
    assert parse_price('$12,34.56')==(None,'parse_failure')

def test_negative_census_codes_not_income():
    assert census_value('-666666666')==(None,'special:-666666666')
    assert census_value('250001',estimate=True)==(None,'top_coded_250000_plus')
    assert census_value('50000')==(50000,'ok')

def test_boundary_tie_deterministic():
    tracts=gpd.GeoDataFrame({'GEOID':['06075000200','06075000100']},geometry=[Polygon([(0,0),(1,0),(1,1),(0,1)]),Polygon([(1,0),(2,0),(2,1),(1,1)])],crs=4326)
    d=pd.DataFrame({'latitude':[.5],'longitude':[1.]})
    out=spatial_assign(d,tracts)
    assert out.GEOID.iloc[0]=='06075000100' and out.spatial_match_count.iloc[0]==2

def test_census_duplicate_blocks():
    d=pd.DataFrame({'GEOID':['06075000100']})
    census=pd.DataFrame({'GEOID':['06075000100']*2,'vintage':[2023]*2})
    with pytest.raises(QualityError,match='Duplicate'):census_join(d,census)

def test_snapshot_reuse_is_not_global_duplicate(rows):
    clean1,_=clean_listings(rows,'2026-09-14')
    rows['last_scraped']='2026-06-14';clean2,_=clean_listings(rows,'2026-06-14')
    combined=pd.concat([clean1,clean2])
    assert not combined.duplicated(['city','snapshot_date','id']).any()


def test_missing_minimum_nights_quarantined(rows):
    rows.loc[0,'minimum_nights']=float('nan')
    clean,bad=clean_listings(rows,'2026-09-14')
    assert len(clean)==1 and len(bad)==1
    assert bad.quarantine_reason.iloc[0]=='minimum_nights_missing'

def test_scrape_days_can_vary_within_provider_quarter(rows):
    rows.loc[1,'last_scraped']='2026-09-22'
    clean,bad=clean_listings(rows,'2026-09-14')
    assert len(clean)==2 and len(bad)==0

def test_failed_batch_removes_publication_marker(tmp_path,monkeypatch):
    import pipeline
    (tmp_path/'reports').mkdir();(tmp_path/'evidence').mkdir()
    for name in ['qc_report.html','READY.json']:(tmp_path/'reports'/name).write_text('stale')
    def corrupt():raise QualityError('intentional corruption')
    monkeypatch.setattr(pipeline,'ROOT',tmp_path)
    monkeypatch.setattr(pipeline,'verify_downloads',corrupt)
    with pytest.raises(QualityError):pipeline.run()
    assert not (tmp_path/'reports/READY.json').exists()
    assert not (tmp_path/'reports/qc_report.html').exists()
