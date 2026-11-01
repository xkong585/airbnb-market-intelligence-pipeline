import sys
from pathlib import Path
import duckdb
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'benchmark'))
from run_benchmark import materialize,compare

def test_missing_price_and_weighted_mean_are_conserved():
 c=duckdb.connect();c.execute("CREATE TABLE source_listings(snapshot_date VARCHAR,neighbourhood_cleansed VARCHAR,room_type VARCHAR,price_cents BIGINT,availability_365 INTEGER)")
 c.execute("INSERT INTO source_listings VALUES ('2026-09-14','A','Private room',NULL,5),('2026-09-14','A','Private room',10000,10),('2026-09-14','B','Private room',30000,20),('2026-09-14','B','Private room',30000,20),('2026-09-14','C','Private room',NULL,15)")
 materialize(c);a,b=compare(c);assert c.execute(a).fetchall()==c.execute(b).fetchall()
 assert c.sql('SELECT SUM(price_sum_cents)/SUM(known_price_count) FROM report_mart').fetchone()[0]==70000/3
 assert c.sql("SELECT price_sum_cents FROM report_mart WHERE neighbourhood_cleansed='C'").fetchone()[0] is None
