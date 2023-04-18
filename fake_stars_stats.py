import duckdb
from pathlib import Path
import time

query = """
SELECT
    COUNT(*) as total_users_starring_repo,
    SUM(CASE WHEN fake_acct LIKE '%suspected%' THEN 1 ELSE 0 END) as fake_stars,
    SUM(CASE WHEN fake_acct LIKE '%cluster%' THEN 1 ELSE 0 END) as fake_stars_activity_cluster,
    SUM(CASE WHEN fake_acct LIKE '%low%' THEN 1 ELSE 0 END) as fake_stars_low_activity,
    SUM(CASE WHEN fake_acct = 'unknown' THEN 1 ELSE 0 END) as real_stars,
    (SUM(CASE WHEN fake_acct LIKE '%suspected%' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 as p_fake
FROM read_parquet('../data/parquet/starring_actor_summary.parquet')
"""

start = time.time()
print(duckdb.sql(query).show())
print(f"Time: {time.time() - start}")

q_output = f'copy ({query}) to \'../data/parquet/fake_stars_stats.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start = time.time()
duckdb.sql(q_output)
print(f"Time IO Query: {time.time() - start}")