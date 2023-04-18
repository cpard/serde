import duckdb
from pathlib import Path
import time

query = """
SELECT
    *,
    CASE
        WHEN repo IN (SELECT repo FROM  read_parquet('../data/parquet/stg_spammy_repos.parquet') )
        THEN 'suspected-activity_cluster'
        ELSE 'unknown'
    END as fake_acct
FROM read_parquet('../data/parquet/stg_starring_actor_repo_clusters.parquet') 
"""

start = time.time()
print(duckdb.sql(query).show())
print(f"Time: {time.time() - start}")

q_output = f'copy ({query}) to \'../data/parquet/starring_actor_repo_summary.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start = time.time()
duckdb.sql(q_output)
print(f"Time IO Query: {time.time() - start}")
