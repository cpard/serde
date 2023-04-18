import duckdb
from pathlib import Path
import time

target_repo = 'frasermarlow/tap-bls'
start_star_lookabck_yymmdd = '230101'
end_yymmdd = '231231'

query = f"""
WITH watch_event_actors AS (
  SELECT
    DISTINCT actor.login as actor
  FROM read_parquet('../data/parquet/output.parquet')
  WHERE type = 'WatchEvent' -- starred
    AND repo.name = '{target_repo}'
    AND actor.login NOT LIKE '%[bot]'-- reduce table size
)
SELECT
  actor.login as actor,
  CONCAT(type, IFNULL(JSON_EXTRACT(payload, '$.action'), '')) as event,
  created_at::DATE date,
  created_at,
  actor.avatar_url,
  repo.name repo,
  org.login org,
  payload,
  IF(repo.name = '{target_repo}', TRUE, FALSE) as is_target_repo,
  IF(type = 'WatchEvent', TRUE, FALSE) as is_star,
FROM read_parquet('../data/parquet/output.parquet')
WHERE actor.login IN (SELECT actor FROM watch_event_actors)
"""
pragma_q = "PRAGMA temp_directory='/tmp/tmp.tmp';"

start_time = time.time()
print(duckdb.sql(pragma_q + query))
print(f"Time: {time.time() - start_time}")

q_output = f'copy ({query}) to \'../data/parquet/stg_all_actions_for_actors_who_starred_repo.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print(f"Time IO Query: {time.time() - start_time}")