import duckdb
from pathlib import Path
import time

repo_name = 'netdata/netdata'

query = f"""
WITH raw_data AS (
select * FROM read_json('../data/*.json.gz',
columns={{
      id: 'BIGINT',
      type: 'VARCHAR',
      actor: 'STRUCT(id UBIGINT,
                     login VARCHAR,
                     display_login VARCHAR,
                     gravatar_id VARCHAR,
                     url VARCHAR,
                     avatar_url VARCHAR)',
      repo: 'STRUCT(id UBIGINT, name VARCHAR, url VARCHAR)',
      payload: 'JSON',
      public: 'BOOLEAN',
      created_at: 'TIMESTAMP',
      org: 'STRUCT(id UBIGINT, login VARCHAR, gravatar_id VARCHAR, url VARCHAR, avatar_url VARCHAR)'
    }}, 
        maximum_object_size=20000000, sample_size=-1, lines='true', timestampformat='%Y-%m-%dT%H:%M:%SZ')
),
watch_event_actors AS (
  SELECT
    DISTINCT actor.login as actor
  FROM raw_data
    WHERE type = 'WatchEvent' -- starred
    AND repo.name = {repo_name}
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
  IF(repo.name = {repo_name}, TRUE, FALSE) as is_target_repo,
  IF(type = 'WatchEvent', TRUE, FALSE) as is_star,
FROM raw_data
WHERE actor.login IN (SELECT actor FROM watch_event_actors);"""

q_parquet = """
with parquet_data AS (
    select * FROM read_parquet('../data/parquet/output.parquet')
),
watch_event_actors AS (
  SELECT
    DISTINCT actor.login as actor
  FROM parquet_data
    WHERE type = 'WatchEvent' -- starred
    AND repo.name = 'netdata/netdata'
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
  IF(repo.name = 'netdata/netdata', TRUE, FALSE) as is_target_repo,
  IF(type = 'WatchEvent', TRUE, FALSE) as is_star,
FROM parquet_data
WHERE actor.login IN (SELECT actor FROM watch_event_actors)
"""

pragma_q = "PRAGMA temp_directory='/tmp/tmp.tmp';"
#start_time = time.time()
#print(duckdb.sql(query).show())
#print("--- %s seconds for JSON---" % (time.time() - start_time))


start_time = time.time()
print(duckdb.sql(f'{pragma_q} {q_parquet}').show())
print("--- %s seconds for Parquet---" % (time.time() - start_time))


q_output = f'copy ({q_parquet}) to \'../data/parquet/stg_all_actions_for_actors_who_starred_repo.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print("--- %s seconds for Parquet---" % (time.time() - start_time))



