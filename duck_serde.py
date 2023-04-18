import duckdb
from pathlib import Path
import time

data_folder = Path("../data/parquet/")
filename = '2023-01-01-15.json'
json_file = data_folder / filename
output_file_name = data_folder / 'output.parquet'

q_input = """
SELECT *
  FROM read_json(
    '../data/*.json.gz',
    columns={
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
    },
    lines='true', maximum_object_size=20000000, timestampformat='%Y-%m-%dT%H:%M:%SZ'
  )
"""
q_output = f'copy ({q_input}) to \'{output_file_name}\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print("--- %s seconds Not Partitioned---" % (time.time() - start_time))
