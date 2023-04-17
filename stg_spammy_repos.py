import duckdb
from pathlib import Path
import time

repo_actor_overlap_gte = 4
min_repo_actor_overlap_gte = 3
min_p_actor_overlap_gte = 0.5

query = f"""
SELECT repo
FROM read_parquet('../data/parquet/stg_starring_actor_repo_clusters.parquet')
WHERE -- at least x overlapping actors from this set interacted with repo
    n_actor_overlap >= {repo_actor_overlap_gte}
    OR ( -- fewer actors, but at least z% of this repos' interactions were from those actors
        n_actor_overlap >= {min_repo_actor_overlap_gte}
        AND p_actor_overlap >= {min_p_actor_overlap_gte}
    )
"""

start = time.time()
print(duckdb.sql(query))
print(f"--- {time.time() - start} seconds for modelling query---")

q_output = f'copy ({query}) to \'../data/parquet/stg_spammy_repos.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start = time.time()
duckdb.sql(q_output)
print(f"--- {time.time() - start} seconds for IO query---")
