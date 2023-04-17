import duckdb
from pathlib import Path
import time

n_spammy_repo_overlap_gte = 2
p_spammy_repo_overlap_gt = 0.5
actions_per_repo_lt = 2
actor_dates = 1 
actor_repos = 1 
actor_orgs = 1
actor_actions_lte = 2 

query = f"""
SELECT
    *,
    ARRAY_LENGTH(spammy_repo_overlap) AS n_spammy_repo_overlap,
    ARRAY_LENGTH(spammy_repo_overlap) / n_repos AS p_spammy_repo_overlap,
    CASE 
    WHEN 1=1 -- activity cluster heuristic
        -- user interacted with at least x suspicious repos in set
        AND ARRAY_LENGTH(spammy_repo_overlap) >= {n_spammy_repo_overlap_gte}
        -- more than y% of actos' repos are suspicious
        AND ARRAY_LENGTH(spammy_repo_overlap) / n_repos > {p_spammy_repo_overlap_gt}
        -- user has on average less than z actions per repo they interacted with
        AND actions_per_repo < {actions_per_repo_lt}
    THEN 'suspected-activity_cluster'
    WHEN 1=1 -- low activity heuristic
        AND n_dates = {actor_dates} -- user has activity on 1 date
        AND n_repos = {actor_repos} -- user has activity on 1 repo (the target repo)
        AND n_orgs = {actor_orgs} -- user has activity on 1 org
        AND n <= {actor_actions_lte} -- user has no more than 2 total actions
    THEN 'suspected-low_activity'
    ELSE 'unknown'
    END as fake_acct,
FROM ( -- cross join actor table with spammy repo array, to calculate overlap
    SELECT
    a.*,
    ARRAY(
        SELECT * FROM a.repos
        INTERSECT DISTINCT
        SELECT * FROM b.repos
    ) AS spammy_repo_overlap
    FROM read_parquet('../data/parquet/stg_starring_actor_overlap.parquet') a
        CROSS JOIN (
            SELECT ARRAY_AGG(repo) repos FROM read_parquet('../data/parquet/stg_spammy_repos.parquet')
        ) b
 )
"""

start_time = time.time()
print(duckdb.sql(query).show())
print("--- %s seconds ---" % (time.time() - start_time))