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
WITH spammy_repos AS (
    SELECT repo FROM read_parquet('../data/parquet/stg_spammy_repos.parquet')
),
actor_repos AS (
    SELECT
        actor,
        events,
        min_activity,
        max_activity,
        star_time,
        dates,
        n,
        actor_avatars,
        repos,
        orgs,
        n_events,
        n_dates,
        n_repos,
        n_orgs,
        n_events_overlap,
        n_dates_overlap,
        n_repo_overlap,
        n_org_overlap,
        p_events_overlap,
        p_dates_overlap,
        p_repo_overlap,
        p_org_overlap,
        events_overlap_distinct,
        dates_overlap_distinct,
        repo_overlap_distinct,
        org_overlap_distinct,
        actions_per_repo,
        UNNEST(a.repos) AS repo
    FROM read_parquet('../data/parquet/stg_starring_actor_overlap.parquet') a
),
intersected_repos AS (
    SELECT
        actor_repos.*,
        spammy_repos.repo AS spammy_repo
    FROM actor_repos
    INNER JOIN spammy_repos ON actor_repos.repo = spammy_repos.repo
),
final as(
SELECT
    *,
    ARRAY_AGG(ir.spammy_repo) AS spammy_repo_overlap
FROM actor_repos a
JOIN intersected_repos ir ON a.actor = ir.actor
GROUP BY a.actor, 
ir.actor,
ir.n,
ir.spammy_repo,
ir.repo, 
ir.events, 
ir.min_activity, 
ir.max_activity, 
ir.star_time, 
ir.dates, a.n, 
ir.actor_avatars, 
ir.repos, 
ir.orgs, 
ir.n_events, 
ir.n_dates, 
ir.n_repos, 
ir.n_orgs, 
ir.n_events_overlap,
ir.n_dates_overlap,
ir.n_repo_overlap, 
ir.n_org_overlap, 
ir.p_events_overlap, 
ir.p_dates_overlap, 
ir.p_repo_overlap, 
ir.p_org_overlap, 
ir.events_overlap_distinct, 
ir.dates_overlap_distinct, 
ir.repo_overlap_distinct, 
ir.org_overlap_distinct, 
ir.actions_per_repo, 
a.repo, 
a.events, 
a.min_activity, 
a.max_activity, 
a.star_time, 
a.dates, a.n, 
a.actor_avatars, 
a.repos, 
a.orgs, 
a.n_events, 
a.n_dates, 
a.n_repos, 
a.n_orgs, 
a.n_events_overlap,
a.n_dates_overlap,
a.n_repo_overlap, 
a.n_org_overlap, 
a.p_events_overlap, 
a.p_dates_overlap, 
a.p_repo_overlap, 
a.p_org_overlap, 
a.events_overlap_distinct, 
a.dates_overlap_distinct, 
a.repo_overlap_distinct, 
a.org_overlap_distinct, 
a.actions_per_repo
)
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
FROM final
"""

start_time = time.time()
print(duckdb.sql(query).show())
print("--- %s seconds ---" % (time.time() - start_time))

q_output = f'copy ({query}) to \'../data/parquet/starring_actor_summary.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print("--- %s seconds IO Query ---" % (time.time() - start_time))