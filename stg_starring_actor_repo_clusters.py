import duckdb
from pathlib import Path
import time

query = """
WITH repo_summary AS (
    SELECT
        repo,
        ARRAY_AGG(DISTINCT event) as events,
        MIN(created_at) as min_activity,
        MAX(created_at) as max_activity,
        MIN(CASE WHEN is_target_repo AND is_star THEN created_at ELSE NULL END) as star_time,
        ARRAY_AGG(DISTINCT created_at::DATE) dates,
        COUNT(*)n,
        ARRAY_AGG(DISTINCT actor) actors,
        ARRAY_AGG(DISTINCT COALESCE(org, 'no org')) orgs
    FROM read_parquet('../data/parquet/stg_all_actions_for_actors_who_starred_repo.parquet')
    WHERE actor NOT IN (
        SELECT actor FROM read_parquet('../data/parquet/stg_starring_actor_overlap.parquet') WHERE n_repos > 200
    )
    GROUP BY 1
),
repo_overlap AS (
    SELECT
        a.*,
        b.repo as repo2,
        events_overlap,
        dates_overlap,
        actor_overlap,
        org_overlap,
        ARRAY_LENGTH(events_overlap) n_events_overlap,
        ARRAY_LENGTH(dates_overlap) n_dates_overlap,
        ARRAY_LENGTH(actor_overlap) n_actor_overlap,
        ARRAY_LENGTH(org_overlap) n_org_overlap,
        ARRAY_LENGTH(events_overlap)/n_events as p_events_overlap,
        ARRAY_LENGTH(dates_overlap)/n_dates as p_dates_overlap,
        ARRAY_LENGTH(actor_overlap)/n_actors as p_actor_overlap,
        ARRAY_LENGTH(org_overlap)/n_orgs as p_org_overlap
    FROM (
        SELECT 
            *,
            ARRAY_LENGTH(events) as n_events,
            ARRAY_LENGTH(dates) as n_dates,
            ARRAY_LENGTH(actors) as n_actors,
            ARRAY_LENGTH(orgs) as n_orgs
        FROM repo_summary
    ) a
    CROSS JOIN repo_summary b
    INNER JOIN (
        SELECT a.repo as repo1, b.repo as repo2, LIST(val) AS events_overlap
        FROM repo_summary a, repo_summary b, UNNEST(a.events) val 
        WHERE a.repo != b.repo AND val IN (SELECT val FROM UNNEST(b.events))
        GROUP BY a.repo, b.repo
    ) eo ON a.repo = eo.repo1 AND b.repo = eo.repo2
    INNER JOIN (
        SELECT a.repo as repo1, b.repo as repo2, LIST(val) AS dates_overlap
        FROM repo_summary a, repo_summary b, UNNEST(a.dates) val 
        WHERE a.repo != b.repo AND val IN (SELECT val FROM UNNEST(b.dates))
        GROUP BY a.repo, b.repo
    ) dd ON a.repo = dd.repo1 AND b.repo = dd.repo2
    INNER JOIN (
        SELECT a.repo as repo1, b.repo as repo2, LIST(val) AS actor_overlap
        FROM repo_summary a, repo_summary b, UNNEST(a.actors) val 
        WHERE a.repo != b.repo AND val IN (SELECT val FROM UNNEST(b.actors))
        GROUP BY a.repo, b.repo
    ) ao ON a.repo = ao.repo1 AND b.repo = ao.repo2
    INNER JOIN (
        SELECT a.repo as repo1, b.repo as repo2, LIST(val) AS org_overlap
        FROM repo_summary a, repo_summary b, UNNEST(a.orgs) val 
        WHERE a.repo != b.repo AND val IN (SELECT val FROM UNNEST(b.orgs))
        GROUP BY a.repo, b.repo
    ) oo ON a.repo = oo.repo1 AND b.repo = oo.repo2
    WHERE ARRAY_LENGTH(a.actors) >= 2
        AND ARRAY_LENGTH(b.actors) >= 2
        AND ARRAY_LENGTH(actor_overlap) >= 2
)
SELECT
    repo,
    repo2,
    events,
    min_activity,
    max_activity,
    star_time,
    dates,
    n,
    actors,
    orgs,
    events_overlap,
    dates_overlap,
    actor_overlap,
    org_overlap,
    n_events_overlap,
    n_dates_overlap,
    n_actor_overlap,
    n_org_overlap,
    p_events_overlap,
    p_dates_overlap,
    p_actor_overlap,
    p_org_overlap,
    n/n_actors as actions_per_actor
FROM repo_overlap"""

start_time = time.time()
print(duckdb.sql(query))
print("--- %s seconds for modelling query---" % (time.time() - start_time))

q_output = f'copy ({query}) to \'../data/parquet/stg_starring_actor_repo_clusters.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print("--- %s seconds for output query---" % (time.time() - start_time))