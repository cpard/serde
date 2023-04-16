import duckdb
from pathlib import Path
import time

query = """
WITH actor_summary AS (
  SELECT
    actor,
    ARRAY_AGG(DISTINCT event) as events,
    MIN(created_at) as min_activity,
    MAX(created_at) as max_activity,
    MIN(CASE WHEN is_target_repo AND is_star THEN created_at ELSE NULL END) as star_time,
    ARRAY_AGG(DISTINCT created_at::DATE) dates,
    COUNT(*) n,
    ARRAY_AGG(DISTINCT avatar_url) FILTER (WHERE avatar_url IS NOT NULL) actor_avatars,
    ARRAY_AGG(DISTINCT repo) FILTER (WHERE repo IS NOT NULL) repos,
    ARRAY_AGG(DISTINCT COALESCE(org, 'no org')) orgs
  FROM read_parquet('../data/parquet/stg_all_actions_for_actors_who_starred_repo.parquet')
  GROUP BY 1
),
a_events AS (
  SELECT a.actor AS a_actor, unnest(a.events) AS event
  FROM actor_summary a
),
b_events AS (
  SELECT b.actor AS b_actor, unnest(b.events) AS event
  FROM actor_summary b
),
a_dates AS (
  SELECT a.actor AS a_actor, unnest(a.dates) AS date
  FROM actor_summary a
),
b_dates AS (
  SELECT b.actor AS b_actor, unnest(b.dates) AS date
  FROM actor_summary b
),
a_repos AS (
  SELECT a.actor AS a_actor, unnest(a.repos) AS repo
  FROM actor_summary a
),
b_repos AS (
  SELECT b.actor AS b_actor, unnest(b.repos) AS repo
  FROM actor_summary b
),
a_orgs AS (
  SELECT a.actor AS a_actor, unnest(a.orgs) AS org
  FROM actor_summary a
),
b_orgs AS (
  SELECT b.actor AS b_actor, unnest(b.orgs) AS org
  FROM actor_summary b
),
actor_overlap AS (
 SELECT
   *,
   ARRAY_LENGTH(events_overlap) n_events_overlap,
   ARRAY_LENGTH(dates_overlap) n_dates_overlap,
   ARRAY_LENGTH(repo_overlap) n_repo_overlap,
   ARRAY_LENGTH(org_overlap) n_org_overlap,
   ARRAY_LENGTH(events_overlap)/n_events::FLOAT as p_events_overlap,
   ARRAY_LENGTH(dates_overlap)/n_dates::FLOAT as p_dates_overlap,
   ARRAY_LENGTH(repo_overlap)/n_repos::FLOAT as p_repo_overlap,
   ARRAY_LENGTH(org_overlap)/n_orgs::FLOAT as p_org_overlap
 FROM (
   SELECT
     a.actor as actor,
        a.events as events,
        a.min_activity as min_activity,
        a.max_activity as max_activity,
        a.star_time as star_time,
        a.dates as dates,
        a.n as n,
        a.actor_avatars as actor_avatars,
        a.repos as repos,
        a.orgs as orgs,
     ARRAY_LENGTH(a.events) as n_events,
     ARRAY_LENGTH(a.dates) as n_dates,
     ARRAY_LENGTH(a.repos) as n_repos,
     ARRAY_LENGTH(a.orgs) as n_orgs,
     b.actor as actor2,
     list(
       (SELECT DISTINCT a_events.event
       FROM a_events
       JOIN b_events ON a_events.event = b_events.event
       WHERE a_events.a_actor != b_events.b_actor)
     ) AS events_overlap,
     list(
       (SELECT DISTINCT a_dates.date
       FROM a_dates
       JOIN b_dates ON a_dates.date = b_dates.date
       WHERE a_dates.a_actor != b_dates.b_actor)
     ) AS dates_overlap,
     list(
       (SELECT DISTINCT a_repos.repo
       FROM a_repos
       JOIN b_repos ON a_repos.repo = b_repos.repo
       WHERE a_repos.a_actor != b_repos.b_actor)
     ) AS repo_overlap,
     list(
       (SELECT DISTINCT a_orgs.org
       FROM a_orgs
       JOIN b_orgs ON a_orgs.org = b_orgs.org
       WHERE a_orgs.a_actor != b_orgs.b_actor)
     ) AS org_overlap
   FROM actor_summary a
   CROSS JOIN actor_summary b
   WHERE a.actor != b.actor
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
 )
),
final_inner AS (
SELECT
   actor,
   ANY_VALUE(events) events,
   ANY_VALUE(min_activity) min_activity,
   ANY_VALUE(max_activity) max_activity,
   ANY_VALUE(star_time) star_time,
   ANY_VALUE(dates) dates,
   ANY_VALUE(n) n,
   ANY_VALUE(actor_avatars) actor_avatars,
   ANY_VALUE(repos) repos,
   ANY_VALUE(orgs) orgs,
   ANY_VALUE(n_events) n_events,
   ANY_VALUE(n_dates) n_dates,
   ANY_VALUE(n_repos) n_repos,
   ANY_VALUE(n_orgs) n_orgs,
   ANY_VALUE(events_overlap) events_overlap,
   ANY_VALUE(dates_overlap) dates_overlap,
   ANY_VALUE(repo_overlap) repo_overlap,
   ANY_VALUE(org_overlap) org_overlap,
   AVG(n_events_overlap) n_events_overlap,
   AVG(n_dates_overlap) n_dates_overlap,
   AVG(n_repo_overlap) n_repo_overlap,
   AVG(n_org_overlap) n_org_overlap,
   AVG(p_events_overlap) p_events_overlap,
   AVG(p_dates_overlap) p_dates_overlap,
   AVG(p_repo_overlap) p_repo_overlap,
   AVG(p_org_overlap) p_org_overlap
 FROM actor_overlap
 GROUP BY 1
)
SELECT actor,
 list( (SELECT DISTINCT UNNEST(events_overlap) from final_inner) ) as events_overlap_distinct,
 list( (SELECT DISTINCT UNNEST(dates_overlap) from final_inner) ) as dates_overlap_distinct,
 list( (SELECT DISTINCT UNNEST(repo_overlap) from final_inner) ) as repo_overlap_distinct,
 list( (SELECT DISTINCT UNNEST(org_overlap) from final_inner) ) as org_overlap_distinct,
 n/n_repos::FLOAT as actions_per_repo
FROM final_inner group by 1,6
"""

start_time = time.time()
print(duckdb.sql(query))
print("--- %s seconds for modelling query---" % (time.time() - start_time))

q_output = f'copy ({query}) to \'../data/parquet/stg_starring_actor_overlap.parquet\' (format  PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);'

start_time = time.time()
duckdb.sql(q_output)
print("--- %s seconds for Parquet I/O query---" % (time.time() - start_time))