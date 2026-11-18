# Data Warehouse Overview

Events pipeline: API → Airflow → BigQuery → dbt.

```
Events API → Airflow DAG → raw → silver → gold → datamart
```

## Components

| Component | Where | What it does |
|---|---|---|
| **Events API** | external | Source of events and users. |
| **Airflow DAG** `ingest_events` | `dags/ingest_events.py` | Runs every 5 minutes. Fetches new events since a cursor, loads events and users into BigQuery `raw`. |
| **raw** | BigQuery | Data exactly as loaded by Airflow. Append-only. |
| **silver** | `dwh/models/silver/` | Cleaned data. `silver_events` and `silver_users`. |
| **gold** | `dwh/models/gold/` | Business-ready tables. `gold_users` and `gold_user_daily_events` (events per user, type and day). |
| **datamart** | `dwh/models/datamart/` | Final tables for consumers. `dm_events_users` (events with user details) and `dm_top_spenders_daily` (top users by spend, last 30 days). |
| **dbt project** | `dwh/` | Models, sources, tests and settings (`dbt_project.yml` vars). |
| **Analyses** | `dwh/analyses/` | Ad-hoc SQL queries for top spenders. |

## Your task

Review the pipeline end to end. Find the problems and explain how you would fix them. Think about:

- correctness
- performance and cost
- reliability
- data privacy and security
