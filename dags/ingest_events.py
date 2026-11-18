# Ingests raw events and users from the events API into BigQuery (raw layer).
# Cursor-based: the last loaded event timestamp is stored in an Airflow Variable;
# each run fetches everything since that cursor (extract -> XCom -> load).
import json
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

API_URL = "https://api.events-provider.io/v1"
API_TOKEN = Variable.get("events_api_token")
PROJECT = Variable.get("gcp_project", "my-project")
CURSOR_VAR = "events_cursor"
INITIAL_CURSOR = "1970-01-01T00:00:00Z"
PAGE_SIZE = 500

client = bigquery.Client(project=PROJECT)
HEADERS = {"Authorization": API_TOKEN}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),
    "email_on_failure": False,
    "retries": 0,
}

dag = DAG(
    "ingest_events",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    max_active_runs=1,  # the cursor must not be advanced by overlapping runs
)


def already_loaded_ids(event_ids):
    """Return a list of the given event ids that already exist in raw.events."""
    if not event_ids:
        return []
    job = client.query(
        "SELECT event_id FROM `" + PROJECT + ".raw.events`",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", [str(i) for i in event_ids])]
        ),
    )
    return [str(row["event_id"]) for row in job.result()]


def fetch_events(**context):
    """Fetch all events created at/after the stored cursor and push them to XCom."""
    cursor = Variable.get(CURSOR_VAR, INITIAL_CURSOR)
    events = []
    page = 1
    while True:
        r = requests.get(
            API_URL + "/events",
            params={"since": cursor, "page": page, "page_size": PAGE_SIZE},
            headers=HEADERS,
            timeout=30,
        )
        data = r.json()["data"]
        if not data:
            break
        events.extend(data)
        page += 1

    print("fetched", len(events), "events since", cursor)

    # deduplication: skip events that were already read (list of known event ids)
    known_ids = already_loaded_ids([e["id"] for e in events])
    new_events = []
    for e in events:
        if str(e["id"]) in known_ids:
            continue
        known_ids.append(str(e["id"]))  # also drops duplicates inside this batch
        new_events.append(e)

    print("skipped", len(events) - len(new_events), "already read events")
    return new_events  # returned value is pushed to XCom (key: return_value)


def load_to_bigquery(**context):
    """Pull events from XCom, load events and their users to BigQuery, advance the cursor."""
    events = context["ti"].xcom_pull(task_ids="fetch_events")
    if not events:
        print("nothing to load")
        return

    now = datetime.utcnow().isoformat()

    # users referenced by the events (one API call per distinct user)
    users = []
    for user_id in {e["user_id"] for e in events}:
        r = requests.get(API_URL + "/users/" + str(user_id), headers=HEADERS, timeout=30)
        r.raise_for_status()
        u = r.json()
        users.append(
            {
                "user_id": u["id"],
                "email": u["email"],
                "name": u["name"],
                "surname": u["surname"],
                "registration_date": u["registration_date"],
                "ip": u["ip"],
                "passport_number": u["passport_number"],
                "ingestion_timestamp": now,
            }
        )

    rows = [
        {
            "event_id": e["id"],
            "user_id": e["user_id"],
            "event_type": e["event_type"],
            "event_attributes": json.dumps(e["attributes"]),
            "ingestion_timestamp": now,
        }
        for e in events
    ]

    # batch load jobs; .result() raises on failure so the cursor only moves on success
    client.load_table_from_json(users, PROJECT + ".raw.users").result()
    client.load_table_from_json(rows, PROJECT + ".raw.events").result()

    new_cursor = max(e["created_at"] for e in events)
    Variable.set(CURSOR_VAR, new_cursor)
    print("loaded", len(rows), "events,", len(users), "users; cursor ->", new_cursor)


t_fetch = PythonOperator(task_id="fetch_events", python_callable=fetch_events,
                         provide_context=True, dag=dag)
t_load = PythonOperator(task_id="load_to_bigquery", python_callable=load_to_bigquery,
                        provide_context=True, dag=dag)

t_fetch >> t_load
