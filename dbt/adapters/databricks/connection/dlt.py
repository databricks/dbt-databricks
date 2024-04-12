import re
from typing import Dict
from typing import Optional
from typing import Tuple

from dbt_common.exceptions import DbtRuntimeError
from requests import Session


def get_pipeline_update_error(session: Session, host: str, pipeline_id: str, update_id: str) -> str:
    events_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}/events"
    response = session.get(events_url)
    if response.status_code != 200:
        raise DbtRuntimeError(
            f"Error getting pipeline event info for {pipeline_id}: {response.text}"
        )

    events = response.json().get("events", [])
    update_events = [
        e
        for e in events
        if e.get("event_type", "") == "update_progress"
        and e.get("origin", {}).get("update_id") == update_id
    ]

    error_events = [
        e
        for e in update_events
        if e.get("details", {}).get("update_progress", {}).get("state", "") == "FAILED"
    ]

    msg = ""
    if error_events:
        msg = error_events[0].get("message", "")

    return msg


def find_pipeline_update(pipeline: dict, id: str = "") -> Optional[Dict]:
    updates = pipeline.get("latest_updates", [])
    if not updates:
        raise DbtRuntimeError(f"No updates for pipeline: {pipeline.get('pipeline_id', '')}")

    if not id:
        return updates[0]

    matches = [x for x in updates if x.get("update_id") == id]
    if matches:
        return matches[0]

    return None


def get_pipeline_state(session: Session, host: str, pipeline_id: str) -> dict:
    pipeline_url = f"https://{host}/api/2.0/pipelines/{pipeline_id}"

    response = session.get(pipeline_url)
    if response.status_code != 200:
        raise DbtRuntimeError(f"Error getting pipeline info for {pipeline_id}: {response.text}")

    return response.json()


def get_table_view_pipeline_id(session: Session, host: str, name: str) -> str:
    table_url = f"https://{host}/api/2.1/unity-catalog/tables/{name}"
    resp1 = session.get(table_url)
    if resp1.status_code != 200:
        raise DbtRuntimeError(
            f"Error getting info for materialized view/streaming table {name}: {resp1.text}"
        )

    pipeline_id = resp1.json().get("pipeline_id", "")
    if not pipeline_id:
        raise DbtRuntimeError(
            f"Materialized view/streaming table {name} does not have a pipeline id"
        )

    return pipeline_id


mv_refresh_regex = re.compile(r"refresh\s+materialized\s+view\s+([`\w.]+)", re.IGNORECASE)
st_refresh_regex = re.compile(
    r"create\s+or\s+refresh\s+streaming\s+table\s+([`\w.]+)", re.IGNORECASE
)


def should_poll_refresh(sql: str) -> Tuple[bool, str]:
    # if the command was to refresh a materialized view we need to poll
    # the pipeline until the refresh is finished.
    name = ""
    refresh_search = mv_refresh_regex.search(sql)
    if not refresh_search:
        refresh_search = st_refresh_regex.search(sql)

    if refresh_search:
        name = refresh_search.group(1).replace("`", "")

    return refresh_search is not None, name
