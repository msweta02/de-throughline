"""The FastAPI app Airflow mounts.

Handlers are deliberately ``def`` and not ``async def``. Everything below makes
a blocking DuckDB call, and a coroutine doing that would block the API server's
event loop; a sync handler runs in FastAPI's threadpool instead and leaves the
loop free.
"""

from __future__ import annotations

import json
import urllib.parse
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from throughline import errors, registry, store, views
from throughline import replay as replay_mod

#: Airflow mounts the app with ``app.mount(url_prefix, subapp)``, so every route
#: here is served beneath this prefix. The plugin's ``external_views`` href must
#: agree with it or RBAC denies access to the page.
URL_PREFIX = "/throughline"

app = FastAPI(
    title="Throughline",
    description="Follow one record through a DAG.",
    version="0.1.0",
)


def _html(body: str) -> HTMLResponse:
    return HTMLResponse(body)


async def _payload(request: Request) -> dict[str, Any]:
    """Read a request body as JSON or as an HTML form.

    Parsed by hand rather than with ``fastapi.Form``, which would drag in
    python-multipart. One less thing to install into the Airflow image for the
    sake of a three-field form.
    """
    raw = (await request.body()).decode() or ""
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return {}
    parsed = urllib.parse.parse_qs(raw, keep_blank_values=True)
    payload = {k: v[0] for k, v in parsed.items()}
    payload.update({k: v for k, v in request.query_params.items()})
    return payload


# ---------------------------------------------------------------- HTML pages


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def index_page() -> HTMLResponse:
    return _html(views.index(URL_PREFIX))


@app.get("/runs/{dag_id}/{run_id}", response_class=HTMLResponse, include_in_schema=False)
def records_page(dag_id: str, run_id: str) -> HTMLResponse:
    return _html(views.records(URL_PREFIX, dag_id, run_id))


@app.get(
    "/runs/{dag_id}/{run_id}/{record_key}", response_class=HTMLResponse, include_in_schema=False
)
def trace_page(dag_id: str, run_id: str, record_key: str) -> HTMLResponse:
    return _html(views.trace(URL_PREFIX, dag_id, run_id, record_key))


@app.get("/diff", response_class=HTMLResponse, include_in_schema=False)
def diff_page(dag_id: str, record_key: str, left: str, right: str) -> HTMLResponse:
    return _html(views.diff(URL_PREFIX, dag_id, record_key, left, right))


# ---------------------------------------------------------------- JSON API


@app.get("/traces")
def list_traces() -> JSONResponse:
    """Every captured DAG run."""
    return JSONResponse({"traces": json.loads(json.dumps(store.list_traces(), default=str))})


@app.get("/traces/{dag_id}/{run_id}")
def list_trace_records(dag_id: str, run_id: str) -> JSONResponse:
    """The records captured in one run."""
    payload = {"dag_id": dag_id, "run_id": run_id, "records": store.list_records(dag_id, run_id)}
    return JSONResponse(json.loads(json.dumps(payload, default=str)))


@app.get("/traces/{dag_id}/{run_id}/{record_key}")
def get_trace(dag_id: str, run_id: str, record_key: str) -> JSONResponse:
    """One record's throughline through one run."""
    payload = views.trace_json(dag_id, run_id, record_key)
    return JSONResponse(json.loads(json.dumps(payload, default=str)))


@app.get("/replays")
def get_replays() -> JSONResponse:
    return JSONResponse(
        {
            "replays": json.loads(json.dumps(store.list_replays(), default=str)),
            "replayable_dags": registry.registered(),
        }
    )


@app.post("/replays")
async def post_replay(request: Request) -> Any:
    """Replay one record against real task code, in a sandbox.

    Production is attached read-only for the duration; a DAG whose tasks are
    not marked replay-safe is refused before anything runs.
    """
    payload = await _payload(request)
    dag_id = str(payload.get("dag_id", "")).strip()
    scope = str(payload.get("scope", "")).strip()
    bundle_version = str(payload.get("bundle_version") or "current").strip()
    wants_html = "text/html" in request.headers.get("accept", "")

    if not dag_id or not scope:
        message = "dag_id and scope are both required"
        return JSONResponse({"error": message}, status_code=400)

    try:
        result = replay_mod.run(dag_id, scope, bundle_version)
    except errors.ReplayRefused as exc:
        # A refusal is the feature working, not a server fault.
        if wants_html:
            return RedirectResponse(URL_PREFIX + "/", status_code=303)
        return JSONResponse({"status": "refused", "error": str(exc)}, status_code=409)

    if wants_html and result.ok and result.record_key:
        return RedirectResponse(
            f"{URL_PREFIX}/runs/{dag_id}/{result.replay_id}/{result.record_key}",
            status_code=303,
        )

    return JSONResponse(
        {
            "status": result.status,
            "replay_id": result.replay_id,
            "record_key": result.record_key,
            "bundle_version": result.bundle_version,
            "tasks": result.tasks,
            "note": result.note,
        },
        status_code=200 if result.ok else 500,
    )
