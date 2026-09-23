"""Throughline, mounted inside the Airflow UI.

``fastapi_apps`` hands Airflow a FastAPI app, which the API server mounts on
itself — no second process and no second port, and FastAPI already ships inside
Airflow so nothing extra is installed. ``external_views`` puts a "Throughline" item
in the left navigation and Airflow renders the page inside its own chrome.

The ``href`` and the ``url_prefix`` have to agree. They are both derived from
``throughline.api.URL_PREFIX`` here rather than written out twice, because when
they disagree Airflow's RBAC denies access to the page and the failure looks
like a permissions problem rather than a typo.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from throughline.api import URL_PREFIX, app

# Registers the replay plans, so POST /replays knows what it is allowed to run.
# Kept in a try: a DAG that fails to import should cost the trace view its
# replay button, not the entire plugin.
try:
    import include.throughline_replays  # noqa: F401
except Exception:  # pragma: no cover
    pass


class ThroughlinePlugin(AirflowPlugin):
    """Serve the trace view from Airflow, and link to it from the nav."""

    name = "throughline"

    fastapi_apps = [
        {
            "app": app,
            "url_prefix": URL_PREFIX,
            "name": "Throughline",
        }
    ]

    external_views = [
        {
            "name": "Throughline",
            # Relative, no leading slash, and matching url_prefix exactly.
            "href": f"{URL_PREFIX.lstrip('/')}/",
            "url_route": URL_PREFIX.lstrip("/"),
            "destination": "nav",
            "category": "browse",
            "icon": "fa-solid fa-magnifying-glass-chart",
        },
        {
            # A tab on Airflow's own DAG page, beside Overview / Runs / Tasks,
            # which is where somebody looking at a DAG actually is. Airflow
            # renders this destination in an iframe and substitutes {DAG_ID}
            # from the route, so the tab lands on that DAG's traces rather
            # than the full index.
            "name": "Throughline",
            "href": f"{URL_PREFIX}/dags/{{DAG_ID}}",
            "url_route": f"{URL_PREFIX.lstrip('/')}-dag",
            "destination": "dag",
            "icon": "fa-solid fa-magnifying-glass-chart",
        },
    ]
