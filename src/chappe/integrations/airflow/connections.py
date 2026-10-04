# src/chappe/integrations/airflow/connections.py
"""Reads Airflow connections. Chappe uses one: `chappe_slack`, with the bot token in `password`."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ConnectionInfo:
    host: str | None
    login: str | None
    password: str | None = field(repr=False)  # the Slack bot token: never in logs or reprs
    port: int | None


def airflow_connection(conn_id: str) -> ConnectionInfo:
    from airflow.sdk import Connection

    conn = Connection.get(conn_id)
    return ConnectionInfo(conn.host, conn.login, conn.password, conn.port)
