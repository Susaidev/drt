"""Live MySQL proof for ``match_policy`` create/update-only semantics (#757)."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from drt.config.models import MySQLDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

pymysql = pytest.importorskip("pymysql")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


def _config() -> MySQLDestinationConfig:
    return MySQLDestinationConfig(
        type="mysql",
        host="localhost",
        dbname="testdb",
        user="testuser",
        password="testpass",
        table="scores",
        upsert_key=["tenant_id", "user_id"],
        introspect_schema=False,
    )


def _connect(mysql: Any) -> Any:
    return pymysql.connect(
        host=mysql.get_container_host_ip(),
        port=int(mysql.get_exposed_port(3306)),
        user=mysql.username,
        password=mysql.password,
        database=mysql.dbname,
    )


def test_mysql_create_only_and_update_only_match_policy() -> None:
    """Existing rows stay untouched, missing updates stay absent, and an
    unchanged existing UPDATE still counts as success rather than no-match.

    The unchanged-row assertion needs a real server: MySQL reports rows
    changed, not rows matched, so a mock cursor cannot validate the dialect's
    zero-row ambiguity on its own.
    """
    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as mysql:
        setup_conn = _connect(mysql)
        try:
            with setup_conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE scores ("
                    "tenant_id INT NOT NULL, "
                    "user_id INT NOT NULL, "
                    "score INT NOT NULL, "
                    "email VARCHAR(255) NOT NULL UNIQUE, "
                    "PRIMARY KEY (tenant_id, user_id))"
                )
                cur.execute("INSERT INTO scores VALUES (5, 1, 10, 'taken@example.com')")
            setup_conn.commit()
        finally:
            setup_conn.close()

        create_conn = _connect(mysql)
        with patch.object(MySQLDestination, "_connect", return_value=create_conn):
            create_result = MySQLDestination().load(
                [
                    {
                        "tenant_id": 5,
                        "user_id": 1,
                        "score": 99,
                        "email": "other@example.com",
                    },
                    {
                        "tenant_id": 5,
                        "user_id": 2,
                        "score": 20,
                        "email": "new@example.com",
                    },
                ],
                _config(),
                SyncOptions(match_policy="create_only"),
            )

        assert create_result.success == 1
        assert create_result.skipped == 1
        assert create_result.skipped_no_match == 1
        assert create_result.failed == 0

        collision_conn = _connect(mysql)
        with patch.object(MySQLDestination, "_connect", return_value=collision_conn):
            collision_result = MySQLDestination().load(
                [
                    {
                        "tenant_id": 7,
                        "user_id": 1,
                        "score": 30,
                        # The email exists, but the configured composite
                        # upsert key does not. This is a real constraint
                        # error, not a create_only no-match skip.
                        "email": "taken@example.com",
                    }
                ],
                _config(),
                SyncOptions(match_policy="create_only", on_error="skip"),
            )

        assert collision_result.success == 0
        assert collision_result.skipped == 0
        assert collision_result.skipped_no_match == 0
        assert collision_result.failed == 1

        update_conn = _connect(mysql)
        with patch.object(MySQLDestination, "_connect", return_value=update_conn):
            update_result = MySQLDestination().load(
                [
                    # Same value: MySQL UPDATE rowcount is zero, but this is
                    # still an existing-row success.
                    {"tenant_id": 5, "user_id": 1, "score": 10},
                    {"tenant_id": 7, "user_id": 1, "score": 30},
                ],
                _config(),
                SyncOptions(match_policy="update_only"),
            )

        assert update_result.success == 1
        assert update_result.skipped == 1
        assert update_result.skipped_no_match == 1
        assert update_result.failed == 0

        verify_conn = _connect(mysql)
        try:
            with verify_conn.cursor() as cur:
                query = "SELECT tenant_id, user_id, score FROM scores ORDER BY tenant_id, user_id"
                cur.execute(query)
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == ((5, 1, 10), (5, 2, 20))
