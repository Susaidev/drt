"""Focused tests for SQS config and connector scaffolding."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from drt.config import models
from drt.config.connectors import install_target
from drt.connectors.registry import get_destination
from drt.destinations.base import Destination


def test_sqs_config_parses_in_sync() -> None:
    sync = models.SyncConfig(
        name="work_items",
        model="work_items",
        destination={"type": "sqs", "queue_url_env": "SQS_QUEUE_URL"},
    )
    config = sync.destination
    assert isinstance(config, models.SQSDestinationConfig)
    assert config.queue_url_env == "SQS_QUEUE_URL"
    assert config.region is None
    assert config.message_group_id_field is None
    assert config.deduplication_id_field is None
    assert config.describe_safe() == "sqs"


def test_sqs_config_accepts_fifo_field_names() -> None:
    sync = models.SyncConfig(
        name="work_items",
        model="work_items",
        destination={
            "type": "sqs",
            "queue_url_env": "SQS_QUEUE_URL",
            "region": "ap-northeast-1",
            "message_group_id_field": "customer_id",
            "deduplication_id_field": "event_id",
        },
    )
    config = sync.destination
    assert isinstance(config, models.SQSDestinationConfig)
    assert config.region == "ap-northeast-1"
    assert config.message_group_id_field == "customer_id"
    assert config.deduplication_id_field == "event_id"


def test_sqs_config_requires_queue_url_env() -> None:
    with pytest.raises(ValidationError, match="queue_url_env"):
        models.SyncConfig(name="work_items", model="work_items", destination={"type": "sqs"})


@pytest.mark.parametrize("records", [[], [{"id": 1}]])
def test_sqs_registry_returns_unimplemented_destination(records: list[dict[str, int]]) -> None:
    sync = models.SyncConfig(
        name="work_items",
        model="work_items",
        destination={"type": "sqs", "queue_url_env": "SQS_QUEUE_URL"},
    )
    destination = get_destination(sync.destination)
    assert isinstance(destination, Destination)
    with pytest.raises(NotImplementedError, match="SQS"):
        destination.load(records, sync.destination, sync.sync)


def test_sqs_install_target_uses_optional_extra() -> None:
    assert install_target("sqs") == "drt-core[sqs]"
