"""SQS delivery tests with the AWS boundary mocked; no credentials required."""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from requests.exceptions import ConnectionError

from drt.config.credentials import BigQueryProfile
from drt.config.models import (
    RateLimitConfig,
    RetryConfig,
    SQSDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.sqs import SQSDestination
from drt.engine.observer import DlqObserver
from drt.engine.sync import run_sync
from drt.sources.fake import FakeSource
from drt.state.dlq import LocalDlqStore
from drt.state.idempotency import successful_indices

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/work-items"


def _config(**overrides: Any) -> SQSDestinationConfig:
    return SQSDestinationConfig.model_validate(
        {"type": "sqs", "queue_url_env": "SQS_QUEUE_URL", **overrides}
    )


def _options(**overrides: Any) -> SyncOptions:
    return SyncOptions(
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=100000),
        retry=RetryConfig(max_attempts=3, initial_backoff=0),
        **overrides,
    )


def _success(*ids: str) -> dict[str, Any]:
    return {
        "Successful": [
            {"Id": id_, "MessageId": f"message-{id_}", "MD5OfMessageBody": "0" * 32} for id_ in ids
        ],
        "Failed": [],
        "ResponseMetadata": {"HTTPStatusCode": 200},
    }


def _failure(id_: str, *, sender_fault: bool) -> dict[str, Any]:
    return {
        "Id": id_,
        "SenderFault": sender_fault,
        "Code": "InvalidMessageContents" if sender_fault else "InternalError",
        "Message": "invalid message" if sender_fault else "try again",
    }


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL)
    boto3 = MagicMock()
    sqs = boto3.session.Session.return_value.client.return_value
    sqs.send_message_batch.side_effect = lambda **kw: _success(*(e["Id"] for e in kw["Entries"]))
    monkeypatch.setitem(sys.modules, "boto3", boto3)
    return sqs


def test_empty_batch_needs_neither_boto3_nor_queue_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "boto3", None)
    monkeypatch.delenv("SQS_QUEUE_URL", raising=False)
    result = SQSDestination().load([], _config(), _options())
    assert result == SyncResult()


def test_missing_optional_sdk_has_installation_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL)
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ImportError, match=r"pip install drt-core\[sqs\]"):
        SQSDestination().load([{"id": 1}], _config(), _options())


def test_missing_queue_url_names_config_field(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", "")
    with pytest.raises(ValueError, match="queue_url_env"):
        SQSDestination().load([{"id": 1}], _config(), _options())


@pytest.mark.parametrize(
    ("suffix", "fields", "message"),
    [
        (".fifo", {}, r"message_group_id_field.*FIFO.*\.fifo"),
        ("", {"message_group_id_field": "customer"}, r"message_group_id_field.*standard.*\.fifo"),
        ("", {"deduplication_id_field": "event"}, r"deduplication_id_field.*standard.*\.fifo"),
    ],
)
def test_fifo_validation_precedes_sdk_import(
    monkeypatch: pytest.MonkeyPatch, suffix: str, fields: dict[str, str], message: str
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + suffix)
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ValueError, match=message):
        SQSDestination().load([{"id": 1}], _config(**fields), _options())


@pytest.mark.parametrize("mode", ["full", "incremental", "upsert"])
def test_standard_queue_appends_json_messages(client: MagicMock, mode: str) -> None:
    records = [{"id": 1}, {"id": 2}]
    result = SQSDestination().load(records, _config(), _options(mode=mode, cursor_field="id"))
    assert result.success == 2
    assert result.failed == 0
    assert client.send_message_batch.call_args.kwargs == {
        "QueueUrl": QUEUE_URL,
        "Entries": [
            {"Id": "0", "MessageBody": '{"id": 1}'},
            {"Id": "1", "MessageBody": '{"id": 2}'},
        ],
    }


def test_region_and_standard_credential_chain(client: MagicMock) -> None:
    SQSDestination().load([{"id": 1}], _config(region="us-east-1"), _options())
    boto3 = sys.modules["boto3"]
    boto3.session.Session.assert_called_once_with(region_name="us-east-1")
    boto3.session.Session.return_value.client.assert_called_once_with("sqs")


@pytest.mark.parametrize("dedup_field", [None, "event"])
def test_fifo_message_fields(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, dedup_field: str | None
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    record = {"customer": 42, "event": "event-1"}
    result = SQSDestination().load(
        [record],
        _config(message_group_id_field="customer", deduplication_id_field=dedup_field),
        _options(),
    )
    assert result.success == 1
    expected = {"Id": "0", "MessageBody": json.dumps(record), "MessageGroupId": "42"}
    if dedup_field:
        expected["MessageDeduplicationId"] = "event-1"
    assert client.send_message_batch.call_args.kwargs["Entries"] == [expected]


@pytest.mark.parametrize(
    ("group", "dedup"),
    [
        (Decimal("42"), UUID("12345678-1234-1234-1234-123456789abc")),
        (UUID("12345678-1234-1234-1234-123456789abc"), Decimal("42")),
    ],
)
def test_native_fifo_ids_are_converted_to_strings(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, group: Any, dedup: Any
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    record = {"customer": group, "event": dedup}
    result = SQSDestination().load(
        [record],
        _config(message_group_id_field="customer", deduplication_id_field="event"),
        _options(),
    )
    assert result.success == 1
    assert result.failed == 0
    assert client.send_message_batch.call_args.kwargs["Entries"] == [
        {
            "Id": "0",
            "MessageBody": json.dumps(record, default=str),
            "MessageGroupId": str(group),
            "MessageDeduplicationId": str(dedup),
        }
    ]


def test_partial_retry_resubmits_only_transient_failures(client: MagicMock) -> None:
    first = _success("0", "2")
    first["Failed"] = [_failure("1", sender_fault=False), _failure("3", sender_fault=True)]
    client.send_message_batch.side_effect = [first, _success("1")]
    records = [{"id": i} for i in range(4)]
    with patch("drt.destinations.retry.time.sleep") as sleep:
        result = SQSDestination().load(records, _config(), _options())
    assert [call.kwargs["Entries"] for call in client.send_message_batch.call_args_list] == [
        [{"Id": str(i), "MessageBody": json.dumps(record)} for i, record in enumerate(records)],
        [{"Id": "1", "MessageBody": '{"id": 1}'}],
    ]
    assert result.success == 3
    assert result.failed == 1
    assert result.row_errors[0].batch_index == 3
    assert "InvalidMessageContents" in result.row_errors[0].error_message
    assert sleep.call_count == 1


def test_retries_keep_shrinking_pending_entries(client: MagicMock) -> None:
    first, second = _success("0"), _success("1")
    first["Failed"] = [_failure("1", sender_fault=False), _failure("2", sender_fault=False)]
    second["Failed"] = [_failure("2", sender_fault=False)]
    client.send_message_batch.side_effect = [first, second, _success("2")]
    result = SQSDestination().load([{"id": i} for i in range(3)], _config(), _options())
    assert [
        [entry["Id"] for entry in call.kwargs["Entries"]]
        for call in client.send_message_batch.call_args_list
    ] == [["0", "1", "2"], ["1", "2"], ["2"]]
    assert result.success == 3
    assert result.failed == 0


def test_sender_fault_is_not_retried(client: MagicMock) -> None:
    response = _success()
    response["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [response]
    with patch("drt.destinations.retry.time.sleep") as sleep:
        result = SQSDestination().load([{"id": 1}], _config(), _options())
    assert client.send_message_batch.call_count == 1
    sleep.assert_not_called()
    assert result.success == 0
    assert result.failed == 1


def test_retry_exhaustion_records_only_remaining_failure(client: MagicMock) -> None:
    first, retry = _success("0"), _success()
    first["Failed"] = retry["Failed"] = [_failure("1", sender_fault=False)]
    client.send_message_batch.side_effect = [first, retry, retry]
    with patch("drt.destinations.retry.time.sleep") as sleep:
        result = SQSDestination().load([{"id": 0}, {"id": 1}], _config(), _options())
    assert client.send_message_batch.call_count == 3
    assert sleep.call_count == 2
    assert result.success == 1
    assert result.failed == 1
    assert result.row_errors[0].batch_index == 1
    assert "InternalError" in result.row_errors[0].error_message


def test_batches_at_ten_entries(client: MagicMock) -> None:
    result = SQSDestination().load([{"id": i} for i in range(23)], _config(), _options())
    assert result.success == 23
    assert [len(c.kwargs["Entries"]) for c in client.send_message_batch.call_args_list] == [
        10,
        10,
        3,
    ]


def test_batches_at_utf8_payload_limit(client: MagicMock) -> None:
    records = [{"text": "é" * 200000} for _ in range(3)]
    result = SQSDestination().load(records, _config(), _options())
    assert result.success == 3
    assert [len(c.kwargs["Entries"]) for c in client.send_message_batch.call_args_list] == [2, 1]


def test_oversized_message_is_row_error_and_keeps_original_indices(client: MagicMock) -> None:
    result = SQSDestination().load([{"text": "é" * 530000}, {"id": 1}], _config(), _options())
    assert result.success == 1
    assert result.failed == 1
    assert result.row_errors[0].batch_index == 0
    assert "1 MiB" in result.row_errors[0].error_message
    assert client.send_message_batch.call_args.kwargs["Entries"] == [
        {"Id": "1", "MessageBody": '{"id": 1}'}
    ]


def test_missing_fifo_row_field_becomes_row_error(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    result = SQSDestination().load(
        [{"id": 1}], _config(message_group_id_field="customer"), _options()
    )
    assert result.failed == 1
    assert "customer" in result.row_errors[0].error_message
    client.send_message_batch.assert_not_called()


@pytest.mark.parametrize("value", [None, {}, "", "x" * 129])
def test_invalid_fifo_id_is_row_error(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, value: Any
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    result = SQSDestination().load(
        [{"customer": value}], _config(message_group_id_field="customer"), _options()
    )
    assert result.failed == 1
    assert "customer" in result.row_errors[0].error_message
    client.send_message_batch.assert_not_called()


def test_missing_fifo_deduplication_value_is_row_error(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    result = SQSDestination().load(
        [{"customer": "42"}],
        _config(message_group_id_field="customer", deduplication_id_field="event"),
        _options(),
    )
    assert result.failed == 1
    assert "event" in result.row_errors[0].error_message
    client.send_message_batch.assert_not_called()


def test_later_chunk_failure_keeps_original_record_index(client: MagicMock) -> None:
    failure = _success()
    failure["Failed"] = [_failure("10", sender_fault=True)]
    client.send_message_batch.side_effect = [_success(*(str(i) for i in range(10))), failure]
    result = SQSDestination().load([{"id": i} for i in range(11)], _config(), _options())
    assert result.success == 10
    assert result.failed == 1
    assert result.row_errors[0].batch_index == 10
    assert json.loads(result.row_errors[0].record_preview) == {"id": 10}


def test_skipped_row_and_later_chunk_retries_preserve_indices(client: MagicMock) -> None:
    second, retry = _success("11", "13"), _success()
    second["Failed"] = retry["Failed"] = [_failure("12", sender_fault=False)]
    client.send_message_batch.side_effect = [
        _success(*(str(i) for i in range(1, 11))),
        second,
        retry,
        retry,
    ]
    records = [{"text": "é" * 530000}] + [{"id": i} for i in range(1, 14)]
    result = SQSDestination().load(records, _config(), _options())
    assert [call.kwargs["Entries"] for call in client.send_message_batch.call_args_list] == [
        [{"Id": str(i), "MessageBody": json.dumps(records[i])} for i in range(1, 11)],
        [{"Id": str(i), "MessageBody": json.dumps(records[i])} for i in range(11, 14)],
        [{"Id": "12", "MessageBody": '{"id": 12}'}],
        [{"Id": "12", "MessageBody": '{"id": 12}'}],
    ]
    assert result.success == 12
    assert result.failed == 2
    assert [error.batch_index for error in result.row_errors] == [0, 12]
    assert json.loads(result.row_errors[1].record_preview) == {"id": 12}


def test_on_error_fail_stops_before_next_batch(client: MagicMock) -> None:
    response = _success(*(str(i) for i in range(1, 10)))
    response["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [response]
    options = _options().model_copy(update={"on_error": "fail"})
    records = [{"id": i} for i in range(11)]
    result = SQSDestination().load(records, _config(), options)
    assert result.success == 9
    assert result.failed == 1
    assert result.skipped == 1
    assert result.total == len(records)
    assert [error.batch_index for error in result.row_errors] == [0]
    assert successful_indices(records, result) == set()
    assert client.send_message_batch.call_count == 1


@pytest.mark.parametrize("kind", ["sender_fault", "retry_exhaustion", "invalid_record"])
def test_fail_mode_preserves_engine_counts_and_dlq(
    client: MagicMock, tmp_path: Path, kind: str
) -> None:
    records = [{"id": i} for i in range(4)]
    if kind == "invalid_record":
        records[1]["payload"] = "x" * 1_048_576
        calls, successes, skipped = 0, 0, 1
    else:
        sender_fault = kind == "sender_fault"
        first, retry = _success("0"), _success()
        first["Failed"] = retry["Failed"] = [_failure("1", sender_fault=sender_fault)]
        client.send_message_batch.side_effect = [first, retry, retry]
        calls, successes, skipped = (1 if sender_fault else 3), 1, 0
    options = _options().model_copy(update={"on_error": "fail", "batch_size": 2})
    sync = SyncConfig(name="work_items", model="SELECT 1", destination=_config(), sync=options)
    store = LocalDlqStore(tmp_path)
    result = run_sync(
        sync,
        FakeSource(records),
        SQSDestination(),
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
        observer=DlqObserver(store),
    )
    assert result.success == successes
    assert result.failed == 1
    assert result.skipped == skipped
    assert result.rows_extracted == 2
    assert [error.batch_index for error in result.row_errors] == [1]
    assert [letter.record for letter in store.read(sync.name)] == [records[1]]
    assert client.send_message_batch.call_count == calls


def test_incomplete_http_200_response_is_not_all_success(client: MagicMock) -> None:
    client.send_message_batch.side_effect = [_success()]
    with pytest.raises(RuntimeError, match="batch response"):
        SQSDestination().load([{"id": 1}], _config(), _options())
    assert client.send_message_batch.call_count == 1


def test_send_transport_error_fails_sync(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL)

    client.send_message_batch.side_effect = ConnectionError("network unavailable")

    with pytest.raises(ConnectionError, match="network unavailable"):
        SQSDestination().load(
            [{"id": 1}],
            _config(),
            _options(),
        )


def test_fifo_retry_confirms_first_message_before_sending_next_in_group(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")

    def send(**kwargs: Any) -> dict[str, Any]:
        ids = [entry["Id"] for entry in kwargs["Entries"]]
        if client.send_message_batch.call_count == 1:
            response = _success(*(id_ for id_ in ids if id_ != "0"))
            response["Failed"] = [_failure("0", sender_fault=False)]
            return response
        return _success(*ids)

    client.send_message_batch.side_effect = send

    records = [{"id": 0, "group": "orders"}, {"id": 1, "group": "orders"}]
    result = SQSDestination().load(
        records,
        _config(message_group_id_field="group"),
        _options(),
    )

    assert [call.kwargs["Entries"] for call in client.send_message_batch.call_args_list] == [
        [{"Id": "0", "MessageBody": '{"id": 0, "group": "orders"}', "MessageGroupId": "orders"}],
        [{"Id": "0", "MessageBody": '{"id": 0, "group": "orders"}', "MessageGroupId": "orders"}],
        [{"Id": "1", "MessageBody": '{"id": 1, "group": "orders"}', "MessageGroupId": "orders"}],
    ]
    assert result.success == 2
    assert result.failed == 0


def test_fifo_batches_different_groups_without_resending_confirmed_entries(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")

    def send(**kwargs: Any) -> dict[str, Any]:
        ids = [entry["Id"] for entry in kwargs["Entries"]]
        if client.send_message_batch.call_count == 1:
            response = _success(*(id_ for id_ in ids if id_ != "0"))
            response["Failed"] = [_failure("0", sender_fault=False)]
            return response
        return _success(*ids)

    client.send_message_batch.side_effect = send
    records = [{"id": i, "group": group} for i, group in enumerate(["a", "b", "a", "b"])]
    result = SQSDestination().load(records, _config(message_group_id_field="group"), _options())
    assert [
        [entry["Id"] for entry in call.kwargs["Entries"]]
        for call in client.send_message_batch.call_args_list
    ] == [["0", "1"], ["0"], ["2", "3"]]
    assert result.success == 4
    assert result.failed == 0


@pytest.mark.parametrize("sender_fault", [True, False])
def test_fifo_failed_group_blocks_followers_but_other_groups_continue(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, sender_fault: bool
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")

    def send(**kwargs: Any) -> dict[str, Any]:
        ids = [entry["Id"] for entry in kwargs["Entries"]]
        response = _success(*(id_ for id_ in ids if id_ != "0"))
        if "0" in ids:
            response["Failed"] = [_failure("0", sender_fault=sender_fault)]
        return response

    client.send_message_batch.side_effect = send
    records = [{"id": i, "group": group} for i, group in enumerate(["a", "b", "a", "b", "a"])]
    result = SQSDestination().load(records, _config(message_group_id_field="group"), _options())
    expected = [["0", "1"], ["3"]] if sender_fault else [["0", "1"], ["0"], ["0"], ["3"]]
    assert [
        [entry["Id"] for entry in call.kwargs["Entries"]]
        for call in client.send_message_batch.call_args_list
    ] == expected
    assert result.success == 2
    assert result.failed == 3
    assert result.total == len(records)
    assert [error.batch_index for error in result.row_errors] == [0, 2, 4]
    assert all("blocked" in error.error_message for error in result.row_errors[1:])


@pytest.mark.parametrize("invalid", ["oversized", "missing_dedup"])
def test_fifo_invalid_record_blocks_later_records_in_its_group(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    records = [
        {"id": i, "group": group, "event": str(i)} for i, group in enumerate(["a", "a", "a", "b"])
    ]
    if invalid == "oversized":
        records[1]["payload"] = "x" * 1_048_576
    else:
        del records[1]["event"]
    result = SQSDestination().load(
        records, _config(message_group_id_field="group", deduplication_id_field="event"), _options()
    )
    assert [
        [entry["Id"] for entry in call.kwargs["Entries"]]
        for call in client.send_message_batch.call_args_list
    ] == [["0"], ["3"]]
    assert result.success == 2
    assert [error.batch_index for error in result.row_errors] == [1, 2]


def test_fifo_failure_dlq_keeps_original_group_order(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    failure = _success()
    failure["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [failure, _success("3")]
    records = [
        {"id": 0, "group": "a", "event": "0"},
        {"id": 1, "group": "a"},
        {"id": 2, "group": "a", "event": "2"},
        {"id": 3, "group": "b", "event": "3"},
    ]
    sync = SyncConfig(
        name="fifo",
        model="SELECT 1",
        destination=_config(message_group_id_field="group", deduplication_id_field="event"),
        sync=_options(),
    )
    store = LocalDlqStore(tmp_path)
    result = run_sync(
        sync,
        FakeSource(records),
        SQSDestination(),
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
        observer=DlqObserver(store),
    )
    assert result.success == 1
    assert result.failed == 3
    assert [error.batch_index for error in result.row_errors] == [0, 1, 2]
    assert [letter.record["id"] for letter in store.read(sync.name)] == [0, 1, 2]


def test_fifo_blocked_groups_survive_engine_batches_and_reset_after_run(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    failure = _success()
    failure["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [failure, _success("0"), _success("0")]
    records = [{"id": i, "group": group} for i, group in enumerate(["a", "a", "b"])]
    sync = SyncConfig(
        name="fifo",
        model="SELECT 1",
        destination=_config(message_group_id_field="group"),
        sync=_options(batch_size=1),
    )
    destination = SQSDestination()
    profile = BigQueryProfile(type="bigquery", project="p", dataset="d")
    store = LocalDlqStore(tmp_path)
    result = run_sync(
        sync, FakeSource(records), destination, profile, tmp_path, observer=DlqObserver(store)
    )
    assert result.success == 1
    assert result.failed == 2
    assert [letter.record["id"] for letter in store.read(sync.name)] == [0, 1]
    again = run_sync(sync, FakeSource([{"id": 3, "group": "a"}]), destination, profile, tmp_path)
    assert again.success == 1
    assert again.failed == 0
    assert [
        json.loads(call.kwargs["Entries"][0]["MessageBody"])["id"]
        for call in client.send_message_batch.call_args_list
    ] == [0, 2, 3]


def test_fifo_failed_group_does_not_block_another_queue(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    failure = _success()
    failure["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [failure, _success("0")]
    destination = SQSDestination()
    config = _config(message_group_id_field="group")
    destination.load([{"id": 0, "group": "a"}], config, _options())
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + "-other.fifo")
    result = destination.load([{"id": 1, "group": "a"}], config, _options())
    assert result.success == 1
    assert result.failed == 0


@pytest.mark.parametrize("large", [False, True])
def test_fifo_distinct_groups_respect_batch_limits(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, large: bool
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    records = [
        {"id": i, "group": str(i), "payload": "é" * 200000 if large else ""}
        for i in range(3 if large else 23)
    ]
    result = SQSDestination().load(records, _config(message_group_id_field="group"), _options())
    assert result.success == len(records)
    batches = [call.kwargs["Entries"] for call in client.send_message_batch.call_args_list]
    assert [len(batch) for batch in batches] == ([2, 1] if large else [10, 10, 3])
    for batch in batches:
        assert len({entry["MessageGroupId"] for entry in batch}) == len(batch)
        assert sum(len(entry["MessageBody"].encode("utf-8")) for entry in batch) <= 1_048_576


def test_fifo_fail_policy_stops_all_further_sends(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    failure = _success()
    failure["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [failure]
    records = [{"id": i, "group": group} for i, group in enumerate(["a", "a", "b"])]
    options = _options().model_copy(update={"on_error": "fail"})
    result = SQSDestination().load(records, _config(message_group_id_field="group"), options)
    assert client.send_message_batch.call_count == 1
    assert result.success == 0
    assert result.failed == 1
    assert result.skipped == 2
    assert result.total == len(records)


def test_fifo_blocked_groups_reset_after_source_exception(
    client: MagicMock, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SQS_QUEUE_URL", QUEUE_URL + ".fifo")
    failure = _success()
    failure["Failed"] = [_failure("0", sender_fault=True)]
    client.send_message_batch.side_effect = [failure, _success("0")]
    sync = SyncConfig(
        name="fifo",
        model="SELECT 1",
        destination=_config(message_group_id_field="group"),
        sync=_options(batch_size=1),
    )
    destination = SQSDestination()
    profile = BigQueryProfile(type="bigquery", project="p", dataset="d")

    def rows() -> Iterator[dict[str, Any]]:
        yield {"id": 0, "group": "a"}
        raise RuntimeError("source interrupted")

    source = FakeSource()
    with patch.object(source, "extract", return_value=rows()):
        with pytest.raises(RuntimeError, match="source interrupted"):
            run_sync(sync, source, destination, profile, tmp_path)
    result = run_sync(sync, FakeSource([{"id": 1, "group": "a"}]), destination, profile, tmp_path)
    assert result.success == 1
    assert result.failed == 0
    assert client.send_message_batch.call_count == 2
