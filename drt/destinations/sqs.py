"""Amazon SQS destination scaffolding; message delivery is not implemented yet."""

from __future__ import annotations

from typing import Any
from json import dumps
from decimal import Decimal
from uuid import UUID

from drt.config.models import DestinationConfig, SQSDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


class SQSDestination:
    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SQSDestinationConfig)
        raise NotImplementedError("SQS message delivery is not implemented yet.")

    @staticmethod
    def _client(config: SQSDestinationConfig) -> Any:
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("SQS destination requires: pip install drt-core[sqs]") from exc
        session = boto3.session.Session(**({"region_name": config.region} if config.region else {}))
        return session.client("sqs")

    @staticmethod
    def _entry(index: int, record: dict[str, Any], config: SQSDestinationConfig) -> dict[str, str]:
        entry = {
            "Id": str(index),
            "MessageBody": dumps(record, default=str, ensure_ascii=False),
        }
        for field, parameter in (
            (config.message_group_id_field, "MessageGroupId"),
            (config.deduplication_id_field, "MessageDeduplicationId"),
        ):
            if field is not None:
                value = record.get(field)
                if not isinstance(value, (str, int, float, Decimal, UUID)):
                    raise ValueError(
                        f"SQS {parameter}: record field {field!r} must have a scalar ID."
                    )
                text = str(value)
                if not text or len(text) > 128:
                    raise ValueError(
                        f"SQS {parameter}: record field {field!r} must contain 1–128 characters."
                    )
                entry[parameter] = text
        return entry