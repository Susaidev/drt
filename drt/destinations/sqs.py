"""Amazon SQS destination scaffolding; message delivery is not implemented yet."""

from __future__ import annotations

from typing import Any

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