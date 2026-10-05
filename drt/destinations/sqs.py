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
