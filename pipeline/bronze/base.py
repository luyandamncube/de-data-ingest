"""Base contract for Bronze ingest adapters."""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from datetime import datetime


class BronzeIngestAdapter(ABC):
    """Engine-backed Bronze ingest operations."""

    engine_name: str

    @abstractmethod
    def ingest_customers(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        """Ingest customers.csv into the Bronze layer."""

    @abstractmethod
    def ingest_accounts(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        """Ingest accounts.csv into the Bronze layer."""

    @abstractmethod
    def ingest_transactions(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        """Ingest transactions.jsonl into the Bronze layer."""

    def close(self) -> None:
        """Release engine resources if needed."""
