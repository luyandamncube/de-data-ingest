"""Base contract for Silver transform adapters."""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod


class SilverTransformAdapter(ABC):
    """Engine-backed Silver transformation operations."""

    engine_name: str

    @abstractmethod
    def transform_customers(
        self,
        *,
        input_path: str,
        output_path: str,
    ) -> None:
        """Transform bronze/customers into silver/customers."""

    def close(self) -> None:
        """Release engine resources if needed."""
