from abc import ABC, abstractmethod
from typing import BinaryIO, Optional


class AbstractStorage(ABC):
    """
    Storage-agnostic interface.
    """

    @abstractmethod
    async def upload(
        self,
        *,
        bucket: str,
        key: str,
        data: BinaryIO,
        length: int,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Upload bytes and return a stable reference (URL / path / etag).
        """
        ...

    @abstractmethod
    async def health(self) -> bool:
        """
        Lightweight health-check.
        """
        ...