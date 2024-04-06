import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Generator, Self

logger = logging.getLogger("sequence_storages")


class BaseSequenceStorage(ABC):
    def __init__(
        self,
        cache_size: int | None = None,
        autocommit: bool = True,
        wrap: int | None = None,
    ) -> None:
        self._cache_size = cache_size
        self._autocommit = autocommit
        self._wrap = wrap
        self._cache: OrderedDict[str, str] = OrderedDict()
        self._deleted: set[str] = set()
        self._updated: OrderedDict[str, str] = OrderedDict()

    def _get_headers_of_updated_items(self) -> set[str]:
        return self._deleted | set(self._updated.keys())

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            logger.error("Exception occured: %s", exc_value)
        elif self._autocommit:
            self.commit()

    def __getitem__(self, key: str) -> str:
        if key in self._deleted:
            logger.debug("Key marked as deleted: %s", key)
            raise KeyError(key)
        if key in self._updated:
            logger.debug("Return sequence from updated: %s", key)
            return self._updated[key]
        if key in self._cache:
            logger.debug("Return sequence from cache: %s", key)
            self._cache.move_to_end(key)
            return self._cache[key]
        logger.debug("Getting sequence from source: %s", key)
        sequence = self._get_from_source(key)
        if sequence is None:
            raise KeyError(key)
        self._put_to_cache(key, sequence)
        return sequence

    def __setitem__(self, key: str, sequence: str) -> None:
        if key in self._deleted:
            self._deleted.remove(key)
        if key in self._cache:
            del self._cache[key]
        logger.debug("Saving sequence to updated: %s", key)
        self._updated[key] = sequence
        self._updated.move_to_end(key)

    def __delitem__(self, key: str) -> None:
        if key in self._updated:
            del self._updated[key]
        if key in self._cache:
            del self._cache[key]
        logger.debug("Mark key as deleted: %s", key)
        self._deleted.add(key)

    def __contains__(self, key: str) -> bool:
        if key in self._deleted:
            return False
        if key in self._updated:
            return True
        if key in self._cache:
            return True
        return self._contains_in_source(key)

    def _put_to_cache(self, key: str, sequence: str) -> None:
        logger.debug("Caching for key: %s", key)
        self._cache[key] = sequence
        if self._cache_size and len(self._cache) > self._cache_size:
            self._cache.popitem(last=False)

    @abstractmethod
    def _get_from_source(self, key: str) -> str | None:
        """Get sequence from the source."""

    @abstractmethod
    def _contains_in_source(self, key: str) -> bool:
        """Check if key is in source."""

    @abstractmethod
    def commit(self) -> None:
        """Save changes to the source."""

    @abstractmethod
    def headers(self) -> Generator[str, None, None]:
        """Iterate over headers."""

    @abstractmethod
    def items(self) -> Generator[tuple[str, str], None, None]:
        """Iterate over headers and sequences."""
