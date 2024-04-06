import logging
import tempfile
from pathlib import Path
from typing import IO, Generator

from sequence_storages.base_storage import BaseSequenceStorage
from sequence_storages.utils import clean_header, to_fasta

logger = logging.getLogger("sequence_storages")


def _read_one_sequence(fi: IO[str], *, position: int | None = None) -> tuple[str, str]:
    if position is not None:
        fi.seek(position)

    header = clean_header(fi.readline())
    lines = []
    while True:
        line = fi.readline().strip()
        if not line or line.startswith(">"):
            break
        lines.append(line)
    return header, "".join(lines)


def _iter_header_positions(fi: IO[str]) -> Generator[tuple[str, int], None, None]:
    while True:
        pos = fi.tell()
        line = fi.readline()
        if not line:
            break
        if line.startswith(">"):
            header = clean_header(line)
            yield header, pos


class FastaStorage(BaseSequenceStorage):
    def __init__(
        self,
        path: Path,
        cache_size: int | None = None,
        autocommit: bool = True,
        wrap: int | None = None,
    ) -> None:
        super().__init__(cache_size, autocommit, wrap)
        if not path.exists():
            path.touch()
        self._path = path
        self.__index: dict[str, int] | None = None

    @property
    def _index(self) -> dict[str, int]:
        if self.__index is None:
            self.__index = self._build_index()
        return self.__index

    def _build_index(self) -> dict[str, int]:
        logger.debug("Building index: %s", self._path)
        index = {}
        with open(self._path, "r") as fi:
            for header, pos in _iter_header_positions(fi):
                if header in index:
                    logger.warning("Duplicated header skipped: %s", header)
                else:
                    index[header] = pos
        return index

    def _get_from_source(self, key: str) -> str | None:
        if key not in self._index:
            return None
        with open(self._path, "r") as fi:
            header, sequence = _read_one_sequence(fi, position=self._index[key])
            if key != header:
                logger.error(
                    "Incorrect index, key '%s' doesn't match header: %s", key, header
                )
                return None
        return sequence

    def _contains_in_source(self, key: str) -> bool:
        return key in self._index

    def commit(self) -> None:
        if not (self._deleted or self._updated):
            logger.debug("Nothing to commit.")
            return
        logger.debug("Committing")
        with tempfile.NamedTemporaryFile("w", delete=False) as temp:
            for header, sequence in self.items():
                fasta = to_fasta(header, sequence, wrap=self._wrap)
                temp.write(fasta)
            Path(temp.name).rename(self._path)
        self.__index = None

    def headers(self) -> Generator[str, None, None]:
        updated_headers = self._get_headers_of_updated_items()
        for header in self._index.keys():
            if header not in updated_headers:
                yield header
        yield from self._updated.keys()

    def items(self) -> Generator[tuple[str, str], None, None]:
        with open(self._path, "r") as fi:
            updated_headers = self._get_headers_of_updated_items()
            for header, pos in self._index.items():
                if header in updated_headers:
                    continue
                yield _read_one_sequence(fi, position=pos)

        yield from self._updated.items()
