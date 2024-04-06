import logging
from pathlib import Path
from typing import Callable, Generator

from sequence_storages.base_storage import BaseSequenceStorage
from sequence_storages.utils import clean_filename, clean_header, to_fasta

logger = logging.getLogger("sequence_storages")


def _read_first_from_file(path: Path) -> tuple[str, str]:
    with open(path, "r") as fi:
        header = clean_header(fi.readline())
        lines = []
        for line in fi.readlines():
            decoded_line = line.strip()
            if decoded_line.startswith(">"):
                logger.warning(
                    "More than one sequences in file. First: %s, next: %s",
                    header,
                    decoded_line.removeprefix(">"),
                )
                break
            lines.append(decoded_line)
    return header, "".join(lines)


class FolderStorage(BaseSequenceStorage):
    def __init__(
        self,
        path: Path,
        glob: str = "**/*.fasta",
        cache_size: int | None = None,
        autocommit: bool = True,
        wrap: int | None = None,
    ) -> None:
        super().__init__(cache_size, autocommit, wrap)
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise ValueError(f"Path should be a folder: {path}")
        self._path = path
        self._glob = glob
        self.__header_to_path: dict[str, Path] | None = None

    @property
    def _header_to_path(self) -> dict[str, Path]:
        if self.__header_to_path is None:
            self.__header_to_path = {}
            for path in self._path.glob(self._glob):
                try:
                    with open(path, "r") as fi:
                        header = clean_header(fi.readline())
                    self.__header_to_path[header] = path
                except Exception as exc:
                    logger.warning("Exception during reading fasta: %s", exc)

        return self.__header_to_path

    def _get_from_source(self, key: str) -> str | None:
        if key not in self._header_to_path:
            return None
        header, sequence = _read_first_from_file(self._header_to_path[key])
        if key != header:
            logger.error(
                "Incorrect index, key '%s' doesn't match header: %s", key, header
            )
            return None
        return sequence

    def _contains_in_source(self, key: str) -> bool:
        return key in self._header_to_path

    def _get_new_path_generator(self) -> Callable[[str], Path]:
        header_to_path = self._header_to_path.copy()

        def helper(header: str) -> Path:
            if header in header_to_path:
                return header_to_path[header]
            cleaned = clean_filename(header)
            i = 0
            while True:
                path = self._path / (
                    f"{cleaned}.fasta" if i == 0 else f"{cleaned}_{i}.fasta"
                )
                if not path.exists():
                    header_to_path[header] = path
                    return path
                i += 1

        return helper

    def commit(self) -> None:
        if not (self._deleted or self._updated):
            logger.debug("Nothing to commit.")
            return
        logger.debug("Committing")
        for header in self._deleted:
            self._header_to_path[header].unlink()
        path_generator = self._get_new_path_generator()
        for header, sequence in self._updated.items():
            path = (
                self._header_to_path[header]
                if header in self._header_to_path
                else path_generator(header)
            )
            with open(path, "w") as fo:
                fasta = to_fasta(header, sequence, wrap=self._wrap)
                fo.write(fasta)

    def headers(self) -> Generator[str, None, None]:
        updated_headers = self._get_headers_of_updated_items()
        for header in self._header_to_path.keys():
            if header not in updated_headers:
                yield header
        yield from self._updated.keys()

    def items(self) -> Generator[tuple[str, str], None, None]:
        updated_headers = self._get_headers_of_updated_items()
        for header, path in self._header_to_path.items():
            if header in updated_headers:
                continue
            yield _read_first_from_file(path)
        yield from self._updated.items()
