import io
import logging
import tarfile
import tempfile
import time
from pathlib import Path
from typing import IO, Callable, Generator, Literal

from sequence_storages.base_storage import BaseSequenceStorage
from sequence_storages.utils import clean_filename, clean_header, to_fasta

logger = logging.getLogger("sequence_storages")


def _read_first_from_buf(buf: IO[bytes]) -> tuple[str, str]:
    header = clean_header(buf.readline().decode())
    lines = []
    for line in buf.readlines():
        decoded_line = line.decode().strip()
        if decoded_line.startswith(">"):
            logger.warning(
                "More than one sequences in file. First: %s, next: %s",
                header,
                decoded_line.removeprefix(">"),
            )
            break
        lines.append(decoded_line)
    return header, "".join(lines)


class TarStorage(BaseSequenceStorage):
    def __init__(
        self,
        path: Path,
        compression: Literal["", "gz", "bz2", "xz"] = "xz",
        cache_size: int | None = None,
        autocommit: bool = True,
        wrap: int | None = None,
    ) -> None:
        super().__init__(cache_size, autocommit, wrap)
        self._path = path
        self._compression = compression
        self.__header_to_tar_info_name: dict[str, str] | None = None

    @property
    def _header_to_tar_info_name(self) -> dict[str, str]:
        if not self._path.exists():
            return {}
        if self.__header_to_tar_info_name is None:
            logger.debug("Building index: %s", self._path)
            self.__header_to_tar_info_name = {}
            with tarfile.open(self._path, f"r:{self._compression}") as tar:
                for tar_info in tar.getmembers():
                    buf = tar.extractfile(tar_info)
                    if buf is None:
                        continue
                    header = clean_header(buf.readline().decode())
                    if header in self.__header_to_tar_info_name:
                        logger.warning("Duplicated header skipped: %s", header)
                    else:
                        self.__header_to_tar_info_name[header] = tar_info.name
        return self.__header_to_tar_info_name

    def _get_from_source(self, key: str) -> str | None:
        if key not in self._header_to_tar_info_name:
            return None
        with tarfile.open(self._path, f"r:{self._compression}") as tar:
            buf = tar.extractfile(self._header_to_tar_info_name[key])
            if buf is None:
                return
            header, sequence = _read_first_from_buf(buf)
        if header != key:
            logger.error(
                "Incorrect index, key '%s' doesn't match header: %s", key, header
            )
            return None
        return sequence

    def _contains_in_source(self, key: str) -> bool:
        return key in self._header_to_tar_info_name

    def _get_new_filename_generator(self) -> Callable[[str], str]:
        header_to_filename = self._header_to_tar_info_name.copy()
        existing_filenames = set(header_to_filename.values())

        def helper(header: str) -> str:
            if header in header_to_filename:
                return header_to_filename[header]
            cleaned = clean_filename(header)
            i = 0
            while True:
                filename = f"{cleaned}.fasta" if i == 0 else f"{cleaned}_{i}.fasta"
                if filename not in existing_filenames:
                    header_to_filename[header] = filename
                    existing_filenames.add(filename)
                    return filename
                i += 1

        return helper

    def commit(self) -> None:
        if not (self._deleted or self._updated):
            logger.debug("Nothing to commit.")
            return
        logger.debug("Committing")
        with tempfile.NamedTemporaryFile("wb", delete=False) as temp:
            filename_generator = self._get_new_filename_generator()
            with tarfile.open(fileobj=temp, mode=f"w:{self._compression}") as tar:
                for header, sequence in self.items():
                    tar_info_name = filename_generator(header)
                    tar_info = tarfile.TarInfo(name=tar_info_name)
                    fasta = to_fasta(header, sequence, wrap=self._wrap)
                    buf = io.BytesIO()
                    tar_info.size = buf.write(fasta.encode())
                    buf.seek(0)
                    tar_info.mtime = int(time.time())
                    tar.addfile(tarinfo=tar_info, fileobj=buf)
            Path(temp.name).rename(self._path)
        self.__header_to_tar_info_name = None

    def headers(self) -> Generator[str, None, None]:
        updated_headers = self._get_headers_of_updated_items()
        for header in self._header_to_tar_info_name.keys():
            if header not in updated_headers:
                yield header
        yield from self._updated.keys()

    def items(self) -> Generator[tuple[str, str], None, None]:
        updated_headers = self._get_headers_of_updated_items()
        if self._path.exists():
            with tarfile.open(self._path, f"r:{self._compression}") as tar:
                for header, tar_info_name in self._header_to_tar_info_name.items():
                    if header in updated_headers:
                        continue
                    buf = tar.extractfile(tar_info_name)
                    if buf is None:
                        continue
                    yield _read_first_from_buf(buf)
        yield from self._updated.items()
