import re
import textwrap

re_clean_filename = re.compile(r"[/\\?%*:|\"<>\x7F\x00-\x1F]")


def to_fasta(header: str, sequence: str, wrap: int | None = None) -> str:
    """Make fasta string from header and sequence."""
    if wrap:
        sequence = "\n".join(textwrap.wrap(sequence, width=wrap))
    return f">{header}\n{sequence}\n"


def clean_header(header: str) -> str:
    """Remove prefix and newline from raw header."""
    if not header.startswith(">"):
        raise ValueError(f"Invalid header: {header}")
    return header.removeprefix(">").strip()


def clean_filename(s: str) -> str:
    """Make string a save filename."""
    return re_clean_filename.sub("_", s)
