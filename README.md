# Sequence Storages

A set of classes for accessing sequence data in a sequence storage in a dict-like manner. The original idea was to reduce IO operations for storages that does not support random writes. Changes made to sequences are stored in memory and written to disk in one go. Caching is also implemented.

## Storages
### FastaStorage
Class to store sequences in a plain text fasta file.

### TarStorage
Class to store sequences in a tar archive, one file per sequence, with compression (xz by default).

### FolderStorage
Class to store sequences in a folder, one file per sequence, flat folder stucture.

## Common parameters
- *cache_size* - maximum number of sequences to store in cache after reading them.
- *autocommit* - automatically save changes to source files on context manager exit (intermediate commits won't work at the moment).
- *wrap* - line width to wrap sequences in fasta (one line if *None*).

## Example 
```python
from sequence_storages import FastaStorage

with FastaStorage("mysequences.fasta") as storage:
    # dict-like accessing
    sequence = storage["myheader"]
    storage["myheader"] = "MYSEQUENCE"
    if "myheader" in storage:
        # do stuff
    del storage["myheader"]

    for header in storage.headers():
        # do stuff

    for header, sequence in storage.items():
        # do stuff
```

