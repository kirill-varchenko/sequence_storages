import random
import shutil
import string
import tempfile
import unittest
from pathlib import Path

import sequence_storages as storages


class TestWriteRead(unittest.TestCase):
    def setUp(self) -> None:
        self.sequences = {a: a * 100 for a in string.ascii_uppercase}
        self.temp_folder = Path(tempfile.mkdtemp())

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_folder)

    def test_fasta_storage(self):
        random_path = self.temp_folder / "".join(
            random.choices(string.ascii_letters, k=10)
        )

        with storages.FastaStorage(random_path) as storage:
            headers = list(storage.headers())
            self.assertEqual(len(headers), 0)
            for header, sequence in self.sequences.items():
                storage[header] = sequence
            headers = list(storage.headers())
            self.assertEqual(len(headers), len(self.sequences))

        with storages.FastaStorage(random_path) as storage:
            all_sequences = {header: sequence for header, sequence in storage.items()}
            self.assertDictEqual(all_sequences, self.sequences)

    def test_tar_storage(self):
        random_path = self.temp_folder / "".join(
            random.choices(string.ascii_letters, k=10)
        )

        with storages.TarStorage(random_path) as storage:
            headers = list(storage.headers())
            self.assertEqual(len(headers), 0)
            for header, sequence in self.sequences.items():
                storage[header] = sequence
            headers = list(storage.headers())
            self.assertEqual(len(headers), len(self.sequences))

        with storages.TarStorage(random_path) as storage:
            all_sequences = {header: sequence for header, sequence in storage.items()}
            self.assertDictEqual(all_sequences, self.sequences)

    def test_folder_storage(self):
        with storages.FolderStorage(self.temp_folder) as storage:
            headers = list(storage.headers())
            self.assertEqual(len(headers), 0)
            for header, sequence in self.sequences.items():
                storage[header] = sequence
            headers = list(storage.headers())
            self.assertEqual(len(headers), len(self.sequences))

        with storages.FolderStorage(self.temp_folder) as storage:
            all_sequences = {header: sequence for header, sequence in storage.items()}
            self.assertDictEqual(all_sequences, self.sequences)


if __name__ == "__main__":
    unittest.main()
