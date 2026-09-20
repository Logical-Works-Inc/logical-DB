#!/usr/bin/env python3
"""Falsifiers for the checked execution-access registry.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
`evidence/SOURCES.md`.
"""

import tempfile
import unittest
from pathlib import Path

import access_graph


class AccessGraphContract(unittest.TestCase):
    def test_registry_has_one_writer_for_every_resource(self):
        document = access_graph.load()
        self.assertEqual(document["version"], 1)
        self.assertEqual(len(document["resource"]), 8)
        self.assertEqual(len(document["system"]), 5)

    def test_unknown_resource_is_refused(self):
        source = access_graph.GRAPH_PATH.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "access-graph.toml"
            path.write_text(source.replace('reads = ["replica.state", "replica.outbox"]',
                                            'reads = ["unknown.resource"]', 1), encoding="utf-8")
            with self.assertRaisesRegex(access_graph.AccessGraphError, "unknown resource"):
                access_graph.load(path)

    def test_second_writer_is_refused(self):
        source = access_graph.GRAPH_PATH.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "access-graph.toml"
            path.write_text(source + '\n[[system]]\nname="bad"\nowner="x"\nreads=["replica.state"]\nwrites=["replica.state"]\nreceipt="x"\ncontinuation="bounded"\n', encoding="utf-8")
            with self.assertRaisesRegex(access_graph.AccessGraphError, "multiple writers"):
                access_graph.load(path)


if __name__ == "__main__":
    unittest.main()
