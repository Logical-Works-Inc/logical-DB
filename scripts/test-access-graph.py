#!/usr/bin/env python3
"""Falsifiers for the source-anchored execution-access registry.

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
    def source_with(self, old: str, new: str) -> str:
        return access_graph.GRAPH_PATH.read_text(encoding="utf-8").replace(old, new, 1)

    def assert_refused(self, source: str, message: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "access-graph.toml"
            path.write_text(source, encoding="utf-8")
            with self.assertRaisesRegex(access_graph.AccessGraphError, message):
                access_graph.load(path)

    def test_registry_is_source_anchored_and_deterministic(self):
        document = access_graph.load()
        self.assertEqual(document["version"], 2)
        self.assertEqual(len(document["system"]), 16)
        self.assertEqual(access_graph.render(), access_graph.render())
        self.assertEqual(access_graph.render_dot(), access_graph.render_dot())
        self.assertIn('"embedded.remote-page" -> "replica.state" [label="write"];', access_graph.render_dot())

    def test_unknown_resource_is_refused(self):
        self.assert_refused(self.source_with('reads = ["replica.state", "replica.outbox"]', 'reads = ["unknown.resource"]'), "unknown resource")

    def test_absent_source_anchor_is_refused(self):
        self.assert_refused(
            self.source_with(
                'source = "crates/logical-db/src/lib.rs#Database::edit"',
                'source = "crates/logical-db/src/lib.rs#Database::absent"',
            ),
            "anchor is absent",
        )

    def test_missing_required_source_is_refused(self):
        self.assert_refused(self.source_with('  "crates/logical-db/src/lib.rs#Database::edit",\n', ""), "source inventory mismatch")

    def test_unknown_predecessor_is_refused(self):
        self.assert_refused(self.source_with("predecessors = []", 'predecessors = ["missing"]'), "unknown predecessor")

    def test_causal_cycle_is_refused(self):
        self.assert_refused(self.source_with("predecessors = []", 'predecessors = ["embedded.migrate"]'), "causal predecessor cycle")

    def test_missing_atomicity_is_refused(self):
        self.assert_refused(self.source_with('atomicity = "state and outbox commit together"', '# atomicity omitted'), "atomicity is required")

    def test_unbounded_limit_is_refused(self):
        self.assert_refused(self.source_with('limit = "one document and MAX_CHANGE_BYTES"', 'limit = "unbounded"'), "must declare a bound")


if __name__ == "__main__":
    unittest.main()
