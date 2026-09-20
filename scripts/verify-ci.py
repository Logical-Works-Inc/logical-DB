#!/usr/bin/env python3
"""Reject hosted or unresolved runner routing, including ambiguous YAML.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
"""
from pathlib import Path
import sys
import unittest

import yaml


class UniqueLoader(yaml.SafeLoader):
    """Do not let duplicate YAML keys overwrite the routing being reviewed."""

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in result:
                raise ValueError(f"duplicate YAML key: {key}")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def verify(text):
    document = yaml.load(text, Loader=UniqueLoader)
    if not isinstance(document, dict):
        raise ValueError("workflow must be a mapping")
    jobs = document.get("jobs")
    if not isinstance(jobs, dict) or not jobs:
        raise ValueError("workflow must declare jobs")
    for name, job in jobs.items():
        if not isinstance(job, dict):
            raise ValueError(f"{name}: job must be a mapping")
        labels = job.get("runs-on")
        if not isinstance(labels, list) or not labels:
            raise ValueError(f"{name}: explicit self-hosted label list required")
        if any(not isinstance(label, str) or "${{" in label for label in labels):
            raise ValueError(f"{name}: runner labels must be literal strings")
        if "self-hosted" not in labels:
            raise ValueError(f"{name}: missing self-hosted label")
    return len(jobs)


class RoutingRegression(unittest.TestCase):
    def test_literal_self_hosted(self):
        self.assertEqual(verify("jobs: {test: {runs-on: [self-hosted, macOS, ARM64]}}"), 1)

    def test_hosted_and_unresolved_routes_fail(self):
        routes = [
            "ubuntu-latest", "windows-latest", "macos-latest",
            "[Linux, X64]", '"${{ matrix.os }}"',
            '[self-hosted, "${{ matrix.os }}"]', "[]", "null",
            "{group: unverified}",
        ]
        for route in routes:
            with self.subTest(route=route), self.assertRaises(ValueError):
                verify(f"jobs:\n  test:\n    runs-on: {route}\n")

    def test_one_hosted_job_rejects_whole_workflow(self):
        with self.assertRaises(ValueError):
            verify("jobs: {good: {runs-on: [self-hosted]}, bad: {runs-on: ubuntu-latest}}")

    def test_duplicate_route_is_not_silently_overwritten(self):
        with self.assertRaises(ValueError):
            verify("jobs: {test: {runs-on: ubuntu-latest, runs-on: [self-hosted]}}")

    def test_reusable_workflow_cannot_hide_external_routing(self):
        with self.assertRaises(ValueError):
            verify("jobs: {test: {uses: external/repo/.github/workflows/ci.yml@main}}")

    def test_missing_jobs_fails(self):
        for document in ["", "[]", "{}", "jobs: {}", "jobs: {test: []}"]:
            with self.subTest(document=document), self.assertRaises(ValueError):
                verify(document)

    def test_required_check_names_remain_available(self):
        root = Path(__file__).resolve().parents[1]
        document = yaml.load((root / ".github/workflows/ci.yml").read_text(), Loader=UniqueLoader)
        names = set()
        for job in document["jobs"].values():
            if job["name"] == "${{ matrix.check-name }} / test-clippy":
                names.update(f"{row['check-name']} / test-clippy"
                             for row in job["strategy"]["matrix"]["include"])
            else:
                names.add(job["name"])
        self.assertEqual(names, {
            "clean-room evidence", "ubuntu-latest / test-clippy",
            "macos-latest / test-clippy", "windows-latest / test-clippy",
            "rustfmt", "advisory, license, and source policy",
            "official FDB client / daemon feature",
        })


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RoutingRegression)
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        sys.exit(1)
    root = Path(__file__).resolve().parents[1]
    workflows = sorted((root / ".github/workflows").glob("*.y*ml"))
    if not workflows:
        raise SystemExit("no workflows found")
    for path in workflows:
        count = verify(path.read_text())
        print(f"PASS {path.relative_to(root)}: {count} explicit self-hosted jobs")
