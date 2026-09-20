#!/usr/bin/env python3
"""Falsify the domain allowed-edge gate with a forbidden edge per domain.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
`evidence/SOURCES.md`.

Issue #73 requires a deliberate forbidden-edge falsifier for each domain. A
gate that only ever passes proves nothing, so every test here injects an edge
the contract forbids and asserts the real `domain_contract` functions refuse
it. The unmutated graph is asserted clean first, so a refusal cannot come from
an unrelated failure.
"""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import domain_contract as contract  # noqa: E402


def live() -> tuple[dict, dict, tuple[str, ...], list[str]]:
    """Return the committed owners, graph, composition, and denylist."""
    owners = contract.load_owners()
    graph = contract.load_crate_graph()
    composition = contract.load_composition()
    denylist = contract.load_denylist()
    return owners, graph, composition, denylist


def failures_for(owners: dict, graph: dict, composition: tuple[str, ...]) -> list[str]:
    """Run the real ownership, contract, and edge checks on a mutated graph."""
    ownership, assigned = contract.ownership_failures(owners, graph)
    return (
        ownership
        + contract.contract_failures(owners, graph)
        + contract.edge_failures(owners, assigned, graph, composition)
    )


class DomainEdgeFalsifier(unittest.TestCase):
    """Every forbidden edge must be refused by the real checker."""

    def setUp(self) -> None:
        self.owners, self.graph, self.composition, self.denylist = live()

    def assert_clean(self, owners: dict, graph: dict) -> None:
        """The unmutated fixture must pass, so a refusal is the injected edge."""
        self.assertEqual(
            failures_for(owners, graph, self.composition),
            [],
            "the unmutated fixture is not clean; the falsifier would be vacuous",
        )

    def inject(self, consumer: str, dependency: str) -> tuple[dict, dict]:
        """Return a copy of the live contract with one extra edge."""
        owners = copy.deepcopy(self.owners)
        graph = copy.deepcopy(self.graph)
        graph.setdefault(consumer, [])
        if dependency not in graph[consumer]:
            graph[consumer] = sorted([*graph[consumer], dependency])
        return owners, graph

    def test_cloud_may_not_depend_on_cairn(self) -> None:
        owners, graph = self.inject("logical-db-crdt", "logical-db-cairn")
        self.assert_clean(self.owners, self.graph)
        failures = failures_for(owners, graph, self.composition)
        self.assertTrue(
            any("logical-db-crdt -> logical-db-cairn" in failure for failure in failures),
            f"cloud -> cairn was not refused: {failures}",
        )

    def test_cairn_may_not_import_cloud_implementation(self) -> None:
        owners, graph = self.inject("logical-db-cairn", "logical-db-store")
        self.assert_clean(self.owners, self.graph)
        failures = failures_for(owners, graph, self.composition)
        self.assertTrue(
            any("logical-db-cairn -> logical-db-store" in failure for failure in failures),
            f"cairn -> cloud implementation was not refused: {failures}",
        )

    def test_lem_may_not_import_cloud_implementation(self) -> None:
        owners, graph = self.inject("logical-db-episode", "logical-db-store-fdb")
        self.assert_clean(self.owners, self.graph)
        failures = failures_for(owners, graph, self.composition)
        self.assertTrue(
            any("logical-db-episode -> logical-db-store-fdb" in failure for failure in failures),
            f"lem -> cloud implementation was not refused: {failures}",
        )

    def test_forge_may_not_depend_on_cloud(self) -> None:
        owners, graph = self.inject("logical-db-forge-runtime", "logical-db-store")
        owners["forge"]["crates"] = sorted(
            [*owners["forge"]["crates"], "logical-db-forge-runtime"]
        )
        self.assert_clean(self.owners, self.graph)
        failures = failures_for(owners, graph, self.composition)
        self.assertTrue(
            any("logical-db-forge-runtime -> logical-db-store" in failure for failure in failures),
            f"forge -> cloud was not refused: {failures}",
        )

    def test_undeclared_crate_has_no_owner(self) -> None:
        owners = copy.deepcopy(self.owners)
        graph = copy.deepcopy(self.graph)
        graph["logical-db-orphan"] = []
        failures = failures_for(owners, graph, self.composition)
        self.assertTrue(
            any("logical-db-orphan has no declared owner" in failure for failure in failures),
            f"an undeclared crate passed: {failures}",
        )

    def test_double_owned_crate_is_refused(self) -> None:
        owners = copy.deepcopy(self.owners)
        owners["cairn"]["crates"] = sorted([*owners["cairn"]["crates"], "logical-db-store"])
        failures = failures_for(owners, self.graph, self.composition)
        self.assertTrue(
            any("logical-db-store is owned by both" in failure for failure in failures),
            f"a double-owned crate passed: {failures}",
        )

    def test_predecessor_twin_is_refused(self) -> None:
        for token in self.denylist:
            with self.subTest(token=token):
                graph = copy.deepcopy(self.graph)
                twin = f"logical-db-{token}"
                graph[twin] = []
                failures = contract.twin_failures(graph, self.denylist)
                self.assertTrue(
                    any(twin in failure for failure in failures),
                    f"twin {twin} passed: {failures}",
                )

    def test_committed_graph_and_receipt_are_clean(self) -> None:
        failures, document = contract.enforce()
        self.assertEqual(failures, [], f"the committed graph is not clean: {failures}")
        self.assertEqual(
            contract.receipt_failures(document),
            [],
            "the committed receipt does not match the live graph",
        )


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(DomainEdgeFalsifier)
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        sys.exit(1)