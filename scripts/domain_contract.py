#!/usr/bin/env python3
"""Enforce the four-domain ownership and allowed-edge contract.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
`evidence/SOURCES.md`.

This module is the mechanical half of issue #73. It parses
`contract/domains/*.toml`, reads the real workspace graph from `cargo metadata`,
and refuses:

* a workspace crate with no declared owner, or two owners;
* a declared crate that is not a workspace member;
* a cross-domain import the consumer's `may_depend_on` does not allow;
* a product domain importing another domain's implementation crate instead of
  its published contract;
* a predecessor twin (`smb`, `wm-core`, ...) reappearing as a crate.

It also renders the committed `evidence/domain-graph.md` receipt. A clean
compile is not proof of a legal authority path; this gate proves only the
declared import graph, and the falsifier in `scripts/test-domain-contract.py`
proves the gate refuses a deliberately injected forbidden edge.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_DIR = ROOT / "contract" / "domains"
GRAPH_PATH = DOMAIN_DIR / "GRAPH.toml"
RECEIPT_PATH = ROOT / "evidence" / "domain-graph.md"
RECEIPT_BEGIN = "<!-- domain-graph-receipt:begin -->"
RECEIPT_END = "<!-- domain-graph-receipt:end -->"

# The four product domains declare their own records; the layer records live in
# GRAPH.toml. Together they must cover every workspace crate exactly once.
DOMAIN_MANIFESTS = ("cloud", "cairn", "lem", "forge")
KNOWN_OWNERS = ("kernel", "cloud", "cairn", "lem", "forge", "facade", "adapter", "e2e")
# `approved-estate` is an external dependency class, not an internal owner.
EXTERNAL_LABELS = ("approved-estate",)
COMPOSITION = ("facade", "adapter", "e2e")


class ContractError(RuntimeError):
    """A malformed or unreadable contract document."""


def _load_toml(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ContractError(f"invalid contract document {path}: {error}") from error


def _string_list(document: dict, key: str, where: str) -> list[str]:
    values = document.get(key, [])
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise ContractError(f"{where} field {key} must be a list of strings")
    return values


def _owner_record(name: str, document: dict, where: str) -> dict:
    record = {
        "crates": _string_list(document, "crates", where),
        "public_contracts": _string_list(document, "public_contracts", where),
        "may_depend_on": _string_list(document, "may_depend_on", where),
        "forbidden_domains": _string_list(document, "forbidden_domains", where),
    }
    for label in record["may_depend_on"] + record["forbidden_domains"]:
        if label not in KNOWN_OWNERS and label not in EXTERNAL_LABELS:
            raise ContractError(f"{where} names unknown owner label {label!r}")
    return record


def load_owners(root: Path = ROOT) -> dict[str, dict]:
    """Return owner -> record for the four domains plus the layer records."""
    owners: dict[str, dict] = {}
    for name in DOMAIN_MANIFESTS:
        path = root / "contract" / "domains" / f"{name}.toml"
        document = _load_toml(path)
        if document.get("domain") != name:
            raise ContractError(
                f"{path} declares domain {document.get('domain')!r}, expected {name!r}"
            )
        owners[name] = _owner_record(name, document, str(path))
    graph = _load_toml(root / "contract" / "domains" / "GRAPH.toml")
    layers = graph.get("layers")
    if not isinstance(layers, dict) or not layers:
        raise ContractError(f"{GRAPH_PATH} must declare a nonempty [layers] table")
    for name, document in layers.items():
        if name not in KNOWN_OWNERS:
            raise ContractError(f"{GRAPH_PATH} declares unknown layer {name!r}")
        if not isinstance(document, dict):
            raise ContractError(f"{GRAPH_PATH} layer {name!r} must be a table")
        owners[name] = _owner_record(name, document, f"{GRAPH_PATH} layer {name}")
    missing = sorted(set(KNOWN_OWNERS) - set(owners))
    if missing:
        raise ContractError(f"no ownership record for: {' '.join(missing)}")
    return owners


def load_composition(root: Path = ROOT) -> tuple[str, ...]:
    """Return the declared composition layers."""
    graph = _load_toml(root / "contract" / "domains" / "GRAPH.toml")
    section = graph.get("composition", {})
    domains = _string_list(section, "domains", f"{GRAPH_PATH} [composition]")
    unknown = sorted(set(domains) - set(KNOWN_OWNERS))
    if unknown:
        raise ContractError(f"{GRAPH_PATH} composition names unknown owners: {' '.join(unknown)}")
    return tuple(domains)


def load_denylist(root: Path = ROOT) -> list[str]:
    """Return the predecessor twin denylist."""
    graph = _load_toml(root / "contract" / "domains" / "GRAPH.toml")
    section = graph.get("twin_denylist", {})
    return _string_list(section, "crates", f"{GRAPH_PATH} [twin_denylist]")


def load_crate_graph(root: Path = ROOT) -> dict[str, list[str]]:
    """Return crate -> sorted internal dependency crate names from cargo metadata."""
    process = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=root,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        raise ContractError(
            f"cargo metadata failed: {process.stderr.decode('utf-8', 'replace')}"
        )
    metadata = json.loads(process.stdout)
    members = {package["name"] for package in metadata["packages"]}
    graph: dict[str, list[str]] = {}
    for package in metadata["packages"]:
        graph[package["name"]] = sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in members
        )
    return graph


def ownership_failures(
    owners: dict[str, dict], graph: dict[str, list[str]]
) -> tuple[list[str], dict[str, str]]:
    """Return ownership failures and the crate -> owner assignment."""
    failures: list[str] = []
    assigned: dict[str, str] = {}
    for owner, record in owners.items():
        for crate in record["crates"]:
            if crate in assigned:
                failures.append(f"crate {crate} is owned by both {assigned[crate]} and {owner}")
            assigned[crate] = owner
    for crate in sorted(graph):
        if crate not in assigned:
            failures.append(f"workspace crate {crate} has no declared owner")
    for crate in sorted(assigned):
        if crate not in graph:
            failures.append(f"declared crate {crate} is not a workspace member")
    return failures, assigned


def contract_failures(owners: dict[str, dict], graph: dict[str, list[str]]) -> list[str]:
    """Return public-contract and policy-consistency failures."""
    failures: list[str] = []
    for owner, record in owners.items():
        overlap = sorted(set(record["may_depend_on"]) & set(record["forbidden_domains"]))
        if overlap:
            failures.append(f"{owner} lists {overlap} as both allowed and forbidden")
        for contract in record["public_contracts"]:
            if contract in graph and contract not in record["crates"]:
                failures.append(
                    f"{owner} publishes {contract} but does not own it"
                )
    return failures


def edge_failures(
    owners: dict[str, dict],
    assigned: dict[str, str],
    graph: dict[str, list[str]],
    composition: tuple[str, ...],
) -> list[str]:
    """Return every cross-owner import the declared policy does not allow."""
    failures: list[str] = []
    for consumer, record in owners.items():
        allowed = set(record["may_depend_on"])
        for crate in record["crates"]:
            for dependency in graph.get(crate, []):
                dependency_owner = assigned.get(dependency)
                if dependency_owner is None or dependency_owner == consumer:
                    continue
                if dependency_owner not in allowed:
                    failures.append(
                        f"{crate} -> {dependency}: {consumer} may not depend on {dependency_owner}"
                    )
                    continue
                if consumer in composition:
                    continue
                surface = set(owners[dependency_owner]["public_contracts"])
                if dependency_owner == "kernel" or dependency in surface:
                    continue
                failures.append(
                    f"{crate} -> {dependency}: {consumer} may import only "
                    f"{dependency_owner} public contracts, not implementation"
                )
    return failures


def twin_failures(graph: dict[str, list[str]], denylist: list[str]) -> list[str]:
    """Return every workspace crate that matches a predecessor twin name."""
    failures: list[str] = []
    for crate in sorted(graph):
        lowered = crate.lower()
        for token in denylist:
            if lowered == token or lowered.endswith(f"-{token}") or f"-{token}-" in lowered:
                failures.append(f"crate {crate} is a forbidden predecessor twin of {token}")
    return failures


def receipt(
    owners: dict[str, dict],
    assigned: dict[str, str],
    graph: dict[str, list[str]],
    composition: tuple[str, ...],
    denylist: list[str],
) -> dict:
    """Return the canonical dependency-graph receipt."""
    edges = []
    for consumer, record in owners.items():
        for crate in record["crates"]:
            for dependency in graph.get(crate, []):
                dependency_owner = assigned.get(dependency)
                if dependency_owner is None or dependency_owner == consumer:
                    continue
                edges.append(
                    {
                        "consumer": crate,
                        "dependency": dependency,
                        "consumer_owner": consumer,
                        "dependency_owner": dependency_owner,
                    }
                )
    return {
        "owners": {owner: sorted(record["crates"]) for owner, record in sorted(owners.items())},
        "edges": sorted(edges, key=lambda edge: (edge["consumer"], edge["dependency"])),
        "composition": sorted(composition),
        "twin_denylist": sorted(denylist),
    }


def enforce(root: Path = ROOT) -> tuple[list[str], dict]:
    """Return every failure and the receipt for the live workspace graph."""
    owners = load_owners(root)
    composition = load_composition(root)
    denylist = load_denylist(root)
    graph = load_crate_graph(root)
    ownership, assigned = ownership_failures(owners, graph)
    failures = (
        ownership
        + contract_failures(owners, graph)
        + edge_failures(owners, assigned, graph, composition)
        + twin_failures(graph, denylist)
    )
    return failures, receipt(owners, assigned, graph, composition, denylist)


def render_receipt(document: dict) -> str:
    """Render the committed markdown receipt around a canonical JSON block."""
    return (
        "# Domain dependency graph receipt\n"
        "\n"
        "> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15\n"
        "> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32\n"
        "> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in\n"
        "> `SOURCES.md`.\n"
        "\n"
        "Generated by `scripts/domain_contract.py --emit`. The clean-room evidence\n"
        "lane regenerates this graph and fails when the committed receipt no longer\n"
        "matches the workspace. Do not hand-edit the JSON block.\n"
        "\n"
        f"{RECEIPT_BEGIN}\n"
        "```json\n"
        f"{json.dumps(document, indent=2, sort_keys=True)}\n"
        "```\n"
        f"{RECEIPT_END}\n"
    )


def extract_receipt(markdown: str) -> dict:
    """Return the JSON receipt embedded in a committed markdown document."""
    start = markdown.find(RECEIPT_BEGIN)
    end = markdown.find(RECEIPT_END)
    if start < 0 or end < 0 or end < start:
        raise ContractError("committed receipt lacks its begin/end markers")
    block = markdown[start + len(RECEIPT_BEGIN) : end]
    opening = block.find("```json")
    closing = block.rfind("```")
    if opening < 0 or closing <= opening:
        raise ContractError("committed receipt lacks its fenced JSON block")
    return json.loads(block[opening + len("```json") : closing])


def receipt_failures(document: dict, root: Path = ROOT) -> list[str]:
    """Return failures comparing the committed receipt with the live graph."""
    path = root / "evidence" / "domain-graph.md"
    try:
        committed = extract_receipt(path.read_text(encoding="utf-8"))
    except (OSError, ContractError, json.JSONDecodeError) as error:
        return [f"committed domain-graph receipt is unreadable: {error}"]
    if committed != document:
        return [
            "committed domain-graph receipt is stale; "
            "regenerate with `python3 scripts/domain_contract.py --emit`"
        ]
    return []


def main(argv: list[str]) -> int:
    """Emit the receipt, or enforce the graph and the committed receipt."""
    emit = "--emit" in argv
    failures, document = enforce()
    if emit:
        RECEIPT_PATH.write_text(render_receipt(document), encoding="utf-8")
        print(f"wrote {RECEIPT_PATH.relative_to(ROOT)}")
        return 0
    failures += receipt_failures(document)
    print(f"domain_owners={len(document['owners'])}")
    print(f"domain_edges={len(document['edges'])}")
    print(f"domain_failures={len(failures)}")
    for failure in failures:
        print(f"FAIL domain: {failure}")
    if failures:
        return 1
    print("PASS domain ownership and allowed-edge contract")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))