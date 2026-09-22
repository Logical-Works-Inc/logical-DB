#!/usr/bin/env python3
"""Validate and render the source-anchored execution-access registry for #25.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
`evidence/SOURCES.md`.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GRAPH_PATH = ROOT / "contract" / "access-graph.toml"
KINDS = {"admission", "commit", "http", "migration", "recovery", "replication", "synchronization"}


class AccessGraphError(RuntimeError):
    """Raised when the execution-access registry is not closed and auditable."""


def _strings(record: dict, key: str, location: str, *, allow_empty: bool = False) -> list[str]:
    values = record.get(key)
    if not isinstance(values, list) or (not values and not allow_empty) or not all(isinstance(value, str) and value for value in values):
        qualifier = "a string list" if allow_empty else "a non-empty string list"
        raise AccessGraphError(f"{location}.{key} must be {qualifier}")
    return values


def _required(record: dict, key: str, location: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value:
        raise AccessGraphError(f"{location}.{key} is required")
    return value


def _source_anchor(source: str, location: str) -> None:
    path_text, separator, marker = source.partition("#")
    if not separator or not path_text or not marker:
        raise AccessGraphError(f"{location}.source must be path#anchor")
    path = ROOT / path_text
    try:
        contents = path.read_text(encoding="utf-8")
    except OSError as error:
        raise AccessGraphError(f"{location}.source cannot read {path_text}: {error}") from error
    if marker not in contents:
        raise AccessGraphError(f"{location}.source anchor is absent: {source}")


def _acyclic(systems: dict[str, dict]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visiting:
            raise AccessGraphError(f"causal predecessor cycle includes {name}")
        if name in visited:
            return
        visiting.add(name)
        for predecessor in systems[name]["predecessors"]:
            visit(predecessor)
        visiting.remove(name)
        visited.add(name)

    for name in systems:
        visit(name)


def load(path: Path = GRAPH_PATH) -> dict:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise AccessGraphError(f"cannot read access graph {path}: {error}") from error
    if document.get("version") != 2:
        raise AccessGraphError("access graph version must be 2")
    required_sources = _strings(document, "required_source", "document")
    if len(set(required_sources)) != len(required_sources):
        raise AccessGraphError("document.required_source must not contain duplicates")
    resources = document.get("resource", [])
    systems = document.get("system", [])
    if not isinstance(resources, list) or not resources or not isinstance(systems, list) or not systems:
        raise AccessGraphError("access graph needs resource and system records")
    names: set[str] = set()
    for index, resource in enumerate(resources):
        location = f"resource[{index}]"
        if not isinstance(resource, dict):
            raise AccessGraphError(f"{location} must be a table")
        name = _required(resource, "name", location)
        if name in names:
            raise AccessGraphError(f"{location}.name must be unique")
        _required(resource, "owner", location)
        if resource.get("durability") not in {"durable", "ephemeral"}:
            raise AccessGraphError(f"{location}.durability must be durable or ephemeral")
        names.add(name)
    systems_by_name: dict[str, dict] = {}
    for index, system in enumerate(systems):
        location = f"system[{index}]"
        if not isinstance(system, dict):
            raise AccessGraphError(f"{location} must be a table")
        name = _required(system, "name", location)
        if name in systems_by_name:
            raise AccessGraphError(f"{location}.name must be unique")
        _required(system, "owner", location)
        _source_anchor(_required(system, "source", location), location)
        if system.get("kind") not in KINDS:
            raise AccessGraphError(f"{location}.kind must be a declared transition kind")
        reads = _strings(system, "reads", location)
        writes = _strings(system, "writes", location)
        if len(set(reads)) != len(reads) or len(set(writes)) != len(writes):
            raise AccessGraphError(f"{location} resource lists must not contain duplicates")
        for resource in reads + writes:
            if resource not in names:
                raise AccessGraphError(f"{location} references unknown resource {resource!r}")
        for field in ("receipt", "limit", "atomicity", "failure"):
            value = _required(system, field, location)
            if field == "limit" and "unbounded" in value.lower():
                raise AccessGraphError(f"{location}.limit must declare a bound")
        system["predecessors"] = _strings(system, "predecessors", location, allow_empty=True)
        systems_by_name[name] = system
    for name, system in systems_by_name.items():
        for predecessor in system["predecessors"]:
            if predecessor not in systems_by_name:
                raise AccessGraphError(f"system {name} has unknown predecessor {predecessor!r}")
    _acyclic(systems_by_name)
    declared_sources = {system["source"] for system in systems}
    required_sources = set(required_sources)
    missing_sources = sorted(required_sources - declared_sources)
    unexpected_sources = sorted(declared_sources - required_sources)
    if missing_sources or unexpected_sources:
        details = []
        if missing_sources:
            details.append(f"missing {missing_sources}")
        if unexpected_sources:
            details.append(f"unexpected {unexpected_sources}")
        raise AccessGraphError(f"source inventory mismatch: {'; '.join(details)}")
    writers = {resource: sorted(system["name"] for system in systems if resource in system["writes"]) for resource in sorted(names)}
    missing = [resource for resource, writers_for_resource in writers.items() if not writers_for_resource]
    if missing:
        raise AccessGraphError(f"resources without a declared writer: {', '.join(missing)}")
    return document


def render(path: Path = GRAPH_PATH) -> str:
    document = load(path)
    resources = {record["name"]: record for record in document["resource"]}
    writers = {resource: sorted(system["name"] for system in document["system"] if resource in system["writes"])
               for resource in sorted(resources)}
    return json.dumps({"resources": resources, "systems": document["system"], "version": 2, "writers": writers}, indent=2, sort_keys=True) + "\n"


def render_dot(path: Path = GRAPH_PATH) -> str:
    document = load(path)
    lines = ["digraph access_graph {", "  rankdir=LR;"]
    for system in sorted(document["system"], key=lambda item: item["name"]):
        lines.append(f'  "{system["name"]}" [shape=box];')
        for predecessor in sorted(system["predecessors"]):
            lines.append(f'  "{predecessor}" -> "{system["name"]}" [label="receipt"];')
        for resource in sorted(system["reads"]):
            lines.append(f'  "{resource}" -> "{system["name"]}" [label="read"];')
        for resource in sorted(system["writes"]):
            lines.append(f'  "{system["name"]}" -> "{resource}" [label="write"];')
    for resource in sorted(document["resource"], key=lambda item: item["name"]):
        lines.append(f'  "{resource["name"]}" [shape=ellipse];')
    lines.append("}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    print(render(), end="")
