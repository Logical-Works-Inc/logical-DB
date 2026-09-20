#!/usr/bin/env python3
"""Validate and render the checked execution-access registry for issue #25.

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


class AccessGraphError(RuntimeError):
    """Raised when the execution-access registry is not closed and unambiguous."""


def _strings(record: dict, key: str, location: str) -> list[str]:
    values = record.get(key)
    if not isinstance(values, list) or not values or not all(isinstance(v, str) and v for v in values):
        raise AccessGraphError(f"{location}.{key} must be a non-empty string list")
    return values


def load(path: Path = GRAPH_PATH) -> dict:
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise AccessGraphError(f"cannot read access graph {path}: {error}") from error
    if document.get("version") != 1:
        raise AccessGraphError("access graph version must be 1")
    resources = document.get("resource", [])
    systems = document.get("system", [])
    if not isinstance(resources, list) or not resources or not isinstance(systems, list) or not systems:
        raise AccessGraphError("access graph needs resource and system records")
    resource_names: set[str] = set()
    for index, resource in enumerate(resources):
        location = f"resource[{index}]"
        if not isinstance(resource, dict):
            raise AccessGraphError(f"{location} must be a table")
        name = resource.get("name")
        if not isinstance(name, str) or not name or name in resource_names:
            raise AccessGraphError(f"{location}.name must be unique and non-empty")
        if not isinstance(resource.get("owner"), str) or not resource["owner"]:
            raise AccessGraphError(f"{location}.owner is required")
        if resource.get("durability") not in {"durable", "ephemeral"}:
            raise AccessGraphError(f"{location}.durability must be durable or ephemeral")
        resource_names.add(name)
    writers: dict[str, list[str]] = {name: [] for name in resource_names}
    system_names: set[str] = set()
    for index, system in enumerate(systems):
        location = f"system[{index}]"
        if not isinstance(system, dict):
            raise AccessGraphError(f"{location} must be a table")
        name = system.get("name")
        if not isinstance(name, str) or not name or name in system_names:
            raise AccessGraphError(f"{location}.name must be unique and non-empty")
        system_names.add(name)
        if not isinstance(system.get("owner"), str) or not system["owner"]:
            raise AccessGraphError(f"{location}.owner is required")
        reads = _strings(system, "reads", location)
        writes = _strings(system, "writes", location)
        if len(set(reads)) != len(reads):
            raise AccessGraphError(f"{location}.reads must not contain duplicate resources")
        if len(set(writes)) != len(writes):
            raise AccessGraphError(f"{location}.writes must not contain duplicate resources")
        for resource in reads + writes:
            if resource not in resource_names:
                raise AccessGraphError(f"{location} references unknown resource {resource!r}")
        if not isinstance(system.get("receipt"), str) or not system["receipt"]:
            raise AccessGraphError(f"{location}.receipt is required")
        if not isinstance(system.get("continuation"), str) or not system["continuation"]:
            raise AccessGraphError(f"{location}.continuation is required")
        for resource in writes:
            writers[resource].append(name)
    missing = sorted(name for name, owners in writers.items() if not owners)
    multiple = {name: owners for name, owners in writers.items() if len(owners) > 1}
    if missing:
        raise AccessGraphError(f"resources without a declared writer: {', '.join(missing)}")
    if multiple:
        raise AccessGraphError(f"resources with multiple writers: {multiple}")
    return document


def render(path: Path = GRAPH_PATH) -> str:
    document = load(path)
    resources = {record["name"]: record for record in document["resource"]}
    writers = {resource: [] for resource in resources}
    for system in document["system"]:
        for resource in system["writes"]:
            writers[resource].append(system["name"])
    return json.dumps({"version": document["version"], "resources": resources, "writers": writers,
                       "systems": document["system"]}, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    print(render(), end="")
