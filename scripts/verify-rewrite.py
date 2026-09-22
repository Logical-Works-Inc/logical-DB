#!/usr/bin/env python3
"""Fail until the clean-room rewrite satisfies its mechanical evidence gates.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
evidence/SOURCES.md.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import domain_contract  # noqa: E402

BASELINE = "7cffbc07b6f312854986ae306b583ef891623b2d"
MINIMUM_CITATIONS = 20
MINIMUM_HASH_CHURN = 85.0
SOURCE_PATTERN = re.compile(r"\bS\d{2}\b")
SOURCE_DECLARATION_PATTERN = re.compile(r"^- \*\*(S\d{2}) — [^*]+\*\*", re.MULTILINE)
URL_PATTERN = re.compile(r"<https://[^>]+>")
EXPECTED_SOURCE_IDS = {f"S{number:02d}" for number in range(1, 44)}
MAX_INSPECTABLE_BYTES = 16 * 1_048_576
CITATION_HEADER_BYTES = 4_096
ROOT = Path(__file__).resolve().parents[1]
ALLOWED_INTERNAL_DEPENDENCIES = {
    "logical-db-model": set(),
    "logical-db-platform": set(),
    "logical-db-protocol": {"logical-db-model"},
    "logical-db-crdt": {"logical-db-model"},
    "logical-db-store": {"logical-db-model"},
    "logical-db-sync": {"logical-db-model", "logical-db-protocol"},
    "logical-db-service": {
        "logical-db-crdt",
        "logical-db-model",
        "logical-db-protocol",
    },
    "logical-db-store-fdb": {
        "logical-db-model",
        "logical-db-protocol",
        "logical-db-service",
    },
    "logical-db-http": {"logical-db-model", "logical-db-protocol", "logical-db-sync"},
    "logical-db-server": {
        "logical-db-crdt",
        "logical-db-model",
        "logical-db-protocol",
        "logical-db-service",
    },
    "logical-db-e2e": {
        "logical-db-cairn",
        "logical-db-episode",
        "logical-db",
        "logical-db-http",
        "logical-db-model",
        "logical-db-protocol",
        "logical-db-server",
        "logical-db-service",
        "logical-db-store-fdb",
    },
    "logical-db-daemon": {
        "logical-db-crdt",
        "logical-db-model",
        "logical-db-protocol",
        "logical-db-server",
        "logical-db-service",
        "logical-db-store-fdb",
    },
    "logical-db": {
        "logical-db-cairn",
        "logical-db-episode",
        "logical-db-model",
        "logical-db-crdt",
        "logical-db-http",
        "logical-db-platform",
        "logical-db-protocol",
        "logical-db-store",
        "logical-db-sync",
    },
    "logical-db-kv": {"logical-db", "logical-db-model"},
    "logical-db-cairn": set(),
    "logical-db-episode": {"logical-db-cairn"},
    "logical-db-cloud-contract": {"logical-db-model"},
}


def git(*arguments: str, check: bool = True, stdin: bytes | None = None) -> bytes:
    """Run Git at the repository root and return exact standard output."""
    process = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=False,
        input=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and process.returncode != 0:
        raise RuntimeError(process.stderr.decode("utf-8", "replace"))
    return process.stdout


def repository_files() -> list[Path]:
    """Return tracked and new files while excluding ignored build products."""
    output = git("ls-files", "--cached", "--others", "--exclude-standard", "-z")
    files = [ROOT / os.fsdecode(name) for name in output.split(b"\0") if name]
    return [path for path in files if path.exists() or path.is_symlink()]


def source_ids() -> set[str]:
    """Return the exact, uniquely declared source identifiers in the ledger."""
    ledger = (ROOT / "evidence/SOURCES.md").read_text(encoding="utf-8")
    declarations = list(SOURCE_DECLARATION_PATTERN.finditer(ledger))
    declared = [match.group(1) for match in declarations]
    if len(declared) != len(set(declared)):
        raise RuntimeError("source ledger contains duplicate declarations")
    if set(declared) != EXPECTED_SOURCE_IDS:
        missing = sorted(EXPECTED_SOURCE_IDS - set(declared))
        extra = sorted(set(declared) - EXPECTED_SOURCE_IDS)
        raise RuntimeError(
            f"source ledger declarations differ from the pinned set; "
            f"missing={' '.join(missing)} extra={' '.join(extra)}"
        )
    urls: set[str] = set()
    for index, declaration in enumerate(declarations):
        end = declarations[index + 1].start() if index + 1 < len(declarations) else len(ledger)
        entry_urls = URL_PATTERN.findall(ledger[declaration.start() : end])
        if len(entry_urls) != 1:
            raise RuntimeError(
                f"source {declaration.group(1)} must declare exactly one HTTPS URL"
            )
        if entry_urls[0] in urls:
            raise RuntimeError(f"source ledger repeats URL {entry_urls[0]}")
        urls.add(entry_urls[0])
    return set(declared)


def citation_failures(files: list[Path], allowed: set[str]) -> list[str]:
    """Return every text file lacking twenty distinct valid source IDs."""
    failures: list[str] = []
    for path in files:
        relative = path.relative_to(ROOT)
        inspected = path
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            failures.append(f"repository symlink cannot carry independent citations: {relative}")
            continue
        if not stat.S_ISREG(metadata.st_mode):
            failures.append(f"non-regular repository entry: {relative}")
            continue
        if metadata.st_size > MAX_INSPECTABLE_BYTES:
            failures.append(
                f"file exceeds {MAX_INSPECTABLE_BYTES} byte verifier ceiling: {relative}"
            )
            continue
        payload = inspected.read_bytes()
        if b"\0" in payload:
            failures.append(f"binary file has no inspectable citations: {relative}")
            continue
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            failures.append(f"non-UTF-8 file has no inspectable citations: {relative}")
            continue
        found = set(SOURCE_PATTERN.findall(text))
        unknown = sorted(found - allowed)
        if unknown:
            failures.append(
                f"unknown source IDs in {relative}: {' '.join(unknown)}"
            )
        if relative == Path("evidence/SOURCES.md"):
            valid = found & allowed
        else:
            # The 4096-byte ceiling is a byte bound, so a multibyte character
            # can straddle it. Decode the prefix leniently rather than crashing
            # on a boundary split; the header check is unaffected.
            header = payload[:CITATION_HEADER_BYTES].decode("utf-8", "ignore")
            if "Evidence" not in header:
                failures.append(f"missing explicit evidence header: {relative}")
            valid = set(SOURCE_PATTERN.findall(header)) & allowed
        if len(valid) < MINIMUM_CITATIONS:
            failures.append(
                f"{relative} cites {len(valid)}, requires {MINIMUM_CITATIONS}"
            )
    return failures


def baseline_entries() -> list[tuple[str, int, str]]:
    """Return baseline path, byte size, and blob identity triples."""
    output = git("ls-tree", "-rl", "-z", BASELINE)
    entries: list[tuple[str, int, str]] = []
    for record in output.split(b"\0"):
        if not record:
            continue
        metadata, encoded_path = record.split(b"\t", 1)
        _mode, object_type, object_id, encoded_size = metadata.decode().split()
        if object_type == "blob":
            entries.append((os.fsdecode(encoded_path), int(encoded_size), object_id))
    return entries


def current_blob(path: str) -> str | None:
    """Return the working-tree blob hash, or none when the path was deleted."""
    candidate = ROOT / path
    if not candidate.exists() and not candidate.is_symlink():
        return None
    if candidate.is_symlink():
        target = os.fsencode(os.readlink(candidate))
        return git("hash-object", "--stdin", stdin=target).decode().strip()
    if not candidate.is_file():
        return None
    return git("hash-object", f"--path={path}", "--", path).decode().strip()


def line_churn(entries: list[tuple[str, int, str]]) -> float:
    """Return the percentage of baseline text lines deleted or replaced."""
    baseline_lines: dict[str, int] = {}
    for path, _size, object_id in entries:
        payload = git("cat-file", "blob", object_id)
        if b"\0" in payload:
            continue
        baseline_lines[path] = payload.count(b"\n") + int(bool(payload) and not payload.endswith(b"\n"))

    deleted_lines: dict[str, int] = {}
    output = git("diff", "--numstat", "--no-renames", "-z", BASELINE, "--")
    for record in output.split(b"\0"):
        if not record:
            continue
        added, deleted, encoded_path = record.split(b"\t", 2)
        if added == b"-" or deleted == b"-":
            continue
        deleted_lines[os.fsdecode(encoded_path)] = int(deleted)

    total = sum(baseline_lines.values())
    changed = sum(
        min(lines, deleted_lines.get(path, 0)) for path, lines in baseline_lines.items()
    )
    return 100.0 * changed / total if total else 100.0


def dependency_failures() -> list[str]:
    """Reject inward or undeclared dependencies among the current v3 crates."""
    process = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.returncode != 0:
        return [f"cargo metadata failed: {process.stderr.decode('utf-8', 'replace')}"]
    metadata = json.loads(process.stdout)
    failures: list[str] = []
    present = {package["name"] for package in metadata["packages"]}
    missing = sorted(ALLOWED_INTERNAL_DEPENDENCIES.keys() - present)
    if missing:
        failures.append(f"required rewrite crates are missing: {' '.join(missing)}")
    unexpected = sorted(present - ALLOWED_INTERNAL_DEPENDENCIES.keys())
    if unexpected:
        failures.append(f"undeclared workspace crates are present: {' '.join(unexpected)}")
    for package in metadata["packages"]:
        name = package["name"]
        if name not in ALLOWED_INTERNAL_DEPENDENCIES:
            continue
        actual = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["name"] in ALLOWED_INTERNAL_DEPENDENCIES
        }
        forbidden = sorted(actual - ALLOWED_INTERNAL_DEPENDENCIES[name])
        if forbidden:
            failures.append(f"{name} has forbidden inward dependencies: {' '.join(forbidden)}")
    return failures


def domain_contract_failures() -> list[str]:
    """Validate the four domain manifests and enforce the allowed-edge graph.

    The shape checks keep the descriptive records complete; the delegated
    `domain_contract` checks prove the live `cargo metadata` graph obeys the
    declared ownership and allowed edges, and that the committed
    `evidence/domain-graph.md` receipt still matches. This is a structure gate,
    not proof that the target crates or services exist.
    """
    required = {"cloud", "cairn", "lem", "forge"}
    directory = ROOT / "contract" / "domains"
    if not directory.is_dir():
        return ["missing contract/domains directory"]
    manifests = sorted(path for path in directory.glob("*.toml") if path.name != "GRAPH.toml")
    found: set[str] = set()
    failures: list[str] = []
    for path in manifests:
        try:
            parsed = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            failures.append(f"invalid domain manifest {path.relative_to(ROOT)}: {error}")
            continue
        domain = parsed.get("domain")
        if not isinstance(domain, str):
            failures.append(f"domain manifest lacks string domain: {path.relative_to(ROOT)}")
            continue
        if domain in found:
            failures.append(f"duplicate domain manifest declaration: {domain}")
        found.add(domain)
        for key in ("owner", "status", "release_lane"):
            if not isinstance(parsed.get(key), str) or not parsed[key]:
                failures.append(f"domain {domain} lacks nonempty {key}")
        for key in ("crates", "may_depend_on", "forbidden_domains", "evidence", "blocked_claims"):
            values = parsed.get(key)
            if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                failures.append(f"domain {domain} lacks string list {key}")
    missing = sorted(required - found)
    unexpected = sorted(found - required)
    if missing:
        failures.append(f"missing domain manifests: {' '.join(missing)}")
    if unexpected:
        failures.append(f"unexpected domain manifests: {' '.join(unexpected)}")
    try:
        edge_failures, document = domain_contract.enforce(ROOT)
    except domain_contract.ContractError as error:
        failures.append(f"domain contract is unreadable: {error}")
        return failures
    failures.extend(edge_failures)
    failures.extend(domain_contract.receipt_failures(document, ROOT))
    return failures


def churn() -> tuple[float, float, list[str]]:
    """Measure changed baseline blob count and changed baseline byte coverage."""
    entries = baseline_entries()
    unchanged: list[str] = []
    changed_bytes = 0
    total_bytes = 0
    for path, size, baseline_blob in entries:
        total_bytes += size
        if current_blob(path) == baseline_blob:
            unchanged.append(path)
        else:
            changed_bytes += size
    changed_count = len(entries) - len(unchanged)
    count_percent = 100.0 * changed_count / len(entries) if entries else 100.0
    byte_percent = 100.0 * changed_bytes / total_bytes if total_bytes else 100.0
    return count_percent, byte_percent, unchanged


def main() -> int:
    """Print deterministic evidence and return nonzero until every gate passes."""
    files = repository_files()
    try:
        allowed_sources = source_ids()
    except RuntimeError as error:
        print(f"FAIL source ledger: {error}")
        return 1
    citations = citation_failures(files, allowed_sources)
    dependencies = dependency_failures()
    domains = domain_contract_failures()
    entries = baseline_entries()
    count_percent, byte_percent, unchanged = churn()
    line_percent = line_churn(entries)

    print(f"baseline={BASELINE}")
    print(f"current_files={len(files)}")
    print(f"hash_churn_by_file={count_percent:.2f}%")
    print(f"hash_churn_by_baseline_bytes={byte_percent:.2f}%")
    print(f"line_churn_by_baseline_lines={line_percent:.2f}%")
    print(f"unchanged_baseline_blobs={len(unchanged)}")
    print(f"citation_failures={len(citations)}")
    print(f"dependency_failures={len(dependencies)}")
    print(f"domain_contract_failures={len(domains)}")

    for failure in citations:
        print(f"FAIL citation: {failure}")
    for failure in dependencies:
        print(f"FAIL dependency: {failure}")
    for failure in domains:
        print(f"FAIL domain contract: {failure}")
    for path in unchanged:
        print(f"FAIL unchanged: {path}")

    if citations or dependencies or domains or unchanged:
        return 1
    if (
        count_percent < MINIMUM_HASH_CHURN
        or byte_percent < MINIMUM_HASH_CHURN
        or line_percent < MINIMUM_HASH_CHURN
    ):
        print(f"FAIL hash and line churn must exceed {MINIMUM_HASH_CHURN:.0f}%")
        return 1
    print("PASS clean-room rewrite evidence gates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
