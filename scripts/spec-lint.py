#!/usr/bin/env python3
"""Enforce the 80-column prose contract over markdown sources.

The spec of record (docs/DB_PATHWAY.md) and every governance document are
prose artifacts; lines over 80 columns break diffs, review, and terminal
reading. Files whose first line carries a GENERATED marker are machine
views (REPO_MAP.md) and are exempt by design. Exit 0 when clean; exit 1
listing file:line:chars otherwise.
"""
import pathlib
import sys

LIMIT = 80
GENERATED = "GENERATED"


def check(paths):
    bad = []
    for p in paths:
        text = pathlib.Path(p).read_text(encoding="utf-8")
        if text.splitlines()[:1] == [] or GENERATED in text.splitlines()[0]:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            if len(line) > LIMIT:
                bad.append(f"{p}:{i}: {len(line)} chars")
    return bad


def expand(args):
    """Expand directory arguments into their markdown files; default to cwd."""
    if not args:
        args = [str(pathlib.Path.cwd())]
    out = []
    for a in args:
        p = pathlib.Path(a)
        if p.is_dir():
            out.extend(
                str(f) for f in sorted(p.rglob("*.md")) if ".git" not in f.parts
            )
        else:
            out.append(a)
    return out


def main():
    args = expand(sys.argv[1:])
    bad = check(args)
    if bad:
        print("\n".join(bad))
        return 1
    print(f"OK   all markdown within {LIMIT} columns ({len(args)} files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
