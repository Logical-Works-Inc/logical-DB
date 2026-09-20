#!/usr/bin/env python3
"""Export Cargo's complete test inventory and execute it in the x86 system VM.

Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. See evidence/SOURCES.md.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path("/workspace")


def relative(path):
    path = Path(path)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"noncanonical artifact path: {path}")
    return str(path.relative_to(ROOT))


def inventory(events, packages):
    manifests = {p["id"]: p["manifest_path"] for p in packages}
    tests = []
    seen = set()
    if [e.get("success") for e in events if e.get("reason") == "build-finished"] != [True]:
        raise ValueError("Cargo did not report a successful build")
    for event in events:
        if event.get("reason") != "compiler-artifact" or not event.get("profile", {}).get("test"):
            continue
        executable = event.get("executable")
        if not executable:
            continue
        executable = relative(executable)
        if not executable.startswith("target/") or not executable.endswith(".exe"):
            raise ValueError("expected a Windows test executable under target/")
        if executable in seen:
            raise ValueError("duplicate test executable")
        seen.add(executable)
        manifest = relative(manifests[event["package_id"]])
        tests.append({"executable": executable, "manifest": manifest,
                      "name": event["target"]["name"]})
    if not tests or sum(t["name"] == "dependency_contract" for t in tests) != 1:
        raise ValueError("incomplete test inventory or missing dependency contract")
    return tests


def executables(events):
    # Integration tests can spawn Cargo-built binaries via CARGO_BIN_EXE_*.
    # Those are not test harnesses, but must travel with the test executables.
    paths = []
    for event in events:
        if event.get("reason") != "compiler-artifact" or not event.get("executable"):
            continue
        path = relative(event["executable"])
        if not path.startswith("target/") or not path.endswith(".exe") or path in paths:
            raise ValueError("invalid or duplicate Windows executable artifact")
        paths.append(path)
    return paths


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def transfer_growth(root, sizes):
    growth = 0
    for name, size in sizes.items():
        if relative(ROOT / name) != name:
            raise ValueError("artifact size path must be relative")
        if not isinstance(size, int) or size < 0:
            raise ValueError("invalid artifact size")
        target = root / name
        allocated = target.stat().st_blocks * 512 if target.is_file() else 0
        growth += max(0, ((size + 4095) // 4096) * 4096 - allocated)
    return growth


def within_budget(used, growth, available):
    return used + growth <= 12 * 1024**3 and available >= growth + 1024**3


def reuse_artifacts(root, manifest, base):
    # Preserve each revision's complete immutable view without duplicating
    # identical executable bytes. Transfer uses --skip-old-files afterwards.
    transfer_growth(root, manifest["sizes"])
    previous = root / ".ci-windows/tests.json"
    if previous.exists() and json.loads(previous.read_text()) != manifest:
        raise ValueError("retained revision has a different artifact manifest")
    candidates = []
    for directory in sorted(base.iterdir()):
        if directory == root or directory.is_symlink() or not directory.is_dir():
            continue
        receipt = directory / ".ci-windows/tests.json"
        if receipt.is_file():
            candidates.append((directory, json.loads(receipt.read_text())["sha256"]))
    reused = 0
    for name, expected in manifest["sha256"].items():
        target = root / name
        if target.resolve() != target:
            raise ValueError("artifact destination contains a symlink")
        if target.exists():
            if not target.is_file() or digest(target) != expected:
                raise ValueError("retained artifact differs from incoming checksum")
            continue
        for directory, hashes in candidates:
            source = directory / name
            if hashes.get(name) != expected or source.resolve() != source:
                continue
            if source.is_file() and digest(source) == expected:
                target.parent.mkdir(parents=True, exist_ok=True)
                os.link(source, target)
                reused += manifest["sizes"][name]
                break
    return reused


def plan_transfer():
    manifest = json.load(sys.stdin)
    if set(manifest["sizes"]) != set(manifest["sha256"]):
        raise ValueError("size inventory differs from checksummed artifacts")
    root = Path(sys.argv[2])
    base = Path("/var/lib/logical-db-ci")
    if root.parent != base or len(root.name) != 40 or any(c not in "0123456789abcdef" for c in root.name):
        raise ValueError("expected an owned source-tree directory")
    reused = reuse_artifacts(root, manifest, base)
    used = int(subprocess.check_output(["du", "-sk", str(base)], text=True).split()[0]) * 1024
    growth = transfer_growth(root, manifest["sizes"]) + int(sys.argv[3]) * 1024 + 1024**2
    available = shutil.disk_usage(base).free
    print(f"TRANSFER reused_bytes={reused} used_bytes={used} growth_bytes={growth} available_bytes={available}", flush=True)
    if not within_budget(used, growth, available):
        raise SystemExit("Windows VM storage budget reached; recoverable maintenance required")


def verify_transfer(root, manifest):
    tests = manifest["tests"]
    if not tests or sum(t["name"] == "dependency_contract" for t in tests) != 1:
        raise ValueError("runtime inventory lost its dependency gate")
    executables = [t["executable"] for t in tests]
    if len(set(executables)) != len(executables):
        raise ValueError("duplicate runtime executable")
    if digest(root / "Cargo.lock") != manifest["lock_sha256"]:
        raise ValueError("source lockfile differs from compiled inventory")
    for test in tests:
        relative(ROOT / test["manifest"])
        if test["executable"] not in manifest["sha256"]:
            raise ValueError("test executable has no transfer checksum")
    for name, expected in manifest["sha256"].items():
        relative(ROOT / name)
        if digest(root / name) != expected:
            raise ValueError(f"transferred artifact hash mismatch: {name}")


def export():
    events = [json.loads(line) for line in Path("/tmp/ci-test-artifacts.jsonl").read_text().splitlines()]
    packages = json.loads(Path("/tmp/ci-cargo-metadata.json").read_text())["packages"]
    tests = inventory(events, packages)
    output = Path("/tmp/ci-windows-runtime")
    control = output / ".ci-windows"
    control.mkdir(parents=True, exist_ok=True)
    artifacts = executables(events)
    for executable in artifacts:
        target = output / executable
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / executable, target)
    sdk = Path("/root/.rustup/toolchains/1.98.0-x86_64-pc-windows-gnu/bin")
    shutil.copytree(sdk, control / "sdk", dirs_exist_ok=True)
    dlls = control / "dlls"
    dlls.mkdir(exist_ok=True)
    for name in ("libwinpthread-1.dll", "libstdc++-6.dll", "libgcc_s_seh-1.dll"):
        source = subprocess.check_output(
            ["x86_64-w64-mingw32-g++-posix", f"-print-file-name={name}"], text=True).strip()
        shutil.copy2(source, dlls / name)
    shutil.copy2("/tmp/ci-windows-smoke.exe", control / "smoke.exe")
    hashes = {str(p.relative_to(output)): digest(p) for p in output.rglob("*")
              if p.is_file() and p != control / "tests.json"}
    (control / "tests.json").write_text(json.dumps({"tests": tests, "sha256": hashes,
        "sizes": {name: (output / name).stat().st_size for name in hashes},
        "artifact_bytes": sum((output / name).stat().st_size for name in hashes),
        "lock_sha256": digest(ROOT / "Cargo.lock")}, indent=2) + "\n")
    print(f"EXPORTED {len(tests)} Cargo-discovered Windows test executables and {len(artifacts) - len(tests)} support binaries")


def run():
    control = ROOT / ".ci-windows"
    manifest = json.loads((control / "tests.json").read_text())
    verify_transfer(ROOT, manifest)
    env = dict(os.environ, WINEDEBUG="-fixme", RUST_BACKTRACE="1", RUST_TEST_THREADS="4",
               WINEPATH=f"{control / 'sdk'};{control / 'dlls'}")
    # Wine's default three-second server teardown can race the next harness.
    # Keep this owned session alive between binaries; the VM trap ends it.
    subprocess.run(["/usr/lib/x86_64-linux-gnu/wine/wineserver", "--persistent=60"],
                   check=True, env=env, timeout=30)
    subprocess.run(["wine", "--version"], check=True, env=env)
    subprocess.run(["wine", str(control / "smoke.exe")], check=True, env=env, timeout=180)
    tests = manifest["tests"]
    # Preserve the original separately invoked dependency gate, then workspace tests.
    selected = [t for t in tests if t["name"] == "dependency_contract"] + tests
    failures = 0
    for test in selected:
        cwd = (ROOT / test["manifest"]).parent
        print(f"RUN Windows {test['name']} cwd={cwd}", flush=True)
        try:
            subprocess.run(["wine", str(ROOT / test["executable"])], cwd=cwd,
                           env=env, check=True, timeout=300)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            print(f"FAIL Windows {test['name']}: {error}", flush=True)
            failures += 1
    print(f"WINDOWS runtime executables={len(selected)} failures={failures}", flush=True)
    return int(failures != 0)


class InventoryTests(unittest.TestCase):
    def fixture(self):
        return [{"reason": "compiler-artifact", "profile": {"test": True}, "package_id": "p",
                 "executable": "/workspace/target/dependency.exe", "target": {"name": "dependency_contract"}},
                {"reason": "build-finished", "success": True}], [{"id": "p", "manifest_path": "/workspace/crates/p/Cargo.toml"}]

    def test_complete_inventory(self):
        events, packages = self.fixture()
        self.assertEqual(inventory(events, packages)[0]["manifest"], "crates/p/Cargo.toml")

    def test_support_binaries_travel_without_becoming_test_harnesses(self):
        events, packages = self.fixture()
        events.insert(1, {"reason": "compiler-artifact", "profile": {"test": False},
                          "executable": "/workspace/target/crash_writer.exe"})
        self.assertEqual(len(inventory(events, packages)), 1)
        self.assertEqual(executables(events), ["target/dependency.exe", "target/crash_writer.exe"])
        with self.assertRaises(ValueError):
            executables(events + [events[1]])

    def test_failed_build_empty_or_duplicate_inventory(self):
        events, packages = self.fixture()
        for bad in (events[:-1], events[1:], [events[0]] + events):
            with self.assertRaises(ValueError):
                inventory(bad, packages)

    def test_host_binary_or_path_escape(self):
        for path in ("/workspace/target/test", "/tmp/test.exe", "/workspace/target/../../test.exe"):
            events, packages = self.fixture()
            events[0]["executable"] = path
            with self.assertRaises(ValueError):
                inventory(events, packages)

    def test_transfer_tampering_and_empty_runtime_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "Cargo.lock").write_text("locked")
            (root / "test.exe").write_bytes(b"compiled fixture")
            manifest = {"tests": [{"name": "dependency_contract", "executable": "test.exe",
                                   "manifest": "Cargo.toml"}],
                        "lock_sha256": digest(root / "Cargo.lock"),
                        "sha256": {"test.exe": digest(root / "test.exe")}}
            verify_transfer(root, manifest)
            (root / "test.exe").write_bytes(b"corrupt transfer")
            with self.assertRaises(ValueError):
                verify_transfer(root, manifest)
            manifest["tests"] = []
            with self.assertRaises(ValueError):
                verify_transfer(root, manifest)

    def test_repeated_transfer_charges_growth_not_the_whole_bundle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "test.exe").write_bytes(b"compiled fixture")
            allocated = (root / "test.exe").stat().st_blocks * 512
            self.assertEqual(transfer_growth(root, {"test.exe": 16}), max(0, 4096 - allocated))
            self.assertEqual(transfer_growth(root, {"new.exe": 4097}), 8192)
            self.assertEqual(transfer_growth(root, {"test.exe": allocated + 4096}), 4096)
            for sizes in ({"../escape": 1}, {"/workspace/test.exe": 1}, {"test.exe": -1}):
                with self.assertRaises(ValueError):
                    transfer_growth(root, sizes)

    def test_budget_refuses_overflow_and_preserves_reserve(self):
        gib = 1024**3
        self.assertTrue(within_budget(11 * gib, gib, 2 * gib))
        self.assertFalse(within_budget(11 * gib, gib + 1, 3 * gib))
        self.assertFalse(within_budget(0, gib, 2 * gib - 1))

    def test_unchanged_artifacts_share_blocks_without_replacing_old_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            old, new = base / "old", base / "new"
            (old / ".ci-windows").mkdir(parents=True)
            (old / "test.exe").write_bytes(b"compiled fixture")
            manifest = {"sha256": {"test.exe": digest(old / "test.exe")},
                        "sizes": {"test.exe": 16}}
            (old / ".ci-windows/tests.json").write_text(json.dumps(manifest))
            self.assertEqual(reuse_artifacts(new, manifest, base), 16)
            self.assertTrue((old / "test.exe").samefile(new / "test.exe"))
            self.assertEqual(reuse_artifacts(new, manifest, base), 0)
            self.assertEqual(transfer_growth(new, manifest["sizes"]), 0)
            changed = {"sha256": {"test.exe": "different"}, "sizes": {"test.exe": 16}}
            self.assertEqual(reuse_artifacts(base / "changed", changed, base), 0)
            self.assertFalse((base / "changed/test.exe").exists())
            self.assertEqual((old / "test.exe").read_bytes(), b"compiled fixture")
            with self.assertRaises(ValueError):
                reuse_artifacts(new, changed, base)

    def test_reuse_rejects_corruption_and_destination_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory).resolve()
            old, new = base / "old", base / "new"
            (old / ".ci-windows").mkdir(parents=True)
            (old / "test.exe").write_bytes(b"corrupt")
            manifest = {"sha256": {"test.exe": "not-the-file-hash"}, "sizes": {"test.exe": 7}}
            (old / ".ci-windows/tests.json").write_text(json.dumps(manifest))
            self.assertEqual(reuse_artifacts(new, manifest, base), 0)
            new.symlink_to(old, target_is_directory=True)
            with self.assertRaises(ValueError):
                reuse_artifacts(new, manifest, base)


if __name__ == "__main__":
    action = sys.argv[1]
    if action == "export":
        export()
    elif action == "run":
        sys.exit(run())
    elif action == "plan":
        plan_transfer()
    elif action == "test":
        result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(InventoryTests))
        sys.exit(not result.wasSuccessful())
    else:
        raise SystemExit("expected export, run, plan, or test")
