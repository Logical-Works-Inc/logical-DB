//! Local CI: the one definition of the gate.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! ```sh
//! cargo run -p logical-db-e2e --bin ci_local                 # full gate
//! cargo run -p logical-db-e2e --bin ci_local -- --fast       # compile + lint + fmt
//! cargo run -p logical-db-e2e --bin ci_local -- --receipt    # also write evidence/ci-local-<sha>.md
//! ```
//!
//! `.github/workflows/ci.yml` runs the same commands; a disagreement between
//! this gate and the workflow is a workflow defect. Every lane runs whatever
//! the lanes before it reported. Exit 0 means every lane passed on this tree;
//! exit 1 means at least one refused, and the table names which.

#![forbid(unsafe_code)]

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};

/// A gate failure that stops the gate itself, not a lane.
type Failure = Box<dyn std::error::Error>;

/// The gate was not started by cargo, so it has no cargo to drive.
#[derive(Debug)]
struct NotLaunchedByCargo(std::env::VarError);

impl std::fmt::Display for NotLaunchedByCargo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ci_local must be launched through `cargo run` (CARGO is unset)")
    }
}

impl std::error::Error for NotLaunchedByCargo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Paths that are derived build output and may never be staged.
const ARTIFACT_PREFIXES: [&str; 5] = [
    "target/",
    "graphify-out/",
    ".lgwks/",
    "node_modules/",
    ".codegraph/",
];
/// Lines from a suppression attribute in which its `reason` must appear.
const REASON_WINDOW: usize = 7;

/// How one lane ended.
enum Outcome {
    /// Every command in the lane exited zero.
    Pass,
    /// The lane refused, with the exit or finding count it reported.
    Fail(String),
    /// The lane's tool is absent on this host.
    Skip(&'static str),
}

/// One recorded lane.
struct Lane {
    /// Gate name in the receipt.
    name: &'static str,
    /// How it ended.
    outcome: Outcome,
    /// UTC start, `YYYY-MM-DDTHH:MM:SSZ`.
    started: String,
    /// UTC end.
    ended: String,
}

/// The gate's run state: repository, cargo, and every lane so far.
struct Gate {
    /// Workspace root.
    root: PathBuf,
    /// The cargo that launched this gate.
    cargo: String,
    /// Lanes in run order.
    lanes: Vec<Lane>,
}

/// The current UTC time from the host clock, for receipts only.
fn timestamp() -> Result<String, Failure> {
    let output = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()?;
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// Writes `text` to standard output.
fn say(text: &str) -> Result<(), Failure> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(text.as_bytes())?;
    stdout.flush()?;
    Ok(())
}

impl Gate {
    /// Records one lane whose outcome `body` decides.
    fn lane(
        &mut self,
        name: &'static str,
        body: impl FnOnce(&Self) -> Result<Outcome, Failure>,
    ) -> Result<(), Failure> {
        say(&format!("\n==> {name}\n"))?;
        let started = timestamp()?;
        let outcome = body(self)?;
        let ended = timestamp()?;
        say(&match outcome {
            Outcome::Pass => format!("<== {name}: pass\n"),
            Outcome::Fail(ref reason) => format!("<== {name}: FAIL ({reason})\n"),
            Outcome::Skip(reason) => format!("<== {name}: SKIP ({reason})\n"),
        })?;
        self.lanes.push(Lane {
            name,
            outcome,
            started,
            ended,
        });
        Ok(())
    }

    /// Runs `program` with `arguments` at the root, output inherited.
    fn command(&self, program: &str, arguments: &[&str]) -> Result<Outcome, Failure> {
        let status = Command::new(program)
            .args(arguments)
            .current_dir(&self.root)
            .env("CARGO_INCREMENTAL", "0")
            .env("CARGO_TERM_COLOR", "never")
            .stdin(Stdio::null())
            .status()?;
        Ok(match status.code() {
            Some(0) => Outcome::Pass,
            Some(code) => Outcome::Fail(format!("exit {code}")),
            None => Outcome::Fail(String::from("terminated by a signal")),
        })
    }

    /// Runs this gate's cargo with `arguments`.
    fn cargo(&self, arguments: &[&str]) -> Result<Outcome, Failure> {
        self.command(&self.cargo, arguments)
    }

    /// Runs Git at the root and returns its stdout.
    fn git(&self, arguments: &[&str]) -> Result<String, Failure> {
        let output = Command::new("git")
            .args(arguments)
            .current_dir(&self.root)
            .output()?;
        if !output.status.success() {
            return Err(format!(
                "git {}: {}",
                arguments.join(" "),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        Ok(String::from_utf8(output.stdout)?)
    }
}

/// Every Rust source under `directory`, skipping build output.
fn rust_sources(directory: &Path, found: &mut Vec<PathBuf>) -> Result<(), Failure> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_dir() && entry.file_name() != "target" {
            rust_sources(&path, found)?;
        } else if kind.is_file() && path.extension().is_some_and(|extension| extension == "rs") {
            found.push(path);
        }
    }
    Ok(())
}

/// Whether `line` is an `#[allow(` / `#[expect(` attribute (inner or outer).
///
/// Line-anchored: a comment or string that mentions one is not a suppression.
fn is_suppression(line: &str) -> bool {
    let attribute = line.trim_start();
    let body = match attribute.strip_prefix("#![") {
        Some(body) => Some(body),
        None => attribute.strip_prefix("#["),
    };
    body.is_some_and(|body| body.starts_with("allow(") || body.starts_with("expect("))
}

/// The 1-based line of every suppression in `text` with no `reason` in its window.
fn reasonless(text: &str) -> Vec<usize> {
    let lines: Vec<&str> = text.lines().collect();
    let mut found = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if is_suppression(line)
            && !lines
                .iter()
                .skip(index)
                .take(REASON_WINDOW)
                .any(|candidate| candidate.contains("reason"))
        {
            found.push(index.saturating_add(1));
        }
    }
    found
}

/// The suppression lane: every attribute in `crates/` states its reason.
fn suppressions(gate: &Gate) -> Result<Outcome, Failure> {
    let mut sources = Vec::new();
    rust_sources(&gate.root.join("crates"), &mut sources)?;
    sources.sort();
    let mut findings = Vec::new();
    for path in &sources {
        let text = std::fs::read_to_string(path)?;
        for line in reasonless(&text) {
            findings.push(format!(
                "reasonless suppression: {}:{line}\n",
                path.display()
            ));
        }
    }
    say(&findings.concat())?;
    Ok(if findings.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail(format!("{} finding(s)", findings.len()))
    })
}

/// The artifact lane: no derived build output is staged.
fn artifacts(gate: &Gate) -> Result<Outcome, Failure> {
    let staged = gate.git(&["diff", "--cached", "--name-only"])?;
    let findings: Vec<String> = staged
        .lines()
        .filter(|path| {
            ARTIFACT_PREFIXES
                .iter()
                .any(|prefix| path.starts_with(prefix))
        })
        .map(|path| format!("derived build output staged: {path}\n"))
        .collect();
    say(&findings.concat())?;
    Ok(if findings.is_empty() {
        Outcome::Pass
    } else {
        Outcome::Fail(format!("{} finding(s)", findings.len()))
    })
}

/// The receipt table for every recorded lane.
fn table(lanes: &[Lane]) -> String {
    let rows: String = lanes
        .iter()
        .map(|lane| {
            let result = match lane.outcome {
                Outcome::Pass => String::from("pass"),
                Outcome::Fail(ref reason) => format!("**FAIL ({reason})**"),
                Outcome::Skip(reason) => format!("SKIP ({reason})"),
            };
            format!(
                "| {} | {result} | {} | {} |\n",
                lane.name, lane.started, lane.ended
            )
        })
        .collect();
    format!("| Gate | Result | Started | Ended |\n|---|---|---|---|\n{rows}")
}

/// Parses arguments, runs every lane, prints the table, and writes a receipt.
fn run() -> Result<ExitCode, Failure> {
    let (mut fast, mut receipt) = (false, false);
    for argument in std::env::args().skip(1) {
        match argument.as_str() {
            "--fast" => fast = true,
            "--receipt" => receipt = true,
            "-h" | "--help" => {
                say("cargo run -p logical-db-e2e --bin ci_local -- [--fast] [--receipt]\n")?;
                return Ok(ExitCode::SUCCESS);
            }
            unknown => {
                say(&format!("ci-local: unknown argument: {unknown}\n"))?;
                return Ok(ExitCode::from(2));
            }
        }
    }
    if !(cfg!(target_os = "macos") || cfg!(target_os = "linux")) {
        // The test lane rebuilds this binary while it runs, which Windows
        // refuses for a running executable; hosted CI runs Windows natively.
        say("ci-local: run the local gate on macOS or Linux\n")?;
        return Ok(ExitCode::from(2));
    }
    let cargo = std::env::var("CARGO").map_err(NotLaunchedByCargo)?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or("e2e crate lives two levels below the workspace root")?
        .to_path_buf();
    let mut gate = Gate {
        root,
        cargo,
        lanes: Vec::new(),
    };
    let sha = gate
        .git(&["rev-parse", "--short", "HEAD"])?
        .trim()
        .to_owned();
    let dirty = if gate
        .git(&["status", "--porcelain", "--untracked-files=no"])?
        .is_empty()
    {
        ""
    } else {
        " (dirty)"
    };
    let mode = if fast { "fast" } else { "full" };
    let (os, arch) = (std::env::consts::OS, std::env::consts::ARCH);
    say(&format!(
        "logical-DB local CI\n  head:    {sha}{dirty}\n  host:    {os} {arch}\n  mode:    {mode}\n  started: {}\n",
        timestamp()?,
    ))?;

    gate.lane("lockfile", |gate| {
        gate.cargo(&[
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--no-deps",
            "--quiet",
        ])
    })?;
    gate.lane("compile", |gate| {
        gate.cargo(&["check", "--workspace", "--all-targets", "--locked"])
    })?;
    gate.lane("lint", |gate| {
        gate.cargo(&[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ])
    })?;
    gate.lane("format", |gate| {
        gate.cargo(&["fmt", "--all", "--", "--check"])
    })?;
    if fast {
        say("\nfast mode: skipping tests, FDB, policy, contract, and evidence lanes\n")?;
    } else {
        gate.lane("tests", |gate| {
            gate.cargo(&["test", "--workspace", "--locked"])
        })?;
        // FDB lanes typecheck and lint without a cluster; the live-cluster
        // journeys run in the hosted `fdb-lane` composite action.
        gate.lane("fdb-check-store", |gate| {
            gate.cargo(&[
                "check",
                "-p",
                "logical-db-store-fdb",
                "--features",
                "fdb",
                "--all-targets",
                "--locked",
            ])
        })?;
        gate.lane("fdb-check-daemon", |gate| {
            gate.cargo(&[
                "check",
                "-p",
                "logical-db-daemon",
                "--features",
                "fdb",
                "--all-targets",
                "--locked",
            ])
        })?;
        gate.lane("fdb-lint-daemon", |gate| {
            gate.cargo(&[
                "clippy",
                "-p",
                "logical-db-daemon",
                "--features",
                "fdb",
                "--all-targets",
                "--locked",
                "--",
                "-D",
                "warnings",
            ])
        })?;
        gate.lane("deny", |gate| {
            let present = Command::new(&gate.cargo)
                .args(["deny", "--version"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            match present {
                Ok(status) if status.success() => gate.cargo(&["deny", "check"]),
                Ok(_) | Err(_) => Ok(Outcome::Skip("cargo-deny not installed")),
            }
        })?;
        gate.lane("domain-graph", |gate| {
            gate.command("python3", &["scripts/domain_contract.py"])
        })?;
        gate.lane("domain-contract", |gate| {
            gate.command("python3", &["scripts/test-domain-contract.py"])
        })?;
        gate.lane("access-graph", |gate| {
            gate.command("python3", &["scripts/test-access-graph.py"])
        })?;
        gate.lane("evidence", |gate| {
            gate.cargo(&[
                "test",
                "-p",
                "logical-db-e2e",
                "--locked",
                "--test",
                "clean_room_evidence",
                "--",
                "--nocapture",
            ])
        })?;
        gate.lane("ci-routing", |gate| {
            gate.cargo(&[
                "test",
                "-p",
                "logical-db-e2e",
                "--locked",
                "--test",
                "ci_routing",
            ])
        })?;
        gate.lane("suppressions", suppressions)?;
        gate.lane("artifacts", artifacts)?;
    }

    let rows = table(&gate.lanes);
    let failed: Vec<&str> = gate
        .lanes
        .iter()
        .filter(|lane| matches!(lane.outcome, Outcome::Fail(_)))
        .map(|lane| lane.name)
        .collect();
    let passed = gate
        .lanes
        .iter()
        .filter(|lane| matches!(lane.outcome, Outcome::Pass))
        .count();
    let summary = format!("passed: {passed}   failed: {}\n", failed.len());
    say(&format!(
        "\nlogical-DB local CI — {sha}{dirty}\nfinished: {}\n\n{rows}\n{summary}",
        timestamp()?
    ))?;
    if receipt {
        let path = gate
            .root
            .join("evidence")
            .join(format!("ci-local-{sha}.md"));
        let document = format!(
            "# Local CI receipt — {sha}{dirty}\n\n\
             > Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15\n\
             > S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32\n\
             > S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.\n\n\
             Host: {os} {arch}  \nMode: {mode}  \nRecorded: {}\n\n{rows}\n{summary}",
            timestamp()?,
        );
        std::fs::write(&path, document)?;
        say(&format!("receipt: {}\n", path.display()))?;
    }
    if failed.is_empty() {
        say("\nall gates passed\n")?;
        Ok(ExitCode::SUCCESS)
    } else {
        say(&format!(
            "\nrefused gates:\n  - {}\n",
            failed.join("\n  - ")
        ))?;
        Ok(ExitCode::FAILURE)
    }
}

/// Runs the gate; a failure of the gate itself exits 2.
fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            let mut stderr = std::io::stderr().lock();
            match stderr.write_all(format!("ci-local: {error}\n").as_bytes()) {
                Ok(()) => ExitCode::from(2),
                Err(_closed) => ExitCode::from(3),
            }
        }
    }
}

/// Only line-anchored attributes are suppressions, and each needs a reason.
#[cfg(test)]
mod tests {
    use super::{is_suppression, reasonless};

    /// Attribute forms count; mentions in comments and strings do not.
    #[test]
    fn suppression_attributes_are_line_anchored() {
        assert!(is_suppression("    #[allow(dead_code)]"), "outer allow");
        assert!(
            is_suppression("#![expect(clippy::exit, reason = \"x\")]"),
            "inner expect"
        );
        assert!(!is_suppression("// #[allow(dead_code)]"), "comment mention");
        assert!(
            !is_suppression("let text = \"#[allow(\";"),
            "string mention"
        );
    }

    /// A reason inside the window passes; one beyond it does not.
    #[test]
    fn reason_must_follow_within_the_window() {
        assert_eq!(
            reasonless("#[expect(\n    clippy::exit,\n    reason = \"r\"\n)]\n"),
            Vec::<usize>::new(),
            "reasoned"
        );
        let distant = format!("#[allow(dead_code)]\n{}reason\n", "x\n".repeat(7));
        assert_eq!(reasonless(&distant), vec![1], "reason beyond the window");
    }
}
