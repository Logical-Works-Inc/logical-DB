# Workflow: delivery loop, research, one architecture

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Sections 2–4 of [WORKFLOW.md](../../WORKFLOW.md), moved verbatim under the
120-line document law.

## 2. The delivery loop

```
1. Read the task and query WWFD            (1 min, mandatory)
2. Find the best OSS implementations       (5 min, non-trivial work only)
3. Navigate with CodeGraph                 (2 min, find code and callers)
4. Implement to the codebook               (CODEBOOK.md)
5. Test and verify                         (affected tests + regression tests)
6. WWFD state sync, then PR                (mandatory before delivery)
```

**Rule 1, first tool call.** The first substantive tool call reads source you
will edit, runs a build or test, writes source, or queries CodeGraph to navigate
to the code you will edit. It is not reading this file, loading a skill, running
`wwfd boot`, or producing a plan document.

**Rule 2, five-minute code.** Within five minutes of session start you have
written or modified a source file, run a build/test/lint command, or named a
specific `file:line` and the exact change it needs. "Inventorying owners and
consumers" is not this.

**Rule 3, narration ratio.** Process words must never exceed 20% of any
response. Compliance citations, skill-loading narration, time accounting, and
scope inventorying are process words. Code, architecture decisions, error
analysis, test results, and diffs are implementation words.

**Rule 4, one repro then fix.** A prior CI failure is a defect verdict and the
receipt already exists. One repro is authorized only when the cause is genuinely
uncertain from the receipt alone. A second repro of the same state is forbidden.
Fix the code, run the new state once.

**Rule 5, skill budget.** At most two skills loaded before the first code edit.
Additional skills load only when a specific API question blocks you
mid-implementation.

**Rule 6, table proportionality.** Product runtime changes get the full nine-axis
SLO evidence matrix with measured evidence per axis. Config, docs, CI, README,
and script changes get one line of receipt per file. No table. No axes.

**Rule 7, refuse the theater.** No `wwfd boot` + `mem show` as a session-start
ritual. No prophylactic skill loading. No re-reading source already held in
context. No re-grepping something already found.

## 3. RAG yourself into expertise

Before implementing anything non-trivial, find two or three production OSS
implementations of the same thing and study their structural decisions: module
organization, trait design, error handling, concurrency primitives, tests. Five
to ten minutes. Record what you are adopting and why, and what you rejected and
why, in your first edit or message. Then implement at that level.

Skip this for trivial changes under twenty lines with no new abstraction, bug
fixes whose root cause is already identified, and config/docs/CI work.

This is not copying code. It is studying architecture and making an informed
decision. The mechanism is RAG for latent implementation expertise: reading real
code activates specifics that prompts alone do not.

## 4. One path, one opinion, one architecture

A capability is either **the** architecture or it does not exist.

- **No default-off parallel paths.** A feature flag that makes an implementation
  a *candidate* is how work fails to carry forward: nothing exercises it, the
  gate never compiles it, and it rots into a second opinion nobody chose. If it
  is how the estate does the thing, it is on by default and the gate runs it.
- **One implementation of a job.** Two builders, two executors, or two
  "equivalent" entry points for the same work is one too many. Pick the right
  one, make it the only one, delete the other including its tests, docs, and
  feature flag.
- **Unification beats a seam.** "Parallel seams, not demolition" governs *how* a
  change lands while the tree stays green. It is not a licence to leave both
  halves standing. The seam closes in the same task.
- A storefront may keep capability features default-off, because selecting is
  its stated purpose. A **consumer** states its opinion: it turns on what it
  uses, in its own defaults, and the workspace gate compiles and tests it.
