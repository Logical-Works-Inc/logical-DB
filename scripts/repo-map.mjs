#!/usr/bin/env node
// Validates repo-map.json against repo-map.schema.json (structural subset,
// no external deps) and renders REPO_MAP.md. `check` re-renders and diffs
// against the committed file to detect drift.
import fs from "node:fs";
import path from "node:path";

const root = path.resolve(import.meta.dirname, "..");
const map = JSON.parse(fs.readFileSync(path.join(root, "repo-map.json"), "utf8"));
const schema = JSON.parse(
  fs.readFileSync(path.join(root, "repo-map.schema.json"), "utf8"),
);

const errors = [];
const req = (obj, keys, where) =>
  keys.forEach((k) => {
    if (!(k in obj)) errors.push(`${where}: missing required key "${k}"`);
  });

req(map, schema.required, "root");
for (const [id, r] of Object.entries(map.repos ?? {})) {
  req(r, ["role"], `repos.${id}`);
}
if (Object.values(map.repos ?? {}).filter((r) => r.this).length !== 1) {
  errors.push("repos: exactly one entry must have this:true");
}
const statuses = new Set(Object.keys(map.statuses ?? {}));
const pillars = new Set(Object.keys(map.pillars ?? {}));
const nodeIds = new Set();
for (const n of map.nodes ?? []) {
  req(n, ["id", "path", "status", "summary"], `node`);
  if (statuses.size && !statuses.has(n.status))
    errors.push(`node ${n.id}: unknown status "${n.status}"`);
  if (n.pillar && !pillars.has(n.pillar))
    errors.push(`node ${n.id}: unknown pillar "${n.pillar}"`);
  if (nodeIds.has(n.id)) errors.push(`duplicate node id ${n.id}`);
  nodeIds.add(n.id);
}
const rels = new Set(
  schema.properties.edges.items.properties.rel.enum,
);
for (const e of map.edges ?? []) {
  req(e, ["from", "rel", "to"], "edge");
  if (!rels.has(e.rel)) errors.push(`edge: unknown rel "${e.rel}"`);
  if (!nodeIds.has(e.from)) errors.push(`edge: unknown from "${e.from}"`);
  if (!nodeIds.has(e.to)) errors.push(`edge: unknown to "${e.to}"`);
}

if (process.argv[2] === "check" && errors.length === 0) {
  const rendered = render();
  const file = path.join(root, "REPO_MAP.md");
  if (!fs.existsSync(file) || fs.readFileSync(file, "utf8") !== rendered) {
    console.error("FAIL REPO_MAP.md is stale; run: node scripts/repo-map.mjs render");
    process.exit(1);
  }
  console.log("OK   repo-map valid; REPO_MAP.md current");
  process.exit(0);
}

if (errors.length > 0) {
  console.error("FAIL repo-map invalid:");
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}

if (process.argv[2] === "render") {
  fs.writeFileSync(path.join(root, "REPO_MAP.md"), render());
  console.log("OK   REPO_MAP.md rendered from repo-map.json");
} else {
  console.log("OK   repo-map valid");
}

function render() {
  const by = (s) => map.nodes.filter((n) => n.status === s);
  const line = (n) => {
    const comps = n.components ? ` Components: ${n.components.join(", ")}.` : "";
    const adrs = n.adrs ? ` (${n.adrs.join(", ")})` : "";
    return `| \`${n.path}\` | ${n.pillar ?? "—"} | ${n.summary}${comps}${adrs} |`;
  };
  const section = (title, st) => {
    const rows = by(st).map(line);
    return rows.length
      ? [`## ${title}`, "", "| Path | Pillar | What it is |", "|------|--------|------------|", ...rows, ""]
      : [];
  };
  return [
    "<!-- GENERATED from repo-map.json by scripts/repo-map.mjs — DO NOT EDIT BY HAND. -->",
    "# REPO MAP",
    "",
    `Rendered view of \`repo-map.json\`. Canon: **${map.canon}**. Updated: ${map.updated}.`,
    "Reconcile with `node scripts/repo-map.mjs check`.",
    "",
    "## Repos (cross-repo)",
    "",
    ...Object.entries(map.repos).map(([name, r]) => {
      const t = r.this ? " (this repo)" : "";
      return `- \`${name}\`${t} — ${r.role}`;
    }),
    "",
    ...section("LIVE — authoritative today", "live"),
    ...section("META — agent/process tooling", "meta"),
    ...section("PLANNED — designed, not built", "planned"),
    ...section("RETIRED — provenance only", "retired"),
    "## Reading order",
    "",
    ...map.reading_order.map((r, i) => `${i + 1}. \`${r.ref}\` — ${r.why}`),
    "",
  ].join("\n");
}
