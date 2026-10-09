// SPDX-License-Identifier: Apache-2.0
//
// Stages the annual-report evidence and drives CodeRabbit to draft it.
//
// Three entry points, all called from .github/workflows/generate-reports.yml:
//
//   generate()          fetch the data API, write the evidence digest, resolve
//                       the figures, write the capture plan, and create the
//                       report scaffold if it is not there yet.
//   captureChartPngs()  drive the dashboard's own Print-preview dialog to save
//                       one PNG per figure (Playwright, from web/node_modules).
//   promptCodeRabbit()  post the writing prompt as a PR comment, deduped.
//
// Kept dependency-light: only Node built-ins plus Playwright (already a web-app
// devDependency). All data comes from the static JSON API, never scraping HTML.

const fs = require("node:fs");
const path = require("node:path");

const REPORT_DIR = "hiero_reports/annual_report";
const EVIDENCE_DIR = path.join(REPORT_DIR, "evidence");
const FIGURES_DIR = path.join(REPORT_DIR, "figures");
const STRUCTURE_PATH = path.join("hiero_reports", "report_schema", "hiero_annual_report_structure.md");
const PROMPT_PATH = path.join(".github", "AI_prompts", "report_pr_prompt.md");
const DIGEST_PATH = path.join(EVIDENCE_DIR, "data-digest.md");
const CAPTURE_PLAN_PATH = path.join(EVIDENCE_DIR, "figures-to-capture.json");
// Hidden comment markers used to dedupe prompt postings. The full prompt goes
// up once (FULL_MARKER); later data refreshes post only a short nudge keyed to
// the data_as_of date, so the PR is not spammed every week.
const FULL_MARKER = "<!-- hiero-report-prompt:full -->";
const NUDGE_MARKER = "<!-- hiero-report-prompt:nudge -->";

// The three charts the report leads with. Each is resolved live against the
// manifest: an absent card, a chart whose title moved, or a chart with no
// interactive document is skipped with a log line — never guessed.
//
// `chartIndex` picks which chart on a multi-chart card (repo-growth and
// org-diversity hold several); `variantIndex` picks the view within that chart
// (all three want the default, index 0). `chartTitle` is what the Print button
// is labelled with, so it must match the manifest exactly. `id` is the dashboard
// deep-link card id.
const FIGURE_SPECS = [
  {
    id: "repo-growth",
    chartIndex: 1,
    variantIndex: 0,
    chartTitle: "Cumulative repo count",
    captionTitle: "Cumulative Hiero repositories with a release over time",
    why: "Growth in the number of Hiero repositories that have shipped a release.",
  },
  {
    id: "maintainer-pipeline",
    chartIndex: 0,
    variantIndex: 0,
    chartTitle: "Unique active contributors by role",
    captionTitle: "Unique active contributors by role, per year",
    why: "How many people were active in each role across the project.",
  },
  {
    id: "org-diversity",
    chartIndex: 0,
    variantIndex: 0,
    chartTitle: "Role-holders by organisation",
    captionTitle: "Governance role-holders by employer organisation (maintainers)",
    why: "The spread of governance role-holders across employer organisations.",
  },
];

// A card lives on a fixed tab in the app; map the report's three ids to their
// macro so a deep link lands on the right tab (the widget param only scrolls).
const CARD_TAB = {
  "repo-growth": "Contributors",
  "maintainer-pipeline": "Governance",
  "org-diversity": "Governance",
};

function workspace() {
  return process.env.GITHUB_WORKSPACE || process.cwd();
}

/** Absolute fetch URL for one data-API document path. */
function docUrl(cfg, docPath) {
  return `${cfg.api}/${docPath}`;
}

/**
 * The annual review is filed early in the year after the period it covers (the
 * 2026 review covers 2025), so from October onwards the draft targets the next
 * calendar year. REPORT_YEAR always wins when set.
 */
function reportYear() {
  if (process.env.REPORT_YEAR && /^\d{4}$/.test(process.env.REPORT_YEAR)) {
    return process.env.REPORT_YEAR;
  }
  const now = new Date();
  return String(now.getUTCMonth() >= 9 ? now.getUTCFullYear() + 1 : now.getUTCFullYear());
}

function config() {
  const base = (process.env.ANALYTICS_BASE_URL || "https://analytics.hiero.org").replace(/\/+$/, "");
  const year = reportYear();
  return {
    base,
    api: `${base}/data/api/v1`,
    org: process.env.REPORT_ORG || "hiero-ledger",
    year,
    reportPath: path.posix.join(REPORT_DIR, `${year}-annual-Hiero.md`),
  };
}

/** Fetch a JSON document from the data API; throw with the URL on failure. */
async function getJson(url) {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`GET ${url} -> HTTP ${res.status} ${res.statusText}`);
  }
  return res.json();
}

/** Log through github-script's core when present, else stdout. */
function log() {
  const c = globalThis.__reportCore;
  const msg = Array.from(arguments).join(" ");
  if (c && typeof c.info === "function") c.info(msg);
  else console.log(msg);
}
function warn() {
  const c = globalThis.__reportCore;
  const msg = Array.from(arguments).join(" ");
  if (c && typeof c.warning === "function") c.warning(msg);
  else console.warn(msg);
}

function writeFile(rel, contents) {
  const abs = path.join(workspace(), rel);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  fs.writeFileSync(abs, contents);
  return abs;
}

function readIfExists(rel) {
  const abs = path.join(workspace(), rel);
  return fs.existsSync(abs) ? fs.readFileSync(abs, "utf8") : null;
}

/** The org's subtree of the top-level manifest, or throw listing valid orgs. */
function orgEntry(manifest, orgName) {
  const orgs = (manifest && manifest.orgs) || {};
  if (!orgs[orgName]) {
    const known = Object.keys(orgs).join(", ") || "(none)";
    throw new Error(`manifest has no org "${orgName}". Known orgs: ${known}`);
  }
  return orgs[orgName];
}

/** Human stamp "YYYY-MM-DD" from an ISO timestamp, in UTC. */
function dateStamp(iso) {
  if (!iso || typeof iso !== "string") return "unknown";
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(iso);
  return m ? m[1] : iso;
}

/**
 * Resolve one figure spec to a concrete chart, or null to skip it.
 *
 * A card (`chart_sections[].id`) holds several charts; a chart holds several
 * variant views. Navigate card -> charts[chartIndex] -> variants[variantIndex]
 * and take that variant's interactive document path. The returned object
 * carries the dashboard deep link (with the card's slide/tab state so it opens
 * the exact chart), the chart's own Print-button label, and the JSON url, so
 * the capture step, the digest and the report all cite one thing.
 */
function resolveFigure(cfg, entry, spec) {
  const card = (entry.chart_sections || []).find((c) => c.id === spec.id);
  if (!card) {
    warn(`figure "${spec.id}" skipped: no card with that id in the manifest`);
    return null;
  }
  const charts = card.charts || [];
  const chart = charts[spec.chartIndex];
  if (!chart) {
    warn(`figure "${spec.id}" skipped: card has no chart at index ${spec.chartIndex}`);
    return null;
  }
  if (chart.title !== spec.chartTitle) {
    warn(
      `figure "${spec.id}" skipped: expected chart "${spec.chartTitle}" at index ${spec.chartIndex}, found "${chart.title}". Update FIGURE_SPECS to match the dashboard.`,
    );
    return null;
  }
  const variants = chart.variants || [];
  const variant = variants[spec.variantIndex] || variants[0];
  if (!variant) {
    warn(`figure "${spec.id}" skipped: chart "${chart.title}" has no variant at index ${spec.variantIndex}`);
    return null;
  }
  const docPath = variant.interactive && variant.interactive.path;
  if (!docPath) {
    warn(`figure "${spec.id}" skipped: variant "${variant.label || "?"}" has no interactive document`);
    return null;
  }
  const tab = CARD_TAB[spec.id] || card.macro || "";
  return {
    id: spec.id,
    cardTitle: card.title,
    chartTitle: chart.title,
    variantLabel: variant.label || "",
    tab,
    docPath,
    jsonUrl: docUrl(cfg, docPath),
    // Deep link. `tab` selects the macro; `widget` scrolls to the card. A
    // multi-chart card needs `<id>.slide=<chartIndex>` so the link opens on the
    // right chart (verified against the app's urlState keys). Every figure here
    // uses the default variant (index 0), so no variant param is written — a
    // shared-axis card's variant lives in `<id>.tab`, which we leave untouched
    // because we always want the first view.
    dashboardUrl:
      `${cfg.base}/#tab=${encodeURIComponent(tab)}&org=${encodeURIComponent(cfg.org)}` +
      `&widget=${encodeURIComponent(spec.id)}` +
      (charts.length > 1 ? `&${encodeURIComponent(spec.id)}.slide=${spec.chartIndex}` : ""),
    captionTitle: spec.captionTitle,
    why: spec.why,
    // The PNG file name, stable across runs so a re-run overwrites in place.
    fileName: `${spec.id}.png`,
  };
}

// --- evidence digest -------------------------------------------------------

/**
 * Build the evidence digest: everything a writer needs to draft the report
 * without opening a browser — provenance, the headline tiles per macro, the
 * full table inventory, the chart-card inventory, and the staged figures with
 * their relative image paths and dashboard links.
 */
function buildDigest(cfg, manifest, entry, figures) {
  const prov = manifest.provenance || {};
  const asOf = dateStamp(prov.data_as_of || prov.generated_at);
  const lines = [];
  const push = (s = "") => lines.push(s);

  push(`<!-- Generated by .github/scripts/report_generating_pr.js. Data as of ${asOf}. Do not edit by hand. -->`);
  push();
  push(`# Evidence digest — Hiero ${cfg.year} annual review`);
  push();
  push("Everything below is quoted from the published analytics data API so the report can be");
  push("written from facts, not memory. Tiles are headline numbers; tables and charts carry a");
  push("dashboard deep link and a JSON path. Re-generate with the weekly workflow.");
  push();
  push("## Provenance");
  push();
  push(`- Data as of: **${asOf}**`);
  if (prov.generated_at) push(`- Generated at: ${prov.generated_at}`);
  push(`- Source commit: ${prov.git_sha || "unknown"}`);
  push(`- API root: ${cfg.api}/`);
  push(`- Dashboard: ${cfg.base}/ (org \`${cfg.org}\`)`);
  push();

  const metrics = entry.metrics || {};
  const macros = Object.keys(metrics);
  if (macros.length) {
    push("## Headline tiles");
    for (const macro of macros) {
      push();
      push(`### ${macro}`);
      push();
      for (const tile of metrics[macro] || []) {
        push(`- **${tile.label}: ${tile.value}**${tile.note ? ` — ${tile.note}` : ""}`);
      }
    }
    push();
  }

  const sections = entry.sections || [];
  push("## Tables");
  push();
  if (!sections.length) push("_No tables in the manifest._");
  for (const s of sections) {
    const link = `${cfg.base}/#tab=${encodeURIComponent(s.macro || "")}&org=${encodeURIComponent(cfg.org)}&widget=${encodeURIComponent(s.id)}`;
    push(`- **${s.title}** (${s.macro || "—"}, ${s.row_count ?? "?"} rows) — ${link} — JSON: \`${docUrl(cfg, s.path)}\``);
  }
  push();

  push("## Chart cards");
  push();
  if (!(entry.chart_sections || []).length) push("_No chart cards in the manifest._");
  for (const card of entry.chart_sections || []) {
    const link = `${cfg.base}/#tab=${encodeURIComponent(card.macro || "")}&org=${encodeURIComponent(cfg.org)}&widget=${encodeURIComponent(card.id)}`;
    const titles = (card.charts || []).map((c) => c.title).join("; ");
    push(`- **${card.title}** (${card.macro || "—"}) — ${link} — charts: ${titles}`);
  }
  push();

  push("## Figures staged for this report");
  push();
  if (!figures.length) push("_No figures resolved; the report will cite charts by dashboard link only._");
  const fromReport = path.posix.dirname(cfg.reportPath);
  for (const f of figures) {
    const rel = path.posix.relative(fromReport, path.posix.join(FIGURES_DIR, f.fileName));
    push(`### ${f.captionTitle}`);
    push();
    push(`- Card: ${f.cardTitle} — ${f.chartTitle}${f.variantLabel ? ` (${f.variantLabel})` : ""}`);
    push(`- Why it matters: ${f.why}`);
    push(`- PNG (relative to the report): \`${rel}\``);
    push(`- Dashboard: ${f.dashboardUrl}`);
    push(`- JSON: ${f.jsonUrl}`);
    push();
  }
  return lines.join("\n");
}


// --- report scaffold -------------------------------------------------------

/**
 * The one-time report scaffold, built from the schema file with its {YEAR}
 * placeholder filled in and a figures block appended. Written only when the
 * report does not exist yet, so a report CodeRabbit has already written is
 * never clobbered by a later data refresh.
 */
function buildScaffold(cfg, schemaText, figures) {
  const body = schemaText.replace(/\{YEAR\}/g, cfg.year).replace(/\{year\}/g, cfg.year);
  const figureBlock = figures.length
    ? [
        "",
        "## Figures",
        "",
        "<!-- Staged PNGs from the analytics dashboard. Keep the alt text; keep captions to one line. -->",
        "",
        ...figures.map(
          (f, i) =>
            `![${f.captionTitle}](${path.posix.relative(path.posix.dirname(cfg.reportPath), path.posix.join(FIGURES_DIR, f.fileName))})\n\n*Figure ${i + 1}. ${f.captionTitle}. Source: [${f.chartTitle}](${f.dashboardUrl}).*`,
        ),
        "",
      ].join("\n")
    : "";
  return `${body.trimEnd()}\n${figureBlock}`;
}


// --- entry points ----------------------------------------------------------

/**
 * Before the PR: fetch the API, write the digest, resolve the figures, write
 * the capture plan, and create the report scaffold if it is not there yet.
 * Pure JSON/text; no browser.
 */
async function generate() {
  const cfg = config();
  log(`Report year ${cfg.year}; org ${cfg.org}; API ${cfg.api}`);

  const manifest = await getJson(`${cfg.api}/manifest.json`);
  const entry = orgEntry(manifest, cfg.org);
  const asOf = dateStamp((manifest.provenance || {}).data_as_of || (manifest.provenance || {}).generated_at);

  const figures = FIGURE_SPECS.map((spec) => resolveFigure(cfg, entry, spec)).filter(Boolean);
  for (const f of figures) log(`resolved figure "${f.id}": ${f.chartTitle} (${f.fileName})`);

  // Digest (always refreshed — it is derived evidence, safe to overwrite).
  writeFile(DIGEST_PATH, buildDigest(cfg, manifest, entry, figures));
  log(`wrote ${DIGEST_PATH}`);

  // Capture plan consumed by captureChartPngs(); also carries the data date so
  // promptCodeRabbit() can dedupe nudges on data_as_of.
  writeFile(
    CAPTURE_PLAN_PATH,
    `${JSON.stringify({ org: cfg.org, base: cfg.base, as_of: asOf, figures }, null, 2)}\n`,
  );
  log(`wrote ${CAPTURE_PLAN_PATH} (${figures.length} figure(s))`);

  // Scaffold only when absent — never overwrite a written report.
  const reportAbs = path.join(workspace(), cfg.reportPath);
  if (fs.existsSync(reportAbs)) {
    log(`report already exists at ${cfg.reportPath}; leaving it untouched`);
  } else {
    const schemaText = readIfExists(STRUCTURE_PATH);
    if (!schemaText) throw new Error(`report schema not found at ${STRUCTURE_PATH}`);
    writeFile(cfg.reportPath, buildScaffold(cfg, schemaText, figures));
    log(`wrote scaffold ${cfg.reportPath}`);
  }

  return { cfg, figures };
}

/**
 * After generate(), before the PR: drive the dashboard's own Print-preview
 * dialog to save one PNG per resolved figure. Isolated so a browser problem
 * only loses the images (the workflow marks this `continue-on-error`); the
 * evidence digest and report scaffold are already staged either way.
 *
 * Reuses the web app's export rather than re-drawing charts, so the PNGs match
 * the dashboard exactly — same palette, legend, data labels and footnotes.
 */
async function captureChartPngs() {
  const cfg = config();
  const planAbs = path.join(workspace(), CAPTURE_PLAN_PATH);
  if (!fs.existsSync(planAbs)) {
    warn(`no capture plan at ${CAPTURE_PLAN_PATH}; skipping PNG capture`);
    return { captured: 0, skipped: 0 };
  }
  const plan = JSON.parse(fs.readFileSync(planAbs, "utf8"));
  if (!plan.figures || !plan.figures.length) {
    log("no figures to capture");
    return { captured: 0, skipped: 0 };
  }

  // Playwright ships with the web app's devDependencies; resolve it from there.
  let chromium;
  try {
    ({ chromium } = require(path.join(workspace(), "web", "node_modules", "playwright")));
  } catch (err) {
    warn(`playwright not available (${err.message}); skipping PNG capture`);
    return { captured: 0, skipped: plan.figures.length };
  }

  const figuresAbs = path.join(workspace(), FIGURES_DIR);
  fs.mkdirSync(figuresAbs, { recursive: true });

  const browser = await chromium.launch({ headless: true, args: ["--no-sandbox", "--disable-dev-shm-usage"] });
  const context = await browser.newContext({ acceptDownloads: true });
  let captured = 0;
  let skipped = 0;
  try {
    for (const fig of plan.figures) {
      const page = await context.newPage();
      try {
        await captureOne(page, fig, path.join(figuresAbs, fig.fileName));
        captured += 1;
        log(`captured ${fig.fileName}`);
      } catch (err) {
        skipped += 1;
        warn(`figure "${fig.id}" not captured: ${err.message.split("\n")[0]}`);
      } finally {
        await page.close();
      }
    }
  } finally {
    await browser.close();
  }
  return { captured, skipped };
}

/** Capture a single figure's PNG by driving its Print-preview dialog. */
async function captureOne(page, fig, destAbs) {
  await page.goto(fig.dashboardUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  // Wait for the app to finish its initial data work before touching controls.
  await page.waitForFunction(() => document.querySelectorAll("[data-print-pending]").length === 0, null, {
    timeout: 60000,
  });
  // The chart's own Print button is labelled with the chart title.
  const printButton = page.getByRole("button", { name: `Print chart: ${fig.chartTitle}`, exact: true });
  await printButton.waitFor({ state: "visible", timeout: 30000 });
  await printButton.click();

  const preview = page.getByRole("dialog", { name: "Print preview" });
  await preview.waitFor({ state: "visible", timeout: 15000 });
  await preview.getByRole("radio", { name: "PNG", exact: true }).click();
  const [download] = await Promise.all([
    page.waitForEvent("download", { timeout: 90000 }),
    preview.getByRole("button", { name: "Download PNG" }).click(),
  ]);
  await download.saveAs(destAbs);
  // Sanity-check the PNG magic bytes so a 0-byte or HTML file fails loudly.
  const head = fs.readFileSync(destAbs).subarray(1, 4);
  if (head.toString("latin1") !== "PNG") {
    throw new Error(`downloaded file is not a PNG (${destAbs})`);
  }
}

// --- CodeRabbit prompt comment --------------------------------------------

/**
 * Substitute the {{TOKENS}} in the prompt template. Throws if any token is left
 * unreplaced, so a renamed variable surfaces as a loud failure rather than a
 * prompt that silently asks about "{{REPORT_PATH}}".
 */
function substitute(template, vars) {
  const out = template.replace(/\{\{\s*([A-Z0-9_]+)\s*\}\}/g, (whole, name) => {
    if (!(name in vars)) throw new Error(`prompt template has no value for {{${name}}}`);
    return vars[name];
  });
  const leftover = out.match(/\{\{\s*[A-Z0-9_]+\s*\}\}/g);
  if (leftover) throw new Error(`unsubstituted tokens in prompt: ${leftover.join(", ")}`);
  return out;
}

/**
 * After the PR exists: post the writing prompt as a comment, deduped so weekly
 * data refreshes only ever post once. Posts the full prompt the first time;
 * on a later run whose data_as_of has moved, posts only a short nudge.
 *
 * Returns a status string so the workflow step can surface what happened.
 */
async function promptCodeRabbit({ github, context, prNumber }) {
  const issueNumber = Number(prNumber);
  if (!Number.isInteger(issueNumber) || issueNumber <= 0) {
    warn(`invalid pull-request number "${prNumber}"; skipping CodeRabbit prompt`);
    return "skipped: bad PR number";
  }
  const cfg = config();
  const template = readIfExists(PROMPT_PATH);
  if (!template) throw new Error(`prompt template not found at ${PROMPT_PATH}`);

  // The data date is recorded by generate() in the capture plan; the nudge
  // dedup keys on it so a re-run only nudges when the data actually moved.
  let asOf = "";
  try {
    asOf = JSON.parse(readIfExists(CAPTURE_PLAN_PATH) || "{}").as_of || "";
  } catch {
    asOf = "";
  }

  const figuresList = FIGURE_SPECS.map((s) => `- ${s.captionTitle} (card \`${s.id}\`)`).join("\n");
  const body = substitute(template, {
    REPORT_YEAR: cfg.year,
    REPORT_PATH: cfg.reportPath,
    DATA_AS_OF: asOf || "see the digest",
    EVIDENCE_PATH: DIGEST_PATH,
    API_URL: `${cfg.api}/`,
    DASHBOARD_URL: `${cfg.base}/`,
    ORG: cfg.org,
    FIGURES: figuresList,
  });

  // List the PR's comments once; our markers tell us what we have posted before.
  const comments = await github.paginate(github.rest.issues.listComments, {
    ...context.repo,
    issue_number: issueNumber,
    per_page: 100,
  });

  // Idempotency across all our marker comments. The most recent one — full or
  // nudge — records the data date we last prompted on, so a re-run whose data
  // has not moved posts nothing, and a moved data date posts exactly one nudge.
  const hasFull = comments.some((c) => c.body && c.body.includes(FULL_MARKER));
  const ours = comments.filter(
    (c) => c.body && (c.body.includes(FULL_MARKER) || c.body.includes(NUDGE_MARKER)),
  );
  const latest = ours[ours.length - 1];
  const lastKey = (latest && (latest.body.match(/data_as_of=([0-9-]+)/) || [])[1]) || "";

  if (!hasFull) {
    const comment = [`@coderabbitai`, "", FULL_MARKER, "", body, "", `data_as_of=${asOf}`].join("\n");
    await github.rest.issues.createComment({ ...context.repo, issue_number: issueNumber, body: comment });
    log("posted full CodeRabbit writing prompt");
    return "posted";
  }

  if (asOf && lastKey === asOf) {
    log(`CodeRabbit prompt already posted for data_as_of=${asOf}; nothing to do`);
    return "already-posted";
  }

  const nudge = [
    NUDGE_MARKER,
    "",
    `@coderabbitai the analytics data was refreshed (**data_as_of=${asOf}**). The evidence digest at`,
    `\`${DIGEST_PATH}\` and the staged figures were updated. Please update the numbers, tables and`,
    `figure captions in \`${cfg.reportPath}\` to match, keeping the existing structure and voice.`,
    "",
    `data_as_of=${asOf}`,
  ].join("\n");
  await github.rest.issues.createComment({ ...context.repo, issue_number: issueNumber, body: nudge });
  log(`posted refresh nudge for data_as_of=${asOf}`);
  return "nudged";
}

module.exports = { generate, captureChartPngs, promptCodeRabbit, config, buildDigest, resolveFigure };
