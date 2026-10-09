[//]: # (SPDX-License-Identifier: CC-BY-4.0)

# Annual report generation prompt

> Posted as a pull-request comment by `.github/workflows/generate-reports.yml`.
> Uppercase placeholders in double curly braces (for example REPORT_YEAR,
> REPORT_PATH, DATA_AS_OF) are substituted by
> `.github/scripts/report_generating_pr.js` before posting.

## Task

Write Hiero's **{{REPORT_YEAR}} annual review** into the Markdown file
`{{REPORT_PATH}}` in this pull request, then commit that file to this branch.

**The committed file is the deliverable** — it is complete, with its tables
rendered into it and its figures embedded inside it. The reply comment is not
the deliverable: it only tells you how to build it.

## Outline (mandatory)

Follow `hiero_reports/report_schema/hiero_annual_report_structure.md` exactly:
same headings, same order, `{YEAR}` replaced by {{REPORT_YEAR}}. The file must
start with this line, unchanged:

    [//]: # (SPDX-License-Identifier: CC-BY-4.0)

The file may already exist as a scaffold carrying those headings — fill it in.
Do not add top-level sections the structure does not define.

## Register and depth

Match the register of Hiero's previously filed review. (Its text lived at
`https://github.com/LF-Decentralized-Trust/governance/blob/main/tac/project-updates/annual-review-instructions.md` ; the
guidance below is self-contained, so do not depend on that file being present.)
Write the report to this register throughout:

- three-to-five sentence paragraphs built from what the data actually shows;
- at most one short markdown table per section where a comparison helps;
- inline dashboard links instead of bare URLs;
- the staged figures embedded directly in the file as images (tables and
  figures *are* the content of the deliverable — not links to a digest).

Every number here must trace to the staged evidence or a document you fetch
from the data API, and the description must match {{REPORT_YEAR}}.

## Data, tables and charts

Every number comes from the analytics data API, never from memory:

- Staged evidence: `{{EVIDENCE_PATH}}` — headline tiles, table inventory with
  row counts, chart inventory with dashboard links, and provenance (data as of
  {{DATA_AS_OF}}).
- Live API: `{{API_URL}}` — `manifest.json` indexes every document; fetch the
  documents you cite and read their `population`, `note` and `methodology`
  fields before describing what they count.
- Build the report's tables from the API's own `columns` and `rows`; never
  invent columns, totals or subtotals the documents do not contain.

Cite figures with dashboard deep links in the form
`{{DASHBOARD_URL}}#tab=<Macro>&org={{ORG}}&widget=<id>`; the digest already
lists ready-made links for every card and table.

### Figures staged in this PR

{{FIGURES}}

Embed them with a relative path from `{{REPORT_PATH}}` (for example
`![Unique active contributors by role, 2018 to 2027](figures/maintainer-pipeline.png)`), alt
text that says what the picture shows, and a one-sentence plain caption. If the
list says none were staged, cite dashboard deep links instead of images.

## Caveats to carry forward

- Partial or in-progress periods are marked in the documents — never present a
  partial bucket as a final figure.
- GitHub accounts are not people; GitHub Releases only; unknown affiliation is
  not independence.
- A number the documents flag as stale is written as stale.

## What not to write

- No number that is not in the digest or an API document you fetched. Where a
  section has no data, leave a maintainer slot instead of estimating:

  > [MAINTAINER INPUT] …

- No marketing language, no speculation about causes the data cannot show, no
  references to how the API or this prompt is structured.
- Human dates in prose (for example "6 October 2027"), never "this year".

## Sections the data cannot answer

For narrative sections (for example community-call context or interpretation of
trends), write one to three grounded sentences built from the evidence, or
leave a `> [MAINTAINER INPUT] …` blockquote stating exactly what a maintainer
should add. Prefer a maintainer slot over speculation.

## Deliverable checklist

1. File at `{{REPORT_PATH}}`; first line is the SPDX comment above.
2. Headings match the structure file exactly.
3. Every number traceable to the digest or an API document you fetched.
4. Tables use API columns; staged figures are embedded with captions.
5. Commit the file to this pull request's branch. If you cannot push directly
   to the branch, reply to this comment with the complete file content and say
   clearly that a maintainer must commit it.

