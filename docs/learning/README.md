# Chronon learning notes

These personal learning notes use public source code, official documentation,
and publicly available industry reports. They are not upstream documentation or
evidence of any company's private implementation or selection process.

The main source analysis uses Chronon commit
`5bbc4d176a205e32b4172b8f0ecd360072cdb0fe`; the Feast comparison uses v0.67.0.
Each report retains its research date and version limits. Historical findings
are not guarantees about later releases.

## Reading map

| Topic | Canonical note |
|---|---|
| Glossary, repository root, Scala and IDE setup, contribution directions | [Getting started](getting-started.md) |
| Architecture, computation, and storage boundaries | [Architecture](architecture.md) |
| Request threads, executors, and blocking boundaries | [Thread model](thread-model.md) |
| Feast, Chronon, and open-source Feathr: capabilities, selection examples, and evidence | [Platform comparison](feast-chronon-comparison.md) |
| Purchase example, computation/schema/operations diagrams, training, and serving | [Feature workflow](feature-workflow.md) |
| Industry adoption and evidence strength | [Industry report](industry-adoption.md) |
| SBT concepts, multi-module commands, and build boundaries | [SBT guide](sbt-guide.md) |

## Format and wiki portability

All prose and diagram labels are English. Each topic has one maintained Markdown
document; each diagram has one current PNG, embedded using a relative path.
There are no parallel translations, SVG twins, historical variants, HTML pages,
JavaScript, or generator scripts.

Copy this directory with `assets/` and its relative paths to a Markdown/GFM wiki
or documentation site. Standard headings, lists, fenced code, tables, and images
require no site-specific components or build. Attachment-based platforms may
need image URL remapping; platforms without native Markdown support need their
own import mechanism.

Diagrams summarize their companion text. Reading the technical explanations
does not require hover interactions, expandable panels, or JavaScript.

## Interpretation boundaries

Framework-provided implementation does not mean the framework operates every
dependency. Ownership diagrams illustrate reference deployments, not fixed
organization charts. Purchases and labels are synthetic examples.

The Feast workflow uses a common external-compute path without excluding
v0.67.0's experimental compute extensions. Chronon's nominal windows must not
be assumed to be strict sliding windows; validate the business oracle and the
selected runtime separately.

The Feathr overview concerns its public project only. Company adoption claims
retain their sources and evidence limitations.
