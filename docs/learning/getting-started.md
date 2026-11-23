# Getting started: repository, Scala, IDE, and contribution directions

Baseline: Chronon commit `5bbc4d176a205e32b4172b8f0ecd360072cdb0fe`. Issue and
PR states were checked on 2026-11-23 and will drift. Module internals are in the
[architecture source code map](architecture.md#source-code-map), build commands
in the [SBT guide](sbt-guide.md), and serving threads in the
[thread model](thread-model.md); they are not repeated here.

## 1. Glossary

The running example is "user 42's purchase amount over the past 24 hours".

| Term | Meaning in Chronon |
|---|---|
| Entity key | Whose feature it is, such as `user_id = 42` |
| Source | Where raw data comes from and which columns and time column participate; event, entity, and other source types ([Source.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/Source.md)) |
| GroupBy | A keyed computation definition, such as 24h `SUM` and 7d `COUNT` per `user_id`; more than the SQL keyword |
| Join | Combines GroupBys; offline it adds feature columns to observation rows, online it returns the combined features for request keys |
| StagingQuery | Free-form Spark SQL ETL inside Chronon, often used to prepare a Join's left side ([StagingQuery.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/StagingQuery.md)) |
| Derivation | Expressions over already computed features, such as total divided by count |
| `TEMPORAL` / `SNAPSHOT` | Accuracy: computed as of each request or observation time, or from a periodic snapshot; unrelated to the Temporal workflow engine and to model accuracy |
| Point-in-time correct backfill | Informally a temporal join: each observation row only sees events before its own timestamp, so a 15:30 sample never includes a 16:00 purchase ([Join.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/Join.md#L211-L227)) |
| Backfill | Recomputing past feature values from retained raw history, such as three months of training rows |
| Online / offline | Low-latency reads for inference, versus batch tables for training and analysis |
| Parquet | A columnar, typed, compressed file format for tables, readable by Spark, pandas, DuckDB and others; not a database or a scheduler ([overview](https://parquet.apache.org/docs/overview/)) |
| KV store | Key-value storage holding online batch data, streaming data, and serving metadata; deployments implement the `KVStore` interface |
| IR | Intermediate representation: mergeable partial state, such as `(sum, count)` for an average |
| Tile, tiling | Pre-aggregated IR for a time slice; the tiled path merges tiles instead of raw events ([Tiled_Architecture.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/Tiled_Architecture.md)) |
| Sawtooth window | Chronon's window: fixed hops at the old end, exact at the recent end ([GroupBy.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/GroupBy.md#windowing)) |
| Fetcher | The online read library (`Fetcher`, `JavaFetcher`) that merges batch and streaming data; the REST service wraps it |
| Materialization | Actually writing results or intermediate state for later reads, not just saving a definition |
| `compile` | Validates Python definitions and writes Thrift JSON configs ([compile.py#L686](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/api/py/ai/chronon/repo/compile.py#L686)); not bytecode or native code |
| `analyze` | Checks schemas, join keys, timestamps, permissions, and partitions before a backfill ([Test.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/test_deploy_serve/Test.md)) |

GroupBy and Join are computation definitions, Parquet and KV are storage, and
`TEMPORAL`/`SNAPSHOT` are time semantics. They are different layers.

## 2. Repository root walkthrough

Verified with `git ls-files` at the baseline. Code modules (`api/`, `aggregator/`,
`spark/`, `flink/`, `online/`, `service/`, `airflow/`, `quickstart/`) are
described in the architecture source code map. `api/` is not a REST API: it holds
Thrift definitions (`api/thrift/`), JVM helpers, and the Python SDK and CLI
(`api/py/`).

| Entries | Purpose |
|---|---|
| `build.sbt`, `version.sbt`, `project/` | SBT build: modules, dependencies, Scala/Spark matrix. `version.sbt` is the artifact version (`0.0.116-SNAPSHOT`), not the SBT version, which is in `project/build.properties` (`1.8.2`) |
| `WORKSPACE`, `BUILD.bazel`, `.bazelrc`, `.bazeliskrc`, `jvm/`, `tools/`, `third_party/` | Bazel build. `.bazeliskrc` pins Bazel 6.4.0; root `BUILD.bazel` mainly defines Python lock-file rules; `jvm/` and `third_party/` declare dependencies, not vendored sources |
| `requirements.txt`, `requirements_lock.txt` | Bazel Python dependencies and their lock file |
| `tox.ini` | Python tests; installs `api/py/requirements/dev.txt`, compiles sample configs, runs pytest |
| `.scalafmt.conf` | Scala formatting rules, checked in CI |
| `.circleci/` | The CI that actually runs; `.github/` holds only the PR template |
| `build.sh` | Release packaging: Thrift codegen, Python package, docs, Spark assemblies per Scala version; not a daily compile command |
| `init.sh` | Initializes a user's feature-config repository and modifies shell settings; not needed to develop the framework |
| `Dockerfile`, `docker-compose.yml` | Quickstart images and services (Chronon, MongoDB, Kafka, ZooKeeper). Compose pulls a prebuilt image; the Dockerfile expects a locally built JAR (`your_build.jar` placeholder) |
| `maven_upload.sh`, `maven_upload_all.sh`, `pom.xml.tpl`, `gcloud_release.sh`, `release.md` | Maintainer release steps for Maven Central and the docs site |
| `README.md`, `docs/`, `devnotes.md` | User overview and quickstart, documentation sources, developer setup notes |
| `CONTRIBUTING.md`, `proposals/` | Contribution rules and CHIP design proposals |
| `GOVERNANCE.md`, `PROJECT_MANAGEMENT_COMMITTEE`, `AUTHORS`, `LICENSE` | Project governance, membership, and license |
| `*.png` at the root | Figures for governance and release documents, not runtime assets |
| `.claude/` | Context and commands for an AI coding assistant; not part of the product |
| `.ijwb/` | IntelliJ Bazel plugin project view |

Ignored local outputs you may also see: `bazel-*` symlinks, `target/`,
`derby.log` (local Hive metastore), and IDE state such as `.bloop/`, `.metals/`,
and `.vscode/`.

A reading order that works: README concepts, a sample GroupBy/Join under
`api/py/test/sample/`, the matching Python API in `api/py/ai/chronon/`, the module
list in `build.sbt`, then `aggregator/`, `spark/`, and `online/`.

## 3. Scala for a Java developer

### Which Scala

Learn Scala 2, practicing on 2.12; skip Scala 3 for now. `build.sbt` defines
2.11.12, 2.12.12, and 2.13.6 and defaults to 2.12.12
([build.sbt#L9-L21](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/build.sbt#L9-L21));
Bazel defaults to 2.12.18 ([.bazelrc#L14](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/.bazelrc#L14)).
The code guidelines note that Spark APIs are mainly Scala 2. Scala 3 tutorials
teach `given`/`using` and indentation syntax that this code does not use; on the
[Tour of Scala](https://docs.scala-lang.org/tour/tour-of-scala.html), select the
Scala 2 tabs. The project build chooses the compiler; versions coexist per
project, and `sbt 'show scalaVersion'` prints the one in use.

Writing features needs the Python API and SQL, not Scala. Scala matters for
reading, debugging, or changing the engine. A workable order:

1. `val`/`var`, `def`, classes, `object`, traits, case classes, lambdas.
2. `Option`, `Seq`, `Map`, `map`/`flatMap`/`filter`/`groupBy`, pattern matching.
3. For-comprehensions, implicits, Java collection conversions, `Try`/`Either`.
4. Chronon itself: a quickstart GroupBy/Join, then its implementation and tests.

JVM knowledge transfers. Differences that bite: `val` does not make the object
immutable, `==` is value equality, and `Future` still needs correct executors.

### Why the code guidelines ban familiar Scala in inner loops

[Code_Guidelines.md](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/Code_Guidelines.md)
splits code into a hot path (any row- or column-level operation, online and
offline) and a control path. The rules apply only to the hot path, where a small
per-row cost is multiplied by billions of rows.

| Rule | Cost it avoids |
|---|---|
| No `for` loops or ranges (`foreach`, `until`, `to`) | `for` desugars to `foreach`/`map`/`flatMap`/`withFilter` with closures; a `while` loop with an index has a predictable cost. The JIT may remove some overhead, so it is a predictability rule, not a law |
| No `Option` or `Tuple` | Allocating `Some(...)` or a tuple per value, plus boxing; `None` is a singleton |
| No immutable collections | Each `updated` on an immutable map allocates new nodes (structure is shared, not fully copied), raising allocation and GC pressure. At 32 extra bytes per record, 100 million records allocate about 3.2 GB in total |
| Naked arrays when the size is known | No resizing, and `Array[Double]` stores primitives unboxed |
| Branches moved to the control path | Choose the conversion function once from the schema instead of type-testing every value |

Outside inner loops (config parsing, planning, error handling), normal Scala is
fine. The `implicit` restriction is for readability; the testing rule is for
correctness.

### `Option` for Java developers

`Option[Int]` is `Some(123)` or `None`, similar to `Optional<Integer>` but used
throughout the standard library. `None` is not Scala's `null`, and the guideline
does not mean "return `None` instead". Scala does not remove NPEs: a `null` from
a Java API still fails on `.length`. Convert at the boundary with
`Option(rawValue)`, which maps `null` to `None`; `Some(null)` is still unsafe.

In a hot path, either pass already validated plain values, or store presence
separately:

```scala
val values = new Array[Int](rowCount)
val present = new Array[Boolean](rowCount)
```

A missing value that should never happen, such as a table that was just
created, is a bug to fix, not a case to hide as `None`.

## 4. IDE choice

VS Code with [Metals](https://scalameta.org/metals/docs/editors/vscode/) works for
reading, Python feature definitions, and local Scala changes. IntelliJ is usually
easier for heavy cross Java/Scala refactoring and JVM debugging; `devnotes.md`
documents an IntelliJ import from `build.sbt`. Neither gives semantic navigation
across Python, Thrift, and Scala.

Indexing pitfalls seen with VS Code and Metals:

- Import through SBT first; Bazel IDE integration is a separate effort.
- Thrift 0.13 must be on the `PATH` that the IDE's build server inherits
  (`devnotes.md` explains why not 0.14). Otherwise the SBT export fails, the `api`
  sources fall outside every build target, and dependent modules report missing
  types.
- "Indexed workspace" in the Metals log does not mean the import succeeded. Check
  for a failed import and for compiler-support errors; a recent Metals release
  logged Scala 2.12.12 as no longer directly supported and threw
  `NoSuchMethodError` from its fallback, while an older Metals server (1.3.3 with
  Bloop 1.6.0) indexed the project.
- If a separate Java language extension is also active, it analyzes Java files
  without the SBT classpath and reports false errors such as missing `scala.Option`
  or Thrift types. Disable it for this workspace only and let Metals handle Java.
- `scala-2.11/` and `scala-2.13/` source directories under `api/` and `online/` are
  outside the 2.12 build; weak navigation there is expected.
- Configure a Python interpreter for `api/py` separately.

## 5. Contribution directions

These favor work with real users over exercises and suit storage and serving
experience: caching, serialization, version switching, tail latency. Two themes
recur in the issue tracker: performance, and debuggability or tooling. Ranked by
learning value and evidence of need:

| Direction | Concrete deliverable | Upstream evidence (state on 2026-11-23) |
|---|---|---|
| Fetcher latency diagnosis and optimization | Explain where p99 goes (KV, executor queues, decoding, aggregation, derivation, GC) on a reproducible load, then a benchmarked fix | [#630](https://github.com/airbnb/chronon/issues/630) CHIP-1, open; production and load-test reports of 39-43% lower p99 from batch IR caching. The cache is already implemented; see the [thread model](thread-model.md#performance-and-visibility-checkpoints) for metric traps |
| Local feature testing and failure reproduction | Run the real engine on a few rows and show which row and column differ | [#1086](https://github.com/airbnb/chronon/issues/1086) open CHIP; [#1104](https://github.com/airbnb/chronon/pull/1104) open PR (`LocalTestingPlatform`); collaborate rather than duplicate |
| Backfill progress per JoinPart | Progress and timing by JoinPart and date range, linked to Spark stages | [#685](https://github.com/airbnb/chronon/issues/685) CHIP-3, open; mentions Joins with up to 135 JoinParts |
| Precompute derivations at upload | Apply eligible snapshot derivations in the upload path instead of per request | [#1102](https://github.com/airbnb/chronon/issues/1102) open CHIP, no comments; excludes `TEMPORAL` and request-time expressions |
| Online data versions and cache consistency | Detect and serve the right batch after a same-day re-run without restarting | [#1171](https://github.com/airbnb/chronon/pull/1171) merged 2026-11-11 for rollback; it lists same-day re-upload as still unsolved |
| Incremental batch aggregation | Reuse daily IRs for long windows instead of rescanning raw events | [#984](https://github.com/airbnb/chronon/issues/984) with [#998](https://github.com/airbnb/chronon/pull/998), both open; join the existing work |

Existing tooling to build on rather than rebuild: compile debug logging
([#845](https://github.com/airbnb/chronon/pull/845)), analyzer output schema
([#1011](https://github.com/airbnb/chronon/pull/1011)), definition-time validation
([#1042](https://github.com/airbnb/chronon/pull/1042)), PySpark and notebook
execution ([#981](https://github.com/airbnb/chronon/pull/981)), column lineage
([#933](https://github.com/airbnb/chronon/pull/933)), `explore.py` search, online
fetch by `--conf-path` ([#1148](https://github.com/airbnb/chronon/pull/1148)),
consistency metrics ([#529](https://github.com/airbnb/chronon/pull/529)), and AI
assistant commands ([#1084](https://github.com/airbnb/chronon/pull/1084)), all
merged. The gap is connecting them and attributing errors correctly; for example
[#1168](https://github.com/airbnb/chronon/pull/1168), merged 2026-11-11, stopped
the analyzer from reporting a missing partition as a permission problem.

A new SQL DSL or a DuckDB executor is not a good first contribution: no upstream
request was found, and the semantic-compatibility cost is large (see the
[co-location idea](feast-chronon-comparison.md#open-idea-serving-features-next-to-the-application)).

How to engage: after reading one concrete code path and before writing much code,
ask on the issue or through the community channel in the README whether the
problem still matters and what would count as acceptance. CHIPs are proportional
to the change; small ones need a few paragraphs
([CONTRIBUTING.md#L126](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/CONTRIBUTING.md#L126)).
External contributors can become committers, but PMC seats are reserved for
Airbnb and Stripe
([GOVERNANCE.md#L40](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/GOVERNANCE.md#L40)).

## 6. The ecosystem lock: should you push upgrades?

Adopters often cannot move Scala or Python because Spark distributions,
connectors, internal libraries, and images are pinned together; Scala 2.12 and
2.13 artifacts do not mix. Raising Chronon's minimum version transfers that
migration cost to every adopter, so a forced upgrade is not a good default
contribution.

Adding support for newer versions while keeping older ones is useful, and the
repository is already set up for it: version-specific code is isolated in
`scala-2.11/`, `scala-2.12/`, and `scala-2.13/` directories, and CI runs Scala 2.12
Spark tests, Scala 2.13 tests, and a Scala 2.11 compile-only job
([.circleci/config.yml](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/.circleci/config.yml#L34-L125)).

| Avoid | Prefer |
|---|---|
| Dropping an old version on the main branch | Add a new build and tests while the old one keeps working |
| Language features the adopters' runtime cannot compile | Isolate version-specific code |
| Treating "it compiles" as support | Verify connectors, serialization, runtime, and offline/online results |
| One upgrade for every component | Separate constraints: the Python SDK that writes configs need not share a version with PySpark jobs, though PySpark driver and workers must agree |

Retire a version only for security issues, end of support, or a dependency that
no longer supports it, and give a migration window. The same applies locally: fix
the IDE or build to match the project's versions instead of upgrading the project.
