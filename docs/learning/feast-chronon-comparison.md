# Feast, Chronon, and open-source Feathr: public architectural evidence

Research cutoff: 2026-10-14. Scope: public upstream documentation, original Airbnb engineering posts, GitHub releases and tagged source.

## Main conclusion

“Feast serves already-computed features; Chronon owns declarative computation” is a useful description of architectural emphasis, especially around Feast's 2021 redesign, but is not a correct binary capability comparison in 2026. Feast v0.10 explicitly placed batch feature creation in external Spark/SQL ETL while owning definitions, historical retrieval and online materialization. Chronon's central abstraction describes how to compute features from raw sources, including temporal aggregation, historical reconstruction and online update/serving behavior. See S1 and S6 below.

Three corrections matter:

1. Feast has executed on-demand transformations on both training and online paths since experimental ODFVs in v0.13, released 2021-09-22. Current v0.67 contains batch/stream feature computation, DAGs and time-windowed aggregation; the batch and stream feature-view constructors still explicitly warn that they are alpha/experimental. This is shipped functionality with scope/maturity caveats, not merely a roadmap. Sources: `feast-dev/feast:docs/reference/beta-on-demand-feature-view.md:7-15` at v0.67.0; `feast-dev/feast:sdk/python/feast/batch_feature_view.py:81-112`; `feast-dev/feast:sdk/python/feast/stream_feature_view.py:133-138`.
2. Chronon is not just a computation layer without storage/serving. Its framework includes online KV integration, feature fetchers, metadata deployment, orchestration, backfills and consistency measurements. Its fetcher may combine batch-derived state and streaming events/tiles at read time, rather than only fetch a final scalar. Sources: `airbnb/chronon:docs/source/setup/Components.md:7-40`; `airbnb/chronon:docs/source/Tiled_Architecture.md:15-35`.
3. Feast and Chronon need not be alternatives. Feast v0.67.0, released 2026-10-13, added Chronon online/offline integrations. The shipped documentation states that Chronon remains responsible for feature computation/materialization and online serving, while Feast provides registry/retrieval integration. This is unusually direct evidence of the architectural boundary. Sources: `feast-dev/feast:docs/reference/offline-stores/chronon.md:5-7`; `feast-dev/feast:docs/reference/online-stores/chronon.md:5-7`, both at v0.67.0.

## Evidence-backed comparison matrix

Chronon documentation references below were read from main at commit `5bbc4d176a205e32b4172b8f0ecd360072cdb0fe` (2026-10-08). Feast current code/docs were checked against v0.67.0. “Interpretation” labels architectural deductions rather than product guarantees.

| Dimension | Feast | Chronon | Meaningful distinction / caveat |
|---|---|---|---|
| Architectural center | Historically definitions/registry + point-in-time retrieval + online materialization/serving over existing feature data. Current compute extensions expand this. | Source/GroupBy/Join declarations drive temporal feature computation, training backfills, online maintenance and serving. | Interpretation: composable feature access/control plane versus a more integrated temporal feature-processing platform; not “no computation versus computation.” S1, S3, S6. |
| Registry/control plane | Central catalog for applied objects; file/protobuf and SQL implementations; version-controlled definitions synced through CI/CD. | Python definitions compile to Thrift configs; committed definitions drive generated jobs/DAGs; online Join metadata uploaded to KV. | Both have metadata/control planes. Chronon's configuration more directly specifies execution and temporal semantics. `feast-dev/feast:docs/getting-started/components/registry.md:3-12`; `airbnb/chronon:docs/source/setup/Components.md:5-20`; `airbnb/chronon:docs/source/test_deploy_serve/Deploy.md:3-18`. |
| Transformations | ODFVs use existing features and request inputs, with shared historical/online logic; read-time by default, optional write-time precomputation. Current BatchFeatureView supports UDFs, aggregation and dependent views. | SQL-like selects/filters, custom registered UDFs, derived features, GroupBy aggregation, Join composition and offline StagingQuery; nonaggregating GroupBy can expose source attributes directly. | Not restricted to simple aggregations. UDF support is present in v0.0.93, but arbitrary raw PyFlink programs and portable UDF execution across every context are not established by that fact. `feast-dev/feast:docs/reference/beta-on-demand-feature-view.md:7-15,29-46`; `airbnb/chronon:api/py/ai/chronon/query.py:36-60` and `airbnb/chronon:online/src/test/scala/ai/chronon/online/test/CatalystUtilTest.scala:262-307` at v0.0.93; S3, S6. |
| Batch execution | Traditional materialization loads the latest feature values; current compute engines execute supported transformation DAGs. Spark compute-on-read was added in v0.63.0 (2026-05-04). | Spark computes batch values, historical joins, snapshots and uploads; orchestration coordinates dependencies and refreshes. | Materializing existing values is distinct from recomputing missing feature history. Avoid treating every Feast connector as supporting all current computation APIs. `feast-dev/feast:docs/getting-started/concepts/data-ingestion.md:21-27`; `airbnb/chronon:docs/source/setup/Components.md:9-16`; v0.63.0 release notes. |
| Streaming execution | Push ingestion can accept externally computed features for online/offline use. Current StreamFeatureView adds transform/aggregation execution through supported engines; alpha warning remains. | Source declarations connect warehouse history with live streams. Spark Streaming or Flink updates online state; Flink tiled path maintains pre-aggregates. | Fast push ingestion alone is not a complete stream-processing system. Chronon supplies more of this runtime but adopters still integrate sources, sinks and orchestration. `feast-dev/feast:docs/getting-started/concepts/data-ingestion.md:15-19`; `airbnb/chronon:flink/src/main/scala/ai/chronon/flink/FlinkJob.scala:33-44,72-84,114-125` at v0.0.115. |
| Windowed aggregation | No longer absent: current Aggregation API and tiling exist; tiling released in v0.58.0 (2025-12-16). Docs describe Spark/Ray support but no local-engine time-window aggregation, and a pandas conversion in the tiling path. | Core GroupBy abstraction supports windowing, buckets, first/last-k, sketches and entity mutations. Docs describe sawtooth windows, not universally exact sliding windows. | Compare boundary semantics, supported functions, hot-key behavior and implementation scale, not the presence of “window” in an API. `feast-dev/feast:docs/getting-started/concepts/tiling.md:230-280`; `airbnb/chronon:docs/source/authoring_features/GroupBy.md:34-88,143-165`. |
| Historical point-in-time retrieval | Finds eligible feature rows at or before each entity timestamp, within feature TTL. By default event time is constrained; created time deduplicates but is not availability-filtered. New optional created-time filtering is backend-dependent. | Join's left-side events define the times at which raw-source GroupBy features are recomputed. Source types and SNAPSHOT/TEMPORAL accuracy define what can be reconstructed. | “Latest historical feature snapshot” and “compute a temporal aggregate at each label time” differ. Neither magically reconstructs missing history or actual production availability. S4, S6; `airbnb/chronon:docs/source/authoring_features/Join.md:3-17`. |
| Online serving | Normal path is latest feature lookup, potentially with ODFV computation; latency depends on store, fanout and transformations. | Java/Scala fetcher or wrapped service retrieves online state; untiled path may aggregate event records, tiled path merges IRs. | Neither has a universal latency SLA. Query latency and source-event-to-feature freshness are separate axes. `feast-dev/feast:docs/concepts/architecture.md:20-28` at v0.10.0; `airbnb/chronon:docs/source/setup/Components.md:22-28`; S8. |
| Freshness | Depends on external computation, push cadence/materialization and online write availability; current compute integrations can own more of the path. | Batch plus live-stream maintenance; 2026 Airbnb embedding example reports 10-30 second updates using Push Mode and NRT Model Transform. | The Airbnb measurement is a particular pipeline's end-to-end freshness, not Chronon fetch latency or a general product guarantee. S10; `airbnb/chronon:docs/source/test_deploy_serve/Online_Offline_Consistency.md:17-29`. |
| Training/serving consistency | Shared definitions, historical joins, and ODFV reuse reduce skew. External transformation implementations, late arrival and availability timestamp modeling remain relevant. | Shared compute definitions reduce duplicated logic; built-in logged-online-versus-backfilled comparisons detect residual skew. Docs explicitly reject absolute 100% consistency. | Logical/event-time consistency is not identical to reproducing the exact value available online during a historical request. S4, S7; `airbnb/chronon:docs/source/test_deploy_serve/Online_Offline_Consistency.md:33-80`. |
| Backfills | Existing timestamped history can be retrieved without waiting to log new online requests. Traditional Feast does not invent the upstream history for a newly defined aggregate; external pipeline or supported current compute path must create it. | A first-class objective: reconstruct new features from historical raw events/snapshots/mutations; Join backfill/frontfill is part of orchestration. | Conditional Chronon advantage where replacing log-and-wait and duplicate batch/stream logic is the main bottleneck. Requires retained, correctly modeled source history. S1, S6; `airbnb/chronon:docs/source/setup/Orchestration.md:13-21`. |
| Storage ownership / portability | User owns underlying source data; pluggable registries/offline stores/online stores/providers. Chronon bridge delegates writes/materialization rather than making Chronon Feast-managed. | User supplies warehouse/stream/KV environment. KV interface includes multiGet, multiPut, bulkPut and timestamp/range-related semantics; Mongo example is available. | Chronon is backend-extensible, not hardwired to one database; extensibility does not erase deeper semantic/integration requirements. `feast-dev/feast:docs/getting-started/concepts/data-ingestion.md:3-7`; `airbnb/chronon:docs/source/setup/Online_Integration.md:17-52`; S5. |
| Operations | Production owner operates source pipelines, compatible stores, ingestion/materialization, registry deployment and feature servers; scope grows if using compute engines. | Platform owner integrates/runs Spark, streaming runtime, KV uploads/fetchers, metadata, orchestration, backfills and consistency logs/jobs. Airflow best supported, not mandatory. | Interpretation: lighter composability can preserve an existing data platform; integrated computation can reduce per-feature work but increases platform integration scope. Not an unconditional “Feast cheap / Chronon expensive” conclusion. `airbnb/chronon:docs/source/setup/Orchestration.md:3-26,48-51`; S1, S6. |

## Historical evolution: do not flatten the timeline

- Pre-v0.10 Feast already had Core, Job Service and batch/stream ingestion jobs. It was not always a tiny library. Nevertheless, feature creation remained external in the architecture. The v0.9-tagged architecture has a stale “Feast 0.8” limitations block: use its component/flow descriptions cautiously, not its old support matrix. `feast-dev/feast:docs/concepts/architecture.md:7-15,23-44` at v0.9.0.
- Feast v0.10.0 (2021-04-15) explicitly emphasized a lightweight SDK/CLI and extensible provider model. The tag's architecture separates external Spark/SQL feature engineering from Feast registry, retrieval and materialization. S1.
- Feast v0.13.0 (2021-09-22) introduced experimental on-demand transformations consistently applied to historical and online retrieval, including request data. v0.21/v0.22 (May/June 2022) advertised alpha stream transformations. Thus “Feast never transforms” was already inaccurate then. S2; corresponding upstream release notes.
- Feast v0.36.0 (2024-04-16) added native-Python ODFV transformations; v0.41 releases (October 2024) added ODFV online-write capability. v0.58.0 (2025-12-16) introduced tiling; v0.63.0 (2026-05-04) added Spark batch-feature compute-on-read; v0.67.0 (2026-10-13) added Chronon integration and corrected several tiled aggregation cases. The current computation layer should not be projected onto earlier selection decisions. Evidence: upstream release entries retrieved directly.
- Chronon's original declarative-framework and open-source-announcement posts explicitly target duplicated offline/online pipelines and log-and-wait delays. The original description generated a lambda architecture using Spark/Spark Streaming, Hive, Kafka, Airflow and a customizable KV store. Original article bodies were verified, but their exact publication dates were not visible in the fetch result; direct metadata retrieval was blocked with HTTP 403. Do not invent exact dates from this research. S6, S9.
- Chronon Flink/tiling documentation landed in commit `f5c39f157bf374648edb3b673bc744dc23c864ad`, 2024-03-13. The docs still label Flink experimental / in the process of open-sourcing. However, actual Flink tiled/untiled implementation is present in tag v0.0.115; do not misread the old warning as “only a future roadmap.” `airbnb/chronon:docs/source/setup/Flink.md:3-18`; `airbnb/chronon:flink/src/main/scala/ai/chronon/flink/FlinkJob.scala:33-44,114-125`.
- Latest Chronon GitHub Release object returned was v0.0.101 (2025-07-10), but later tags exist. Latest observed tag was v0.0.115 at `a0afcdf92fc178916058c2fa43bf64f9218ebe1c` (2026-09-23); main was updated 2026-10-08. GitHub Releases alone would incorrectly make the project appear stale.
- Airbnb's 2026-09-23 post introduces Push Mode and NRT Model Transform and explicitly says both were contributed to the public repo. Push Mode notifies downstream after a new feature is committed; this is different from Feast's PushSource, which is an ingestion API. Tag v0.0.115 contains `PushModeConfig.scala` and model-transform code. The post reports typical embedding freshness under a minute, often 10-30 seconds, replacing roughly two-day staleness. This is evidence of current direction toward event-driven computed features/model outputs, not evidence these capabilities existed at initial open source. S10; `airbnb/chronon:spark/src/main/scala/ai/chronon/spark/streaming/PushModeConfig.scala:23-40`; `airbnb/chronon:spark/src/main/scala/ai/chronon/spark/ModelTransformBatchJob.scala:17-35`.

## Consistency and maturity cautions worth preserving

Feast's newest point-in-time docs make a subtle but important distinction explicit: a correction created after the historical request can still win an event-time join. `filter_by_created_timestamp=True` adds availability filtering only on supported stores and only represents online availability if the created timestamp actually models it. This was verified in the v0.67.0 tag, not just moving website docs. S4.

Chronon also has unavoidable delays: its consistency documentation cites milliseconds-to-seconds for stream propagation and minutes-to-hours for batch refresh. Moreover, non-reversible aggregates over entity mutations may be batch-corrected only eventually. Shared feature logic does not eliminate delayed/missing input, approximate aggregation, snapshot granularity or serving-side execution effects. Sources: S7; `airbnb/chronon:docs/source/authoring_features/GroupBy.md:57-68`.

Chronon's documented sawtooth window can include, for example, 1:20-2:27 for a nominal one-hour request at 2:27, rather than the exact sliding interval 1:27-2:27. The important promise is consistency with the chosen semantics, not that every advertised window is exact sliding time. `airbnb/chronon:docs/source/authoring_features/GroupBy.md:70-88`.

Feast documentation is internally uneven: a generic overview still says batch transformations are future work while current BatchFeatureView docs and tagged code implement them. Likewise a stream-view support checklist and broad engine descriptions differ. Therefore this report claims existence of shipped alpha compute paths, not universal engine/store support or production parity with Chronon. Tagged constructor warnings are stronger maturity evidence than a high-level marketing page. S3.

The new Feast/Chronon bridge has explicit limits: offline joins execute locally in pandas over Chronon-produced Parquet; it is not delegating a distributed temporal backfill to Chronon. Online writes, infrastructure update/teardown and online on-demand transforms are marked unsupported in the bridge matrix. Offline Python ODFVs are documented separately as supported. `feast-dev/feast:docs/reference/offline-stores/chronon.md:51-59`; `feast-dev/feast:docs/reference/online-stores/chronon.md:81-99`.

## Combining Feast and Chronon

Standalone execution diagrams and responsibilities are maintained in the
[feature workflow](feature-workflow.md). The bridge is a distinct integration:

```text
Possible combination, shipped in Feast v0.67:

 Feast registry / consumer APIs
          +--> read Chronon-produced Parquet (local historical join)
          +--> call Chronon feature-serving service
                      |
              Chronon still owns computation/materialization/serving
```

## Ten primary source URLs, quotations and sections

S1. Feast v0.10 architecture, release-pinned:
<https://github.com/feast-dev/feast/blob/v0.10.0/docs/concepts/architecture.md#L5-L28>
Quotation: “ELT/ETL systems like Spark and SQL are used to transform data in the batch store.” Sections Functionality and Components establish the external-compute versus materialize/retrieve boundary. Release date 2021-04-15 verified through GitHub release metadata.

S2. Feast v0.13 release:
<https://github.com/feast-dev/feast/releases/tag/v0.13.0>
Quotation: “On demand feature views, which allow for consistently applied transformations in both training and online paths.” Dated 2021-09-22; explicitly experimental at introduction.

S3. Feast current BatchFeatureView implementation:
<https://github.com/feast-dev/feast/blob/v0.67.0/sdk/python/feast/batch_feature_view.py#L81-L112>
Quotation: “Batch feature views are experimental features in alpha development.” Signature directly exposes dependent BatchFeatureViews, transformation and aggregation arguments. This refutes “no computation” without overstating maturity.

S4. Feast current point-in-time semantics:
<https://github.com/feast-dev/feast/blob/v0.67.0/docs/getting-started/concepts/point-in-time-joins.md#L65-L85>
Quotation: “By default, point-in-time joins only constrain the feature's event timestamp.” See created-time filtering and unsupported-backend caveats.

S5. Feast current Chronon online bridge:
<https://github.com/feast-dev/feast/blob/v0.67.0/docs/reference/online-stores/chronon.md#L3-L7>
Quotation: “This integration is a bridge, not a Feast-managed online store.” Same file lines 81-99 give the functionality limits. Integration appeared in v0.67.0, released 2026-10-13.

S6. Original Airbnb declarative-framework explanation:
<https://medium.com/airbnb-engineering/chronon-a-declarative-feature-engineering-framework-b7b8ce796e04>
Quotation: “Chronon can compute in two contexts, online and offline with the same compute definition.” Read Background; Understanding accuracy; Understanding data sources; Understanding computation contexts; Understanding computation types. Exact publication date unverified in this bounded research.

S7. Chronon consistency limitations:
<https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/test_deploy_serve/Online_Offline_Consistency.md#L13-L29>
Quotation: “While 100% consistency is desirable, it is not always possible.” Lines 33-80 describe comparing logged online results with historical recomputation.

S8. Chronon tiled architecture:
<https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/Tiled_Architecture.md#L5-L37>
Quotation: “This architecture shifts a significant part of aggregation work to the write path.” Contrasts per-event reads with pre-aggregate merging; explicitly requires Flink for this documented tiled architecture.

S9. Original Airbnb open-source announcement:
<https://medium.com/airbnb-engineering/chronon-airbnbs-ml-feature-platform-is-now-open-source-d9c4dba859e8>
Quotation: “Chronon requires ML practitioners to define their features only once.” Read Background; Backfills/offline computation; Online computation; Online-offline consistency. Batch seeding and window compression are explicitly described. Natural-language authoring and agentic model improvement appear under “What's next?”, not shipped feature claims. Exact publication date unverified here.

S10. Airbnb 2026 near-real-time evolution:
<https://airbnb.tech/ai-ml/the-guest-journey-updated-in-real-time-extending-airbnbs-sequence-recommender-with-chronon/>
Quotation: “these capabilities have been contributed back to our public repo.” Read Push Mode in Chronon; Near-real-time model transform in Chronon; Integration; Results. Publication date 2026-09-23 visible in Airbnb's own search index. Code existence cross-checked in v0.0.115.

## Open-source Feathr: scope and comparison boundary

The [public Feathr documentation](https://feathr-ai.github.io/feathr/) and
[repository](https://github.com/feathr-ai/feathr) describe Python APIs for raw
sources, transformations, named features, and registry-based discovery/reuse.
They cover PySpark/Spark SQL UDFs, aggregation and time windows, point-in-time
training joins, and materialization for online use. Documented entry points
include Local Sandbox, Azure Synapse, and Databricks.

Unlike the externally computed Feast example in the [workflow](feature-workflow.md),
this public project explicitly includes feature-computation authoring.
That alone does not establish identical streaming/window behavior across
deployments or Chronon-style batch/stream state merging. Compare the actual
runtime, storage integration, and temporal semantics rather than the presence
of Python definitions or a registry.

This is a public-project overview checked on 2026-11-18, not a version-pinned
deployment validation. It does not infer private implementations from a shared
project name.

## Architectural fit

If an organization already has a mature distributed computation platform and primarily needs a common feature contract, catalog, historical retrieval and low-latency access across heterogeneous teams, a Feast-like boundary may fit without transferring ownership of every feature pipeline.

If its dominant problem is that every new temporal feature requires duplicate batch/stream code, custom backfills and skew debugging, a Chronon-like integrated definition/execution model addresses that problem more directly.

These are architectural fit hypotheses. Compare ownership of transformation definitions, temporal semantics, orchestration, state, materialization, backfills and correctness monitoring rather than API resemblance alone.

## Choosing between them: worked reasoning

The three cost budgets, evidence quality and the absence of public head-to-head
selections are in [industry adoption](industry-adoption.md#selection-and-engineering-costs).
This section only works through the reasoning with examples.

### Why Chronon taking on more computation does not cause broad migration

A team weighs the work it saves after adoption, not the work a framework could do.

- If purchase totals already come from mature Spark/Flink jobs that also handle
  refunds, corrections and deduplication, and reports or rule engines consume the
  same outputs, a Feast-style integration keeps those jobs and adds registration,
  writes and reads. Getting Chronon's benefit requires moving that computation into
  GroupBy/Join definitions, comparing old and new outputs, preparing state, and
  often keeping the old jobs for their other consumers.
- Chronon can also serve precomputed values, but then its window, backfill and
  batch/stream merge machinery is unused.
- Many features are hard for reasons other than windowed aggregation, as in the
  embedding example below.
- Framework-provided implementation is not operated for you. Adopting open-source
  Chronon still means integrating and running Spark, streaming, KV and
  orchestration (see [architecture](architecture.md)).

### A new company with no existing pipelines

"Simple use cases fit Feast" is too coarse. A simple business formula does not
mean a simple computation system.

| Need | Lean | Why |
|---|---|---|
| Daily membership tier or lifetime spend, read by user ID | Scheduled SQL plus a store; add Feast when shared registration and retrieval matter | No window state to maintain; Chronon's fixed integration cost buys little |
| 1-hour, 1-day and 7-day spend updated in real time, plus backfills over past requests | Chronon | State updates, window evaluation and point-in-time backfill are exactly what would otherwise be built |
| Embeddings produced by a GPU job, read by ID | A Feast-style access layer | The hard part is inference, not windowed aggregation |

No legacy pipelines strengthens the case for Chronon in the second row; it does
not change the first or third.

### Example where a Feast-style boundary fits: product-image embeddings

An e-commerce site has one million products. A recommender reads a 512-dimension
image embedding per product:

```text
product image -> decode/resize -> image encoder on GPU -> embedding
              -> write keyed by product_id -> recommender reads by ID
```

The hard work is in the producing job: GPU batch sizing and memory, retrying only
failed images, recomputing when the encoder moves from v1 to v2 (two 512-d vectors
from different encoders must not be mixed), and preventing a slow job for an old
image from overwriting the vector of a newer image.

Feast covers what remains: register the feature, write results with fields such as
`image_version` and `encoder_version`, batch-read by product ID online, and keep
timestamped history for point-in-time training. A version field does not by itself
manage a version switch. Chronon could ingest the same precomputed vectors, but
there are no event-to-window aggregations for its extra machinery to do. This does
not show Feast is faster for embeddings or that Chronon cannot serve them.

### How the same system evolves until Chronon fits better

The recommender starts using what the user is doing now:

```text
13:40  user 42 clicks sneaker A
13:55  user 42 clicks sneaker B
14:00  request: score sneaker C for user 42
14:05  user 42 clicks sneaker D
```

At 14:00 the model needs the embedding of C (unchanged pipeline), clicks on the
same category in the last hour (2, excluding the 14:05 click), and 1-day and
7-day purchase counts and amounts in that category. Two weeks later the team wants
to test 10-minute clicks and 14-day spend, trained on 90 days of past requests.

| Work | Feast plus own computation | Chronon |
|---|---|---|
| New windows online | Change the stream job, prepare new state, push results | New GroupBy windows; run the corresponding batch upload and streaming |
| New windows for past requests | Recompute from raw behavior logs, then historical retrieval | Join backfill recomputes from retained behavior history |

Each experiment otherwise needs its own offline recomputation and online update
logic. That repetition, not the growth from one million to 100 million products,
is the point at which Chronon becomes the better fit. Adopt it for the behavior
features first; the GPU embedding pipeline and its versioning stay where they are.

## Write pipelines: Feast resembles Venice

[Venice](https://github.com/linkedin/venice) describes itself as a derived data
storage platform with asynchronous ingestion from batch and streaming sources
(its README names Hadoop and Samza) and low-latency reads via remote queries or
in-process caching. Its users run the jobs that compute and push the data.

In the common external-compute path of the [feature workflow](feature-workflow.md#3-feast-compute-first-then-standardize-feature-access),
Feast is used the same way: producers keep running batch (Spark) and nearline
(Flink) jobs that compute values and push them. Feast adds definitions,
historical retrieval, materialization and online access on top of databases
such as Redis or DynamoDB that it connects to rather than implements. It is not
another storage engine, and its compute extensions (above) can own part of the
computation.

Chronon instead derives the per-feature computation from a declarative
definition, so no per-feature write pipeline is hand-maintained. The jobs
themselves still run:

| Work | With Chronon |
|---|---|
| Hand-written window and aggregation code | Replaced by framework implementations |
| Separate batch and streaming business logic | One definition drives both paths |
| A new supported window or aggregation | A definition change, plus backfill and release |
| Raw schema changes | Still require source mapping and validation |
| Stream lag, job failure, OOM, slow or full KV | Still operated by someone, usually a platform team |
| Framework upgrades, custom connectors, special logic | Still development and maintenance work |

"Integrate once and almost never maintain" is therefore wrong; the work moves to
shared implementations and a platform owner. Chronon also does not compute from
raw data at query time: batch precomputation, streaming updates and fetch-time
merging share the work, depending on accuracy and runtime.

## Open idea: serving features next to the application

This is the note author's idea, not an upstream proposal or a tested design:
merge or co-locate Chronon feature serving with the data on the application's
own nodes, to remove network hops while keeping the same SQL semantics. One candidate examined was
DuckDB as the local executor. Nothing below was implemented or benchmarked.

Verified facts at the baseline commit:

- Fetcher already embeds in a JVM application (`JavaFetcher`); the REST service
  is optional. See [architecture](architecture.md#4-fetch-features-for-an-application).
- An optional on-heap batch IR cache exists. It is created only when the
  `ai.chronon.fetcher.batch_ir_cache_size_elements` system property is set, is
  enabled per GroupBy, and is bounded by entry count rather than bytes
  ([FetcherCache.scala#L24-L46](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/FetcherCache.scala#L24-L46),
  [LRUCache.scala#L19-L37](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/LRUCache.scala#L19-L37)).
- A batch cache hit still sends the streaming request for temporal GroupBys
  ([FetcherBase.scala#L438](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/FetcherBase.scala#L438)),
  so an embedded Fetcher with a warm cache still depends on the KV store.
- Cache keys use dataset, keys and `batchEndTsMillis`; a same-day batch re-run with
  different data is indistinguishable, and the source comment says to restart the
  Fetcher ([FetcherCache.scala#L166-L170](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/FetcherCache.scala#L166-L170)).
  [airbnb/chronon#1171](https://github.com/airbnb/chronon/pull/1171), merged on
  2026-11-11 after the baseline, adds upload rollback support and lists this case
  as an unchanged limitation.
- Window merging uses Chronon's aggregator, not SQL
  ([FetcherBase.scala#L185](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/FetcherBase.scala#L185),
  [#L232](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/FetcherBase.scala#L232)).
- Online derivations skip SQL when empty or rename-only; otherwise they run through
  a pooled Spark Catalyst plan, using whole-stage code generation where the plan
  allows
  ([DerivationUtils.scala#L53-L67](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/DerivationUtils.scala#L53-L67),
  [CatalystUtil.scala#L141-L265](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/online/src/main/scala/ai/chronon/online/CatalystUtil.scala#L141-L265)).
- DuckDB's dialect is based on PostgreSQL, with documented exceptions
  ([dialect](https://duckdb.org/docs/2.0/sql/dialect/overview.html)); it is not a
  Spark SQL implementation. In-process, only one process may open a database for
  writing ([concurrency](https://duckdb.org/docs/2.0/connect/concurrency.html)).

Constraints that follow:

| Change | What it removes | What it adds |
|---|---|---|
| Embed Fetcher in the application | The feature-service hop | Fetcher CPU, heap and GC inside the application |
| Local replica of batch and streaming state | KV round trips | Snapshot plus delta distribution, atomic version switches, freshness tracking, restart warm-up, miss fallback, memory per node |
| Replace the executor with DuckDB | Spark Catalyst and aggregator in the request path | A second engine whose results must match Chronon's |

Locality gains do not require DuckDB. "Same SQL semantics" has three levels:
expression syntax; Spark SQL types, nulls, casts, timestamps and functions; and
Chronon's temporal semantics (sawtooth windows, batch/stream merge, late events).
DuckDB local execution cannot be assumed to give identical results at the second
or third level, so a realistic target is a validated subset.

A test that separates the effects compares remote Chronon, local state with the
existing Fetcher, and local DuckDB on the same data, freshness and expected
output, measuring p50/p99 under concurrent reads and updates, heap/GC and native
memory, and cold start.

## Gaps and tool limitations

No feature code was executed or benchmarked; support was checked in public docs, releases and source. No installation, remote write or third-party integration was performed. Two initially guessed article URLs failed; authoritative links were subsequently recovered from Airbnb's official site and their contents read successfully. Direct Medium requests for publication metadata returned HTTP 403; article bodies were available through web_fetch. No universal performance comparison or first-production-use date is asserted.

## Follow-up: custom UDFs, historical evidence, and the precise ownership distinction

Custom-function expressiveness and control over an execution runtime are different questions. Public source can establish UDF support without establishing that arbitrary PyFlink programs run unchanged inside Chronon.

Historical public evidence is available in v0.0.93, released on 2025-04-11:

- The Python Query API explicitly says “You can also apply udfs” and documents registration with `setups=["ADD JAR YOUR_JAR", "create temporary function YOU_UDF_NAME as YOUR_CLASS"]`. Source: `airbnb/chronon:api/py/ai/chronon/query.py:36-60` at v0.0.93. URL: https://github.com/airbnb/chronon/blob/v0.0.93/api/py/ai/chronon/query.py#L36-L60
- Tests in the online module exercise both simple and complex custom UDFs. `testComplexUdfsWithCommonScalarsShouldWork` registers a two-argument UDF, a composed UDF and a recursive Fibonacci UDF, evaluates them through CatalystUtil, and asserts the results. This is direct test-source evidence of expressiveness beyond simple built-in aggregation, not a performance endorsement of recursive UDFs. Source: `airbnb/chronon:online/src/test/scala/ai/chronon/online/test/CatalystUtilTest.scala:262-307` at v0.0.93. URL: https://github.com/airbnb/chronon/blob/v0.0.93/online/src/test/scala/ai/chronon/online/test/CatalystUtilTest.scala#L262-L307
- Historical online derivation implementation constructs a PooledCatalystUtil from expression definitions and applies it to combined keys and feature values. Source: `airbnb/chronon:online/src/main/scala/ai/chronon/online/OnlineDerivationUtil.scala:38-59,80-91` at v0.0.93. URL: https://github.com/airbnb/chronon/blob/v0.0.93/online/src/main/scala/ai/chronon/online/OnlineDerivationUtil.scala#L38-L59
- Current derived-feature documentation explicitly describes Spark-SQL transformations shared between offline Spark/Hive computation and online production service computation; examples include map/array expressions. Source: `airbnb/chronon:docs/source/authoring_features/DerivedFeatures.md:5-13,99-115` at v0.0.115.
- Current extension interfaces go beyond expressions: ModelBackend exposes online inference and an overridable batch-inference method; KVStore and StreamDecoder integration points were documented above. Source: `airbnb/chronon:online/src/main/scala/ai/chronon/online/ModelBackend.scala:33-53` at v0.0.115. This does not establish when every model-transform capability first existed.

Scope caution: these findings do not prove that any Python closure, stateful PyFlink operator, external dependency or custom UDF automatically executes unchanged in batch, Spark Streaming, Flink and online serving. Registration, packaging, runtime compatibility, state/timer control and operational tooling remain concrete evaluation questions. Nor should one infer that Join derivations and GroupBy derivations have identical support: the v0.0.115 GroupBy API docstring still describes its derivations as offline-only (`api/py/ai/chronon/group_by.py:196-204,546-549`).
