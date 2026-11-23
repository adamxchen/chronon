# System Architecture

Chronon provides feature definitions, batch and streaming computation, and online
feature fetching. It runs on existing compute and storage infrastructure rather
than replacing those systems.

## Runtime and storage boundaries

![Chronon runtime boundaries: optional Airflow orchestration schedules batch and streaming jobs. Historical backfill writes features for training; batch preparation and streaming feed online KV storage. A separate feature-service JVM contains the REST handler, Fetcher, and local KV adapter. Blue denotes repository code, orange third-party infrastructure, and green feature-author or platform integration code.](assets/architecture.png)

[Full-size diagram](assets/architecture.png).

Backfill produces historical training data; batch and streaming prepare online
features. These are separate paths driven by shared definitions, not a requirement
to run a training backfill before serving.

The diagram shows a standalone REST feature service. An embedded Fetcher is an alternative.
Outlines group related computation and application code, not security zones or one
Chronon server. Airflow is shown as an optional scheduler, with Chronon DAG code
and deployment-specific task hooks. Source/KV adapters are nested inside their callers;
implementation colors do not assign on-call ownership.

Inside Airflow, orange arrows show task order, not data transfer: wait for input
data, submit the online-batch Spark job, then trigger the KV loading task after
success. Airflow schedules these tasks and applies dependency and retry policies;
Spark and the loader do the computation and data writes. Its streaming task
checks whether the Spark Streaming job is running and starts it if absent.
Neither event processing nor feature requests pass through Airflow.

## 1. Define features and launch jobs

Chronon has a control plane, but the provided workflow does not require a single
always-running Chronon controller. Configuration tooling, DAG constructors,
`run.py`, and deployment integrations implement its control actions; Airflow is
the default orchestrator. The configuration repository holds feature definitions.
The online KV backend holds published serving configuration and feature data,
not the scheduler's task state. The Fetcher reads and caches serving metadata
from KV to interpret requests; KV does not execute that control logic.

The diagram's upper control lane expands to:

```text
Python definitions -> compile.py -> compiled configurations
                                         |
                         +---------------+----------------+
                         |                                |
                 Job launch integration             metadata-upload
                 (run.py / scheduler)                     |
                         |                                |
                 Batch / streaming runtimes            Online KV
```

Feature authors use Chronon's Python API to declare:

- `Source`: input tables or streams, selected fields, and time columns.
- `GroupBy`: keys, aggregations, windows, and accuracy.
- `Join`: the features to combine and the historical request records to enrich.

`compile.py` validates these objects and writes Thrift JSON configurations under
`production/`. Constructing or compiling a definition does not itself perform the
distributed computation.

Chronon's Airflow constructors build tasks from the compiled configurations.
`run.py` selects execution modes and invokes the appropriate entry points.
Deployments supply cluster submission scripts, configuration distribution, and
other environment-specific integrations.

Airflow is optional: `run.py` can also be invoked manually or by another
orchestrator. The provided Airflow code includes integration points, such as the
KV upload operator, that need to be implemented for the deployment. It is not a
complete hosted control-plane service.

See [Control Plane Overview](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/setup/Overview.md) and
[Orchestration](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/setup/Orchestration.md).

## 2. Compute historical features and prepare online batch data

Chronon's `Join`, `GroupBy`, and `GroupByUpload` code runs on Apache Spark.
There are two distinct outputs:

| Path | Inputs | Output |
|---|---|---|
| Historical Join | Source history and historical request records containing keys and timestamps | A table containing request records and their associated feature columns |
| Online batch preparation | Source history and a GroupBy definition | An upload table containing encoded keys, values or intermediate states, and GroupBy serving information |

For a temporal Join, Chronon computes features for the historical request times.
The Join can compute its GroupBy parts directly; a separate GroupBy snapshot
backfill is not a prerequisite. Model training consumes the resulting feature
table outside Chronon.

Online batch preparation is not the same as writing to the online database.
`GroupByUpload` produces an upload artifact. A deployment-specific uploader then
loads it into the KV store. Depending on the accuracy and data model, uploaded
data can contain final snapshot values or intermediate representations (IRs) of
aggregation state needed for temporal queries.

The platform team connects this loading path, rather than each feature author
implementing a loader. The repository provides `Spark2MongoLoader` as a MongoDB
quickstart example. Other backends need a suitable reused or implemented loader.
The Airflow `get_kv_store_upload_operator` hook is a deployment placeholder;
the example `MongoKvStore.bulkPut` is also unimplemented, so the quickstart uses
the separate Spark loader.

See [Joins](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/Join.md) and
[Online Integration](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/setup/Online_Integration.md).

## 3. Process new events

For features with a streaming input, Chronon provides processing code that runs
on Spark Streaming or Flink. These are alternative execution paths, not two
runtimes that every deployment must run together.

The `run.py --mode streaming` entry point submits the Spark streaming path.
Flink has a separate integration path through `FlinkJob`.

The online storage strategy determines what streaming jobs write:

- Untiled processing writes encoded events or changes for later aggregation.
- Tiled processing pre-aggregates events into tiles. The Fetcher subsequently
  merges these intermediate states instead of processing every individual event.
  This path uses Flink.

Deployments connect the appropriate source and message representation. The Spark
and Flink paths expose different integration interfaces; they do not necessarily
use the same decoder. KV adapters must also support the selected write and
time-based read semantics.

See [Data Integration](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/setup/Data_Integration.md),
[Chronon on Flink](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/setup/Flink.md), and
[The Tiled Architecture](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/Tiled_Architecture.md).

## 4. Fetch features for an application

An application supplies a Join name and entity keys. The Fetcher resolves the
Join, maps keys to its GroupBy parts, and reads the required online data.

The KV store can hold several kinds of data:

- Join configuration and GroupBy serving information.
- Batch values or intermediate aggregation states.
- Streaming events, changes, or pre-aggregated tiles.

For snapshot queries, the Fetcher can decode the batch values without reading a
streaming dataset. For temporal queries, it combines the batch state with the
relevant streaming data, then produces the requested features. Metadata is
cached; it need not be fetched from storage on every request.

The Fetcher is a library, not necessarily a remote service. Applications can
embed `Fetcher` or `JavaFetcher` in their JVM. Alternatively, the `service/`
module provides a REST wrapper using Vert.x. The handler, Fetcher, and storage
adapter can all run in the same service process.

See [Serving](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/test_deploy_serve/Serve.md).

## Shared code and deployment boundaries

The `aggregator/` module implements shared aggregation and intermediate-state
operations used by batch computation, streaming aggregation, and online
evaluation. It is a library included in those processes, not another service to
deploy.

Chronon defines the `Api` and `KVStore` contracts. Deployments provide the
applicable backend implementations and source integrations. These adapters,
omitted from the overview, are loaded into their callers. The streaming job and
Fetcher may use the same integration artifact; they do not call a shared
adapter service.

The infrastructure needed depends on the workload. A snapshot-only deployment
can omit streaming, and an embedded Fetcher does not require the REST service.
Production deployments still need to operate their selected compute, storage,
scheduling, and integration components.

Quality and consistency monitoring, omitted from this runtime overview, involves
multiple jobs: sampled queries and feature values are logged, historical features
are recomputed, and comparison metrics are written to tables.

This diagram omits entity mutations and external features. It does not imply
exact sliding-window boundaries or guarantee that a later backfill reproduces
data that was actually available online.
Window semantics and late-arriving data are covered in
[GroupBy](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/authoring_features/GroupBy.md) and
[Online/Offline Consistency](https://github.com/airbnb/chronon/blob/5bbc4d176a205e32b4172b8f0ecd360072cdb0fe/docs/source/test_deploy_serve/Online_Offline_Consistency.md).

## Source code map

| Area | Entry points |
|---|---|
| Definitions and compilation | [`group_by.py`](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/group_by.py), [`join.py`](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/join.py), [`compile.py`](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/repo/compile.py) |
| Job orchestration | [`run.py`](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/repo/run.py), [`airflow/helpers.py`](https://github.com/airbnb/chronon/blob/main/airflow/helpers.py) |
| Bulk-load integration and example | [`get_kv_store_upload_operator`](https://github.com/airbnb/chronon/blob/main/airflow/helpers.py), [`Spark2MongoLoader.scala`](https://github.com/airbnb/chronon/blob/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/Spark2MongoLoader.scala), [`MongoKvStore.scala`](https://github.com/airbnb/chronon/blob/main/quickstart/mongo-online-impl/src/main/scala/ai/chronon/quickstart/online/MongoKvStore.scala) |
| Batch computation | [`Driver.scala`](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/Driver.scala), [`JoinBase.scala`](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/JoinBase.scala), [`GroupByUpload.scala`](https://github.com/airbnb/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/GroupByUpload.scala) |
| Flink processing | [`FlinkJob.scala`](https://github.com/airbnb/chronon/blob/main/flink/src/main/scala/ai/chronon/flink/FlinkJob.scala), [`FlinkSource.scala`](https://github.com/airbnb/chronon/blob/main/flink/src/main/scala/ai/chronon/flink/FlinkSource.scala) |
| Online integration and fetching | [`Api.scala`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/Api.scala), [`MetadataStore.scala`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/MetadataStore.scala), [`FetcherBase.scala`](https://github.com/airbnb/chronon/blob/main/online/src/main/scala/ai/chronon/online/FetcherBase.scala) |
| Shared aggregation | [`BaseAggregator.scala`](https://github.com/airbnb/chronon/blob/main/aggregator/src/main/scala/ai/chronon/aggregator/base/BaseAggregator.scala) |
| Optional REST service | [`FeaturesHandler.java`](https://github.com/airbnb/chronon/blob/main/service/src/main/java/ai/chronon/service/handlers/FeaturesHandler.java), [`WebServiceVerticle.java`](https://github.com/airbnb/chronon/blob/main/service/src/main/java/ai/chronon/service/WebServiceVerticle.java) |
