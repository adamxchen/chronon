# Feast and Chronon: Adoption Evidence and Engineering Costs

Historical research as of 2026-10-07, covering material from 2019-2026 and retrospective accounts of earlier work. This is not a current deployment inventory or a product benchmark.

## Scope and evidence

The useful distinction is not "internet companies choose Chronon; financial companies choose Feast." It is which responsibilities a shared platform takes on and which infrastructure choices remain with its users. The cases below examine adoption, selection motives, integration work, and reported outcomes separately.

- A: A company engineering article or company-published employee talk transcript. The jointly authored Gojek/Google announcement qualifies because an adopting company's platform lead participated.
- B: An attributable employee talk, interview, roundtable, or personal account. Summary-only sources are identified.
- C: A project maintainer's or commercial vendor's adoption claim, not a substitute for the customer's explanation.
- Unverified: Insufficient evidence found; not evidence of non-use.

Historical deployments do not establish their architecture in 2026. Without a published evaluation, this report does not infer why a competing product was rejected. Adyen, Merpay, and Stripe provide the most detailed company accounts. Credit Karma, OpenAI, and Depop provide useful critical experiences, but all three spoke at one Zipline-associated roundtable, not three independent market studies.

## Feast cases

### Adyen: An open interface within an independently operated platform

Evidence: A. Andreu Mora's company article directly explains the preference for Feast. [F1]

Adyen wanted to avoid external-vendor dependence in its core payments business without rebuilding existing infrastructure and accumulating feature catch-up work, operational debt, and avoidable bugs. Feast connected its real-time and big-data platforms through consistent feature definitions for training and inference. Adyen retained it during a redesign, explicitly saying it "opted for the openness of Feast."

The article also describes evaluating a LinkedIn system it calls "Feather" and finding it too opinionated. That name is preserved rather than silently corrected to another project; this was not a Feast-versus-Chronon evaluation.

The initial system used Kafka -> Hive -> Spark, Airflow orchestration, Feast definitions, and PostgreSQL online storage. For the redesign, Adyen compared Redis, Cassandra, Cockroach, and PostgreSQL and selected Cassandra. In its workload, Redis's memory cost did not suit the target cardinality, Cockroach's consistency trade-offs affected performance, and PostgreSQL no longer met requirements. These are workload-specific judgments, not a general database ranking.

The team still owned Spark, offline data organization, online storage, real-time computation, and monitoring. Flink was under evaluation, not described as fully deployed. The case illustrates an embeddable feature abstraction with independently chosen backends, not outsourced platform operation.

No Feast-specific cost reduction or performance gain was quantified. Sub-100ms latency, very high availability, and 20-fold scaling included business requirements and design targets, not measured Feast outcomes.

### Merpay / Mercari: Reuse first, then online fraud prevention

Evidence: A. Company-published employee talk transcripts describe distinct 2022 and 2023 deployments. [F2][F3]

Feature count quadrupled during 2020-2021, while data was scattered across BigQuery, GCS, and spreadsheets and models repeatedly processed the same features. Feast offered a common data-source interface, reusable features, and FeatureViews/FeatureServices for assembling model inputs. ML engineers could focus on models while data engineers owned feature generation and management.

In the 2022 account, the offline platform had operated for about a year; online storage and stream ingestion were still planned. By 2023, online Feast and streaming prediction were in use because batch risk predictions could arrive only after a transaction had completed, too late to prevent losses:

```text
Real time: Kafka -> Flink -> Feature Server API -> Spanner online store
Offline: dbt / BigQuery -> scheduled materialization for some features
Training and batch prediction: Vertex Pipelines
Streaming prediction: microservices
```

The team continued to integrate computation, scheduling, and inference services. It also reported a concrete consistency failure: when one FeatureView accepted both streaming writes and batch materialization, stale batch values could overwrite newly updated streaming values. It split online features into two FeatureViews by write path and combined them on reads, while retaining a unified offline view.

This was a problem in that 2023 deployment, not proof of a permanent defect in every current Feast backend. Frequent upgrades and some then-alpha streaming capabilities also required balancing functionality against stability. No Feast-specific p99, QPS, or bill reduction was published.

### Gojek: Eliminating duplicated feature engineering

Evidence: A. The joint announcement by Google's Tim Sell and Gojek platform lead Willem Pienaar identified four problems. [F4]

1. Teams independently rebuilt features for the same business concepts.
2. Definitions and documentation were inconsistent.
3. Ingesting, updating, and serving batch and streaming features required specialized infrastructure skills.
4. Separate historical-training and latest-value serving systems risked inconsistency.

Feast therefore addressed definition, discovery, reuse, and historical/online retrieval, not just low-latency caching. A Gojek data scientist described it as an important part of an end-to-end ML system.

A later project-lead retrospective says the project started in 2018 and was open-sourced later that year; the joint announcement appeared in January 2019. [X1] These sources establish its origins and internal use then, not Gojek's deployment scope in 2026. They provide no independently attributable throughput, latency, or cost measurements.

### Shopify: Pano as a component of Merlin

Evidence: A. Isaac Vidas's company article identifies Pano as an offline/online feature store built on Feast within Shopify's Merlin ML platform. Inference services or their clients use Pano for low-latency feature retrieval. [F5]

Merlin also combines Ray, GKE, and MLServer/FastAPI, illustrating Feast's role within a larger platform. The article focuses on multi-team online inference, deployment, scaling, and monitoring. It does not explain why Feast beat another candidate, disclose Pano's storage backend, or isolate Feast's benefits from those of Merlin as a whole.

### Robinhood: Adoption confirmed by a talk summary

Evidence: B, summary only. The employee talk "How Robinhood Built a Feature Store Using Feast" confirms adoption and describes the need for production stack customization, reliability, observability, and advanced-use-case extensions. [F6]

The full talk or transcript was not obtained, so storage choices, scale, and competing-product evaluations remain unspecified.

## Chronon cases

### Stripe: Payment-path latency and fraud-feature freshness

Evidence: A. Ben Mears's Shepherd article is the most complete external Chronon adoption account in this research. [C1]

Rapidly changing fraud patterns required faster feature development, experimentation, and deployment. Reads sat on the payment-processing path, where latency increased payment API latency, while new transactions also needed to become visible in features quickly. Read latency and freshness were separate, simultaneous requirements.

Stripe evaluated modifying its old platform, building from scratch, and proprietary/open-source options. Chronon met requirements for intuitive Python/SQL APIs, efficient windowed aggregation, online/offline computation, and consistency monitoring. The article does not disclose an evaluation that rejected Feast.

Training data could contain thousands of features and billions of rows. Adoption required substantial engineering:

- Two KV paths: cheaper storage for bulk ingestion and write-once/read-many access, plus low-latency storage for frequent reads and writes.
- Flink integration: writing individual events and reading them back for aggregation did not meet read requirements for high-event-volume features.
- Execution of the Spark SQL expressions used in Chronon definitions inside Flink to preserve semantics.
- Preaggregated tiles in Flink state, periodically written to KV and merged at read time instead of fetching every raw event.
- Integration with customized Airflow, different data organization, and Iceberg writers.
- Simulation of actual batch delay in training so offline features were not more up to date than what online prediction could have seen.

Stripe contributed Flink and tiling back to Chronon. It adopted shared computation semantics, algorithms, and consistency mechanisms, not a maintenance-free service.

Reported outcomes need their original context:

- The Flink path achieved p99 freshness of 150ms. This measures feature-update availability, not read latency.
- A SEPA fraud model using more than 200 features entered production.
- Stripe attributed tens of millions of dollars in additional fraud blocked annually to the new model. This is a model-and-feature business outcome, not isolated Chronon software ROI.
- Storage-cost improvement was not quantified, and total platform TCO was not disclosed.

### Airbnb: Unifying internal systems

Evidence: A/B. An official project page and engineering leader Suqiang Song's first-person account describe internal platform consolidation rather than a conventional external-product selection. [C2][C3]

Song traces the internal project to 2017, when it was called Zipline; consolidation and open-source preparation advanced in 2022, followed by public open sourcing in 2024. The early internal name is not the same thing as today's commercial Zipline platform.

The goal was declarative feature development without requiring ML practitioners to handle Spark, batch/stream computation, and low-level data engineering directly. Several internal approaches had competed; unification required architecture work, adoption efforts, and team mergers. Song reported that approximately 99% of Airbnb features were developed and maintained on the platform in 2024, a self-reported coverage figure rather than a current statistic or model-quality metric.

In a separate 2024 forum discussion, Chronon author Nikhil Simha said they had tried StarRocks and evaluated Tecton, which did not meet their particular scale or time-travel requirements then; they had not actually tried Materialize. [C4] He emphasized upstream data corrections, GDPR selective forgetting, and composable GroupBy/Join definitions. These are dated author statements, not independent evidence of competitors' current limitations. Other participants challenged the composability claims. There is no evidence of an evaluation rejecting Feast.

A 2026 employee talk summary introduces Push Mode and NRT Model Transform for generative recommendations: feature updates trigger downstream work, and streaming pipelines run inference whose outputs become features. It claims freshness improved from about two days to under one minute in the presented case. This is a named-employee talk summary, not an independently verified full performance measurement. [C5]

### Intuit Credit Karma: Reducing a small team's maintenance burden

Evidence: B. German Krikorian and Raj Katakam described early Chronon adoption in the practitioner roundtable. [C6]

The existing platform was mature, had evolved from sharded MySQL to Bigtable and Dataflow, and already handled many features. The team wanted to reduce the burden of maintaining its own tools and technical debt, use a structured DSL for batch/stream definitions and safer time semantics, and accelerate development through community and commercial support. It also wanted to keep Bigtable, its own cloud environment, and control of its data.

This combined the Chronon engine with Zipline Hub, forks, and support; the entire experience cannot be attributed to unmodified OSS. Costs included a steep learning curve, initial gaps in GCP support, feature-versioning differences across versions or forks, and difficult debugging across system boundaries.

A large aggregation POC encountered substantial performance problems. Working with maintainers on a skew-free union join reportedly improved that join by roughly 5-10 times. The transcript contains terminology-recognition errors, and the figure concerns that POC improvement, not a general advantage over other platforms. Adoption reduced some long-term ownership burdens without eliminating early integration, learning, or performance work.

### OpenAI: Product-side traditional ML and Sora recommendations

Evidence: B. Mick Jermsurawong spoke about product infrastructure, rules, traditional ML, and recommendations, not training GPT with Chronon. [C6]

The team initially used LLM prompts for classification, but capacity pressure renewed interest in traditional ML without building its entire infrastructure stack. It first used Chronon batch processing to turn offline analysis and aggregations into online-queryable data for rules, then expanded to real-time Sora recommendations.

The speaker compared internal alternatives:

- Custom counters/Flink made bootstrapping long windows inconvenient.
- Internal real-time OLAP offered more flexible queries, while Chronon's constrained, precomputable expressions helped reduce serving latency.

He described the system as complex and difficult to start without Zipline/open-source-team help. Reaching real-time operation in "about a month" followed an existing period of batch use; it was not a from-scratch platform launch in one month.

High-fan-out recommendation queries made repeated time-bucket reads and merges expensive. Moving more computation to Flink's write side introduced larger state, timers, window expiration, and continuous-write costs. Chronon provided a framework for optimizing those trade-offs, not a way to eliminate them.

### Depop: Platform-owned training sets, backfills, and embeddings

Evidence: B. Ben Magyar described an ongoing migration to Zipline for ranking and retrieval, with gradual expansion into trust. His tenure was short, and the moderator did not ask him to represent the original selection decision; this is migration experience, not a complete candidate evaluation. [C6]

The old system had features and feature views, but product teams still assembled complete training sets. Hundreds of features with distinct windows, upstream dependencies, and backfills delayed experiments while teams filled missing data and checked completeness. Depop wanted the platform to centralize these responsibilities and speed up feature experimentation.

Embeddings previously followed a separate path because of online/offline costs. The speaker said the new cost conditions allowed them into a unified feature system but did not quantify savings. Migration still required staging or additional support for warehouse clustering that differed from expected partitioning and for mismatched Kafka schemas and offline tables.

The previous product was not named; the term "feature views" does not establish that it was Feast.

### Roku: Real-time features, with limited selection detail

Evidence: B, summary only, with an official job posting corroborating the stack. Krishna Chaitanya Chakka's talk summary describes open-source Chronon incremental aggregation for efficient feature computation, scalable real-time serving, and shorter training-to-production cycles. The job posting also lists Chronon in the advertising platform stack. [C7]

The full talk was not obtained. Rejected candidates, deployment scale, cost reductions, and model gains therefore remain unverified.

## Netflix and Visa: Limits of the evidence

### Netflix

Evidence: C. The Chronon README's adoption list, the author's 2026 QCon abstract, and Zipline's official material explicitly claim Netflix adoption; these are more than an unexplained logo. [C8]

No Netflix first-party public explanation of usage scope, selection rationale, or outcomes was found. In 2024, a self-described Netflix feature-infrastructure engineer said they operated a system similar to Chronon but with important differences; that identity was not separately verified. [C9] The statement does not establish non-adoption in 2026, and no public migration history connects the two points.

The supported conclusion is a project-side adoption claim with unverified customer decision details, neither a deeply verified Netflix case nor proof of non-use.

### Visa

Unverified. No reliable first-party account of Feast adoption or selection motives was found. Search results about the Featurespace acquisition do not establish Feast adoption. Visa cannot support a claim that financial companies favor Feast; the confirmed financial-company cases above span both projects.

## Cross-case interpretation

### Selection and engineering costs

No sufficiently strong public account in this research documents a complete, contemporaneous Feast-versus-Chronon head-to-head decision. Many detailed Feast accounts date to 2019-2023, before Chronon's public open-source release in 2024.

The cases instead expose three budgets: model-team effort to develop features and assemble training data; platform-team ownership of computation, storage, orchestration, upgrades, and correctness; and runtime cost under SLA constraints. "Less pipeline work" generally means moving repeated implementation into a shared platform, not removing computation or operations.

Evaluate performance along separate axes: read/tail latency, event-to-availability freshness, backfill/join throughput, KV/memory/state/write-amplification costs, time to experiment or deploy, and reconstruction of what online prediction could see at a historical instant. A more computationally integrated framework is not automatically faster for every workload, and a lighter interface is not automatically cheaper end to end. Comparable engineering-efficiency measurements are rare.

### Product boundaries as of the research date

Historical customer articles are not a current capability matrix. Feast's ComputeEngine documentation includes DAGs for transformations, aggregations, joins, materialization, and historical retrieval, with maturity varying by backend. [X2]

The official 2026-10-05 Feast-Chronon integration article describes a combined arrangement: [X3]

```text
Chronon: computation, materialization, online serving
Feast: registry, feature definitions, Python retrieval APIs
Online reads: use existing Chronon services without another online feature copy
Historical reads: as-of retrieval over Parquet exported by Chronon
```

At publication, the integration had merged into master but was not in the latest released Feast version, 0.66.0. Historical joins ran in local pandas and were memory-limited. The integration did not deploy Chronon, write into it, or provide distributed historical joins. Examples and CI coverage are not verified production-throughput results.

This is concrete evidence that a computation engine and a consumption interface need not be an either/or choice. Chronon is not only a computation tool, and Feast is not necessarily computation-free.

### AI demand and open-source participation

The Chronon cases show concrete demand for fresh, time-correct, backfillable data and intermediate results in the LLM era, including model outputs feeding later stages. They do not establish a future market winner. Nor do the mostly 2019-2023 Feast customer articles establish leadership in production agent/RAG systems. Unified feature registration and access remain useful and can coexist with Chronon.

Feast's 2024 project-lead retrospective describes open RFCs, design work, community meetings, foundation governance, and corporate adoption/contribution. Its governance document defines a contributor -> CODEOWNER -> maintainer path. [X1][X4] These formal collaboration mechanisms do not guarantee quick responses in every module.

Stripe's upstream Flink/tiling work and Credit Karma's join work show external workloads influencing Chronon engineering. The latter involved Zipline support and forks, so equivalent functionality or service cannot be assumed in OSS upstream. Assess external production use, public technical feedback, upstream merges, and individual contributors' review channels separately; GitHub activity alone cannot establish that one project is open and the other insular.

For performance-oriented learning, useful mechanisms are:

- Feast: cross-FeatureView read fan-out, backend batching, serialization/data movement, and correctness across write paths.
- Chronon: high-fan-out time-bucket merging, read/write-side computation placement, skewed joins, state size, and bootstrap/backfill.

The Chronon topics include problems directly described by OpenAI and Credit Karma, not only hypothetical exercises or automatic Spark/Flink execution. Overall, the evidence supports real demand for both pluggable feature interfaces and shared time semantics, incremental computation, and backfills. It does not establish which project is universally more promising or worth pursuing.

## Sources and reading scope

### Feast sources

- [F1] Adyen, Andreu Mora, "Building a feature platform guided by your ethos.", 2023-09-27; full article. The body date is used because it differs from the search-index date.
- [F2] Merpay Tech Fest 2022 employee talk, Li; company-published full transcript, 2022-10-18.
- [F3] Merpay / Mercari 2023 employee talk, Liu and Li; company-published full transcript, 2023-10-23.
- [F4] Gojek / Google, Tim Sell and Willem Pienaar, January 2019; full article. The page displays January 18; structured metadata says January 19.
- [F5] Shopify, Isaac Vidas, "Unlocking Real-time Predictions with Shopify's Machine Learning Platform", 2023-03-10; full article.
- [F6] Robinhood employee talk summary; original [video](https://www.youtube.com/watch?v=-DgInoUa0uI) dated 2021-09-29. The full talk was not obtained.

[F1]: https://www.adyen.com/knowledge-hub/build-ethos-guided-platform
[F2]: https://engineering.mercari.com/blog/entry/20221018-mtf2022-day1-2/
[F3]: https://engineering.mercari.com/blog/entry/20231023-mmtf2023-day2-7/
[F4]: https://cloud.google.com/blog/products/ai-machine-learning/introducing-feast-an-open-source-feature-store-for-machine-learning
[F5]: https://shopify.engineering/shopifys-machine-learning-platform-real-time-predictions
[F6]: https://www.tecton.ai/apply/session-video-archive/how-robinhood-built-a-feature-store-using-feast/

### Chronon sources

- [C1] Stripe, Ben Mears, "Shepherd: How Stripe adapted Chronon to scale ML feature development", 2024-04-15; full article.
- [C2] Airbnb engineering leader Suqiang Song, 2024-04-09; full first-person retrospective, not a company engineering blog.
- [C3] Airbnb official project page; read in full. No clear original publication date; includes a 2025 summit recap.
- [C4] Chronon author's 2024-04-09 forum reply and [discussion context](https://news.ycombinator.com/item?id=39983131); author views, not an independent benchmark.
- [C5] Airbnb employees Pengyu Hou and Yuli Han, 2026-10-06 talk summary; full recording not verified.
- [C6] MLOps Community roundtable, published 2026-06-17; the approximately 51-minute transcript was read in full. The Zipline-associated event includes named Depop, Credit Karma, and OpenAI practitioners.
- [C7] Roku, "Real time ML at Roku", 2025-10-14 talk summary; an undated [official job posting](https://www.weareroku.com/jobs/senior-machine-learning-engineer-devops-sre-austin-texas-united-states) corroborates the stack.
- [C8] Chronon README adoption claim, supplemented by the author's [2026 QCon abstract](https://qconlondon.com/presentation/mar2026/chronon-mixed-workload-data-processing-framework) and [Zipline material](https://www.zipline.ai/); not Netflix first-party decision material.
- [C9] Historical statement by a self-described Netflix engineer, 2024-04-09.

[C1]: https://stripe.dev/blog/shepherd-how-stripe-adapted-chronon-to-scale-ml-feature-development
[C2]: https://www.linkedin.com/pulse/airbnb-chronon-goes-open-source-suqiang-song-oiiwc
[C3]: https://airbnb.tech/opensource/chronon/
[C4]: https://news.ycombinator.com/item?id=39982603
[C5]: https://www.featurestoresummit.com/session/26-daily-batch-to-real-time-chronon
[C6]: https://home.mlops.community/public/videos/zipline-roundtable-episode-building-real-time-ml-systems-with-zipline-chronon
[C7]: https://www.featurestoresummit.com/session/real-time-ml-at-roku
[C8]: https://github.com/airbnb/chronon/blob/main/README.md
[C9]: https://news.ycombinator.com/item?id=39982683

### Cross-project architecture and governance

- [X1] Willem Pienaar, "The Future of Feast", 2024-02-15; project-lead retrospective. Its company list covers adoption and/or contribution, not adoption alone.
- [X2] Feast Compute Engines; capability documentation at the research date.
- [X3] Feast, "Bringing Chronon Features to Feast", 2026-10-05; full article.
- [X4] Feast Governance; governance process at the research date.
- [X5] Chip Huyen, "Self-serve feature platforms: architectures and APIs", 2023-01-08; full architecture background article. Its historical Feast capability descriptions are not treated as current product boundaries.

[X1]: https://feast.dev/blog/future-of-feast/
[X2]: https://docs.feast.dev/reference/compute-engine
[X3]: https://feast.dev/blog/feast-chronon-integration/
[X4]: https://github.com/feast-dev/feast/blob/master/community/governance.md
[X5]: https://huyenchip.com/2023/01/08/self-serve-feature-platforms.html

Unexpanded leads included an Agoda article about moving an old Java Feast fork to Rust serving and more detailed historical Gojek posts. Original-page access was restricted, so search snippets were not expanded into full cases. No login bypass or internal company material was used for this public research. Quantitative claims remain attributed to their speakers; no product benchmark was run.
