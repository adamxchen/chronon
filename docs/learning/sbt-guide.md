# SBT for Chronon: A Make / Gradle User's Guide

Scope: this repository. The original notes use SBT 1.8.2, Scala 2.12.12, Spark 3.1.1, and JDK 8; they are not a claim about every version or platform.

## Build model and setup

| Make / Gradle concept | SBT equivalent |
|---|---|
| `Makefile`, `build.gradle.kts` | `build.sbt`, using a Scala DSL |
| Gradle wrapper version configuration | `project/build.properties`, pinning SBT |
| Gradle build plugins | `project/plugins.sbt` |
| Gradle subproject | SBT `Project`, such as `api` or `spark_uber` |
| Target/task dependencies | Task keys and a task dependency graph |
| Main/test source configurations | `Compile`, `Test`, and other configurations |
| Gradle `build/` | Each module's `target/` |

`build.sbt` defines settings and a task graph; it is not a Scala script executed from top to bottom. SBT evaluates dependencies when a task runs and supports incremental compilation. The `project/` directory contains the build's own code, not application sources; its Scala version can differ from the application's `scalaVersion`.

Prepare a compatible JDK, SBT, Thrift, and Python dependencies. Use the SBT version in `project/build.properties`; this is not a complete installation guide. Commands assume `sbt` is available in the terminal, without machine-specific caches or personal shell functions.

From the repository root, verify the checkout:

```bash
sbt "show root / baseDirectory"
```

The output should identify the checkout being edited.

## Scopes and keys

Commands use `project / configuration / key`:

```text
spark_uber / Compile / compile   Compile main sources
spark_uber / Test / compile      Compile test sources without running tests
spark_uber / Test / test         Run tests
```

Common shortcuts are `spark_uber/compile` and `spark_uber/test`.

- A setting holds configuration, such as `scalaVersion` or `libraryDependencies`.
- A task performs work, such as `compile` or `test`.
- An input task takes arguments, such as `testOnly`.

`show` retrieves a value and, for a task, may execute its dependencies. `inspect` shows a key's definition, origin, and dependencies.

## Interactive development

Run `sbt` to keep a shell open and reuse build loading and JVM/JIT state:

```text
projects
project spark_uber
compile
testOnly ai.chronon.spark.test.JoinTest
project root
exit
```

`project spark_uber` changes the current project for unprefixed commands, not the build files. This is a build-tool shell; `console` opens a Scala REPL with the project's classpath.

To rerun after source changes, prefix a task with `~`; press Enter to stop watching:

```text
~compile
~testOnly ai.chronon.spark.test.JoinTest
```

Use `reload` after editing `build.sbt` or build logic under `project/`, not after ordinary source changes.

## Command reference

These are terminal batch commands. Quote commands containing spaces.

| Purpose | Command |
|---|---|
| Compile main sources | `sbt compile` |
| Compile, but do not run, tests | `sbt "Test / compile"` |
| Compile a module and its code dependencies | `sbt "spark_uber/compile"` |
| Test a module | `sbt "spark_uber/test"` |
| Test one class | `sbt "spark_uber/testOnly ai.chronon.spark.test.JoinTest"` |
| Build a regular JAR | `sbt package` |
| Build the Spark assembly JAR | `sbt "spark_uber/assembly"` |
| Show the application Scala version | `sbt "show scalaVersion"` |
| Show the actual test classpath | `sbt "show spark_uber / Test / fullClasspath"` |
| Show the module dependency tree | `sbt "spark_uber/dependencyTree"` |
| Inspect version conflicts and evictions | `sbt "spark_uber/evicted"` |
| Inspect a task definition | `sbt "inspect spark_uber / Test / test"` |

`package` normally includes only the module's compiled output and resources. The `assembly` plugin combines dependencies, subject to `provided`, shading, and merge-strategy settings.

## Multi-module relationships

Chronon's root project resembles:

```scala
lazy val root = (project in file("."))
  .aggregate(api, aggregator, online, spark_uber, spark_embedded, service, flink)
```

`aggregate` forwards root tasks to subprojects. It does not establish code dependencies or execution order; aggregated tasks can run in parallel.

By contrast:

```scala
lazy val aggregator = project
  .dependsOn(api % "compile->compile;test->test")
```

`dependsOn` establishes compilation order and classpath relationships. Here, `test->test` lets aggregator tests use API test helpers; it does not run all API tests when testing aggregator. Verify each module's test result rather than relying on a downstream pass.

## Dependency syntax and version scope

```scala
scalaVersion := "2.12.12"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
```

| Syntax | Meaning |
|---|---|
| `:=` | Define or replace a key's value |
| `+=` / `++=` | Append one item / multiple items to a collection |
| `.value` | Reference another key; also declares dependencies in task definitions |
| `%` | Use the given artifact name |
| `%%` | Append the Scala binary-version suffix, such as `spark-sql_2.12` |
| Trailing `% Test` | Include the dependency only in the test configuration |

Chronon's `build.sbt` controls its Scala/Spark version matrix; `%%` does not make arbitrary combinations compatible.

```text
++ 2.13.6
```

This switches the build's Scala version, not SBT. Related Spark dependencies also change according to the matrix. For the scope of these notes, start with the default Scala 2.12.12 / Spark 3.1.1 combination. Flink declares support only for Scala 2.12, so not every module supports every combination. Do not treat `+ test`, which executes across versions, as the everyday equivalent of `test`.

## Validation boundaries

Compile main/test sources and create regular packages:

```bash
sbt compile "Test / compile" package
```

Run the six independent JVM test groups for the default version:

```bash
sbt "api/test" "aggregator/test" "online/test" \
    "service/test" "flink/test" "spark_uber/test"
```

Batch execution stops on failure. To collect every module's failures, run modules separately and record each exit code.

- Root `test` depends on aggregation and custom module tasks; it does not cover every version or language.
- `spark_embedded / Test / test` is defined as an empty task in the setup described here. A green result is not evidence of independent test execution.
- Python tests are outside SBT's JVM `test`; they need the project's official Python dependencies and `tox.ini`.
- These commands cover the default Scala/Spark combination, not the full upstream matrix, and do not establish that tests have passed in the current environment.
- Check Python/Spark fixture compatibility separately on platforms such as macOS.

Avoid routine `clean`: it discards incremental compilation results, and Chronon's Spark configuration also includes `/tmp/chronon` in its cleanup scope.

For daily work, confirm the project and checkout, use module-level `compile`/`testOnly`, then run the affected module's full tests after a change. Diagnose dependency problems with `fullClasspath`, `dependencyTree`, and `evicted` before changing application code.

## References

- [Running SBT](https://www.scala-sbt.org/1.x/docs/Running.html)
- [Multi-project builds](https://www.scala-sbt.org/1.x/docs/Multi-Project.html)
- Repository files: `build.sbt`, `project/build.properties`, `project/plugins.sbt`
