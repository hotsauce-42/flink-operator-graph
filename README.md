# flink-operator-graph

Generates the **unchained operator graph** of Flink 2.x DataStream jobs as **draw.io** (`.drawio`) and **Mermaid** (`.mmd`) files. It's the same graph you see in the Flink Web UI with `pipeline.operator-chaining.enabled: false`, plus explicit `keyBy` and `union` nodes, sources, sinks, side outputs and partitioning (REBALANCE, BROADCAST, ...).

Sources = green, operators = blue, sinks = orange, keyBy = yellow diamond, union = grey circle.

## How it works
The tool loads the job jar and runs its `main()` in Flink's plan environment (the mechanism behind `flink info`). When the job calls `execute()`, the environment captures the `StreamGraph` and aborts, so **no cluster is needed, nothing is submitted, and job code doesn't change**. Everything `main()` does *before* `execute()` really runs, so pass dummy args/config if it needs them.

## Usage
```bash
mvn package                       # -> target/flink-operator-graph.jar
java -jar target/flink-operator-graph.jar --config graphs.yaml --out docs/graphs [--format drawio,mermaid]
```
See `graphs.example.yaml`. The exit code is non-zero if any job fails, so CI fails.

Set `flink.version` in `pom.xml` to the Flink version your jobs use.

## GitLab CI
This repo's `.gitlab-ci.yml` builds `flink-operator-graph.jar` on every pipeline. Add this job to each Flink job repo:

```yaml
operator-graphs:
  stage: build
  image: maven:3.9-eclipse-temurin-17
  variables:
    GRAPH_TOOL_PROJECT: "my-group%2Fflink-operator-graph"   # URL-encoded path of this repo
  script:
    - mvn -B package -DskipTests                           # builds the job jars
    - >
      curl -fsSL --header "JOB-TOKEN: $CI_JOB_TOKEN" -o flink-operator-graph.jar
      "$CI_API_V4_URL/projects/$GRAPH_TOOL_PROJECT/jobs/artifacts/main/raw/target/flink-operator-graph.jar?job=build"
    - java -jar flink-operator-graph.jar -c graphs.yaml -o docs/graphs --format drawio,mermaid
  artifacts:
    paths: [docs/graphs]
```
`CI_JOB_TOKEN` can only download from this repo if the job repos are on its allowlist: *Settings → CI/CD → Job token permissions*.

To embed the graphs: Mermaid renders natively in GitLab/GitHub Markdown and in MkDocs Material. `.drawio` files open in draw.io, the draw.io Confluence app, or VS Code's draw.io extension.
