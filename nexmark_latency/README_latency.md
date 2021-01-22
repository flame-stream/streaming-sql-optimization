# Measuring Streaming SQL Queries Latency with Nexmark

## Query
See the file `SqlQuery15.java`. The current query is just two joins without much meaning but it could be any query (see that necessary adjustments are made to class `Latency`. I know, I know, it's not the smartest design in the universe).

## How to run
`./gradlew :sdks:java:testing:nexmark:run -Pnexmark.runner=":runners:flink:1.11" -Pnexmark.args="--runner=FlinkRunner --query=15 --queryLanguage=sql --streaming=true --manageResources=false --monitorJobs=true --flinkMaster=[local] --latencyLogDirectory=/home/your/log/directory/" `

## Implementation details
`systemTime` field has been added to `Event` to specify the event generation time. 
This example uses a query with two joins, so the resulting tuples will have three `systemTime` columns for each item that makes up the tuple. The latency is calculated as the difference between the output time of this tuple (`arrivalTime` in code, meaning arrival from join) and the maximum value out of the three `systemTime` column values. For each window, latency can be calculated as the mean value of all latencies in that window or as the latency of the element with the maximum timestamp (meaning, the maximum value out of three `systemTime` values. that's the timestamp. now we select the maximum out of all of those).

## Statistics
This is currently placed in the Nexmark package, can be moved later.

In order to provide custom statistics, we extend `BeamPCollectionTable` in `NexmarkTable`. `NexmarkSqlTransform` extends `SqlTransform` and helps pass the custom values for statistics from the query (in Nexmark terms, not in SQL terms) to the tables.
Currently the only statistics used are the rates but it could be anything, really, as long as it can be represented as `BeamTableStatistics`.