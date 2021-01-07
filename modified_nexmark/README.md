# Modified beam's NEXMark benchmark

## Information
Our new query is now exist in SqlQuery3, replacing old Query3
TODO: move our query to NEW Query13 and return old implementation of Query3

## How to run
./gradlew :run \
    -Pnexmark.runner=":runners:flink:1.10" \
    -Pnexmark.args="
        --runner=FlinkRunner
        --query=3
        --queryLanguage=sql
        --streamTimeout=60
        --streaming=true
        --manageResources=false
        --monitorJobs=true
        --enforceEncodability=false
        --enforceImmutability=false"
