package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.nullness.compatqual.NonNullType;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Interface for main agent of optimization, that coordinates pipeline running
 * and decides if the graph changing is needed using handled stats.
 */
public interface Coordinator {
    UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>
        registerInput(String tag, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source);
    Stream<UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs();

    RunningSqlQueryJob start(SqlQueryJob sqlQueryJob);
    void stop(RunningSqlQueryJob runningSqlQueryJob);
    Stream<RunningSqlQueryJob> runningJobs();

    interface SqlQueryJob {
        String query();
        Stream<PTransform<PCollection<Row>, PDone>> outputs();
    }

    interface RunningSqlQueryJob {
        SqlQueryJob queryJob();
        void addPerformanceStatsListener(Consumer<PerformanceQueryStat> consumer);
    }

    interface PerformanceQueryStat {
        // TODO: 7/17/21 add latency and throughput
    }

    interface QueryJobBuilder {
        QueryJobBuilder addOutput(PTransform<PCollection<Row>, PDone> sink);

        QueryJobBuilder setPreHook(PTransform<PCollection<Row>, PCollection<Row>> hook);
        QueryJobBuilder setPostHook(PTransform<PCollection<Row>, PCollection<Row>> hook);

        QueryJobBuilder registerUdf(String functionName, SerializableFunction sfn);
        QueryJobBuilder registerUdf(String functionName, Class<?> clazz, String method);
        QueryJobBuilder registerUdaf(String functionName, Combine.CombineFn combineFn);

        SqlQueryJob build(String query);
    }
}
