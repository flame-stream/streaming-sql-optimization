package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.latency.Latency;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.*;
import org.apache.flink.api.common.JobStatus;
import org.checkerframework.checker.nullness.compatqual.NonNullType;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Interface for main agent of optimization, that coordinates pipeline running
 * and decides if the graph changing is needed using handled stats.
 */
public interface Coordinator {
    <T> void registerInput(
            UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
            Schema sourceSchema,
            Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping,
            Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms);
    Stream<UnboundedSource<?, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs();

    RunningSqlQueryJob start(SqlQueryJob sqlQueryJob);
    RunningSqlQueryJob start(SqlQueryJob sqlQueryJob, boolean switchGraphs);
    void stop(RunningSqlQueryJob runningSqlQueryJob);
    Stream<? extends RunningSqlQueryJob> runningJobs();
    JobStatus status() throws InterruptedException, ExecutionException;

    interface SqlQueryJob {
        String query();
        Stream<PTransform<PCollection<Row>, PDone>> outputs();
        default Stream<PTransform<PCollection<Latency>, PDone>> latencyOutputs() {
            return Stream.empty();
        }
        WindowFn<Object, ? extends BoundedWindow> windowFunction();
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
