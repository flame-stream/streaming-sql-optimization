package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.stream.Stream;

interface Coordinator {
    UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>
        registerInput(String tag, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source);
    Stream<UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs();

    QueryContext start(SqlQueryJob sqlQueryJob);

    interface SqlQueryJob {
        String query();
        Stream<PTransform<PCollection<Row>, PDone>> outputs();
    }

    interface QueryJobBuilder {
        QueryJobBuilder addOutput(PTransform<@NonNull PCollection<Row>, @NonNull PDone> sink);

        QueryJobBuilder setPreHook(PTransform<@NonNull PCollection<Row>, @NonNull PCollection<Row>> hook);
        QueryJobBuilder setPostHook(PTransform<@NonNull PCollection<Row>, @NonNull PCollection<Row>> hook);

        QueryJobBuilder registerUdf(String functionName, SerializableFunction<?, ?> sfn);
        QueryJobBuilder registerUdf(String functionName, Class<?> clazz, String method);
        QueryJobBuilder registerUdf(String functionName, Combine.CombineFn<?, ?, ?> combineFn);

        SqlQueryJob build(String query);
    }

    interface QueryContext {
//        void addStatsListener(Consumer<QueryStatistics> consumer);
        void stop();
    }
}
