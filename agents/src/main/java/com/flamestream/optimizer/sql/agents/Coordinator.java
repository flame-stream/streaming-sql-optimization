package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import java.util.stream.Stream;

interface Coordinator {
    UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>
        registerInput(String tag, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source);
    Stream<UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs();

    Planner getPlanner();
    CostEstimator getCostEstimator();

    QueryContext start(SqlQueryJob sqlQueryJob);

    interface SqlQueryJob {
        String query();
        Stream<PTransform<PCollection<Row>, PDone>> outputs();
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

    interface QueryContext {
//        void addStatsListener(Consumer<QueryStatistics> consumer);
        void stop();
    }
}
