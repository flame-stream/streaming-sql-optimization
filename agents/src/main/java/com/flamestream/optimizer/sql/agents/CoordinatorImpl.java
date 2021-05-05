package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.nullness.compatqual.NonNullType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class CoordinatorImpl implements Coordinator {
    private final HashMap<String, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>>
            sourcesMap = new HashMap<>();
    private final Planner planner;
    private final CostEstimator estimator;
    private final Executor executor;

    public CoordinatorImpl(Planner planner, CostEstimator estimator, Executor executor) {
        this.planner = planner;
        this.estimator = estimator;
        this.executor = executor;
    }

    @Override
    public UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>
    registerInput(String tag, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source) {
        sourcesMap.put(tag, source);
        return source;
    }

    @Override
    public Stream<UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs() {
        return sourcesMap.values().stream();
    }

    @Override
    public Planner getPlanner() {
        return planner;
    }

    @Override
    public CostEstimator getCostEstimator() {
        return estimator;
    }

    @Override
    public QueryContext start(QueryJob queryJob) {
        Pipeline pipeline = createPipeline(queryJob);
        executor.startOrUpdate(pipeline, null);
        return new QueryContext() {
            @Override
            public void stop() {
                // TODO
            }
        };
    }

    private Pipeline createPipeline(QueryJob queryJob) {
        Pipeline pipeline = Pipeline.create();
        HashMap<String, PCollection<Row>> tagged = new HashMap<>();
        for (Map.Entry<String, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>>
                inputEntry : sourcesMap.entrySet()) {
            tagged.put(inputEntry.getKey(), pipeline.apply(Read.from(inputEntry.getValue())));
        }

        PCollectionTuple withTags = PCollectionTuple.empty(pipeline);

        for (Map.Entry<String, PCollection<Row>> inputStreams : tagged.entrySet()) {
            withTags.and(inputStreams.getKey(), inputStreams.getValue());
        }

        PCollection<Row> results = withTags.apply(SqlTransform
                .query(queryJob.query())
                .withQueryPlannerClass(CalciteQueryPlanner.class)); // HARDCODED

        queryJob.outputs().forEachOrdered(results::apply);

        return pipeline;
    }
}
