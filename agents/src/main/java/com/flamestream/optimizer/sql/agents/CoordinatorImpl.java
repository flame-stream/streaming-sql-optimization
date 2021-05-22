package com.flamestream.optimizer.sql.agents;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@SuppressWarnings({
        "nullness"
})
public class CoordinatorImpl implements Coordinator {
    private final HashMap<String, UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>>
            sourcesMap = new HashMap<>();
    private final Planner planner;
    private final QueryPlanner queryPlanner;
    private final CostEstimator estimator;
    private final Executor executor;
    private BeamRelNode currentGraph = null;

    public CoordinatorImpl(Planner planner, QueryPlanner queryPlanner, CostEstimator estimator, Executor executor) {
        this.planner = planner;
        this.queryPlanner = queryPlanner;
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
    public QueryContext start(SqlQueryJob sqlQueryJob) {
        PTransform<@NonNullType PInput, @NonNullType PCollection<Row>> sqlTransform =
                resolveQuery(sqlQueryJob, ImmutableList.of());

        Pipeline pipeline = createPipeline(sqlQueryJob, sqlTransform);
        executor.startOrUpdate(pipeline, null);
        return new QueryContext() {
            @Override
            public void stop() {
                // TODO
            }
        };
    }

    private void tryNewGraph(SqlQueryJob sqlQueryJob) {
        // here we need to give list of providers to newSqlTransform method
        PTransform<@NonNullType PInput, @NonNullType PCollection<Row>> newSqlTransform =
                updateSqlTransform(sqlQueryJob.query(), ImmutableList.of());
        if (newSqlTransform != null) {
            Pipeline newPipeline = createPipeline(sqlQueryJob, newSqlTransform);
            executor.startOrUpdate(newPipeline, null);
        }
    }

    private boolean isDifferenceProfitable(RelOptCost newGraphCost, RelOptCost oldGraphCost) {
        // logic to decide if graph changing is profitable
        return oldGraphCost.isLt(newGraphCost);
    }

    private PTransform<@NonNullType PInput, @NonNullType PCollection<Row>>
            updateSqlTransform(String query, ImmutableList<RelMetadataProvider> providers) {
        // here our planner implementation should give new graph
        BeamRelNode newGraph = queryPlanner.convertToBeamRel(query, QueryPlanner.QueryParameters.ofNone());
        RelOptCost newGraphCost = estimator.getCumulativeCost(newGraph, providers, RelMetadataQuery.instance());
        RelOptCost oldGraphCost = estimator.getCumulativeCost(currentGraph, providers, RelMetadataQuery.instance());
        if (isDifferenceProfitable(newGraphCost, oldGraphCost)) {
            return new PTransform<>() {
                @Override
                public @UnknownKeyFor @NonNull @Initialized PCollection<Row> expand(PInput input) {
                    return BeamSqlRelUtils.toPCollection(
                            input.getPipeline(), newGraph);
                }
            };
        }
        return null;
    }

    private PTransform<@NonNullType PInput, @NonNullType PCollection<Row>>
            resolveQuery(SqlQueryJob sqlQueryJob, ImmutableList<RelMetadataProvider> providers) {
        return SqlTransform
                .query(sqlQueryJob.query())
                .withQueryPlannerClass(CalciteQueryPlanner.class);
    }

    private Pipeline createPipeline(SqlQueryJob sqlQueryJob, PTransform<@NonNullType PInput,
            @NonNullType PCollection<Row>> sqlTransform) {

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

        PCollection<Row> results = withTags.apply(sqlTransform);
        sqlQueryJob.outputs().forEachOrdered(results::apply);

        return pipeline;
    }
}
