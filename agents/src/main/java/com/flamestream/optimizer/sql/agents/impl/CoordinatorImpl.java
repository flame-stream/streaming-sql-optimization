package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Coordinator;
import com.flamestream.optimizer.sql.agents.CostEstimator;
import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.util.SqlTransform;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.checkerframework.checker.nullness.compatqual.NonNullType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

@SuppressWarnings({
        "nullness"
})
public class CoordinatorImpl implements Coordinator {
    private final HashMap<String, SourceWithSchema> sourcesMap = new HashMap<>();
    private final CostEstimator estimator;
    private final Executor executor;
    private final List<RunningSqlQueryJob> runningJobs = new ArrayList<>();
    private BeamRelNode currentGraph = null;

    public CoordinatorImpl(CostEstimator estimator, Executor executor) {
        this.estimator = estimator;
        this.executor = executor;
    }

    @Override
    public UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>
    registerInput(String tag,
                  UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
                  Schema sourceSchema) {
        sourcesMap.put(tag, new SourceWithSchema(source, sourceSchema));
        return source;
    }

    @Override
    public Stream<UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs() {
        return sourcesMap.values().stream().map(it -> it.source);
    }

    @Override
    public RunningSqlQueryJob start(SqlQueryJob sqlQueryJob) {
        PTransform<@NonNullType PInput, @NonNullType PCollection<Row>> sqlTransform =
                resolveQuery(sqlQueryJob, ImmutableList.of());

        Pipeline pipeline = createPipeline(sqlQueryJob, sqlTransform);
        executor.startOrUpdate(pipeline, null);
        final RunningSqlQueryJob runningSqlQueryJob = new RunningSqlQueryJob() {
            @Override
            public SqlQueryJob queryJob() {
                return sqlQueryJob;
            }

            @Override
            public void addPerformanceStatsListener(Consumer<PerformanceQueryStat> consumer) {
                // TODO: 7/17/21 implement me
            }
        };
        runningJobs.add(runningSqlQueryJob);
        return runningSqlQueryJob;
    }

    @Override
    public void stop(RunningSqlQueryJob runningSqlQueryJob) {
        // TODO: 7/17/21 implement me
    }

    @Override
    public Stream<RunningSqlQueryJob> runningJobs() {
        return runningJobs.stream();
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
        /*BeamRelNode newGraph = queryPlanner.convertToBeamRel(query, QueryPlanner.QueryParameters.ofNone());
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
        }*/
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
        for (Map.Entry<String, SourceWithSchema>
                inputEntry : sourcesMap.entrySet()) {
            PCollection<Row> readFromSource = pipeline.apply(Read.from(inputEntry.getValue().source));
            tagged.put(inputEntry.getKey(), readFromSource.setRowSchema(inputEntry.getValue().schema));
        }

        PCollectionTuple withTags = PCollectionTuple.empty(pipeline);

        for (Map.Entry<String, PCollection<Row>> inputStreams : tagged.entrySet()) {
            withTags = withTags.and(inputStreams.getKey(), inputStreams.getValue());
        }

        PCollection<Row> results = withTags.apply(sqlTransform);
        sqlQueryJob.outputs().forEachOrdered(results::apply);

        return pipeline;
    }

    // TODO visibility
    private class SourceWithSchema {
        final UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source;
        final Schema schema;

        SourceWithSchema(UnboundedSource<Row, @NonNullType ? extends UnboundedSource.CheckpointMark> source, Schema schema) {
            this.source = source;
            this.schema = schema;
        }
    }
}
