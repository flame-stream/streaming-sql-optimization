package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Coordinator;
import com.flamestream.optimizer.sql.agents.CostEstimator;
import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.testutils.TestPipelineOptions;
import com.flamestream.optimizer.sql.agents.util.SqlTransform;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.*;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import org.joda.time.Instant;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({
        "nullness"
})
public class CoordinatorImpl implements Coordinator {
    private final List<SourceWithSchema<?>> sourcesList = new ArrayList<>();
    private final CostEstimator estimator;
    private final Executor executor;
    private final List<RunningSqlQueryJob> runningJobs = new ArrayList<>();
    private BeamRelNode currentGraph = null;

    public void setStats(String stats) {
        this.stats = stats;
    }

    private String stats;

    public CoordinatorImpl(CostEstimator estimator, Executor executor) {
        this.estimator = estimator;
        this.executor = executor;
    }

    public <T> void registerInput(
            UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
            Schema sourceSchema,
            Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping
    ) {
        sourcesList.add(new SourceWithSchema<>(source, sourceSchema, tableMapping));
    }

    @Override
    public Stream<UnboundedSource<?, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs() {
        return sourcesList.stream().map(it -> it.source);
    }

    @Override
    public RunningSqlQueryJob start(SqlQueryJob sqlQueryJob) {
        new Thread(new StatisticsHandling.NIOServer(this, 1111)).start();

        Pipeline pipeline = createPipeline(sqlQueryJob);
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
        SqlTransform newSqlTransform =
                updateSqlTransform(sqlQueryJob.query(), ImmutableList.of());
        if (newSqlTransform != null) {
            Pipeline newPipeline = createPipeline(sqlQueryJob);
            executor.startOrUpdate(newPipeline, null);
        }
    }

    private boolean isDifferenceProfitable(RelOptCost newGraphCost, RelOptCost oldGraphCost) {
        // logic to decide if graph changing is profitable
        return oldGraphCost.isLt(newGraphCost);
    }

    private SqlTransform
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

    private SqlTransform
    resolveQuery(SqlQueryJob sqlQueryJob, ImmutableList<RelMetadataProvider> providers) {
        return SqlTransform
                .query(sqlQueryJob.query())
                .withQueryPlannerClass(CalciteQueryPlanner.class);
    }

    private Pipeline createPipeline(SqlQueryJob sqlQueryJob) {
        Pipeline pipeline = Pipeline.create();
        HashMap<String, PCollection<Row>> tagged = new HashMap<>();

        for (var source : sourcesList) {
            source.applyTransforms(pipeline, sqlQueryJob, tagged);
        }

        PCollectionTuple withTags = PCollectionTuple.empty(pipeline);

        for (Map.Entry<String, PCollection<Row>> inputStreams : tagged.entrySet()) {
            withTags = withTags.and(inputStreams.getKey(), inputStreams.getValue());
        }

        var beamRelNode = BeamSqlEnv.builder(new ReadOnlyTableProvider(
                "TestPCollection",
                withTags.expand().entrySet().stream().collect(Collectors.toMap(
                        table -> table.getKey().getId(),
                        table -> new BeamPCollectionTable<>((PCollection<?>) table.getValue())
                ))
        ))
                .setQueryPlannerClassName(CalciteQueryPlanner.class.getName())
                .setPipelineOptions(new TestPipelineOptions())
                .build().parseQuery(sqlQueryJob.query(), QueryPlanner.QueryParameters.ofNone());

//        var fieldsOfInterest = new SqlQueryInspector().inspectQuery(beamRelNode);

//        for (var interest: fieldsOfInterest.entrySet()) {
//            var stream = withTags.get(interest.getKey().toString());
//            stream.apply(Combine.globally(Count.combineFn()).withoutDefaults())
//                    .apply(new StatisticsHandling.StatsOutput(interest.getKey().toString()));
//        }

        withTags.apply(
                new PTransform<PCollectionTuple, PCollection<Row>>() {
                      @Override
                      public PCollection<Row> expand(PCollectionTuple input) {
                          return BeamSqlRelUtils.toPCollection(
                                  pipeline, beamRelNode);
                      }
                }
              );
        return pipeline;
    }

    // TODO visibility
    private static class SourceWithSchema<T> {
        final UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source;
        final Schema schema;
        final Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping;

        public void applyTransforms(Pipeline pipeline, SqlQueryJob sqlQueryJob, HashMap<String, PCollection<Row>> tagged) {
            PCollection<T> readFromSource = pipeline.apply(Read.from(source)).setRowSchema(schema)
                    .apply(ParDo.of(new DoFn<T, T>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, BoundedWindow window, @Timestamp Instant timestamp) {
                            T row = c.element();
                            if (row != null) {
                                System.out.println(row.toString());
                            }
                            c.output(row);
                        }
                    }))
                    .apply(Window.into(sqlQueryJob.windowFunction()));
            for (var entry: tableMapping.entrySet()) {
                tagged.put(entry.getKey(), readFromSource.apply(entry.getValue()));
            }
        }

        SourceWithSchema(UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
                         Schema schema,
                         Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping) {
            this.source = source;
            this.schema = schema;
            this.tableMapping = tableMapping;
        }
    }
}
