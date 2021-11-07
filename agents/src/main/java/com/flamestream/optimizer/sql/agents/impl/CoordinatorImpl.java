package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Coordinator;
import com.flamestream.optimizer.sql.agents.CostEstimator;
import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.source.SourceWrapper;
import com.flamestream.optimizer.sql.agents.testutils.TestPipelineOptions;
import com.flamestream.optimizer.sql.agents.util.SqlTransform;
import com.google.common.collect.ImmutableList;
import io.grpc.stub.StreamObserver;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({
        "nullness"
})
public class CoordinatorImpl implements Coordinator, AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger("optimizer.coordinator");

    private final List<SourceWithSchema<?>> sourcesList = new ArrayList<>();
    private final CostEstimator estimator;
    private final Executor executor;
    private final StatisticsHandling.NIOServer statisticsServer;
    private final List<Running> runningJobs = new ArrayList<>();
    private BeamRelNode currentGraph = null;

    @Override
    public void close() throws Exception {
        try (statisticsServer) {
        }
    }

    private class Running implements RunningSqlQueryJob {
        private final SqlQueryJob sqlQueryJob;
        private final BeamRelNode beamRelNode;

        private Running(SqlQueryJob sqlQueryJob, BeamRelNode beamRelNode) {
            this.sqlQueryJob = sqlQueryJob;
            this.beamRelNode = beamRelNode;
        }

        @Override
        public SqlQueryJob queryJob() {
            return sqlQueryJob;
        }

        @Override
        public void addPerformanceStatsListener(Consumer<PerformanceQueryStat> consumer) {

        }
    }

    public CoordinatorImpl(CostEstimator estimator, Executor executor) {
        this.estimator = estimator;
        this.executor = executor;
        try {
            LOG.info("starting a statistics server on port 1337");
            statisticsServer = new StatisticsHandling.NIOServer(1337, (target, worker) -> {
                LOG.info("worker servers addresses " + worker);
                Arrays.stream(worker.split(" ")).forEach(executor::submitSource);
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Stats value) {
                        final var relNode = runningJobs.get(0).beamRelNode;
                        final RelOptCost cost = estimator.getCumulativeCost(
                                relNode,
                                value.getCardinalityMap().entrySet().stream().collect(Collectors.toMap(
                                        entry -> "table_column_distinct_row_count:" + entry.getKey().toLowerCase(),
                                        Map.Entry::getValue
                                ))
                        );
                        LOG.info("cost " + cost.toString());
                        LOG.info("hacky " + ExecutorImpl.HACKY_VARIABLE);
                        if (ExecutorImpl.HACKY_VARIABLE <= 1) {
                            tryNewGraph(runningJobs.get(0).sqlQueryJob);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error("statistics server error");
                        t.printStackTrace();
                    }

                    @Override
                    public void onCompleted() {
                        LOG.info("statistics server: completed");
                    }
                };
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void registerInput(
            UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
            Schema sourceSchema,
            Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping
    ) {
        LOG.info("registering user source with schema " + sourceSchema.toString());
        sourcesList.add(new SourceWithSchema<>(source, sourceSchema, tableMapping));
    }

    @Override
    public Stream<UnboundedSource<?, @NonNullType ? extends UnboundedSource.CheckpointMark>> inputs() {
        return sourcesList.stream().map(it -> it.source);
    }

    @Override
    public RunningSqlQueryJob start(SqlQueryJob sqlQueryJob) {
        final var pipeline = Pipeline.create();
        final var beamRelNode = createPipeline(pipeline, sqlQueryJob, QueryPlanner.QueryParameters.ofNone());
        final var runningSqlQueryJob = new Running(sqlQueryJob, beamRelNode);
        runningJobs.add(runningSqlQueryJob);
        try {
            LOG.info("starting job on executor");
            executor.startOrUpdate(pipeline, null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return runningSqlQueryJob;
    }

    @Override
    public void stop(RunningSqlQueryJob runningSqlQueryJob) {
        // TODO: 7/17/21 implement me
    }

    @Override
    public Stream<? extends RunningSqlQueryJob> runningJobs() {
        return runningJobs.stream();
    }

    private void tryNewGraph(SqlQueryJob sqlQueryJob) {
        // here we need to give list of providers to newSqlTransform method
        /*SqlTransform newSqlTransform =
                updateSqlTransform(sqlQueryJob.query(), ImmutableList.of());
        // awesome code 10/10
        if (newSqlTransform != null) {

        }*/
        final var pipeline = Pipeline.create();
        createPipeline(pipeline, sqlQueryJob, QueryPlanner.QueryParameters.ofNone());
        try {
            LOG.info("trying a new graph");
            executor.startOrUpdate(pipeline, null);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private boolean isDifferenceProfitable(RelOptCost newGraphCost, RelOptCost oldGraphCost) {
        // logic to decide if graph changing is profitable
        return oldGraphCost.isLt(newGraphCost);
    }

    private SqlTransform updateSqlTransform(String query, ImmutableList<RelMetadataProvider> providers) {
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

    private BeamRelNode createPipeline(
            Pipeline pipeline, SqlQueryJob sqlQueryJob, QueryPlanner.QueryParameters queryParameters
    ) {
        HashMap<String, PCollection<Row>> tagged = new HashMap<>();
        List<String> sourceAddresses = new ArrayList<>();

        for (var source : sourcesList) {
            source.applyTransforms(pipeline, sqlQueryJob, tagged, sourceAddresses);
        }

        LOG.info("source addresses: " + String.join(" ", sourceAddresses));

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
                .setQueryPlannerClassName(ModifiedCalciteQueryPlanner.class.getName())
                .setPipelineOptions(new TestPipelineOptions())
                .build().parseQuery(sqlQueryJob.query(), queryParameters);

        var fieldsOfInterest = new SqlQueryInspector().inspectQuery(beamRelNode);

        final var stats = fieldsOfInterest.entrySet().stream().flatMap(interest ->
                interest.getKey() instanceof BeamIOSourceRel
                        ? interest.getValue().stream().map(column ->
                        BeamSqlRelUtils.toPCollection(pipeline, interest.getKey())
                                .apply(ParDo.of(new CardinalityKVDoFn<Long>(column)))
                                .setCoder(KvCoder.of(VarLongCoder.of(), VoidCoder.of()))
                                .apply(Combine.perKey(Count.combineFn()))
                                .apply(ParDo.of(new StatisticsHandling.LocalCardinalityDoFn(
                                        "beam." + interest.getKey().getTable().getQualifiedName().get(1) + "." + column
                                )))
                )
                        : Stream.empty()
        ).collect(Collectors.toList());

        LOG.info("stats size " + stats.size());

        if (!stats.isEmpty()) {
            PCollectionList.of(stats).apply(Flatten.pCollections()).apply(ParDo.of(new StatisticsHandling.StatsDoFn(
                    new InetSocketAddress(1337),
                    PCollectionList.of(stats).size(),
                    sourceAddresses
            )));
        }

        withTags.apply(new PTransform<PCollectionTuple, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PCollectionTuple input) {
                return BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
            }
        });
        return beamRelNode;
    }

    // TODO visibility
    private static class SourceWithSchema<T> {
        final UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source;
        final Schema schema;
        final Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping;

        public void applyTransforms(Pipeline pipeline,
                                    SqlQueryJob sqlQueryJob,
                                    HashMap<String, PCollection<Row>> tagged,
                                    List<String> sourceAddresses) {
            SourceWrapper<T, ? extends UnboundedSource.CheckpointMark> sourceWrapper = new SourceWrapper<>(source);
            // TODO what is the actual host address?
            sourceAddresses.add("localhost:" + sourceWrapper.getPortNumber());
            PCollection<T> readFromSource = pipeline.apply(Read.from(sourceWrapper))
                    .apply(Window.into(sqlQueryJob.windowFunction()));

            for (var entry : tableMapping.entrySet()) {
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

    private static class CardinalityKVDoFn<T> extends DoFn<Row, KV<T, Void>> {
        private final String fieldName;

        public CardinalityKVDoFn(String fieldName) {
            this.fieldName = fieldName;
        }

        @ProcessElement
        public void processElement(ProcessContext context) {
            context.output(KV.of(context.element().getValue(fieldName), null));
        }
    }
}
