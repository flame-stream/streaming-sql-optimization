package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.Coordinator;
import com.flamestream.optimizer.sql.agents.CostEstimator;
import com.flamestream.optimizer.sql.agents.Executor;
import com.flamestream.optimizer.sql.agents.Services;
import com.flamestream.optimizer.sql.agents.latency.AddCurrentTimeToRow;
import com.flamestream.optimizer.sql.agents.latency.Latency;
import com.flamestream.optimizer.sql.agents.latency.LatencyCombineFn;
import com.flamestream.optimizer.sql.agents.source.SourceWrapper;
import com.flamestream.optimizer.sql.agents.testutils.TestPipelineOptions;
import com.flamestream.optimizer.sql.agents.util.SqlTransform;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
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
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.checkerframework.checker.nullness.compatqual.NonNullType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final CheckpointsHandling.NIOServer checkpointsServer;
    private final List<Running> runningJobs = new ArrayList<>();
    private BeamRelNode currentGraph = null;
    private ByteString checkpointMarkString = null;

    private Map<String, ?> parametersMap = new HashMap<>();

    private int windowCounter = 0;
    private Map<String, Integer> targetWindowCount = new HashMap<>();

    @Override
    public void close() throws Exception {
        try (statisticsServer) {
        }
        try (checkpointsServer) {
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
            statisticsServer = new StatisticsHandling.NIOServer(1337, target -> {
                LOG.info("target " + target);
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Stats value) {
                        final var relNode = runningJobs.get(0).beamRelNode;
                        final Map<String, ?> currentParametersMap = value.getCardinalityMap().entrySet().stream().collect(Collectors.toMap(
                                entry -> "table_column_distinct_row_count:" + entry.getKey().toLowerCase(),
                                Map.Entry::getValue
                        ));

                        final RelOptCost cost = estimator.getCumulativeCost(
                                relNode,
                                parametersMap
                        );
                        LOG.info("cost " + cost.toString());

                        if (parametersMap.isEmpty()) {
                            parametersMap = currentParametersMap;
                        } else if (currentParametersMap.entrySet().stream().anyMatch((entry -> entry.getValue() instanceof Double && Math.abs((Double)entry.getValue() - (Double)parametersMap.get(entry.getKey())) > 300))) {
                            parametersMap = currentParametersMap;
                            final QueryPlanner.QueryParameters parameters = QueryPlanner.QueryParameters.ofNamed(parametersMap);
                            tryNewGraph(0, parameters);
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

        try {
            checkpointsServer = new CheckpointsHandling.NIOServer(20202, target -> {
                LOG.info("checkpoints target " + target);
                return new StreamObserver<>() {
                    @Override
                    public void onNext(Services.Checkpoint value) {
                        // LOG.info("putting a checkpoint to target " + target);
                        checkpointMarkString = value.getCheckpoint();
                    }

                    @Override
                    public void onError(Throwable t) {
                        LOG.error("error when receiving the checkpoint", t);
                    }

                    @Override
                    public void onCompleted() {

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
            Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping,
            Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms) {
        LOG.info("registering user source with schema " + sourceSchema.toString());
        // TODO by the way i don't think we're even using the schema anymore
        sourcesList.add(new SourceWithSchema<>(source, sourceSchema, tableMapping, additionalTransforms));
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

    private void tryNewGraph(int runningJobIndex) {
        tryNewGraph(runningJobIndex, QueryPlanner.QueryParameters.ofNone());
    }

    private boolean tryNewGraph(int runningJobIndex,
                                QueryPlanner.QueryParameters queryParameters) {
        // here we need to give list of providers to newSqlTransform method
        /*SqlTransform newSqlTransform =
                updateSqlTransform(sqlQueryJob.query(), ImmutableList.of());
        // awesome code 10/10
        if (newSqlTransform != null) {

        }*/
        final SqlQueryJob sqlQueryJob = runningJobs.get(runningJobIndex).sqlQueryJob;

        final var pipeline = Pipeline.create();
        final BeamRelNode relNode = createPipeline(pipeline, sqlQueryJob, queryParameters);
        final RelOptCost newCost = estimator.getCumulativeCost(relNode, parametersMap);
        final RelOptCost currentCost = estimator.getCumulativeCost(runningJobs.get(0).beamRelNode, parametersMap);
        LOG.info("new cost " + newCost + " old cost " + currentCost);
        if (newCost.isLt(currentCost)) {
            try {
                LOG.info("trying a new graph");
                executor.startOrUpdate(pipeline, null);

                runningJobs.remove(runningJobIndex);
                runningJobs.add(runningJobIndex, new Running(sqlQueryJob, relNode));

                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return false;
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

        for (var source : sourcesList) {
            source.applyTransforms(pipeline, sqlQueryJob, tagged, checkpointMarkString);
        }

        PCollectionTuple withTags = PCollectionTuple.empty(pipeline);

        for (Map.Entry<String, PCollection<Row>> inputStreams : tagged.entrySet()) {
            withTags = withTags.and(inputStreams.getKey(), inputStreams.getValue());
        }

        var env = BeamSqlEnv.builder(new ReadOnlyTableProvider(
                        "TestPCollection",
                        withTags.expand().entrySet().stream().collect(Collectors.toMap(
                                table -> table.getKey().getId(),
                                table -> new BeamPCollectionTable<>((PCollection<?>) table.getValue())
                        ))
                ))
                .setQueryPlannerClassName(ModifiedCalciteQueryPlanner.class.getName())
                .setPipelineOptions(new TestPipelineOptions())
                .build();
        var beamRelNode = env.parseQuery(sqlQueryJob.query(), queryParameters);

        var fieldsOfInterest = new SqlQueryInspector().inspectQuery(beamRelNode);

        fieldsOfInterest.values().forEach(it -> it.forEach(LOG::info));

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
                    PCollectionList.of(stats).size()
                    //, sourceAddresses
            )));
        }

        final PCollection<Row> sqlQueryPCollection = withTags.apply(new PTransform<>() {
            @Override
            public PCollection<Row> expand(PCollectionTuple input) {
                return BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
            }
        });

        pipeline.getCoderRegistry().registerCoderForClass(Latency.class, Latency.CODER);

        sqlQueryPCollection.apply(Combine.globally(new LatencyCombineFn()).withoutDefaults())
                .apply(ParDo.of(new LatencyLoggingFunction()));

        sqlQueryJob.outputs().forEach(sqlQueryPCollection::apply);
        return beamRelNode;
    }

    // TODO visibility
    public static class SourceWithSchema<T> {
        final UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source;
        final Schema schema;
        final Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping;
        final Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms;


        public void applyTransforms(Pipeline pipeline,
                                    SqlQueryJob sqlQueryJob,
                                    HashMap<String, PCollection<Row>> tagged,
                                    ByteString checkpointString) {
            // TODO let's start with just one checkpoint to see if this works but it's possible that it will lead us nowhere
            SourceWrapper<T, ? extends UnboundedSource.CheckpointMark> sourceWrapper = checkpointString == null ?
                    new SourceWrapper<>(source) :
                    new SourceWrapper<>(source, true, checkpointString.toByteArray());

            PCollection<T> readFromSource = pipeline.apply(Read.from(sourceWrapper));
            for (var entry : additionalTransforms.entrySet()) {
                readFromSource = readFromSource.apply(entry.getValue());
            }
            readFromSource = readFromSource.apply(Window.into(sqlQueryJob.windowFunction()));

            //.setRowSchema(schema)
            //.apply(Window.into(sqlQueryJob.windowFunction()));

            for (var entry : tableMapping.entrySet()) {
                PCollection<Row> rows = readFromSource.apply(entry.getValue());
                Schema withReceiveTime = Schema.builder()
                        .addFields(rows.getSchema().getFields()).addDateTimeField("receiveTime").build();
                rows = rows
                        .setRowSchema(withReceiveTime)
                        .apply(MapElements.via(new AddCurrentTimeToRow()))
                        .setRowSchema(withReceiveTime);
                tagged.put(entry.getKey(), rows);
            }
        }

        SourceWithSchema(UnboundedSource<T, @NonNullType ? extends UnboundedSource.CheckpointMark> source,
                         Schema schema,
                         Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping,
                         Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms) {
            this.source = source;
            this.schema = schema;
            this.tableMapping = tableMapping;
            this.additionalTransforms = additionalTransforms;
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

    public static class LatencyLoggingFunction extends DoFn<Latency, Void> {
        @Setup
        public void setup() {
            LOG.info("set up the logging function");
        }


        @ProcessElement
        public void processElement(@Element Latency element, PipelineOptions options) {
            LOG.info(options.getJobName());
            LOG.info(element.toString());
        }
    }

    public static class RowLoggingFunction extends DoFn<Row, Row> {
        private boolean logged = false;
        @ProcessElement
        public void processElement(ProcessContext context, BoundedWindow window) {
            Row input = context.element();
            if (input == null) {
                return;
            }
            if (input.getSchema().getFieldNames().contains("dateTime1")) {
//                LOG.info("window max timestamp " + window.maxTimestamp());
                LOG.info("element " + input);
            }
            context.output(input);
        }
    }
}
