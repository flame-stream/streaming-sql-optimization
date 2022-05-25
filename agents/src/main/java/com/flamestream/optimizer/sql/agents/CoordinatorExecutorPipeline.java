package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import com.flamestream.optimizer.sql.agents.latency.Latency;
import com.flamestream.optimizer.sql.agents.latency.LatencyToClickhouse;
import com.flamestream.optimizer.sql.agents.testutils.TestSource;
import com.flamestream.optimizer.sql.agents.util.NetworkUtil;
import org.apache.beam.sdk.io.clickhouse.ClickHouseIO;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.flink.api.common.JobStatus;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class CoordinatorExecutorPipeline {

    public static void fromSqlQueryJob(
            final CostEstimator costEstimator,
            final @NonNull Collection<UserSource> inputs,
            final String optionsArguments,
            final Coordinator.SqlQueryJob job,
            final boolean switchGraphs) {

        final Executor executor = new ExecutorImpl(optionsArguments);
        final Coordinator coordinator = new CoordinatorImpl(costEstimator, executor);

        for (var input : inputs) {
            coordinator.registerInput(input.getSource(), input.getSchema(), input.getTableMapping(),input.getAdditionalTransforms() );
        }

        while (true) {
            coordinator.start(job, switchGraphs);
            try {
                while (coordinator.status().equals(JobStatus.RUNNING)) {
                    System.out.println("running");
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                }
                System.out.println("finished");
                if (!coordinator.status().equals(JobStatus.FAILING) && !coordinator.status().equals(JobStatus.FAILED)) {
                    break;
                }
                System.out.println("Job failed, retrying");
            } catch (Exception e) {
                System.out.println("error!");
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(String.join(" ", args));

        String flinkMaster = args.length > 0 ? args[0] : "localhost";
        System.out.println(flinkMaster);

        // TODO there must be some library that does it better
        int window = args.length >= 7 ? Integer.parseInt(args[6]) : 10;
        TestSourceConfiguration config = args.length < 7 ? null : new TestSourceConfiguration(
                Integer.parseInt(args[1]),
                Integer.parseInt(args[2]),
                Integer.parseInt(args[3]),
                Integer.parseInt(args[4]),
                Integer.parseInt(args[5]),
                window
        );
        int plan = -1;
        if (args.length >= 8) {
            plan = Integer.parseInt(args[7]);
        }
        boolean switchGraphs = true;
        if (args.length >= 9) {
            switchGraphs = Boolean.parseBoolean(args[8]);
        }
        int experimentNumber = 0;
        if (args.length >= 10) {
            experimentNumber = Integer.parseInt(args[9]);
        }

        final UserSource<Event> source = new UserSource<>(
                config == null ? TestSource.getTestSource() : TestSource.getConfiguredTestSource(config.personProportion, config.auctionProportion, config.bidProportion, config.numberEvents, config.ratePerSec),
                TestSource.SCHEMA,
                TestSource.getTestMappingMap(),
                TestSource.getTestAdditionalTransforms()
        );

        // run clickhouse server on the same machine that you launch this from
        final String clickhouseHost = NetworkUtil.getIPHost();

        final Coordinator.SqlQueryJob plan1 = new TestSqlJob(
                " SELECT "
                        + "    *   "
                        + " FROM   "
                        + "    Auction A INNER JOIN Person P on A.seller = P.id "
                        + "       INNER JOIN Bid B on B.bidder = P.id",
                window,
                config,
                clickhouseHost,
                experimentNumber
        );
        final Coordinator.SqlQueryJob plan2 = new TestSqlJob(
                " SELECT "
                        + "    *   "
                        + " FROM   "
                        + "    Bid B INNER JOIN Person P on B.bidder = P.id "
                        + "    INNER JOIN Auction A on A.seller = P.id ",
                window,
                config,
                clickhouseHost,
                experimentNumber
        );

        // should probably be configured some other way but this was the easiest
        final String argsString = "--runner=FlinkRunner --streaming=true --parallelism=4 --flinkMaster=" + flinkMaster + ":8081";
//        final String argsString = "--runner=FlinkRunner --streaming=true --flinkMaster=" + flinkMaster + ":8082";
        final Coordinator.SqlQueryJob job = plan == 1 ? plan1 : plan2;
        CoordinatorExecutorPipeline.fromSqlQueryJob(new CostEstimatorImpl(), List.of(source), argsString, job, switchGraphs);
    }

    public static class RowToStringFunction extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(@Element Row element, OutputReceiver<String> out) {
            final String id = stringOrEmpty(element.getValue("id")) + " " +
                    stringOrEmpty(element.getValue("id0"));
            /*final String dateTime = stringOrEmpty(element.getValue("dateTime")) + " " +
                    stringOrEmpty(element.getValue("dateTime0")) + " " +
                    stringOrEmpty(element.getValue("dateTime1"));*/
//            out.output(id + " // " + dateTime);
            out.output(id);
        }

        private <T> String stringOrEmpty(T value) {
            return value == null ? "" : value.toString();
        }
    }

    private static class TestSqlJob implements Coordinator.SqlQueryJob, Serializable {
        private final String query;
        private final int window;
        private final TestSourceConfiguration config;
        private final String clickhouseHost;
        private final int experimentNumber;

        TestSqlJob(final String query,
                   final int window,
                   final TestSourceConfiguration config,
                   final String clickhouseHost,
                   final int experimentNumber) {
            this.query = query;
            this.window = window;
            this.config = config;
            this.clickhouseHost = clickhouseHost;
            this.experimentNumber = experimentNumber;
        }

        @Override
        public String query() {
            return query;
        }

        @Override
        public Stream<PTransform<PCollection<Row>, PDone>> outputs() {
            return Stream.of(PTransform.compose(
                    (PCollection<Row> rows) -> {
                        PCollection<String> strings = rows.apply(ParDo.of(new RowToStringFunction()));
//                                    .apply(ParDo.of(new LoggingFunction()));
                        return PDone.in(strings.getPipeline());
//                            return PDone.in(rows.getPipeline());
                    }));        }

        @Override
        public WindowFn<Object, ? extends BoundedWindow> windowFunction() {
            return FixedWindows.of(Duration.standardSeconds(window));
        }

        @Override
        public Stream<PTransform<PCollection<Latency>, PDone>> latencyOutputs() {
            return Stream.of(PTransform.compose(
                    (PCollection<Latency> latencies) -> {
                        PCollection<Row> rows = latencies.apply(ParDo.of(new LatencyToClickhouse(
                                config.personProportion, config.auctionProportion, config.bidProportion,
                                // TODO experiment number
                                config.numberEvents, config.ratePerSec, config.window, experimentNumber)))
                                .setRowSchema(LatencyToClickhouse.SCHEMA);
                        return rows.apply("ClickhouseWrite",
                                ClickHouseIO.write("jdbc:clickhouse://" + clickhouseHost + ":8123/optimizer_log?user=default&password=optimizer",
                                        "latency_log"));

                    }
            ));
        }
    }

    public static class TestSourceConfiguration {
        public int personProportion;
        public int auctionProportion;
        public int bidProportion;
        public int numberEvents;
        public int ratePerSec;
        public int window;

        public TestSourceConfiguration(int personProportion,
                                       int auctionProportion,
                                       int bidProportion,
                                       int numberEvents,
                                       int ratePerSec,
                                       int window) {
            this.personProportion = personProportion;
            this.auctionProportion = auctionProportion;
            this.bidProportion = bidProportion;
            this.numberEvents = numberEvents;
            this.ratePerSec = ratePerSec;
            this.window = window;
        }
    }
}