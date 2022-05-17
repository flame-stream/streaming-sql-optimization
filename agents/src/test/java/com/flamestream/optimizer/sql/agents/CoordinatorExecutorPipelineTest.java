package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.flamestream.optimizer.sql.agents.latency.Latency;
import com.flamestream.optimizer.sql.agents.testutils.TestSource;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

public class CoordinatorExecutorPipelineTest {
    public static Logger LOG = LoggerFactory.getLogger("pipeline.test");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCoordinatorExecutorRunNexmark() {
        final UserSource<Event> source = new UserSource<>(
                TestSource.getTestSource(),
                TestSource.SCHEMA,
                TestSource.getTestMappingMap(),
                TestSource.getTestAdditionalTransforms()
        );
        final Coordinator.SqlQueryJob plan1 = new TestSqlJob(
                " SELECT "
                        + "    *   "
                        + " FROM   "
                        + "    Auction A INNER JOIN Person P on A.seller = P.id "
                        + "       INNER JOIN Bid B on B.bidder = P.id",
                10
        );
        final Coordinator.SqlQueryJob plan2 = new TestSqlJob(
                " SELECT "
                        + "    *   "
                        + " FROM   "
                        + "    Bid B INNER JOIN Person P on B.bidder = P.id "
                        + "    INNER JOIN Auction A on A.seller = P.id ", 10
        );

        // should probably be configured some other way but this was the easiest
        final String argsString = "--runner=FlinkRunner --streaming=true --manageResources=false --monitorJobs=true --flinkMaster=localhost:8081 --tempLocation=" + folder.getRoot().getAbsolutePath();
//        final String argsString = "--runner=FlinkRunner --streaming=true --manageResources=false --monitorJobs=true --flinkMaster=[local] --tempLocation=" + folder.getRoot().getAbsolutePath();
        final String[] args = argsString.split(" ");
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);
        CoordinatorExecutorPipeline.fromSqlQueryJob(new CostEstimatorImpl(), List.of(source), argsString, plan1, true);
    }

    public static class LoggingFunction extends DoFn<String, String> {
        @Setup
        public void setup() {
            LOG.info("set up the logging function");
        }

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> out, PipelineOptions options) {
            LOG.info(options.getJobName()); // these should be decidedly different
            LOG.info(element);
            out.output(element);
        }
    }

    public static class RowToStringFunction extends DoFn<Row, String> {
        @Setup
        public void setup() {
            LOG.info("set up the row to string function");
        }

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

        TestSqlJob(final String query, final int window) {
            this.query = query;
            this.window = window;
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
    }
}