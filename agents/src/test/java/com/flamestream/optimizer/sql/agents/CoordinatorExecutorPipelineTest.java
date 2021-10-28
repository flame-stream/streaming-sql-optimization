package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.flamestream.optimizer.sql.agents.testutils.TestSource;
import com.flamestream.optimizer.testutils.TestUnboundedRowSource;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToString;
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

import java.util.List;
import java.util.stream.Stream;

public class CoordinatorExecutorPipelineTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCoordinatorExecutorRunNexmark() {
        final UserSource<Event> source = new UserSource<>(
                TestSource.getTestSource(),
                TestUnboundedRowSource.SCHEMA,
                TestSource.getTestMappingMap());
        final Coordinator.SqlQueryJob job = new Coordinator.SqlQueryJob() {
            @Override
            public String query() {
                return ""
                        + " SELECT "
                        + "    *   "
                        + " FROM   "
                        + "    Auction A INNER JOIN Person P on A.seller = P.id "
                        + "       INNER JOIN Bid B on B.bidder = P.id" +
                        "";
            }

            @Override
            public WindowFn<Object, ? extends BoundedWindow> windowFunction() {
                return FixedWindows.of(Duration.standardSeconds(1));
            }

            @Override
            public Stream<PTransform<PCollection<Row>, PDone>> outputs() {
                return Stream.of(PTransform.compose(
                        (PCollection<Row> rows) -> {
                            PCollection<String> strings = rows.apply(ToString.elements());
                            return strings.apply(TextIO.write()
                                    .to(folder.getRoot().getAbsolutePath())
                                    .withWindowedWrites()
                                    .withNumShards(1)
                                    .withSuffix(".txt"));
                        }));
            }
        };

        // should probably be configured some other way but this was the easiest
        final String[] args = ("--runner=FlinkRunner --query=16 --queryLanguage=sql --streaming=true --manageResources=false --monitorJobs=true --flinkMaster=localhost:8081 --tempLocation=" + folder.getRoot().getAbsolutePath()).split(" ");
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);
        CoordinatorExecutorPipeline.fromUserQuery(new CostEstimatorImpl(), List.of(source), job, options);
    }
}