package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import com.flamestream.optimizer.sql.agents.testutils.TestSource;
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

import java.util.stream.Stream;

public class CoordinatorExecutorStatisticsHandlingTest {
    private static final String QUERY_1 = ""
            + " SELECT "
            + "    *   "
            + " FROM   "
            + "    Auction A INNER JOIN Person P on A.seller = P.id "
            + "       INNER JOIN Bid B on B.bidder = P.id" +
            "";

    private static final String SIMPLE_QUERY = "SELECT * FROM Bid";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testCoordinatorExecutorRunNexmark() {
        final CostEstimator costEstimator = new CostEstimatorImpl();
        final String argsString = "--runner=FlinkRunner --query=16 --queryLanguage=sql --streaming=true --manageResources=false --monitorJobs=true --flinkMaster=[local] --tempLocation=" + folder.getRoot().getAbsolutePath();
        final String[] args = (argsString).split(" ");
        final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NexmarkOptions.class);
        final Executor executor = new ExecutorImpl(argsString);
        final CoordinatorImpl coordinator = new CoordinatorImpl(costEstimator, executor);

        final UserSource<Event> source = new UserSource<>(
                TestSource.getTestSource(),
                TestSource.SCHEMA,
                TestSource.getTestMappingMap(),
                TestSource.getTestAdditionalTransforms()
        );

        coordinator.registerInput(
                source.getSource(),
                source.getSchema(),
                source.getTableMapping(),
                source.getAdditionalTransforms()
        );

        final Coordinator.SqlQueryJob job = new Coordinator.SqlQueryJob() {
            @Override
            public String query() {
                return QUERY_1;
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

        coordinator.start(job);
    }
}