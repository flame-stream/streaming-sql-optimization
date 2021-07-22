package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.testutils.TestUnboundedRowSource;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

public class CoordinatorExecutorPipelineTest {

    @Test
    public void testSomething() {

        final UserSource source = new UserSource("TEST", new TestUnboundedRowSource());
        final Coordinator.SqlQueryJob job = new Coordinator.SqlQueryJob() {
            @Override
            public String query() {
                return "SELECT * FROM TEST";
            }

            @Override
            public Stream<PTransform<PCollection<Row>, PDone>> outputs() {
                return Stream.of(PTransform.compose(
                        (PCollection<Row> rows) -> {
                            PCollection<String> strings =
                                    rows.apply(ToString.elements());
                            return strings.apply(TextIO.write()
                                    .to("/home/darya/Documents/flink_temp_dir")
                                    .withWindowedWrites()
                                    .withNumShards(1)
                                    .withSuffix(".txt"));
                        }));
            }
        };

        CoordinatorExecutorPipeline.fromUserQuery(null, List.of(source), job);


    }

}