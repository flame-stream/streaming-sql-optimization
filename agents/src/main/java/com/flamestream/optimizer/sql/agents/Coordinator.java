package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

import java.util.function.Consumer;
import java.util.stream.Stream;

interface Coordinator {
    PInput registerInput(String tag, PInput source);
    Stream<PInput> inputs();

    QueryContext start(QueryJob queryJob);

    interface QueryJob {
        String query();
        Stream<POutput> outputs();
    }

    interface QueryJobBuilder {
        void addOutput(POutput sink);
        void addPreHook(PTransform<PCollection<Row>, PCollection<Row>> hook);
        void addPostHook(PTransform<PCollection<Row>, PCollection<Row>> hook);
        void registerUdf(PTransform<PCollection<Row>, PCollection<Row>> transform);
        QueryJob build(String query);
    }

    interface QueryContext {
        void addStatsListener(Consumer<QueryStatistics> consumer);
        void stop();
    }
}
