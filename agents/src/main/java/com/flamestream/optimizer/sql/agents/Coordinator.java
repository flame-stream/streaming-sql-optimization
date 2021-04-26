package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
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
        QueryJobBuilder addOutput(POutput sink);

        QueryJobBuilder setPreHook(PTransform<PCollection<Row>, PCollection<Row>> hook);
        QueryJobBuilder setPostHook(PTransform<PCollection<Row>, PCollection<Row>> hook);

        QueryJobBuilder registerUdf(String functionName, SerializableFunction sfn);
        QueryJobBuilder registerUdf(String functionName, Class<?> clazz, String method);
        QueryJobBuilder registerUdaf(String functionName, Combine.CombineFn combineFn);

        QueryJob build(String query);
    }

    interface QueryContext {
//        void addStatsListener(Consumer<QueryStatistics> consumer);
        void stop();
    }
}
