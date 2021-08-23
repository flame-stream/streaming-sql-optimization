package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

class UserSource<T> {
    // TODO should there be any limitations on CheckpointT? should UserSource be a generic class?
    private final UnboundedSource<T, @NonNull ?> source;

    private final Schema schema;

    private final Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping;

    public UserSource(UnboundedSource<T, @NonNull ?> source,
                      Schema schema,
                      Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping) {
        this.source = source;
        this.schema = schema;
        this.tableMapping = tableMapping;
    }


    public UnboundedSource<T, @NonNull ?> getSource() {
        return source;
    }

    public Schema getSchema() {
        return schema;
    }

    public Map<String, PTransform<PCollection<T>, PCollection<Row>>> getTableMapping() {
        return tableMapping;
    }
}
