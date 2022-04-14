package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;

class UserSource<T> {
    // TODO should there be any limitations on CheckpointT? should UserSource be a generic class?
    private final UnboundedSource<T, @NonNull ?> source;

    private final Schema schema;

    private final Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping;

    private final Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms;

    public UserSource(UnboundedSource<T, @NonNull ?> source,
                      Schema schema,
                      Map<String, PTransform<PCollection<T>, PCollection<Row>>> tableMapping,
                      Map<String, PTransform<PCollection<T>, PCollection<T>>> additionalTransforms) {
        this.source = source;
        this.schema = schema;
        this.tableMapping = tableMapping;
        this.additionalTransforms = additionalTransforms;
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

    public Map<String, PTransform<PCollection<T>, PCollection<T>>> getAdditionalTransforms() {
        return additionalTransforms;
    }
}
