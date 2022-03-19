package com.flamestream.optimizer.sql.agents.latency;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;


public class AddCurrentTimeToRow extends SimpleFunction<Row, Row> {
    private final Schema schema;

    public AddCurrentTimeToRow(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public Row apply(Row input) {
        Instant now = Instant.now();
        if (input == null) {
            return null;
        }
        return Row.withSchema(schema).addValues(input.getValues()).addValue(now).build();
    }
}