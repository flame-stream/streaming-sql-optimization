package com.flamestream.optimizer.sql.agents.latency;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;


public class AddCurrentTimeToRow extends SimpleFunction<Row, Row> {

    @Override
    public Row apply(Row input) {
        Instant now = Instant.now();
        if (input == null) {
            return null;
        }
        return Row.fromRow(input).withFieldValue("receiveTime", now).build();
//        return Row.withSchema(schema).addValues(input.getValues()).addValue(now).build();
    }
}