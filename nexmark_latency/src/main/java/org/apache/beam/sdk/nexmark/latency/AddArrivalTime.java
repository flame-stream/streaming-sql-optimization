package org.apache.beam.sdk.nexmark.latency;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

// specified as a separate function bc otherwise there are errors -- it's not serializable apparently
public class AddArrivalTime extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
        return Row.fromRow(input).withFieldValue("arrivalTime", Instant.now()).build();
    }
}
