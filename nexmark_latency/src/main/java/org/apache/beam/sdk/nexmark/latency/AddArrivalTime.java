package org.apache.beam.sdk.nexmark.latency;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// specified as a separate function bc otherwise there are errors -- it's not serializable apparently
public class AddArrivalTime extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
        Instant now = Instant.now();
//        System.out.println("added arrival time: " + now);
        return Row.fromRow(input).withFieldValue("arrivalTime", now).build();
    }
}
