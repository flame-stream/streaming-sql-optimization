package com.flamestream.optimizer.sql.agents.latency;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyToClickhouse extends DoFn<Latency, Row> {
    private static Logger LOG = LoggerFactory.getLogger("latency");

    // TODO this thing literally copies the test source configuration, can we unite that somehow
    public int personProportion;
    public int auctionProportion;
    public int bidProportion;
    public int numberEvents;
    public int ratePerSec;
    public int window;
    public int experimentNumber;

    public static final Schema SCHEMA =
            Schema.builder()
                    .addField(Schema.Field.of("exp_id", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("persons", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("auctions", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("bids", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("window", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("num_elements", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("rate", Schema.FieldType.INT32).withNullable(false))
                    .addField(Schema.Field.of("pipeline", Schema.FieldType.STRING).withNullable(false))
                    .addField(Schema.Field.of("latency", Schema.FieldType.INT64).withNullable(false))
                    .addField(Schema.Field.of("timestamp", Schema.FieldType.DATETIME).withNullable(false))
                    .build();

    public LatencyToClickhouse(int personProportion,
                               int auctionProportion,
                               int bidProportion,
                               int numberEvents,
                               int ratePerSec,
                               int window,
                               int experimentNumber) {
        this.personProportion = personProportion;
        this.auctionProportion = auctionProportion;
        this.bidProportion = bidProportion;
        this.numberEvents = numberEvents;
        this.ratePerSec = ratePerSec;
        this.window = window;
        this.experimentNumber = experimentNumber;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        Latency latency = context.element();
        LOG.info("writing to clickhouse " + latency);
        Row row = Row.withSchema(SCHEMA)
                .addValues(experimentNumber,
                        personProportion, auctionProportion, bidProportion,
                        window, numberEvents, ratePerSec)
                .addValue(context.getPipelineOptions().getJobName())
                .addValue(latency.latency)
                .addValue(DateTime.now())
                .build();
        context.output(row);
    }



}
