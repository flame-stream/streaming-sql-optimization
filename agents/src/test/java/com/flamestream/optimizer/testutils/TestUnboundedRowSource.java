package com.flamestream.optimizer.testutils;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorCheckpoint;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class TestUnboundedRowSource extends UnboundedSource<Row, GeneratorCheckpoint> {
    public static final Logger LOG = LoggerFactory.getLogger(TestUnboundedRowSource.class);

    public static final Schema PERSON_SCHEMA = Schema.builder()
            .addField("id", Schema.FieldType.INT64)
            .addField("name", Schema.FieldType.STRING)
            .addField("emailAddress", Schema.FieldType.STRING)
            .addField("creditCard", Schema.FieldType.STRING)
            .addField("city", Schema.FieldType.STRING)
            .addField("state", Schema.FieldType.STRING)
            .addField("dateTime", Schema.FieldType.DATETIME)
            .addField("extra", Schema.FieldType.STRING)
            .build();
    public static final Schema AUCTION_SCHEMA = Schema.builder()
            .addField("id", Schema.FieldType.INT64)
            .addField("itemName", Schema.FieldType.STRING)
            .addField("description", Schema.FieldType.STRING)
            .addField("initialBid", Schema.FieldType.INT64)
            .addField("reserve", Schema.FieldType.INT64)
            .addField("dateTime", Schema.FieldType.DATETIME)
            .addField("expires", Schema.FieldType.DATETIME)
            .addField("seller", Schema.FieldType.INT64)
            .addField("category", Schema.FieldType.INT64)
            .addField("extra", Schema.FieldType.STRING)
            .build();
    public static final Schema BID_SCHEMA = Schema.builder()
            .addField("auction", Schema.FieldType.INT64)
            .addField("bidder", Schema.FieldType.INT64)
            .addField("price", Schema.FieldType.INT64)
            .addField("dateTime", Schema.FieldType.DATETIME)
            .addField("extra", Schema.FieldType.STRING)
            .build();
    public static final Schema SCHEMA = Schema.builder()
            .addField("newPerson", Schema.FieldType.row(PERSON_SCHEMA).withNullable(true))
            .addField("newAuction", Schema.FieldType.row(AUCTION_SCHEMA).withNullable(true))
            .addField("bid", Schema.FieldType.row(BID_SCHEMA).withNullable(true))
            .build();


    private final UnboundedEventSource source;
    private final NexmarkConfiguration nexmarkConfig;
    private final GeneratorConfig generatorConfig;
    
    public TestUnboundedRowSource() {
        final NexmarkConfiguration config = NexmarkConfiguration.DEFAULT;
        nexmarkConfig = config;
        config.numEvents = 1000;
        generatorConfig = new GeneratorConfig(
                config,
                config.useWallclockEventTime ? System.currentTimeMillis() : 0,
                0,
                config.numEvents,
                0);
        source = new UnboundedEventSource(
                generatorConfig,
                config.numEventGenerators,
                config.watermarkHoldbackSec,
                config.isRateLimited);
    }


    @Override
    public List<? extends UnboundedSource<Row, GeneratorCheckpoint>> split(int desiredNumSplits, PipelineOptions options) {
        List<TestUnboundedRowSource> results = new ArrayList<>();
        for (GeneratorConfig ignored : generatorConfig.split(nexmarkConfig.numEventGenerators)) {
            results.add(new TestUnboundedRowSource());
        }
        return results;
    }

    @Override
    public UnboundedReader<Row> createReader(PipelineOptions options, @Nullable GeneratorCheckpoint checkpointMark) {
        UnboundedReader<Event> eventReader = source.createReader(options, checkpointMark);
        return new RowReader(eventReader);
    }

    @Override
    public Coder<GeneratorCheckpoint> getCheckpointMarkCoder() {
        return source.getCheckpointMarkCoder();
    }

    @Override
    public Coder<Row> getOutputCoder() {
        return SchemaCoder.of(SCHEMA);
    }

    private class RowReader extends UnboundedReader<Row> {
        private final UnboundedReader<Event> eventReader;

        public RowReader(final UnboundedReader<Event> eventReader) {
            this.eventReader = eventReader;
        }

        @Override
        public boolean start() throws IOException {
            return eventReader.start();
        }

        @Override
        public boolean advance() throws IOException {
            return eventReader.advance();
        }

        @Override
        public Row getCurrent() throws NoSuchElementException {
            final Event currentEvent = eventReader.getCurrent();
            if (currentEvent == null) {
                return null;
            }
            final Row res;
            if (currentEvent.newPerson != null) {
                final Person person = currentEvent.newPerson;
                final Row personRow = Row.withSchema(PERSON_SCHEMA).addValues(
                        person.id,
                        person.name,
                        person.emailAddress,
                        person.creditCard,
                        person.city,
                        person.state,
                        person.dateTime,
                        person.extra
                ).build();
                res = Row.withSchema(SCHEMA).withFieldValue("newPerson", personRow).build();
            } else if (currentEvent.newAuction != null) {
                final Auction auction = currentEvent.newAuction;
                final Row auctionRow = Row.withSchema(AUCTION_SCHEMA).addValues(
                        auction.id,
                        auction.itemName,
                        auction.description,
                        auction.initialBid,
                        auction.reserve,
                        auction.dateTime,
                        auction.expires,
                        auction.seller,
                        auction.category,
                        auction.extra
                ).build();
                res = Row.withSchema(SCHEMA).withFieldValue("newAuction", auctionRow).build();
            } else if (currentEvent.bid != null) {
                final Bid bid = currentEvent.bid;
                final Row bidRow = Row.withSchema(BID_SCHEMA).addValues(
                        bid.auction,
                        bid.bidder,
                        bid.price,
                        bid.dateTime,
                        bid.extra
                ).build();
                res = Row.withSchema(SCHEMA).withFieldValue("bid", bidRow).build();
            } else {
                res = null;
            }

            return res;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return eventReader.getCurrentTimestamp();
        }

        @Override
        public void close() throws IOException {
            eventReader.close();
        }

        @Override
        public Instant getWatermark() {
            return eventReader.getWatermark();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return eventReader.getCheckpointMark();
        }

        @Override
        public UnboundedSource<Row, ?> getCurrentSource() {
            return TestUnboundedRowSource.this;
        }
    }
}
