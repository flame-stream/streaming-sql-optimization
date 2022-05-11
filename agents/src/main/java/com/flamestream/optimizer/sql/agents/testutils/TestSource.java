package com.flamestream.optimizer.sql.agents.testutils;

import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Map;

public class TestSource {
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
    public static final Schema SCHEMA = new JavaFieldSchema().schemaFor(TypeDescriptor.of(Event.class));
    /*public static final Schema SCHEMA = Schema.builder()
            .addField("newPerson", Schema.FieldType.row(PERSON_SCHEMA).withNullable(true))
            .addField("newAuction", Schema.FieldType.row(AUCTION_SCHEMA).withNullable(true))
            .addField("bid", Schema.FieldType.row(BID_SCHEMA).withNullable(true))
            .build();*/
    private static final Monitor<Event> eventMonitor = new Monitor<>("TestMonitor" + ".Events", "event");

    public static PTransform<PCollection<Event>, PCollection<Event>> additionalTransform = new PTransform<>() {
        @Override
        public PCollection<Event> expand(PCollection<Event> input) {
            return input // Monitor events as they go by.
                    .apply("Events" + ".Monitor", eventMonitor.getTransform())
                    // Count each type of event.
                    .apply("Events" + ".Snoop", NexmarkUtils.snoop("Events"));
        }
    };

    public static PTransform<PCollection<Event>, PCollection<Row>> auctionTransform = new PTransform<>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input
                    .apply(getName() + ".Filter." + "Auction", Filter.by(e1 -> e1.newAuction != null))
                    .apply(getName() + ".ToRecords." + "Auction", new SelectEvent(Event.Type.AUCTION));
        }
    };

    public static PTransform<PCollection<Event>, PCollection<Row>> bidTransform = new PTransform<>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input
                    .apply(getName() + ".Filter." + "Bid", Filter.by(e1 -> e1.bid != null))
                    .apply(getName() + ".ToRecords." + "Bid", new SelectEvent(Event.Type.BID));
        }
    };

    public static PTransform<PCollection<Event>, PCollection<Row>> personTransform = new PTransform<>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input
                    .apply(getName() + ".Filter." + "Person", Filter.by(e1 -> e1.newPerson != null))
                    .apply(getName() + ".ToRecords." + "Person", new SelectEvent(Event.Type.PERSON));
        }
    };

    public static Map<String, PTransform<PCollection<Event>, PCollection<Row>>> getTestMappingMap() {
        return Map.of(
                "Auction", auctionTransform,
                "Bid", bidTransform,
                "Person", personTransform
        );
    }

    public static Map<String, PTransform<PCollection<Event>, PCollection<Event>>> getTestAdditionalTransforms() {
        return Map.of(
                "Setup", additionalTransform
        );
    }

    public static UnboundedEventSource getTestSource() {
        return getConfiguredTestSource(5, 90, 5, 2000000, 2500);
    }

    public static UnboundedEventSource getConfiguredTestSource(
            int personProportion,
            int auctionProportion,
            int bidProportion,
            int numberEvents,
            int ratePerSec
    ) {
        final NexmarkConfiguration config = NexmarkConfiguration.DEFAULT;
        config.numEvents = numberEvents;
        config.isRateLimited = true;
        config.streamTimeout = 15 * 60;
//        config.useWallclockEventTime = true;
        config.useWallclockEventTime = false;
        config.numEventGenerators = 1;
        config.probDelayedEvent = 0.0;
        config.occasionalDelaySec = 0;
        config.firstEventRate = ratePerSec;
        config.nextEventRate = ratePerSec;
        config.personProportion = personProportion;
        config.auctionProportion = auctionProportion;
        config.bidProportion = bidProportion;
        var generatorConfig = new GeneratorConfig(
                config,
                1637695440000L,
                0,
                config.numEvents,
                0);
        return new UnboundedEventSource(
                generatorConfig,
                config.numEventGenerators,
                config.watermarkHoldbackSec,
                config.isRateLimited);
    }
}
