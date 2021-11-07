package com.flamestream.optimizer.sql.agents.testutils;

import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

public class TestSource {
    private static final Monitor<Event> eventMonitor = new Monitor<>("TestMonitor" + ".Events", "event");

    public static PTransform<PCollection<Event>, PCollection<Row>> auctionTransform = new PTransform<PCollection<Event>, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input // Monitor events as they go by.
                    .apply("Auction" + ".Monitor", eventMonitor.getTransform())
                    // Count each type of event.
                    .apply("Auction" + ".Snoop", NexmarkUtils.snoop("Auction"))
                    .apply(getName() + ".Filter." + "Auction", Filter.by(e1 -> e1.newAuction != null))
                    .apply(getName() + ".ToRecords." + "Auction", new SelectEvent(Event.Type.AUCTION));
        }
    };

    public static PTransform<PCollection<Event>, PCollection<Row>> bidTransform = new PTransform<PCollection<Event>, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input // Monitor events as they go by.
                    .apply("Bid" + ".Monitor", eventMonitor.getTransform())
                    // Count each type of event.
                    .apply("Bid" + ".Snoop", NexmarkUtils.snoop("Bid"))
                    .apply(getName() + ".Filter." + "Bid", Filter.by(e1 -> e1.bid != null))
                    .apply(getName() + ".ToRecords." + "Bid", new SelectEvent(Event.Type.BID));
        }
    };

    public static PTransform<PCollection<Event>, PCollection<Row>> personTransform = new PTransform<PCollection<Event>, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PCollection<Event> input) {
            return input // Monitor events as they go by.
                    .apply("Person" + ".Monitor", eventMonitor.getTransform())
                    // Count each type of event.
                    .apply("Person" + ".Snoop", NexmarkUtils.snoop("Person"))
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

    public static UnboundedEventSource getTestSource() {
        final NexmarkConfiguration config = NexmarkConfiguration.DEFAULT;
        config.numEvents = 10000;
        config.isRateLimited = true;
        var generatorConfig = new GeneratorConfig(
                config,
                config.useWallclockEventTime ? System.currentTimeMillis() : 0,
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
