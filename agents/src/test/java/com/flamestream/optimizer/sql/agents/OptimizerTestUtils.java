package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.testutils.TestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;

import java.util.Map;

public class OptimizerTestUtils {
    private static final String QUERY_1 =
            ""
                    + " SELECT "
                    + "     P.name, B.price"
                    + " FROM "
                    + "     Auction A INNER JOIN Person P on A.seller = P.id "
                    + "         INNER JOIN Bid B on B.bidder = P.id";

    private static final String QUERY_2 =
            ""
                    + " SELECT "
                    + "     P.name, B.price, A.category"
                    + " FROM "
                    + "     Person P INNER JOIN Bid B on B.bidder = P.id"
                    + "         INNER JOIN Auction A on A.seller = P.id";

    public static BeamRelNode getFirstQueryPlan() {
        return getNEXMarkQueryPlan(QUERY_1);
    }

    public static BeamRelNode getSecondQueryPlan() {
        return getNEXMarkQueryPlan(QUERY_2);
    }


    public static BeamRelNode getNEXMarkQueryPlan(String query) {
        var p = Pipeline.create();

        String name = "Test";
        Monitor<Event> eventMonitor = new Monitor<>(name + ".Events", "event");

        PCollection<Event> allEvents = p.apply("Test" + ".ReadUnbounded",
                NexmarkUtils.streamEventsSource(NexmarkConfiguration.DEFAULT));

        allEvents = allEvents
                // Monitor events as they go by.
                .apply(name + ".Monitor", eventMonitor.getTransform())
                .apply(name + ".Snoop", NexmarkUtils.snoop(name));


        PCollection<Event> windowed =
                allEvents.apply(
                        Window.into(FixedWindows.of(Duration.standardSeconds(NexmarkConfiguration.DEFAULT.windowSizeSec))));

        String auctionName = Auction.class.getSimpleName();
        String personName = Person.class.getSimpleName();
        String bidName = Bid.class.getSimpleName();

        PCollection<Row> auctions =
                windowed
                        .apply("Test" + ".Filter." + auctionName, Filter.by(e1 -> e1.newAuction != null))
                        .apply("Test" + ".ToRecords." + auctionName, new SelectEvent(Event.Type.AUCTION));

        PCollection<Row> people =
                windowed
                        .apply("Test" + ".Filter." + personName, Filter.by(e -> e.newPerson != null))
                        .apply("Test" + ".ToRecords." + personName, new SelectEvent(Event.Type.PERSON));

        PCollection<Row> bids =
                windowed
                        .apply("Test" + ".Filter." + bidName, Filter.by(e -> e.bid != null))
                        .apply("Test" + ".ToRecords." + bidName, new SelectEvent(Event.Type.BID));

        Schema auctionsWithReceiveTime = Schema.builder()
                .addFields(auctions.getSchema().getFields()).build();
        auctions = auctions
                .setRowSchema(auctionsWithReceiveTime)
                .setRowSchema(auctionsWithReceiveTime);

        Schema peopleWithReceiveTime = Schema.builder()
                .addFields(people.getSchema().getFields()).build();
        people = people
                .setRowSchema(peopleWithReceiveTime)
                .setRowSchema(peopleWithReceiveTime);

        Schema bidsWithReceiveTime = Schema.builder()
                .addFields(bids.getSchema().getFields()).build();
        bids = bids
                .setRowSchema(bidsWithReceiveTime)
                .setRowSchema(bidsWithReceiveTime);

        TupleTag<Row> bidTag = new TupleTag<>("Bid");
        TupleTag<Row> auctionTag = new TupleTag<>("Auction");
        TupleTag<Row> personTag = new TupleTag<>("Person");

        PCollectionTuple withTags = PCollectionTuple.of(bidTag, bids)
                .and(auctionTag, auctions)
                .and(personTag, people);

        ImmutableMap.Builder<String, BeamSqlTable> tables = ImmutableMap.builder();
        for (Map.Entry<TupleTag<?>, PValue> input : withTags.expand().entrySet()) {
            PCollection<?> pCollection = (PCollection<?>) input.getValue();
            var table = new BeamPCollectionTable<>(pCollection);
            tables.put(input.getKey().getId(), table);
        }

        var tableProvider = new ReadOnlyTableProvider("TestPCollection", tables.build());
        var beamSqlEnv = BeamSqlEnv.builder(tableProvider)
                .setQueryPlannerClassName("org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner")
                .setPipelineOptions(new TestPipelineOptions())
                .build();

        return beamSqlEnv.parseQuery(query);
    }
}
