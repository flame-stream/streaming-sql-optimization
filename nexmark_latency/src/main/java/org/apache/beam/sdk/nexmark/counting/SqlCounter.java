package org.apache.beam.sdk.nexmark.counting;

import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.latency.NexmarkSqlTransform;
import org.apache.beam.sdk.nexmark.utils.LoggingDoFn;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

public class SqlCounter {

    private static final String QUERY_COUNT_PERSON =
            ""
                    + " SELECT "
                    + "    COUNT(*)"
                    + " FROM "
                    + "    Person";

    private static final String QUERY_COUNT_AUCTION =
            ""
                    + " SELECT "
                    + "    COUNT(*)"
                    + " FROM "
                    + "    Auction";

    private static final String QUERY_COUNT_BID =
            ""
                    + " SELECT "
                    + "    COUNT(*)"
                    + " FROM "
                    + "    Bid B";

    public static final NexmarkSqlTransform person_count_query =
            NexmarkSqlTransform.query(QUERY_COUNT_PERSON).withQueryPlannerClass(CalciteQueryPlanner.class);
    public static final NexmarkSqlTransform auction_count_query =
            NexmarkSqlTransform.query(QUERY_COUNT_AUCTION).withQueryPlannerClass(CalciteQueryPlanner.class);
    public static final NexmarkSqlTransform bid_count_query =
            NexmarkSqlTransform.query(QUERY_COUNT_BID).withQueryPlannerClass(CalciteQueryPlanner.class);

    public static void applyCounting(PCollectionTuple withTags, NexmarkConfiguration configuration) {
        PCollection<Row> person_count = withTags.apply(person_count_query);
        PCollection<Row> auction_count = withTags.apply(auction_count_query);
        PCollection<Row> bid_count = withTags.apply(bid_count_query);

//        var directory = configuration.latencyLogDirectory + "_counting";
//
//        person_count.apply(ToString.elements())
//                .apply(TextIO.write().to(directory)
//                        .withWindowedWrites()
//                        .withNumShards(1)
//                        .withSuffix("_PERSON.txt"));
//
//        auction_count.apply(ToString.elements())
//                .apply(TextIO.write().to(directory)
//                        .withWindowedWrites()
//                        .withNumShards(1)
//                        .withSuffix("_AUCTION.txt"));
//
//        bid_count.apply(ToString.elements())
//                .apply(TextIO.write().to(directory)
//                        .withWindowedWrites()
//                        .withNumShards(1)
//                        .withSuffix("_BID.txt"));
    }

    public static void applyCountingVer2(PCollectionTuple withTags, NexmarkConfiguration configuration) {
        var auctions = withTags.get("Auction");
        auctions.apply(Combine.globally(Count.combineFn()).withoutDefaults())
                .apply(ParDo.of(new LoggingDoFn<>()));

        var persons = withTags.get("Person");
        persons.apply(Combine.globally(Count.combineFn()).withoutDefaults())
                .apply(ParDo.of(new LoggingDoFn<>()));

        var bids = withTags.get("Bid");
        bids.apply(Combine.globally(Count.combineFn()).withoutDefaults())
                .apply(ParDo.of(new LoggingDoFn<>()));
    }
}
