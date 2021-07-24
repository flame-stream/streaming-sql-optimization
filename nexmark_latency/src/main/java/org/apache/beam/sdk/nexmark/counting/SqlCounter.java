package org.apache.beam.sdk.nexmark.counting;

import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.latency.NexmarkSqlEnv;
import org.apache.beam.sdk.nexmark.utils.LoggingDoFn;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
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

    public static final NexmarkSqlEnv NEXMARK_SQL_ENV =
            NexmarkSqlEnv.build().withQueryPlannerClass(CalciteQueryPlanner.class);

    public static void applyCounting(PCollectionTuple withTags, NexmarkConfiguration configuration) {
        var directory = configuration.latencyLogDirectory + "_counting";
        withTags.apply(new PTransform<PInput, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PInput input) {
                return BeamSqlRelUtils.toPCollection(
                        input.getPipeline(),
                        NEXMARK_SQL_ENV.apply(input).parseQuery(QUERY_COUNT_PERSON, QueryPlanner.QueryParameters.ofNone())
                );
            }
        }).apply(ToString.elements())
                .apply(TextIO.write().to(directory)
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix("_PERSON.txt"));
        withTags.apply(new PTransform<PInput, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PInput input) {
                return BeamSqlRelUtils.toPCollection(
                        input.getPipeline(),
                        NEXMARK_SQL_ENV.apply(input).parseQuery(QUERY_COUNT_AUCTION, QueryPlanner.QueryParameters.ofNone())
                );
            }
        }).apply(ToString.elements())
                .apply(TextIO.write().to(directory)
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix("_AUCTION.txt"));
        withTags.apply(new PTransform<PInput, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PInput input) {
                return BeamSqlRelUtils.toPCollection(
                        input.getPipeline(),
                        NEXMARK_SQL_ENV.apply(input).parseQuery(QUERY_COUNT_BID, QueryPlanner.QueryParameters.ofNone())
                );
            }
        }).apply(ToString.elements())
                .apply(TextIO.write().to(directory)
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix("_BID.txt"));
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
