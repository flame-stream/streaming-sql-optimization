package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.testutils.TestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

import java.util.stream.Collectors;

public class OptimizerTestUtils {
    private static final String QUERY_1 = ""
            + " SELECT "
            + "     P.name, B.price"
            + " FROM "
            + "     Auction A INNER JOIN Person P on A.seller = P.id "
            + "         INNER JOIN Bid B on B.bidder = P.id";

    private static final String QUERY_2 = ""
            + " SELECT "
            + "     P.name, B.price, A.category"
            + " FROM "
            + "     Person P INNER JOIN Bid B on B.bidder = P.id"
            + "         INNER JOIN Auction A on A.seller = P.id";

    public static BeamRelNode getFirstQueryPlan(QueryPlanner.QueryParameters queryParameters) {
        return getNEXMarkQueryPlan(QUERY_1, queryParameters);
    }

    public static BeamRelNode getSecondQueryPlan(QueryPlanner.QueryParameters queryParameters) {
        return getNEXMarkQueryPlan(QUERY_2, queryParameters);
    }

    public static BeamRelNode getNEXMarkQueryPlan(String query, QueryPlanner.QueryParameters queryParameters) {
        final var name = "Test";
        final var windowed = Pipeline.create().apply(
                name + ".ReadUnbounded",
                NexmarkUtils.streamEventsSource(NexmarkConfiguration.DEFAULT)
        )
                // Monitor events as they go by.
                .apply(name + ".Monitor", new Monitor<Event>(name + ".Events", "event").getTransform())
                .apply(name + ".Snoop", NexmarkUtils.snoop(name)).apply(
                        Window.into(FixedWindows.of(Duration.standardSeconds(NexmarkConfiguration.DEFAULT.windowSizeSec)))
                );

        return BeamSqlEnv.builder(new ReadOnlyTableProvider(
                "TestPCollection",
                PCollectionTuple.of(new TupleTag<>("Bid"), windowed
                        .apply(name + ".Filter.Bid", Filter.by(e -> e.bid != null))
                        .apply(name + ".ToRecords.Bid", new SelectEvent(Event.Type.BID))
                ).and(new TupleTag<>("Auction"), windowed
                        .apply(name + ".Filter.Auction", Filter.by(e -> e.newAuction != null))
                        .apply(name + ".ToRecords.Auction", new SelectEvent(Event.Type.AUCTION))
                ).and(new TupleTag<>("Person"), windowed
                        .apply(name + ".Filter.Person", Filter.by(e -> e.newPerson != null))
                        .apply(name + ".ToRecords.Person", new SelectEvent(Event.Type.PERSON))
                ).expand().entrySet().stream().collect(Collectors.toMap(
                        table -> table.getKey().getId(),
                        table -> new BeamPCollectionTable<>((PCollection<?>) table.getValue())
                ))
        ))
                .setQueryPlannerClassName(ModifiedCalciteQueryPlanner.class.getName())
                .setPipelineOptions(new TestPipelineOptions())
                .build().parseQuery(query, queryParameters);
    }
}
