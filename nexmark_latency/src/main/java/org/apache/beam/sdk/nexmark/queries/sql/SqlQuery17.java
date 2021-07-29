/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark.queries.sql;

import org.apache.beam.sdk.extensions.sql.impl.NexmarkQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.latency.AddArrivalTime;
import org.apache.beam.sdk.nexmark.latency.AddReceiveTime;
import org.apache.beam.sdk.nexmark.latency.LatencyCombineFn;
import org.apache.beam.sdk.nexmark.latency.NexmarkSqlEnv;
import org.apache.beam.sdk.nexmark.model.*;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.beam.sdk.nexmark.counting.SqlCounter.*;

/**
 * SQL query for second graph. For first graph use SqlQuery16.
 */
public class SqlQuery17 extends NexmarkQueryTransform<Latency> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery17.class);

    private static final String QUERY_2 =
            ""
                + " SELECT "
                + "    B.receiveTime AS timestamp1, A.receiveTime AS timestamp2, P.receiveTime AS timestamp3"
                + " FROM "
                + "    Person P INNER JOIN Bid B on B.bidder = P.id"
                + "           INNER JOIN Auction A on A.seller = P.id";
    public static final QueryPlanner.@UnknownKeyFor @NonNull @Initialized QueryParameters QUERY_PARAMETERS = QueryPlanner.QueryParameters.ofNamed(Map.ofEntries(
            Map.entry("table_column_distinct_row_count:Bid.auction", 100),
            Map.entry("table_column_distinct_row_count:Auction.id", 100),
            Map.entry("table_column_distinct_row_count:Person.id", 1000),
            Map.entry("table_column_distinct_row_count:Auction.seller", 1000)
    ));

    private final NexmarkSqlEnv query;

    private final NexmarkConfiguration configuration;

    public SqlQuery17(NexmarkConfiguration configuration) {
        super("SqlQuery17");

        this.configuration = configuration;
        query = NexmarkSqlEnv.build().withQueryPlannerClass(NexmarkQueryPlanner.class);
    }

    @Override
    public PCollection<Latency> expand(PCollection<Event> allEvents) {
        PCollection<Event> windowed =
                allEvents.apply(
                        Window.into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))));

        String auctionName = Auction.class.getSimpleName();
        String personName = Person.class.getSimpleName();
        String bidName = Bid.class.getSimpleName();

        PCollection<Row> auctions =
                windowed
                        .apply(getName() + ".Filter." + auctionName, Filter.by(e1 -> e1.newAuction != null))
                        .apply(getName() + ".ToRecords." + auctionName, new SelectEvent(Type.AUCTION));

        PCollection<Row> people =
                windowed
                        .apply(getName() + ".Filter." + personName, Filter.by(e -> e.newPerson != null))
                        .apply(getName() + ".ToRecords." + personName, new SelectEvent(Type.PERSON));

        PCollection<Row> bids =
                windowed
                        .apply(getName() + ".Filter." + bidName, Filter.by(e -> e.bid != null))
                        .apply(getName() + ".ToRecords." + bidName, new SelectEvent(Type.BID));


        Schema auctionsWithReceiveTime = Schema.builder()
                .addFields(auctions.getSchema().getFields()).addDateTimeField("receiveTime").build();
        auctions = auctions
                .setRowSchema(auctionsWithReceiveTime)
                .apply(MapElements.via(new AddReceiveTime()))
                .setRowSchema(auctionsWithReceiveTime);

        Schema peoplesWithReceiveTime = Schema.builder()
                .addFields(people.getSchema().getFields()).addDateTimeField("receiveTime").build();
        people = people
                .setRowSchema(peoplesWithReceiveTime)
                .apply(MapElements.via(new AddReceiveTime()))
                .setRowSchema(peoplesWithReceiveTime);

        Schema bidsWithReceiveTime = Schema.builder()
                .addFields(bids.getSchema().getFields()).addDateTimeField("receiveTime").build();
        bids = bids
                .setRowSchema(bidsWithReceiveTime)
                .apply(MapElements.via(new AddReceiveTime()))
                .setRowSchema(bidsWithReceiveTime);

        TupleTag<Row> bidTag = new TupleTag<>("Bid");
        TupleTag<Row> auctionTag = new TupleTag<>("Auction");
        TupleTag<Row> personTag = new TupleTag<>("Person");

        PCollectionTuple withTags = PCollectionTuple.of(bidTag, bids)
                .and(auctionTag, auctions)
                .and(personTag, people);

        if (configuration.counting) {
            applyCounting(withTags, configuration);
        }

        PCollection<Row> results = withTags.apply(new PTransform<PInput, PCollection<Row>>() {
            @Override
            public PCollection<Row> expand(PInput input) {
                return BeamSqlRelUtils.toPCollection(
                        input.getPipeline(),
                        query.apply(input).parseQuery(QUERY_2, QUERY_PARAMETERS)
                );
            }
        });

        // adding arrival (from join) time for each tuple to be used for latency calculation
        Schema withArrivalTime = Schema.builder()
                .addFields(results.getSchema().getFields()).addDateTimeField("arrivalTime").build();

        PCollection<Latency> latency = results
                .setRowSchema(withArrivalTime)
                .apply(MapElements.via(new AddArrivalTime()))
                .setRowSchema(withArrivalTime)
//                 calculate latency per window
                .apply(Combine.globally(new LatencyCombineFn()).withoutDefaults());

        latency.apply(ToString.elements())
                .apply(TextIO.write().to(configuration.latencyLogDirectory)
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix(".txt"));

        return latency;
    }
}

