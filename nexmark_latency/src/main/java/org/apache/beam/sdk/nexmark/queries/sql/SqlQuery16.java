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

import org.apache.beam.sdk.extensions.sql.impl.UpdatedCalciteQueryPlanner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.latency.AddArrivalTime;
import org.apache.beam.sdk.nexmark.latency.AddReceiveTime;
import org.apache.beam.sdk.nexmark.latency.LatencyCombineFn;
import org.apache.beam.sdk.nexmark.latency.NexmarkSqlTransform;
import org.apache.beam.sdk.nexmark.model.*;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.beam.sdk.nexmark.counting.SqlCounter.applyCountingVer2;

/**
 * SQL query for first graph. For second graph use SqlQuery17.
 */
public class SqlQuery16 extends NexmarkQueryTransform<Latency> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery16.class);

    private static final String QUERY_1 =
            ""
                + " SELECT "
                + "    B.receiveTime AS timestamp1, A.receiveTime AS timestamp2, P.receiveTime AS timestamp3"
                + " FROM "
                + "    Auction A INNER JOIN Person P on A.seller = P.id "
                + "       INNER JOIN Bid B on B.bidder = P.id";

    private final NexmarkSqlTransform query;

    private final NexmarkConfiguration configuration;

    public SqlQuery16(NexmarkConfiguration configuration) {
        super("SqlQuery16");

        this.configuration = configuration;
        query = NexmarkSqlTransform.query(QUERY_1).withQueryPlannerClass(UpdatedCalciteQueryPlanner.class);
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
            // applyCounting(withTags, configuration);
            applyCountingVer2(withTags, configuration);
        }

        PCollection<Row> results = withTags.apply(query);

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

