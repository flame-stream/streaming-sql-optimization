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
import org.apache.beam.sdk.nexmark.latency.LatencyCombineFn;
import org.apache.beam.sdk.nexmark.latency.NexmarkSqlTransform;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.Latency;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class SqlQuery15 extends NexmarkQueryTransform<Latency> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery15.class);

    private static final String QUERY_TEMPLATE =
            ""
                    // uncomment this for a simple select with windowing
                    /* + "SELECT B.dateTime AS timestamp1, B.dateTime AS timestamp2, B.dateTime AS timestamp3, " +
                      "TUMBLE_START(B.dateTime, INTERVAL '%1$d' SECOND) AS starttime " +
                      "FROM Bid B GROUP BY B.auction, B.price, B.bidder, B.dateTime, B.extra, " +
                      "TUMBLE(B.dateTime, INTERVAL '%1$d' SECOND)";*/

                    // two joins with windowing

            + "SELECT Bid.dateTime AS timestamp1, " +
                    "Auction.dateTime AS timestamp2, " +
                    "Person.dateTime AS timestamp3 " +
                    "FROM (SELECT * FROM Bid B GROUP BY " +
                        "B.auction, B.price, B.bidder, B.dateTime, B.extra,  " +
                        "TUMBLE(B.dateTime, INTERVAL '%1$d' SECOND)) AS Bid " +
                    "JOIN (SELECT * FROM Auction A GROUP BY " +
                        "A.id, A.itemName, A.description, A.initialBid, A.reserve, A.dateTime, A.expires, A.seller, A.category, A.extra,  " +
                        "TUMBLE(A.dateTime, INTERVAL '%1$d' SECOND)) AS Auction ON Bid.auction = Auction.id " +
                    "JOIN (SELECT * FROM Person P GROUP BY " +
                        "P.id, P.name, P.emailAddress, P.creditCard, P.city, P.state, P.dateTime, P.extra, " +
                        "TUMBLE(P.dateTime, INTERVAL '%1$d' SECOND)) AS Person " +
                    "ON Auction.seller = Person.id";

    private final NexmarkSqlTransform query;

    private final NexmarkConfiguration configuration;

    public SqlQuery15(NexmarkConfiguration configuration) {
        super("SqlQuery15");

        this.configuration = configuration;
        String queryString = String.format(QUERY_TEMPLATE, configuration.windowSizeSec);
        query = NexmarkSqlTransform.query(queryString).withQueryPlannerClass(UpdatedCalciteQueryPlanner.class);
    }

    @Override
    public PCollection<Latency> expand(PCollection<Event> allEvents) {
        PCollection<Row> bids =
                allEvents
                        .apply(Filter.by(NexmarkQueryUtil.IS_BID))
                        .apply(getName() + ".SelectEvent", new SelectEvent(Type.BID));
//        bids.apply(ParDo.of(new LoggingDoFn("BIDS"))).setRowSchema(bids.getSchema());

        PCollection<Row> auctions =
                allEvents
                        .apply(Filter.by(NexmarkQueryUtil.IS_NEW_AUCTION))
                        .apply(getName() + ".SelectEvent", new SelectEvent(Type.AUCTION));

        PCollection<Row> people =
                allEvents
                        .apply(Filter.by(NexmarkQueryUtil.IS_NEW_PERSON))
                        .apply(getName() + ".SelectEvent", new SelectEvent(Type.PERSON));

        TupleTag<Row> bidTag = new TupleTag<>("Bid");
        TupleTag<Row> auctionTag = new TupleTag<>("Auction");
        TupleTag<Row> personTag = new TupleTag<>("Person");

        // specify statistics, rn it's just rates. maybe these particular rates are not particularly correct
        Map<TupleTag<Row>, Double> rates = new HashMap<>();
        rates.put(bidTag, configuration.nextEventRate * configuration.hotBiddersRatio / 100.0);
        rates.put(auctionTag, configuration.nextEventRate * configuration.hotAuctionRatio / 100.0);
        rates.put(personTag, configuration.nextEventRate * configuration.hotSellersRatio / 100.0);

        PCollection<Row> results = PCollectionTuple.of(bidTag, bids)
                .and(auctionTag, auctions)
                .and(personTag, people)
                .apply(query.withRates(rates));
             // .apply(query); // <-- for a run with standard rates

        // adding arrival (from join) time for each tuple to be used for latency calculation
        Schema withArrivalTime = Schema.builder().addFields(results.getSchema().getFields()).addDateTimeField("arrivalTime").build();
        PCollection<Latency> latency = results
                .setRowSchema(withArrivalTime)
                .apply(MapElements.via(new AddArrivalTime()))
                .setRowSchema(withArrivalTime)
                // calculate latency per window
                .apply(Combine.globally(new LatencyCombineFn()).withoutDefaults());

        latency.apply(ToString.elements())
                .apply(TextIO.write().to(configuration.latencyLogDirectory)
                        .withWindowedWrites()
                        .withNumShards(1)
                        .withSuffix(".txt"));

        return latency;
    }
}
