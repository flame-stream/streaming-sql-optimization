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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Query 0: Pass events through unchanged.
 *
 * <p>This measures the overhead of the Beam SQL implementation and test harness like conversion
 * from Java model classes to Beam records.
 *
 * <p>{@link Bid} events are used here at the moment, ås they are most numerous with default
 * configuration.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SqlQuery0 extends NexmarkQueryTransform<Bid> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlQuery0.class);

  private final Class<? extends QueryPlanner> plannerClass;

  private SqlQuery0(String name, Class<? extends QueryPlanner> plannerClass) {
    super("SqlQuery0");
    this.plannerClass = plannerClass;
  }

  public static SqlQuery0 zetaSqlQuery0() {
    return new SqlQuery0("ZetaSqlQuery0", ZetaSQLQueryPlanner.class);
  }

  public static SqlQuery0 calciteSqlQuery0() {
    return new SqlQuery0("SqlQuery0", CalciteQueryPlanner.class);
  }

  @Override
  public PCollection<Bid> expand(PCollection<Event> allEvents) {
    PCollection<Row> rows =
        allEvents
            .apply(Filter.by(NexmarkQueryUtil.IS_BID))
            .apply(getName() + ".SelectEvent", new SelectEvent(Type.BID));

    PCollection<Row> result = rows.apply(getName() + ".Serialize", logBytesMetric(rows.getCoder()))
            .setRowSchema(rows.getSchema())
            .apply(ParDo.of(new LoggingDoFn("INPUT")))
            .setRowSchema(rows.getSchema())
            .apply(SqlTransform.query("SELECT * FROM PCOLLECTION B GROUP BY B.auction, B.price, B.bidder, B.dateTime, B.extra, B.systemTime, TUMBLE(B.systemTime, INTERVAL '5' SECOND)").withQueryPlannerClass(plannerClass));

    Schema.Builder builder = Schema.builder();
    for (Schema.Field f : result.getSchema().getFields()) {
      if (!f.getName().equals("timestamp")) {
        builder.addField(f);
      }
    }

    return result.apply(ParDo.of(new LoggingDoFn("FINAL"))).setRowSchema(result.getSchema())
            .apply(ParDo.of(new RemoveTimestamp())).setRowSchema(builder.build())
        .apply(Convert.fromRows(Bid.class));
  }

  private static class LoggingDoFn extends DoFn<Row, Row> {
    final private String prefix;
    public LoggingDoFn(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {
      Row row = c.element();
      if (row != null) {
        Instant timestamp = row.getValue("timestamp");
        if (timestamp != null) {
          LOGGER.error("timestamp " + timestamp.getMillis());
          LOGGER.error(prefix + "  " + Instant.now().minus(timestamp.getMillis()).getMillis());
        }
      }
      c.output(row);
    }
  }

  private static class RemoveTimestamp extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window, @Timestamp Instant timestamp) {
      Row row = c.element();
      Schema.Builder builder = Schema.builder();
      if (row != null) {
        for (Schema.Field f : row.getSchema().getFields()) {
          if (!f.getName().equals("timestamp")) {
            builder.addField(f);
          }
        }
        Schema schema = builder.build();
        Row.Builder rowBuilder = Row.withSchema(schema);
        for (Schema.Field f : schema.getFields()) {
          rowBuilder.addValue(row.getValue(f.getName()));
        }
        c.output(rowBuilder.build());
      } else {
        c.output(null);
      }
    }
  }

  private PTransform<? super PCollection<Row>, PCollection<Row>> logBytesMetric(
      final Coder<Row> coder) {

    return ParDo.of(
        new DoFn<Row, Row>() {
          private final Counter bytesMetric = Metrics.counter(name, "bytes");

          @ProcessElement
          public void processElement(@Element Row element, OutputReceiver<Row> o)
              throws IOException {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            coder.encode(element, outStream, Coder.Context.OUTER);
            byte[] byteArray = outStream.toByteArray();
            bytesMetric.inc((long) byteArray.length);
            ByteArrayInputStream inStream = new ByteArrayInputStream(byteArray);
            Row row = coder.decode(inStream, Coder.Context.OUTER);
            o.output(row);
          }
        });
  }
}
