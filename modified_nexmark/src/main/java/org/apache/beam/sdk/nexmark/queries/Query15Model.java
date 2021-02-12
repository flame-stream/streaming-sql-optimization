package org.apache.beam.sdk.nexmark.queries;


import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.PageView;
import org.apache.beam.sdk.values.TimestampedValue;

/** A direct implementation of {@link Query15}. */
public class Query15Model extends NexmarkQueryModel<PageView> implements Serializable {
  /** Simulator for query 1. */
  private static class Simulator extends AbstractSimulator<Event, PageView> {
    public Simulator(NexmarkConfiguration configuration) {
      super(NexmarkUtils.standardEventIterator(configuration));
    }

    @Override
    protected void run() {
      TimestampedValue<Event> timestampedEvent = nextInput();
      if (timestampedEvent == null) {
        allDone();
        return;
      }
      Event event = timestampedEvent.getValue();
      if (event.pageView == null) {
        // Ignore non page view events.
        return;
      }
      PageView pageView = event.pageView;
       TimestampedValue<PageView> result =
          TimestampedValue.of(pageView, timestampedEvent.getTimestamp());
      addResult(result);
    }
  }

  public Query15Model(NexmarkConfiguration configuration) {
    super(configuration);
  }

  @Override
  public AbstractSimulator<?, PageView> simulator() {
    return new Simulator(configuration);
  }

  @Override
  protected Collection<String> toCollection(Iterator<TimestampedValue<PageView>> itr) {
    return toValueTimestamp(itr);
  }
}
