package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.AuctionPrice;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.PageView;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;

public class Query15 extends NexmarkQueryTransform<PageView> {
	  private final int auctionSkip;

	  public Query15(NexmarkConfiguration configuration) {
	    super("Query15");
	    this.auctionSkip = configuration.auctionSkip;
	  }

	  @Override
	  public PCollection<PageView> expand(PCollection<Event> events) {
	    return events
	        // Only want the bid events.
	        .apply(NexmarkQueryUtil.JUST_VIEWS)

	     
	        // Project just auction id and price.
	        .apply(
	            name + ".Project",
	            ParDo.of(
	                new DoFn<PageView, PageView>() {
	                  @ProcessElement
	                  public void processElement(ProcessContext c) {
	                    c.output(c.element());
	                  }
	                }));
	  }
	}
