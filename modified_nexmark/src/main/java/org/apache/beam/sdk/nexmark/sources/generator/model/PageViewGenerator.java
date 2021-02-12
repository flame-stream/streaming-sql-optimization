package org.apache.beam.sdk.nexmark.sources.generator.model;

import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.lastBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.nextBase0AuctionId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.lastBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextBase0PersonId;
import static org.apache.beam.sdk.nexmark.sources.generator.model.StringsGenerator.nextExtra;

import java.util.Random;

import org.apache.beam.sdk.nexmark.model.PageView;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.joda.time.Instant;

public class PageViewGenerator {

	 /**
	   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
	   * values.
	   */
	  private static final int POPULAR_AUCTION_RATIO = 150;

	  private static final int ACTIVE_VIEWER_RATIO = 50;

	  /** Generate and return a random bid with next available id. */
	  public static PageView nextPageView(long eventId, Random random, long timestamp, GeneratorConfig config) {

	    long auction;
	    // Here P(view will be for a popular auction) = 1 - 1/popularAuctionRatio.
	    if (random.nextInt(config.getPopularActionRatio()) > 0) {
	      // Choose the first auction in the batch of last POPULAR_AUCTION_RATIO auctions.
	      auction = (lastBase0AuctionId(eventId) / POPULAR_AUCTION_RATIO) * POPULAR_AUCTION_RATIO;
	    } else {
	      auction = nextBase0AuctionId(eventId, random, config);
	    }
	    auction += GeneratorConfig.FIRST_AUCTION_ID;

	    long viewer;
	    // Here P(view will be by a hot viewer) = 1 - 1/activeViewersRatio
	    if (random.nextInt(config.getActiveViewersRatio()) > 0) {
	      // Choose the first person in the batch of last ACTIVE_VIEWER_RATIO people.
	    	viewer = (lastBase0PersonId(eventId) / ACTIVE_VIEWER_RATIO) * ACTIVE_VIEWER_RATIO;
	    } else {
	    	viewer = nextBase0PersonId(eventId, random, config);
	    }
	    viewer += GeneratorConfig.FIRST_PERSON_ID;

	    int currentSize = 8 + 8 + 8 + 8;
	    String extra = nextExtra(random, currentSize, config.getAvgPageViewByteSize());
	    return new PageView(eventId, viewer, auction, new Instant(timestamp), extra);
	  }
}
