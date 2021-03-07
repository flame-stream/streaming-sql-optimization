package org.apache.beam.sdk.nexmark.queries.sql;

import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Event.Type;
import org.apache.beam.sdk.nexmark.model.PageView;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.RecommendedAuction;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryTransform;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

public class SqlQuery15 extends NexmarkQueryTransform<RecommendedAuction> {

	private static final String QUERY = "with viewed_auctions (viewedAction, viewer) as \n"
			+ "(select  a.id, v.viewer from Auction a inner join PageView v on v.auction=a.id), \n" + "\n"
			+ "bidder_group(main, competitor, auction) as \n"
			+ "(select b1.bidder, b2.bidder, b1.auction from Bid as b1\n"
			+ "inner join Bid as b2 on b1.auction = b2.auction)\n" + "\n"
			+ "select p.id, p.emailAddress, b.competitor, b.auction, va.viewedAction \n"
			+ "from bidder_group b inner join viewed_auctions va on b.competitor=va.viewer  \n"
			+ "inner join Person p on p.id = b.main";

	private final NexmarkConfiguration configuration;
	private final Class<? extends QueryPlanner> plannerClass;

	private SqlQuery15(String name, NexmarkConfiguration configuration, Class<? extends QueryPlanner> plannerClass) {
		super(name);
		this.configuration = configuration;
		this.plannerClass = plannerClass;
	}

	public static SqlQuery15 calciteSqlQuery15(NexmarkConfiguration configuration) {
		return new SqlQuery15("SqlQuery15", configuration, CalciteQueryPlanner.class);
	}

	public static SqlQuery15 zetaSqlQuery15(NexmarkConfiguration configuration) {
		return new SqlQuery15("ZetaSqlQuery15", configuration, ZetaSQLQueryPlanner.class);
	}

	@Override
	public PCollection<RecommendedAuction> expand(PCollection<Event> allEvents) {
		PCollection<Event> windowed = allEvents
				.apply(Window.into(FixedWindows.of(Duration.standardSeconds(configuration.windowSizeSec))));

		String auctionName = Auction.class.getSimpleName();
		PCollection<Row> auctions = windowed
				.apply(getName() + ".Filter." + auctionName, Filter.by(e1 -> e1.newAuction != null))
				.apply(getName() + ".ToRecords." + auctionName, new SelectEvent(Type.AUCTION));

		String personName = Person.class.getSimpleName();

		PCollection<Row> people = windowed
				.apply(getName() + ".Filter." + personName, Filter.by(e -> e.newPerson != null))
				.apply(getName() + ".ToRecords." + personName, new SelectEvent(Type.PERSON));

		String bidName = Bid.class.getSimpleName();

		PCollection<Row> bid = windowed.apply(getName() + ".Filter." + bidName, Filter.by(e -> e.bid != null))
				.apply(getName() + ".ToRecords." + bidName, new SelectEvent(Type.BID));

		String pageViewName = PageView.class.getSimpleName();

		PCollection<Row> pageView = windowed
				.apply(getName() + ".Filter." + pageViewName, Filter.by(e -> e.pageView != null))
				.apply(getName() + ".ToRecords." + pageViewName, new SelectEvent(Type.PAGE_VIEW));

		PCollectionTuple inputStreams = PCollectionTuple.of(new TupleTag<>("Auction"), auctions)
				.and(new TupleTag<>("Person"), people).and(new TupleTag<>("Bid"), bid)
				.and(new TupleTag<>("PageView"), pageView);

		return inputStreams.apply(SqlTransform.query(QUERY).withQueryPlannerClass(plannerClass))
				.apply(Convert.fromRows(RecommendedAuction.class));
	}
}
