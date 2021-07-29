package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import org.junit.Test;

import java.util.Map;

public class CostEstimatorImplTest {
    @Test
    public void getCumulativeCostFirst() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getFirstQueryPlan();

        System.out.println(relNode);

        var parameters = Map.ofEntries(
                Map.entry("table_column_distinct_row_count:Bid.bidder", 1000),
                Map.entry("table_column_distinct_row_count:Person.id", 1000),
                Map.entry("table_column_distinct_row_count:Auction.seller", 100000)
        );

        var cost = estimator.getCumulativeCost(relNode, parameters,
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }

    @Test
    public void getAnotherCumulativeCostFirst() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getFirstQueryPlan();

        System.out.println(relNode);

        var parameters = Map.ofEntries(
                Map.entry("table_column_distinct_row_count:Bid.bidder", 100000),
                Map.entry("table_column_distinct_row_count:Person.id", 1000),
                Map.entry("table_column_distinct_row_count:Auction.seller", 1000)
        );

        var cost = estimator.getCumulativeCost(relNode, parameters,
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }

    @Test
    public void getCumulativeCostSecond() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getSecondQueryPlan();

        System.out.println(relNode);

        var parameters = Map.ofEntries(
                Map.entry("table_column_distinct_row_count:Bid.bidder", 1000),
                Map.entry("table_column_distinct_row_count:Person.id", 1000),
                Map.entry("table_column_distinct_row_count:Auction.seller", 100000)
        );

        var cost = estimator.getCumulativeCost(relNode, parameters,
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }
}
