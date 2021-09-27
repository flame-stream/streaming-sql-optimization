package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CostEstimatorImplTest {
    @Test
    public void getCumulativeCostFirst() {
        var parameters = Map.ofEntries(
                Map.entry("table_column_distinct_row_count:beam.person.id", 1000),
                Map.entry("table_column_distinct_row_count:beam.auction.seller", 1000)
        );
        var relNode = OptimizerTestUtils.getFirstQueryPlan(QueryPlanner.QueryParameters.ofNamed(parameters));
        assertEquals(
                BeamCostModel.FACTORY.makeCost(920, 0),
                new CostEstimatorImpl().getCumulativeCost(relNode, parameters)
        );
        assertEquals(
                BeamCostModel.FACTORY.makeCost(32600, 0),
                new CostEstimatorImpl().getCumulativeCost(relNode, Map.ofEntries(
                        Map.entry("table_column_distinct_row_count:beam.person.id", 10),
                        Map.entry("table_column_distinct_row_count:beam.auction.seller", 10)
                ))
        );
    }

    @Test
    public void getCumulativeCostSecond() {
        var parameters = Map.ofEntries(
                Map.entry("table_column_distinct_row_count:beam.person.id", 10),
                Map.entry("table_column_distinct_row_count:beam.auction.seller", 10)
        );
        var relNode = OptimizerTestUtils.getFirstQueryPlan(QueryPlanner.QueryParameters.ofNamed(parameters));
        assertEquals(
                BeamCostModel.FACTORY.makeCost(32600, 0),
                new CostEstimatorImpl().getCumulativeCost(relNode, parameters)
        );
        assertEquals(
                BeamCostModel.FACTORY.makeCost(920, 0),
                new CostEstimatorImpl().getCumulativeCost(relNode, Map.ofEntries(
                        Map.entry("table_column_distinct_row_count:beam.person.id", 1000),
                        Map.entry("table_column_distinct_row_count:beam.auction.seller", 1000)
                ))
        );
    }
}
