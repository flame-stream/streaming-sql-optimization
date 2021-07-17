package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CostEstimatorImpl;
import com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.Test;

public class CostEstimatorImplTest {
    @Test
    public void getCumulativeCostFirst() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getFirstQueryPlan();

        System.out.println(relNode);

        RelMetadataQuery.THREAD_PROVIDERS.set(
                JaninoRelMetadataProvider.of(relNode.getCluster().getMetadataProvider()));

        var cost = estimator.getCumulativeCost(relNode, ImmutableList.of(),
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }

    @Test
    public void getAnotherCumulativeCostFirst() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getFirstQueryPlan();

        System.out.println(relNode);

        RelMetadataQuery.THREAD_PROVIDERS.set(
                JaninoRelMetadataProvider.of(relNode.getCluster().getMetadataProvider()));

        var cost = estimator.getCumulativeCost(relNode, ImmutableList.of(),
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }

    @Test
    public void getCumulativeCostSecond() {
        var estimator = new CostEstimatorImpl();
        var relNode = OptimizerTestUtils.getSecondQueryPlan();

        System.out.println(relNode);

        RelMetadataQuery.THREAD_PROVIDERS.set(
                JaninoRelMetadataProvider.of(relNode.getCluster().getMetadataProvider()));

        var cost = estimator.getCumulativeCost(relNode, ImmutableList.of(),
                relNode.getCluster().getMetadataQuery());

        System.out.println(cost);
    }
}