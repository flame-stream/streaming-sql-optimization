package com.flamestream.optimizer.sql.agents;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.Map;

public class CostEstimatorImpl implements CostEstimator {
    @Override
    public RelOptCost getCumulativeCost(BeamRelNode rel, Map<String, ?> parameters, RelMetadataQuery mq) {
        rel.getCluster().setMetadataProvider(ChainedRelMetadataProvider.of(ImmutableList.of(
                ModifiedCalciteQueryPlanner.DistinctRowCountHandler.provider(QueryPlanner.QueryParameters.ofNamed(parameters)),
                ModifiedCalciteQueryPlanner.SelectivityHandler.PROVIDER,
                ModifiedCalciteQueryPlanner.NonCumulativeCostImpl.SOURCE,
                RelMdNodeStats.SOURCE,
                rel.getCluster().getMetadataProvider()
        )));
        return mq.getCumulativeCost(rel);
    }
}
