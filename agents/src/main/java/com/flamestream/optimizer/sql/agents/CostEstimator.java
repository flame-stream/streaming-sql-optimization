package com.flamestream.optimizer.sql.agents;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

public interface CostEstimator {
    RelOptCost getCumulativeCost(BeamRelNode rel, ImmutableList<RelMetadataProvider> providers, RelMetadataQuery mq);
}
