package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.Map;

public interface CostEstimator {
    RelOptCost getCumulativeCost(BeamRelNode rel, Map<String, ?> parameters, RelMetadataQuery mq);
}
