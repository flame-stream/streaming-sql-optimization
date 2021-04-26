package com.flamestream.optimizer.sql.agents;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public interface CostEstimator {
    RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq);
}
