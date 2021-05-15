package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public interface CostEstimator {
    RelOptCost getNonCumulativeCost(BeamRelNode rel, RelMetadataQuery mq);
}
