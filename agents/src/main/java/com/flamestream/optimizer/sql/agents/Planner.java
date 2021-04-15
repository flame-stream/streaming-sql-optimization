package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;

public interface Planner {
    BeamRelNode getPlan(String query, QueryStatistics statistics);
    RelOptCost getCost(BeamRelNode plan, QueryStatistics statistics);
}
