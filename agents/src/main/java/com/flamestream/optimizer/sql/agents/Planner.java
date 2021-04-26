package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;

public interface Planner {
    SqlNode parse(String sqlStatement) throws ParseException;
    BeamRelNode convertToBeamRel(String sqlStatement, QueryPlanner.QueryParameters queryParameters);
    RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq);
}
