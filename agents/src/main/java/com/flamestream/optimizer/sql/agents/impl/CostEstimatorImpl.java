package com.flamestream.optimizer.sql.agents.impl;

import com.flamestream.optimizer.sql.agents.CostEstimator;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCost;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.CachingRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.io.Serializable;
import java.util.Map;

public class CostEstimatorImpl implements CostEstimator, Serializable {
    @Override
    public synchronized RelOptCost getCumulativeCost(BeamRelNode rel, Map<String, ?> parameters) {
        final var metadataProvider = rel.getCluster().getMetadataProvider();
        final var janinoRelMetadataProvider = RelMetadataQuery.THREAD_PROVIDERS.get();
        rel.getCluster().setMetadataProvider(new CachingRelMetadataProvider(
                ChainedRelMetadataProvider.of(ImmutableList.of(
                        metadataProvider,
                        ModifiedCalciteQueryPlanner.DistinctRowCountHandler.provider(QueryPlanner.QueryParameters.ofNamed(parameters)),
                        ModifiedCalciteQueryPlanner.SelectivityHandler.PROVIDER,
                        ModifiedCalciteQueryPlanner.NonCumulativeCostImpl.SOURCE,
                        RelMdNodeStats.SOURCE
                )),
                rel.getCluster().getPlanner()
        ));
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(rel.getCluster().getMetadataProvider()));
        rel.getCluster().invalidateMetadataQuery();
        try {
            final RelOptCost cost = rel.getCluster().getPlanner().getCost(rel, rel.getCluster().getMetadataQuery());
            return cost;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
        finally {
            RelMetadataQuery.THREAD_PROVIDERS.set(janinoRelMetadataProvider);
            rel.getCluster().setMetadataProvider(metadataProvider);
            rel.getCluster().invalidateMetadataQuery();
        }
    }
}
