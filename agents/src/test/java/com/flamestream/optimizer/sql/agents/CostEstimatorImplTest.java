package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.testutils.TestPipelineOptions;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.ModifiedCalciteQueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.nexmark.Monitor;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.model.sql.SelectEvent;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.joda.time.Duration;
import org.junit.Test;

import java.nio.channels.Pipe;
import java.util.Map;

import static org.junit.Assert.*;

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