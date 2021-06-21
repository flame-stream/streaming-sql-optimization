package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

public class SqlQueryInspectorTest {
    @Test
    public void getCumulativeCostFirst() {
        var relNode = OptimizerTestUtils.getSecondQueryPlan();
        var sqlQueryInspector = new SqlQueryInspector();
        var res = sqlQueryInspector.inspectQuery(relNode);
        Assert.assertEquals(4, res.size());
    }
}
