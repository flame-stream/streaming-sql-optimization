package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.SqlQueryInspector;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlQueryInspectorTest {
    @Test
    public void getCumulativeCostFirst() {
        var res = new SqlQueryInspector().inspectQuery(OptimizerTestUtils.getSecondQueryPlan());
        assertEquals(res.toString(), 3, res.size());
        assertEquals(res.toString(), 1, res.entrySet().stream().filter(relNodeSetEntry ->
                Arrays.asList("beam", "Bid").equals(qualifiedName(relNodeSetEntry.getKey().getTable()))
                        && Collections.singleton("bidder").equals(relNodeSetEntry.getValue())
        ).count());
        assertEquals(res.toString(), 1, res.entrySet().stream().filter(relNodeSetEntry ->
                Arrays.asList("beam", "Person").equals(qualifiedName(relNodeSetEntry.getKey().getTable()))
                        && Collections.singleton("id").equals(relNodeSetEntry.getValue())
        ).count());
        assertEquals(res.toString(), 1, res.entrySet().stream().filter(relNodeSetEntry ->
                Arrays.asList("beam", "Auction").equals(qualifiedName(relNodeSetEntry.getKey().getTable()))
                        && Collections.singleton("seller").equals(relNodeSetEntry.getValue())
        ).count());
    }

    private static List<String> qualifiedName(RelOptTable table) {
        return table == null ? null : table.getQualifiedName();
    }
}
