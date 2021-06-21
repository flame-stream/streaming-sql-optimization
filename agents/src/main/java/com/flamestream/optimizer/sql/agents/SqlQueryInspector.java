package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;

import java.util.HashMap;

public class SqlQueryInspector {
    public HashMap<RelNode, Integer> inspectQuery(RelNode root) {
        var map = new HashMap<RelNode, Integer>();
        inspectSubquery(root, map);
        return map;
    }

    private void inspectSubquery(RelNode node, HashMap<RelNode, Integer> map) {
        if (node instanceof BeamJoinRel) {
            var joinInput = (BeamJoinRel) node;

            var condition = joinInput.getCondition();
            RexCall conditionCall;
            RexNode first, second;
            RexInputRef firstRexInput, secondRexInput;

            if (condition instanceof RexCall) {
                conditionCall = (RexCall) condition;
                var operands = conditionCall.getOperands();
                if (operands.size() != 2) {
                    throw new RuntimeException("JOIN condition should be an equal condition with 2 operands");
                }
                first = operands.get(0);
                second = operands.get(1);

                if (first instanceof RexInputRef && second instanceof RexInputRef) {
                    firstRexInput = (RexInputRef) first;
                    secondRexInput = (RexInputRef) second;
                } else {
                    throw new RuntimeException("JOIN condition operands should be RexInputRefs");
                }
            } else {
                throw new RuntimeException("Bad JOIN condition");
            }

            var left = joinInput.getLeft();
            var right = joinInput.getRight();

            map.put(left, firstRexInput.getIndex());
            map.put(right, secondRexInput.getIndex());


            if (left instanceof BeamIOSourceRel) {
                // left is a non-recursive subquery
            } else {
                inspectSubquery(left, map);
            }

            if (right instanceof BeamIOSourceRel) {
                // right is a non-recursive subquery
            } else {
                inspectSubquery(right, map);
            }

        } else {
            var inputs = node.getInputs();
            for (RelNode input: inputs) {
                inspectSubquery(input, map);
            }
        }
    }
}
