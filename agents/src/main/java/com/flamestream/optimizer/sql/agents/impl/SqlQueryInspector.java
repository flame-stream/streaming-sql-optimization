package com.flamestream.optimizer.sql.agents.impl;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Util for parse SQL query to know interesting streams fields for stats handling
 */
public class SqlQueryInspector {
    public HashMap<RelNode, Set<String>> inspectQuery(RelNode root) {
        var map = new HashMap<RelNode, Set<String>>();
        inspectSubquery(root, map, ImmutableBitSet.of());
        return map;
    }

    private void inspectSubquery(
            RelNode node, HashMap<RelNode, Set<String>> map, ImmutableBitSet cardinalitiesInterest
    ) {
        if (node instanceof BeamJoinRel) {
            final var joinRel = (BeamJoinRel) node;
            final var cardinalitiesInterestBuilder = cardinalitiesInterest.rebuild();
            final var condition = joinRel.getCondition();
            if (condition instanceof RexCall) {
                final var rexCall = (RexCall) condition;
                if (rexCall.getKind() == SqlKind.EQUALS) {
                    if (rexCall.getOperands().size() != 2) {
                        throw new RuntimeException("JOIN condition should be an equal condition with 2 operands");
                    }
                    for (var operand : rexCall.getOperands()) {
                        if (operand instanceof RexInputRef) {
                            cardinalitiesInterestBuilder.set(((RexInputRef) operand).getIndex());
                        }
                    }
                }
            } else {
                throw new RuntimeException("Bad JOIN condition");
            }
            final var recursiveCardinalitiesInterest = cardinalitiesInterestBuilder.build();
            inspectSubquery(
                    joinRel.getLeft(), map,
                    leftSplit(recursiveCardinalitiesInterest, joinRel.getLeft().getRowType().getFieldCount())
            );
            inspectSubquery(
                    joinRel.getRight(), map,
                    rightSplit(recursiveCardinalitiesInterest, joinRel.getLeft().getRowType().getFieldCount())
            );
        } else if (node instanceof BeamIOSourceRel) {
            final var names = map.computeIfAbsent(node, __ -> new HashSet<>());
            cardinalitiesInterest.forEach(column ->
                    names.add(node.getTable().getRowType().getFieldList().get(column).getName())
            );
        } else if (node instanceof BeamCalcRel) {
            var inputs = node.getInputs();
            for (RelNode input : inputs) {
                inspectSubquery(input, map, ImmutableBitSet.of());
            }
        } else {
            throw new RuntimeException("unexpected " + node);
        }
    }

    private static ImmutableBitSet leftSplit(ImmutableBitSet set, int pivot) {
        return set.get(0, pivot);
    }

    private static ImmutableBitSet rightSplit(ImmutableBitSet set, int pivot) {
        return set.get(pivot, set.length()).shift(-pivot);
    }
}
