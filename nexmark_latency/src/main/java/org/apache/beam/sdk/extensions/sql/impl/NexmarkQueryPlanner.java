/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl;

import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters.Kind;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.RelMdNodeStats;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelRoot;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.*;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.BuiltInMethod;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.ImmutableBitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.NumberUtil;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The core component to handle through a SQL statement, from explain execution plan, to generate a
 * Beam pipeline.
 */
@SuppressWarnings({
        "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class NexmarkQueryPlanner implements QueryPlanner {
    private static final Logger LOG = LoggerFactory.getLogger(NexmarkQueryPlanner.class);

    private final Planner planner;
    private final JdbcConnection connection;

    /**
     * Called by {@link BeamSqlEnv}.instantiatePlanner() reflectively.
     */
    public NexmarkQueryPlanner(JdbcConnection connection, Collection<RuleSet> ruleSets) {
        this.connection = connection;
        this.planner = Frameworks.getPlanner(defaultConfig(connection, ruleSets));
    }

    public static final Factory FACTORY =
            new Factory() {
                @Override
                public QueryPlanner createPlanner(
                        JdbcConnection jdbcConnection, Collection<RuleSet> ruleSets) {
                    return new NexmarkQueryPlanner(jdbcConnection, ruleSets);
                }
            };

    public FrameworkConfig defaultConfig(JdbcConnection connection, Collection<RuleSet> ruleSets) {
        final CalciteConnectionConfig config = connection.config();
        final SqlParser.ConfigBuilder parserConfig =
                SqlParser.configBuilder()
                        .setQuotedCasing(config.quotedCasing())
                        .setUnquotedCasing(config.unquotedCasing())
                        .setQuoting(config.quoting())
                        .setConformance(config.conformance())
                        .setCaseSensitive(config.caseSensitive());
        final SqlParserImplFactory parserFactory =
                config.parserFactory(SqlParserImplFactory.class, null);
        if (parserFactory != null) {
            parserConfig.setParserFactory(parserFactory);
        }

        final SchemaPlus schema = connection.getRootSchema();
        final SchemaPlus defaultSchema = connection.getCurrentSchemaPlus();

        final ImmutableList<RelTraitDef> traitDefs = ImmutableList.of(ConventionTraitDef.INSTANCE);

        final CalciteCatalogReader catalogReader =
                new CalciteCatalogReader(
                        CalciteSchema.from(schema),
                        ImmutableList.of(defaultSchema.getName()),
                        connection.getTypeFactory(),
                        connection.config());
        final SqlOperatorTable opTab0 =
                connection.config().fun(SqlOperatorTable.class, SqlStdOperatorTable.instance());

        return Frameworks.newConfigBuilder()
                .parserConfig(parserConfig.build())
                .defaultSchema(defaultSchema)
                .traitDefs(traitDefs)
                .context(Contexts.of(connection.config()))
                .ruleSets(ruleSets.toArray(new RuleSet[0]))
                .costFactory(BeamCostModel.FACTORY)
                .typeSystem(connection.getTypeFactory().getTypeSystem())
                .operatorTable(ChainedSqlOperatorTable.of(opTab0, catalogReader))
                .build();
    }

    /**
     * Parse input SQL query, and return a {@link SqlNode} as grammar tree.
     */
    @Override
    public SqlNode parse(String sqlStatement) throws ParseException {
        SqlNode parsed;
        try {
            parsed = planner.parse(sqlStatement);
        } catch (SqlParseException e) {
            throw new ParseException(String.format("Unable to parse query %s", sqlStatement), e);
        } finally {
            planner.close();
        }
        return parsed;
    }

    /**
     * It parses and validate the input query, then convert into a {@link BeamRelNode} tree. Note that
     * query parameters are not yet supported.
     */
    @Override
    public BeamRelNode convertToBeamRel(String sqlStatement, QueryParameters queryParameters)
            throws ParseException, SqlConversionException {
        Preconditions.checkArgument(
                queryParameters.getKind() == Kind.NONE,
                "Beam SQL Calcite dialect does not yet support query parameters.");
        BeamRelNode beamRelNode;
        try {
            SqlNode parsed = planner.parse(sqlStatement);
            TableResolutionUtils.setupCustomTableResolution(connection, parsed);
            SqlNode validated = planner.validate(parsed);
            LOG.info("SQL:\n" + validated);

            // root of original logical plan
            RelRoot root = planner.rel(validated);
            LOG.info("SQLPlan>\n" + RelOptUtil.toString(root.rel));

            RelTraitSet desiredTraits =
                    root.rel
                            .getTraitSet()
                            .replace(BeamLogicalConvention.INSTANCE)
                            .replace(root.collation)
                            .simplify();
            // beam physical plan
            root.rel
                    .getCluster()
                    .setMetadataProvider(
                            ChainedRelMetadataProvider.of(
                                    ImmutableList.of(
                                            DistinctRowCountHandler.provider(queryParameters),
                                            SelectivityHandler.PROVIDER,
                                            NonCumulativeCostImpl.SOURCE,
                                            RelMdNodeStats.SOURCE,
                                            root.rel.getCluster().getMetadataProvider())));
            RelMetadataQuery.THREAD_PROVIDERS.set(
                    JaninoRelMetadataProvider.of(root.rel.getCluster().getMetadataProvider()));
            root.rel.getCluster().invalidateMetadataQuery();
            beamRelNode = (BeamRelNode) planner.transform(0, desiredTraits, root.rel);
            LOG.info("BEAMPlan>\n" + RelOptUtil.toString(beamRelNode));
        } catch (RelConversionException | CannotPlanException e) {
            throw new SqlConversionException(
                    String.format("Unable to convert query %s", sqlStatement), e);
        } catch (SqlParseException | ValidationException e) {
            throw new ParseException(String.format("Unable to parse query %s", sqlStatement), e);
        } finally {
            planner.close();
        }
        return beamRelNode;
    }

    public static class DistinctRowCountHandler implements MetadataHandler<BuiltInMetadata.DistinctRowCount> {
        private final QueryParameters queryParameters;

        public DistinctRowCountHandler(QueryParameters queryParameters) {
            this.queryParameters = queryParameters;
        }

        public static RelMetadataProvider provider(QueryParameters queryParameters) {
            return ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.DISTINCT_ROW_COUNT.method,
                    new DistinctRowCountHandler(queryParameters)
            );
        }

        @Override
        public MetadataDef<BuiltInMetadata.DistinctRowCount> getDef() {
            return BuiltInMetadata.DistinctRowCount.DEF;
        }

        // https://github.com/apache/calcite/blob/70d59fedfdb9fc956f3b1d1764833cbded7ae44d/core/src/main/java/org/apache/calcite/rel/metadata/RelMdDistinctRowCount.java#L301
        @SuppressWarnings("UnusedDeclaration")
        public Double getDistinctRowCount(RelSubset rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
            final RelNode best = rel.getBest();
            if (best != null) {
                return mq.getDistinctRowCount(best, groupKey, predicate);
            }
            Double value = null;
            for (var equivalent : rel.getRels()) {
                try {
                    value = NumberUtil.min(value, mq.getDistinctRowCount(equivalent, groupKey, predicate));
                } catch (CyclicMetadataException e) {
                    // Ignore this relational expression; there will be non-cyclic ones
                    // in this set.
                }
            }
            return value;
        }

        @SuppressWarnings("UnusedDeclaration")
        public Double getDistinctRowCount(BeamIOSourceRel rel, RelMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate) {
            if ((predicate == null || predicate.isAlwaysTrue()) && groupKey.size() == 1) {
                final var distinctRowCount =
                        queryParameters.named().get("table_column_distinct_row_count:" + Stream.concat(
                                rel.getTable().getQualifiedName().stream(),
                                Stream.of(rel.getBeamSqlTable().getSchema().nameOf(groupKey.nextSetBit(0)))
                        ).collect(Collectors.joining(".")));
                if (distinctRowCount instanceof Number) {
                    return ((Number) distinctRowCount).doubleValue();
                }
            }
            return null;
        }
    }

    public enum SelectivityHandler implements MetadataHandler<BuiltInMetadata.Selectivity> {
        INSTANCE;
        public static final RelMetadataProvider PROVIDER =
                ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.SELECTIVITY.method, INSTANCE);

        @Override
        public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
            return BuiltInMetadata.Selectivity.DEF;
        }

        // https://github.com/apache/calcite/blob/d9a81b88ad561e7e4cedae93e805e0d7a53a7f1a/core/src/main/java/org/apache/calcite/rel/metadata/RelMdSelectivity.java#L148
        @SuppressWarnings("UnusedDeclaration")
        public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
            // return RelMdSelectivity#getSelectivity;
            double sel = 1.0D;
            if (predicate != null && !predicate.isAlwaysTrue()) {
                double artificialSel = 1.0D;

                for (var conjunction : RelOptUtil.conjunctions(predicate)) {
                    if (conjunction.getKind() == SqlKind.IS_NOT_NULL) {
                        sel *= 0.9D;
                    } else if (conjunction instanceof RexCall && ((RexCall) conjunction).getOperator() == RelMdUtil.ARTIFICIAL_SELECTIVITY_FUNC) {
                        artificialSel *= RelMdUtil.getSelectivityValue(conjunction);
                    } else if (conjunction.isA(SqlKind.EQUALS)) {
                        final var equals = (RexCall) conjunction;
                        final var uniqueLeft = uniqueValues(rel, mq, equals.getOperands().get(0));
                        final var uniqueRight = uniqueValues(rel, mq, equals.getOperands().get(1));
                        if (uniqueLeft != null && uniqueRight != null) {
                            sel *= Double.min(uniqueLeft, uniqueRight) / uniqueLeft / uniqueRight;
                        } else {
                            sel *= 0.15D;
                        }
                    } else if (conjunction.isA(SqlKind.COMPARISON)) {
                        sel *= 0.5D;
                    } else {
                        sel *= 0.25D;
                    }
                }

                return sel * artificialSel;
            } else {
                return sel;
            }
        }

        private Double uniqueValues(Join rel, RelMetadataQuery mq, RexNode node) {
            if (node instanceof RexInputRef) {
                final var inputRef = (RexInputRef) node;
                final var leftFieldCount = rel.getLeft().getRowType().getFieldCount();
                if (inputRef.getIndex() < leftFieldCount) {
                    return mq.getDistinctRowCount(rel.getLeft(), ImmutableBitSet.builder().set(inputRef.getIndex()).build(), null);
                } else {
                    return mq.getDistinctRowCount(rel.getRight(), ImmutableBitSet.builder().set(inputRef.getIndex() - leftFieldCount).build(), null);
                }
            }
            return null;
        }
    }

    // It needs to be public so that the generated code in Calcite can access it.
    public static class NonCumulativeCostImpl
            implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

        public static final RelMetadataProvider SOURCE =
                ReflectiveRelMetadataProvider.reflectiveSource(
                        BuiltInMethod.NON_CUMULATIVE_COST.method, new NonCumulativeCostImpl());

        @Override
        public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
            return BuiltInMetadata.NonCumulativeCost.DEF;
        }

        @SuppressWarnings("UnusedDeclaration")
        public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
            // This is called by a generated code in calcite MetadataQuery.
            // If the rel is Calcite rel or we are in JDBC path and cost factory is not set yet we should
            // use calcite cost estimation
            if (!(rel instanceof BeamRelNode)) {
                return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
            }

            // Currently we do nothing in this case, however, we can plug our own cost estimation method
            // here and based on the design we also need to remove the cached values

            // We need to first remove the cached values.
            List<List> costKeys =
                    mq.map.entrySet().stream()
                            .filter(entry -> entry.getValue() instanceof BeamCostModel)
                            .filter(entry -> ((BeamCostModel) entry.getValue()).isInfinite())
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

            costKeys.forEach(mq.map::remove);

            return ((BeamRelNode) rel).beamComputeSelfCost(rel.getCluster().getPlanner(), mq);
        }
    }
}
