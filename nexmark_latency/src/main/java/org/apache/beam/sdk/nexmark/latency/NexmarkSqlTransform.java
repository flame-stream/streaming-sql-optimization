package org.apache.beam.sdk.nexmark.latency;

import com.google.auto.value.AutoValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv.BeamSqlEnvBuilder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This is a copy of {@code SqlTransform} but with rates or potentially any other statistics.
 */
@AutoValue
@Experimental
@AutoValue.CopyAnnotations
@SuppressWarnings({
        "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class NexmarkSqlTransform extends PTransform<PInput, PCollectionTuple> {
    static final String PCOLLECTION_NAME = "PCOLLECTION";
    public static final TupleTag<Row> MAIN = new TupleTag<>();
    public static final TupleTag<Row> STATS = new TupleTag<>();

    abstract String queryString();

    abstract String statsQuery();

    abstract QueryParameters queryParameters();

    abstract List<UdfDefinition> udfDefinitions();

    abstract List<UdafDefinition> udafDefinitions();

    abstract boolean autoUdfUdafLoad();

    abstract Map<String, TableProvider> tableProviderMap();

    abstract @Nullable String defaultTableProvider();

    abstract @Nullable String queryPlannerClassName();

    abstract Map<TupleTag<Row>, Double> rates();

    @Override
    public PCollectionTuple expand(PInput input) {
        BeamSqlEnvBuilder sqlEnvBuilder =
                BeamSqlEnv.builder(new ReadOnlyTableProvider(PCOLLECTION_NAME, toTableMap(input)));

        tableProviderMap().forEach(sqlEnvBuilder::addSchema);

        if (defaultTableProvider() != null) {
            sqlEnvBuilder.setCurrentSchema(defaultTableProvider());
        }

        // TODO: validate duplicate functions.
        sqlEnvBuilder.autoLoadBuiltinFunctions();
        registerFunctions(sqlEnvBuilder);

        if (autoUdfUdafLoad()) {
            sqlEnvBuilder.autoLoadUserDefinedFunctions();
        }

        sqlEnvBuilder.setQueryPlannerClassName(
                MoreObjects.firstNonNull(
                        queryPlannerClassName(),
                        input.getPipeline().getOptions().as(BeamSqlPipelineOptions.class).getPlannerName()));

        sqlEnvBuilder.setPipelineOptions(input.getPipeline().getOptions());

        BeamSqlEnv sqlEnv = sqlEnvBuilder.build();

        var beamRelNode = sqlEnv.parseQuery(queryString(), queryParameters());

        final var main = PCollectionTuple.of(MAIN, BeamSqlRelUtils.toPCollection(input.getPipeline(), beamRelNode));
        final var statsQuery = statsQuery();
        if (!statsQuery.isEmpty()) {
            return main.and(STATS, BeamSqlRelUtils.toPCollection(
                    input.getPipeline(), sqlEnv.parseQuery(statsQuery, queryParameters())
            ));
        }
        return main;
    }

    @SuppressWarnings("unchecked")
    private Map<String, BeamSqlTable> toTableMap(PInput inputs) {
        /*
          A single PCollection is transformed to a table named PCOLLECTION, other input types are
          expanded and converted to tables using the tags as names.
         */
        if (inputs instanceof PCollection) {
            PCollection<?> pCollection = (PCollection<?>) inputs;
            BeamPCollectionTable<?> table;
            if (rates().size() == 1) {
                table = new NexmarkTable<>(pCollection, rates().entrySet().iterator().next().getValue());
            } else {
                table = new BeamPCollectionTable(pCollection);
            }
            return ImmutableMap.of(PCOLLECTION_NAME, table);
        }

        ImmutableMap.Builder<String, BeamSqlTable> tables = ImmutableMap.builder();
        for (Map.Entry<TupleTag<?>, PValue> input : inputs.expand().entrySet()) {
            PCollection<?> pCollection = (PCollection<?>) input.getValue();
            BeamPCollectionTable<?> table;
            if (rates().containsKey(input.getKey())) {
                table = new NexmarkTable<>(pCollection, rates().get(input.getKey()));
            } else {
                table = new BeamPCollectionTable(pCollection);
            }
            tables.put(input.getKey().getId(), table);
        }
        return tables.build();
    }

    private void registerFunctions(BeamSqlEnvBuilder sqlEnvBuilder) {
        udfDefinitions()
                .forEach(udf -> sqlEnvBuilder.addUdf(udf.udfName(), udf.clazz(), udf.methodName()));

        udafDefinitions().forEach(udaf -> sqlEnvBuilder.addUdaf(udaf.udafName(), udaf.combineFn()));
    }

    /**
     * Returns a {@link NexmarkSqlTransform} representing an equivalent execution plan.
     *
     * <p>The {@link NexmarkSqlTransform} can be applied to a {@link PCollection} or {@link PCollectionTuple}
     * representing all the input tables.
     *
     * <p>The {@link PTransform} outputs a {@link PCollection} of {@link Row}.
     *
     * <p>If the {@link PTransform} is applied to {@link PCollection} then it gets registered with
     * name <em>PCOLLECTION</em>.
     *
     * <p>If the {@link PTransform} is applied to {@link PCollectionTuple} then {@link
     * TupleTag#getId()} is used as the corresponding {@link PCollection}s name.
     *
     * <ul>
     *   <li>If the sql query only uses a subset of tables from the upstream {@link PCollectionTuple},
     *       this is valid;
     *   <li>If the sql query references a table not included in the upstream {@link
     *       PCollectionTuple}, an {@code IllegalStateException} is thrown during query validati on;
     *   <li>Always, tables from the upstream {@link PCollectionTuple} are only valid in the scope of
     *       the current query call.
     * </ul>
     *
     * <p>Any available implementation of {@link QueryPlanner} can be used as the query planner in
     * {@link NexmarkSqlTransform}. An implementation can be specified globally for the entire pipeline with
     * {@link BeamSqlPipelineOptions#getPlannerName()}. The global planner can be overridden
     * per-transform with {@link #withQueryPlannerClass(Class<? extends QueryPlanner>)}.
     */
    public static NexmarkSqlTransform query(String queryString) {
        return query(queryString, "");
    }

    public static NexmarkSqlTransform query(String queryString, String statsQuery) {
        final var builder = builder()
                .setQueryString(queryString)
                .setQueryParameters(QueryParameters.ofNone())
                .setUdafDefinitions(Collections.emptyList())
                .setUdfDefinitions(Collections.emptyList())
                .setTableProviderMap(Collections.emptyMap())
                .setAutoUdfUdafLoad(false)
                .setRates(Collections.emptyMap());
        if (statsQuery != null) {
            builder.setStatsQuery(statsQuery);
        }
        return builder.build();
    }

    public NexmarkSqlTransform withTableProvider(String name, TableProvider tableProvider) {
        Map<String, TableProvider> map = new HashMap<>(tableProviderMap());
        map.put(name, tableProvider);
        return toBuilder().setTableProviderMap(ImmutableMap.copyOf(map)).build();
    }

    public NexmarkSqlTransform withDefaultTableProvider(String name, TableProvider tableProvider) {
        return withTableProvider(name, tableProvider).toBuilder().setDefaultTableProvider(name).build();
    }

    public NexmarkSqlTransform withQueryPlannerClass(Class<? extends QueryPlanner> clazz) {
        return toBuilder().setQueryPlannerClassName(clazz.getName()).build();
    }

    public NexmarkSqlTransform withNamedParameters(Map<String, ?> parameters) {
        return toBuilder().setQueryParameters(QueryParameters.ofNamed(parameters)).build();
    }

    public NexmarkSqlTransform withPositionalParameters(List<?> parameters) {
        return toBuilder().setQueryParameters(QueryParameters.ofPositional(parameters)).build();
    }

    public NexmarkSqlTransform withAutoUdfUdafLoad(boolean autoUdfUdafLoad) {
        return toBuilder().setAutoUdfUdafLoad(autoUdfUdafLoad).build();
    }

    public NexmarkSqlTransform withRates(Map<TupleTag<Row>, Double> rates) {
        return toBuilder().setRates(rates).build();
    }

    /**
     * register a UDF function used in this query.
     *
     * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDF in BeamSql.
     */
    public NexmarkSqlTransform registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
        return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
    }

    /**
     * Register {@link SerializableFunction} as a UDF function used in this query. Note, {@link
     * SerializableFunction} must have a constructor without arguments.
     */
    public NexmarkSqlTransform registerUdf(String functionName, SerializableFunction sfn) {
        return registerUdf(functionName, sfn.getClass(), "apply");
    }

    private NexmarkSqlTransform registerUdf(String functionName, Class<?> clazz, String method) {
        ImmutableList<NexmarkSqlTransform.UdfDefinition> newUdfDefinitions =
                ImmutableList.<NexmarkSqlTransform.UdfDefinition>builder()
                        .addAll(udfDefinitions())
                        .add(NexmarkSqlTransform.UdfDefinition.of(functionName, clazz, method))
                        .build();

        return toBuilder().setUdfDefinitions(newUdfDefinitions).build();
    }

    /**
     * register a {@link Combine.CombineFn} as UDAF function used in this query.
     */
    public NexmarkSqlTransform registerUdaf(String functionName, Combine.CombineFn combineFn) {
        ImmutableList<NexmarkSqlTransform.UdafDefinition> newUdafs =
                ImmutableList.<NexmarkSqlTransform.UdafDefinition>builder()
                        .addAll(udafDefinitions())
                        .add(NexmarkSqlTransform.UdafDefinition.of(functionName, combineFn))
                        .build();

        return toBuilder().setUdafDefinitions(newUdafs).build();
    }

    abstract NexmarkSqlTransform.Builder toBuilder();

    static NexmarkSqlTransform.Builder builder() {
        return new AutoValue_NexmarkSqlTransform.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract NexmarkSqlTransform.Builder setQueryString(String queryString);

        abstract NexmarkSqlTransform.Builder setStatsQuery(String statsQuery);

        abstract NexmarkSqlTransform.Builder setQueryParameters(QueryParameters queryParameters);

        abstract NexmarkSqlTransform.Builder setUdfDefinitions(List<NexmarkSqlTransform.UdfDefinition> udfDefinitions);

        abstract NexmarkSqlTransform.Builder setUdafDefinitions(List<NexmarkSqlTransform.UdafDefinition> udafDefinitions);

        abstract NexmarkSqlTransform.Builder setAutoUdfUdafLoad(boolean autoUdfUdafLoad);

        abstract NexmarkSqlTransform.Builder setTableProviderMap(Map<String, TableProvider> tableProviderMap);

        abstract NexmarkSqlTransform.Builder setDefaultTableProvider(@Nullable String defaultTableProvider);

        abstract NexmarkSqlTransform.Builder setQueryPlannerClassName(@Nullable String queryPlannerClassName);

        abstract NexmarkSqlTransform.Builder setRates(Map<TupleTag<Row>, Double> rates);

        abstract NexmarkSqlTransform build();
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdfDefinition {
        abstract String udfName();

        abstract Class<?> clazz();

        abstract String methodName();

        static NexmarkSqlTransform.UdfDefinition of(String udfName, Class<?> clazz, String methodName) {
            return new AutoValue_NexmarkSqlTransform_UdfDefinition(udfName, clazz, methodName);
        }
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdafDefinition {
        abstract String udafName();

        abstract Combine.CombineFn combineFn();

        static NexmarkSqlTransform.UdafDefinition of(String udafName, Combine.CombineFn combineFn) {
            return new AutoValue_NexmarkSqlTransform_UdafDefinition(udafName, combineFn);
        }
    }
}
