package com.flamestream.optimizer.sql.agents.util;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv.BeamSqlEnvBuilder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoValue
@Experimental
@AutoValue.CopyAnnotations
@SuppressWarnings({
        "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class SqlTransform extends PTransform<PInput, PCollection<Row>> {
    static final String PCOLLECTION_NAME = "PCOLLECTION";

    public BeamRelNode node;

    abstract String queryString();

    abstract List<String> ddlStrings();

    abstract QueryParameters queryParameters();

    abstract List<UdfDefinition> udfDefinitions();

    abstract List<UdafDefinition> udafDefinitions();

    abstract boolean autoLoading();

    abstract Map<String, TableProvider> tableProviderMap();

    abstract @Nullable String defaultTableProvider();

    abstract @Nullable String queryPlannerClassName();

    @Override
    public PCollection<Row> expand(PInput input) {
        TableProvider inputTableProvider =
                new ReadOnlyTableProvider(PCOLLECTION_NAME, toTableMap(input));
        InMemoryMetaStore metaTableProvider = new InMemoryMetaStore();
        metaTableProvider.registerProvider(inputTableProvider);
        BeamSqlEnvBuilder sqlEnvBuilder = BeamSqlEnv.builder(metaTableProvider);

        // TODO: validate duplicate functions.
        registerFunctions(sqlEnvBuilder);

        // Load automatic table providers before user ones so the user ones will cause a conflict if
        // the same names are reused.
        if (autoLoading()) {
            sqlEnvBuilder.autoLoadUserDefinedFunctions();
            ServiceLoader.load(TableProvider.class).forEach(metaTableProvider::registerProvider);
        }

        tableProviderMap().forEach(sqlEnvBuilder::addSchema);

        if (defaultTableProvider() != null) {
            sqlEnvBuilder.setCurrentSchema(defaultTableProvider());
        }

        sqlEnvBuilder.setQueryPlannerClassName(
                MoreObjects.firstNonNull(
                        queryPlannerClassName(),
                        input.getPipeline().getOptions().as(BeamSqlPipelineOptions.class).getPlannerName()));

        sqlEnvBuilder.setPipelineOptions(input.getPipeline().getOptions());

        BeamSqlEnv sqlEnv = sqlEnvBuilder.build();
        ddlStrings().forEach(sqlEnv::executeDdl);
        node = sqlEnv.parseQuery(queryString(), queryParameters());
        return BeamSqlRelUtils.toPCollection(
                input.getPipeline(), node);
    }

    @SuppressWarnings("unchecked")
    private Map<String, BeamSqlTable> toTableMap(PInput inputs) {
        /**
         * A single PCollection is transformed to a table named PCOLLECTION, other input types are
         * expanded and converted to tables using the tags as names.
         */
        if (inputs instanceof PCollection) {
            PCollection<?> pCollection = (PCollection<?>) inputs;
            return ImmutableMap.of(PCOLLECTION_NAME, new BeamPCollectionTable(pCollection));
        }

        ImmutableMap.Builder<String, BeamSqlTable> tables = ImmutableMap.builder();
        for (Map.Entry<TupleTag<?>, PValue> input : inputs.expand().entrySet()) {
            PCollection<?> pCollection = (PCollection<?>) input.getValue();
            tables.put(input.getKey().getId(), new BeamPCollectionTable(pCollection));
        }
        return tables.build();
    }

    private void registerFunctions(BeamSqlEnvBuilder sqlEnvBuilder) {
        udfDefinitions()
                .forEach(udf -> sqlEnvBuilder.addUdf(udf.udfName(), udf.clazz(), udf.methodName()));

        udafDefinitions().forEach(udaf -> sqlEnvBuilder.addUdaf(udaf.udafName(), udaf.combineFn()));
    }

    /**
     * Returns a {@link SqlTransform} representing an equivalent execution plan.
     *
     * <p>The {@link SqlTransform} can be applied to a {@link PCollection} or {@link PCollectionTuple}
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
     * {@link SqlTransform}. An implementation can be specified globally for the entire pipeline with
     * {@link BeamSqlPipelineOptions#getPlannerName()}. The global planner can be overridden
     * per-transform with {@link #withQueryPlannerClass(Class<? extends QueryPlanner>)}.
     */
    public static SqlTransform query(String queryString) {
        return builder().setQueryString(queryString).build();
    }

    public SqlTransform withTableProvider(String name, TableProvider tableProvider) {
        Map<String, TableProvider> map = new HashMap<>(tableProviderMap());
        map.put(name, tableProvider);
        return toBuilder().setTableProviderMap(ImmutableMap.copyOf(map)).build();
    }

    public SqlTransform withDefaultTableProvider(String name, TableProvider tableProvider) {
        return withTableProvider(name, tableProvider).toBuilder().setDefaultTableProvider(name).build();
    }

    public SqlTransform withQueryPlannerClass(Class<? extends QueryPlanner> clazz) {
        return toBuilder().setQueryPlannerClassName(clazz.getName()).build();
    }

    public SqlTransform withNamedParameters(Map<String, ?> parameters) {
        return toBuilder().setQueryParameters(QueryParameters.ofNamed(parameters)).build();
    }

    public SqlTransform withPositionalParameters(List<?> parameters) {
        return toBuilder().setQueryParameters(QueryParameters.ofPositional(parameters)).build();
    }

    public SqlTransform withDdlString(String ddlString) {
        return toBuilder()
                .setDdlStrings(ImmutableList.<String>builder().addAll(ddlStrings()).add(ddlString).build())
                .build();
    }

    public SqlTransform withAutoLoading(boolean autoLoading) {
        return toBuilder().setAutoLoading(autoLoading).build();
    }
    public SqlTransform registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
        return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
    }

    /**
     * Register {@link SerializableFunction} as a UDF function used in this query. Note, {@link
     * SerializableFunction} must have a constructor without arguments.
     */
    public SqlTransform registerUdf(String functionName, SerializableFunction sfn) {
        return registerUdf(functionName, sfn.getClass(), "apply");
    }

    private SqlTransform registerUdf(String functionName, Class<?> clazz, String method) {
        ImmutableList<UdfDefinition> newUdfDefinitions =
                ImmutableList.<UdfDefinition>builder()
                        .addAll(udfDefinitions())
                        .add(UdfDefinition.of(functionName, clazz, method))
                        .build();

        return toBuilder().setUdfDefinitions(newUdfDefinitions).build();
    }

    /** register a {@link Combine.CombineFn} as UDAF function used in this query. */
    public SqlTransform registerUdaf(String functionName, Combine.CombineFn combineFn) {
        ImmutableList<UdafDefinition> newUdafs =
                ImmutableList.<UdafDefinition>builder()
                        .addAll(udafDefinitions())
                        .add(UdafDefinition.of(functionName, combineFn))
                        .build();

        return toBuilder().setUdafDefinitions(newUdafs).build();
    }

    abstract Builder toBuilder();

    static Builder builder() {
        return new AutoValue_SqlTransform.Builder()
                .setQueryParameters(QueryParameters.ofNone())
                .setDdlStrings(Collections.emptyList())
                .setUdafDefinitions(Collections.emptyList())
                .setUdfDefinitions(Collections.emptyList())
                .setTableProviderMap(Collections.emptyMap())
                .setAutoLoading(true);
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setQueryString(String queryString);

        abstract Builder setQueryParameters(QueryParameters queryParameters);

        abstract Builder setDdlStrings(List<String> ddlStrings);

        abstract Builder setUdfDefinitions(List<UdfDefinition> udfDefinitions);

        abstract Builder setUdafDefinitions(List<UdafDefinition> udafDefinitions);

        abstract Builder setAutoLoading(boolean autoLoading);

        abstract Builder setTableProviderMap(Map<String, TableProvider> tableProviderMap);

        abstract Builder setDefaultTableProvider(@Nullable String defaultTableProvider);

        abstract Builder setQueryPlannerClassName(@Nullable String queryPlannerClassName);

        abstract SqlTransform build();
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdfDefinition {
        abstract String udfName();

        abstract Class<?> clazz();

        abstract String methodName();

        static UdfDefinition of(String udfName, Class<?> clazz, String methodName) {
            return new AutoValue_SqlTransform_UdfDefinition(udfName, clazz, methodName);
        }
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdafDefinition {
        abstract String udafName();

        abstract Combine.CombineFn combineFn();

        static UdafDefinition of(String udafName, Combine.CombineFn combineFn) {
            return new AutoValue_SqlTransform_UdafDefinition(udafName, combineFn);
        }
    }
}
