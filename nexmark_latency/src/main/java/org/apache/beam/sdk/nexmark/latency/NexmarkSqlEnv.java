package org.apache.beam.sdk.nexmark.latency;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv.BeamSqlEnvBuilder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This is a copy of {@code SqlTransform} but with rates or potentially any other statistics and without query string and parameters.
 */
@AutoValue
@Experimental
@AutoValue.CopyAnnotations
@SuppressWarnings({
        "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
        "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class NexmarkSqlEnv implements Function<PInput, BeamSqlEnv> {
    static final String PCOLLECTION_NAME = "PCOLLECTION";

    abstract List<UdfDefinition> udfDefinitions();

    abstract List<UdafDefinition> udafDefinitions();

    abstract boolean autoUdfUdafLoad();

    abstract Map<String, TableProvider> tableProviderMap();

    abstract @Nullable String defaultTableProvider();

    abstract @Nullable String queryPlannerClassName();

    abstract Map<TupleTag<Row>, Double> rates();

    @Override
    public BeamSqlEnv apply(PInput input) {
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

        return sqlEnvBuilder.build();
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
     * Returns a {@link NexmarkSqlEnv} representing an equivalent execution plan.
     *
     * <p>The {@link NexmarkSqlEnv} can be applied to a {@link PCollection} or {@link PCollectionTuple}
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
     * {@link NexmarkSqlEnv}. An implementation can be specified globally for the entire pipeline with
     * {@link BeamSqlPipelineOptions#getPlannerName()}. The global planner can be overridden
     * per-transform with {@link #withQueryPlannerClass(Class<? extends QueryPlanner>)}.
     */
    public static NexmarkSqlEnv build() {
        return builder()
                .setUdafDefinitions(Collections.emptyList())
                .setUdfDefinitions(Collections.emptyList())
                .setTableProviderMap(Collections.emptyMap())
                .setAutoUdfUdafLoad(false)
                .setRates(Collections.emptyMap()).build();
    }

    public NexmarkSqlEnv withTableProvider(String name, TableProvider tableProvider) {
        Map<String, TableProvider> map = new HashMap<>(tableProviderMap());
        map.put(name, tableProvider);
        return toBuilder().setTableProviderMap(ImmutableMap.copyOf(map)).build();
    }

    public NexmarkSqlEnv withDefaultTableProvider(String name, TableProvider tableProvider) {
        return withTableProvider(name, tableProvider).toBuilder().setDefaultTableProvider(name).build();
    }

    public NexmarkSqlEnv withQueryPlannerClass(Class<? extends QueryPlanner> clazz) {
        return toBuilder().setQueryPlannerClassName(clazz.getName()).build();
    }

    public NexmarkSqlEnv withAutoUdfUdafLoad(boolean autoUdfUdafLoad) {
        return toBuilder().setAutoUdfUdafLoad(autoUdfUdafLoad).build();
    }

    public NexmarkSqlEnv withRates(Map<TupleTag<Row>, Double> rates) {
        return toBuilder().setRates(rates).build();
    }

    /**
     * register a UDF function used in this query.
     *
     * <p>Refer to {@link BeamSqlUdf} for more about how to implement a UDF in BeamSql.
     */
    public NexmarkSqlEnv registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
        return registerUdf(functionName, clazz, BeamSqlUdf.UDF_METHOD);
    }

    /**
     * Register {@link SerializableFunction} as a UDF function used in this query. Note, {@link
     * SerializableFunction} must have a constructor without arguments.
     */
    public NexmarkSqlEnv registerUdf(String functionName, SerializableFunction sfn) {
        return registerUdf(functionName, sfn.getClass(), "apply");
    }

    private NexmarkSqlEnv registerUdf(String functionName, Class<?> clazz, String method) {
        ImmutableList<NexmarkSqlEnv.UdfDefinition> newUdfDefinitions =
                ImmutableList.<NexmarkSqlEnv.UdfDefinition>builder()
                        .addAll(udfDefinitions())
                        .add(NexmarkSqlEnv.UdfDefinition.of(functionName, clazz, method))
                        .build();

        return toBuilder().setUdfDefinitions(newUdfDefinitions).build();
    }

    /**
     * register a {@link Combine.CombineFn} as UDAF function used in this query.
     */
    public NexmarkSqlEnv registerUdaf(String functionName, Combine.CombineFn combineFn) {
        ImmutableList<NexmarkSqlEnv.UdafDefinition> newUdafs =
                ImmutableList.<NexmarkSqlEnv.UdafDefinition>builder()
                        .addAll(udafDefinitions())
                        .add(NexmarkSqlEnv.UdafDefinition.of(functionName, combineFn))
                        .build();

        return toBuilder().setUdafDefinitions(newUdafs).build();
    }

    abstract NexmarkSqlEnv.Builder toBuilder();

    static NexmarkSqlEnv.Builder builder() {
        return new AutoValue_NexmarkSqlEnv.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
        abstract NexmarkSqlEnv.Builder setUdfDefinitions(List<NexmarkSqlEnv.UdfDefinition> udfDefinitions);

        abstract NexmarkSqlEnv.Builder setUdafDefinitions(List<NexmarkSqlEnv.UdafDefinition> udafDefinitions);

        abstract NexmarkSqlEnv.Builder setAutoUdfUdafLoad(boolean autoUdfUdafLoad);

        abstract NexmarkSqlEnv.Builder setTableProviderMap(Map<String, TableProvider> tableProviderMap);

        abstract NexmarkSqlEnv.Builder setDefaultTableProvider(@Nullable String defaultTableProvider);

        abstract NexmarkSqlEnv.Builder setQueryPlannerClassName(@Nullable String queryPlannerClassName);

        abstract NexmarkSqlEnv.Builder setRates(Map<TupleTag<Row>, Double> rates);

        abstract NexmarkSqlEnv build();
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdfDefinition {
        abstract String udfName();

        abstract Class<?> clazz();

        abstract String methodName();

        static NexmarkSqlEnv.UdfDefinition of(String udfName, Class<?> clazz, String methodName) {
            return new AutoValue_NexmarkSqlEnv_UdfDefinition(udfName, clazz, methodName);
        }
    }

    @AutoValue
    @AutoValue.CopyAnnotations
    @SuppressWarnings({"rawtypes"})
    abstract static class UdafDefinition {
        abstract String udafName();

        abstract Combine.CombineFn combineFn();

        static NexmarkSqlEnv.UdafDefinition of(String udafName, Combine.CombineFn combineFn) {
            return new AutoValue_NexmarkSqlEnv_UdafDefinition(udafName, combineFn);
        }
    }
}
