package org.apache.beam.sdk.nexmark.latency;

import java.util.List;
import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

@Experimental
@SuppressWarnings({"rawtypes", "nullness"})
@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_NexmarkSqlTransform extends NexmarkSqlTransform {

  private final String queryString;

  private final QueryPlanner.QueryParameters queryParameters;

  private final List<NexmarkSqlTransform.UdfDefinition> udfDefinitions;

  private final List<NexmarkSqlTransform.UdafDefinition> udafDefinitions;

  private final boolean autoUdfUdafLoad;

  private final Map<String, TableProvider> tableProviderMap;

  private final @Nullable String defaultTableProvider;

  private final @Nullable String queryPlannerClassName;

  private final Map<TupleTag<Row>, Double> rates;

  private AutoValue_NexmarkSqlTransform(
      String queryString,
      QueryPlanner.QueryParameters queryParameters,
      List<NexmarkSqlTransform.UdfDefinition> udfDefinitions,
      List<NexmarkSqlTransform.UdafDefinition> udafDefinitions,
      boolean autoUdfUdafLoad,
      Map<String, TableProvider> tableProviderMap,
      @Nullable String defaultTableProvider,
      @Nullable String queryPlannerClassName,
      Map<TupleTag<Row>, Double> rates) {
    this.queryString = queryString;
    this.queryParameters = queryParameters;
    this.udfDefinitions = udfDefinitions;
    this.udafDefinitions = udafDefinitions;
    this.autoUdfUdafLoad = autoUdfUdafLoad;
    this.tableProviderMap = tableProviderMap;
    this.defaultTableProvider = defaultTableProvider;
    this.queryPlannerClassName = queryPlannerClassName;
    this.rates = rates;
  }

  @Override
  String queryString() {
    return queryString;
  }

  @Override
  QueryPlanner.QueryParameters queryParameters() {
    return queryParameters;
  }

  @Override
  List<NexmarkSqlTransform.UdfDefinition> udfDefinitions() {
    return udfDefinitions;
  }

  @Override
  List<NexmarkSqlTransform.UdafDefinition> udafDefinitions() {
    return udafDefinitions;
  }

  @Override
  boolean autoUdfUdafLoad() {
    return autoUdfUdafLoad;
  }

  @Override
  Map<String, TableProvider> tableProviderMap() {
    return tableProviderMap;
  }

  @Override
  @Nullable String defaultTableProvider() {
    return defaultTableProvider;
  }

  @Override
  @Nullable String queryPlannerClassName() {
    return queryPlannerClassName;
  }

  @Override
  Map<TupleTag<Row>, Double> rates() {
    return rates;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof NexmarkSqlTransform) {
      NexmarkSqlTransform that = (NexmarkSqlTransform) o;
      return this.queryString.equals(that.queryString())
          && this.queryParameters.equals(that.queryParameters())
          && this.udfDefinitions.equals(that.udfDefinitions())
          && this.udafDefinitions.equals(that.udafDefinitions())
          && this.autoUdfUdafLoad == that.autoUdfUdafLoad()
          && this.tableProviderMap.equals(that.tableProviderMap())
          && (this.defaultTableProvider == null ? that.defaultTableProvider() == null : this.defaultTableProvider.equals(that.defaultTableProvider()))
          && (this.queryPlannerClassName == null ? that.queryPlannerClassName() == null : this.queryPlannerClassName.equals(that.queryPlannerClassName()))
          && this.rates.equals(that.rates());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= queryString.hashCode();
    h$ *= 1000003;
    h$ ^= queryParameters.hashCode();
    h$ *= 1000003;
    h$ ^= udfDefinitions.hashCode();
    h$ *= 1000003;
    h$ ^= udafDefinitions.hashCode();
    h$ *= 1000003;
    h$ ^= autoUdfUdafLoad ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= tableProviderMap.hashCode();
    h$ *= 1000003;
    h$ ^= (defaultTableProvider == null) ? 0 : defaultTableProvider.hashCode();
    h$ *= 1000003;
    h$ ^= (queryPlannerClassName == null) ? 0 : queryPlannerClassName.hashCode();
    h$ *= 1000003;
    h$ ^= rates.hashCode();
    return h$;
  }

  @Override
  NexmarkSqlTransform.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends NexmarkSqlTransform.Builder {
    private String queryString;
    private QueryPlanner.QueryParameters queryParameters;
    private List<NexmarkSqlTransform.UdfDefinition> udfDefinitions;
    private List<NexmarkSqlTransform.UdafDefinition> udafDefinitions;
    private Boolean autoUdfUdafLoad;
    private Map<String, TableProvider> tableProviderMap;
    private @Nullable String defaultTableProvider;
    private @Nullable String queryPlannerClassName;
    private Map<TupleTag<Row>, Double> rates;
    Builder() {
    }
    private Builder(NexmarkSqlTransform source) {
      this.queryString = source.queryString();
      this.queryParameters = source.queryParameters();
      this.udfDefinitions = source.udfDefinitions();
      this.udafDefinitions = source.udafDefinitions();
      this.autoUdfUdafLoad = source.autoUdfUdafLoad();
      this.tableProviderMap = source.tableProviderMap();
      this.defaultTableProvider = source.defaultTableProvider();
      this.queryPlannerClassName = source.queryPlannerClassName();
      this.rates = source.rates();
    }
    @Override
    NexmarkSqlTransform.Builder setQueryString(String queryString) {
      if (queryString == null) {
        throw new NullPointerException("Null queryString");
      }
      this.queryString = queryString;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setQueryParameters(QueryPlanner.QueryParameters queryParameters) {
      if (queryParameters == null) {
        throw new NullPointerException("Null queryParameters");
      }
      this.queryParameters = queryParameters;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setUdfDefinitions(List<NexmarkSqlTransform.UdfDefinition> udfDefinitions) {
      if (udfDefinitions == null) {
        throw new NullPointerException("Null udfDefinitions");
      }
      this.udfDefinitions = udfDefinitions;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setUdafDefinitions(List<NexmarkSqlTransform.UdafDefinition> udafDefinitions) {
      if (udafDefinitions == null) {
        throw new NullPointerException("Null udafDefinitions");
      }
      this.udafDefinitions = udafDefinitions;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setAutoUdfUdafLoad(boolean autoUdfUdafLoad) {
      this.autoUdfUdafLoad = autoUdfUdafLoad;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setTableProviderMap(Map<String, TableProvider> tableProviderMap) {
      if (tableProviderMap == null) {
        throw new NullPointerException("Null tableProviderMap");
      }
      this.tableProviderMap = tableProviderMap;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setDefaultTableProvider(@Nullable String defaultTableProvider) {
      this.defaultTableProvider = defaultTableProvider;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setQueryPlannerClassName(@Nullable String queryPlannerClassName) {
      this.queryPlannerClassName = queryPlannerClassName;
      return this;
    }
    @Override
    NexmarkSqlTransform.Builder setRates(Map<TupleTag<Row>, Double> rates) {
      if (rates == null) {
        throw new NullPointerException("Null rates");
      }
      this.rates = rates;
      return this;
    }
    @Override
    NexmarkSqlTransform build() {
      String missing = "";
      if (this.queryString == null) {
        missing += " queryString";
      }
      if (this.queryParameters == null) {
        missing += " queryParameters";
      }
      if (this.udfDefinitions == null) {
        missing += " udfDefinitions";
      }
      if (this.udafDefinitions == null) {
        missing += " udafDefinitions";
      }
      if (this.autoUdfUdafLoad == null) {
        missing += " autoUdfUdafLoad";
      }
      if (this.tableProviderMap == null) {
        missing += " tableProviderMap";
      }
      if (this.rates == null) {
        missing += " rates";
      }
      if (!missing.isEmpty()) {
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_NexmarkSqlTransform(
          this.queryString,
          this.queryParameters,
          this.udfDefinitions,
          this.udafDefinitions,
          this.autoUdfUdafLoad,
          this.tableProviderMap,
          this.defaultTableProvider,
          this.queryPlannerClassName,
          this.rates);
    }
  }

}
