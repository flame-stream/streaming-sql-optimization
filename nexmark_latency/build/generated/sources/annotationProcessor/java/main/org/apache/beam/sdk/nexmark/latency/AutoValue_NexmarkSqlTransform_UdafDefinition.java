package org.apache.beam.sdk.nexmark.latency;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.Combine;

@SuppressWarnings("rawtypes")
@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_NexmarkSqlTransform_UdafDefinition extends NexmarkSqlTransform.UdafDefinition {

  private final String udafName;

  private final Combine.CombineFn combineFn;

  AutoValue_NexmarkSqlTransform_UdafDefinition(
      String udafName,
      Combine.CombineFn combineFn) {
    if (udafName == null) {
      throw new NullPointerException("Null udafName");
    }
    this.udafName = udafName;
    if (combineFn == null) {
      throw new NullPointerException("Null combineFn");
    }
    this.combineFn = combineFn;
  }

  @Override
  String udafName() {
    return udafName;
  }

  @Override
  Combine.CombineFn combineFn() {
    return combineFn;
  }

  @Override
  public String toString() {
    return "UdafDefinition{"
         + "udafName=" + udafName + ", "
         + "combineFn=" + combineFn
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof NexmarkSqlTransform.UdafDefinition) {
      NexmarkSqlTransform.UdafDefinition that = (NexmarkSqlTransform.UdafDefinition) o;
      return this.udafName.equals(that.udafName())
          && this.combineFn.equals(that.combineFn());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= udafName.hashCode();
    h$ *= 1000003;
    h$ ^= combineFn.hashCode();
    return h$;
  }

}
