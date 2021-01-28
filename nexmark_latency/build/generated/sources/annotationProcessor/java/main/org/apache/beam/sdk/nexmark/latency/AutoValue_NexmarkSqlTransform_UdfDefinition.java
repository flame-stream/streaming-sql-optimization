package org.apache.beam.sdk.nexmark.latency;

import javax.annotation.Generated;

@SuppressWarnings("rawtypes")
@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_NexmarkSqlTransform_UdfDefinition extends NexmarkSqlTransform.UdfDefinition {

  private final String udfName;

  private final Class<?> clazz;

  private final String methodName;

  AutoValue_NexmarkSqlTransform_UdfDefinition(
      String udfName,
      Class<?> clazz,
      String methodName) {
    if (udfName == null) {
      throw new NullPointerException("Null udfName");
    }
    this.udfName = udfName;
    if (clazz == null) {
      throw new NullPointerException("Null clazz");
    }
    this.clazz = clazz;
    if (methodName == null) {
      throw new NullPointerException("Null methodName");
    }
    this.methodName = methodName;
  }

  @Override
  String udfName() {
    return udfName;
  }

  @Override
  Class<?> clazz() {
    return clazz;
  }

  @Override
  String methodName() {
    return methodName;
  }

  @Override
  public String toString() {
    return "UdfDefinition{"
         + "udfName=" + udfName + ", "
         + "clazz=" + clazz + ", "
         + "methodName=" + methodName
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof NexmarkSqlTransform.UdfDefinition) {
      NexmarkSqlTransform.UdfDefinition that = (NexmarkSqlTransform.UdfDefinition) o;
      return this.udfName.equals(that.udfName())
          && this.clazz.equals(that.clazz())
          && this.methodName.equals(that.methodName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= udfName.hashCode();
    h$ *= 1000003;
    h$ ^= clazz.hashCode();
    h$ *= 1000003;
    h$ ^= methodName.hashCode();
    return h$;
  }

}
