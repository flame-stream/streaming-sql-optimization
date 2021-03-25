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
package org.apache.beam.sdk.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;

/** Result of Query3. */
@DefaultSchema(JavaFieldSchema.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class CountWithArrivalTimes implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final @UnknownKeyFor @NonNull @Initialized InstantCoder INSTANT_CODER = InstantCoder.of();

  public static final Coder<CountWithArrivalTimes> CODER =
      new CustomCoder<CountWithArrivalTimes>() {
        @Override
        public void encode(CountWithArrivalTimes value, OutputStream outStream)
            throws CoderException, IOException {
          LONG_CODER.encode(value.auc_count, outStream);
          INSTANT_CODER.encode(value.arrivalTime, outStream);
        }

        @Override
        public CountWithArrivalTimes decode(InputStream inStream) throws CoderException, IOException {
          long price = LONG_CODER.decode(inStream);
          Instant arrival = INSTANT_CODER.decode(inStream);
          return new CountWithArrivalTimes(price, arrival);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(CountWithArrivalTimes v) {
          return v;
        }
      };

  @JsonProperty public long auc_count;

  @JsonProperty public Instant arrivalTime;

  // For Avro only.
  @SuppressWarnings("unused")
  public CountWithArrivalTimes() {
    auc_count = 0;
  }

  public CountWithArrivalTimes(long auc_count, Instant arrival) {
    this.auc_count = auc_count;
    this.arrivalTime = arrival;
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (this == otherObject) {
      return true;
    }
    if (otherObject == null || getClass() != otherObject.getClass()) {
      return false;
    }

    CountWithArrivalTimes other = (CountWithArrivalTimes) otherObject;
    return Objects.equals(auc_count, other.auc_count)
        && Objects.equals(arrivalTime, other.arrivalTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(auc_count, arrivalTime);
  }

  @Override
  public long sizeInBytes() {
    return 1L + 1L;
  }

  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
