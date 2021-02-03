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

/** Result of Query18. */
@DefaultSchema(JavaFieldSchema.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ReceiveArrivalTimes implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private static final @UnknownKeyFor @NonNull @Initialized InstantCoder INSTANT_CODER = InstantCoder.of();

  public static final Coder<ReceiveArrivalTimes> CODER =
      new CustomCoder<ReceiveArrivalTimes>() {
        @Override
        public void encode(ReceiveArrivalTimes value, OutputStream outStream)
            throws CoderException, IOException {
          INSTANT_CODER.encode(value.timestamp1, outStream);
          INSTANT_CODER.encode(value.timestamp2, outStream);
          INSTANT_CODER.encode(value.timestamp2, outStream);
          INSTANT_CODER.encode(value.arrivalTime, outStream);
        }

        @Override
        public ReceiveArrivalTimes decode(InputStream inStream) throws CoderException, IOException {
          Instant receive1 = INSTANT_CODER.decode(inStream);
          Instant receive2 = INSTANT_CODER.decode(inStream);
          Instant receive3 = INSTANT_CODER.decode(inStream);
          Instant arrival = INSTANT_CODER.decode(inStream);
          return new ReceiveArrivalTimes(receive1, receive2, receive3, arrival);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(ReceiveArrivalTimes v) {
          return v;
        }
      };

  @JsonProperty public Instant timestamp1;

  @JsonProperty public Instant timestamp2;

  @JsonProperty public Instant timestamp3;

  @JsonProperty public Instant arrivalTime;

  // For Avro only.
  @SuppressWarnings("unused")
  public ReceiveArrivalTimes() {
    this.timestamp1 = null;
    this.timestamp2 = null;
    this.timestamp3 = null;
    this.arrivalTime = null;
  }

  public ReceiveArrivalTimes(Instant receive1, Instant receive2, Instant receive3, Instant arrival) {
    this.timestamp1 = receive1;
    this.timestamp2 = receive2;
    this.timestamp3 = receive3;
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

    ReceiveArrivalTimes other = (ReceiveArrivalTimes) otherObject;
    return Objects.equals(timestamp1, other.timestamp1)
        && Objects.equals(timestamp2, other.timestamp2)
        && Objects.equals(timestamp3, other.timestamp3)
        && Objects.equals(arrivalTime, other.arrivalTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp1, timestamp2, timestamp3, arrivalTime);
  }

  @Override
  public long sizeInBytes() {
    return Long.BYTES * 4;
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
