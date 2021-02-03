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
import org.checkerframework.checker.nullness.qual.Nullable;

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
public class NameCityStatePrice implements KnownSize, Serializable {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  public static final Coder<NameCityStatePrice> CODER =
      new CustomCoder<NameCityStatePrice>() {
        @Override
        public void encode(NameCityStatePrice value, OutputStream outStream)
            throws CoderException, IOException {
          STRING_CODER.encode(value.name, outStream);
          STRING_CODER.encode(value.city, outStream);
          STRING_CODER.encode(value.state, outStream);
          LONG_CODER.encode(value.price, outStream);
        }

        @Override
        public NameCityStatePrice decode(InputStream inStream) throws CoderException, IOException {
          String name = STRING_CODER.decode(inStream);
          String city = STRING_CODER.decode(inStream);
          String state = STRING_CODER.decode(inStream);
          long id = LONG_CODER.decode(inStream);
          return new NameCityStatePrice(name, city, state, id);
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(NameCityStatePrice v) {
          return v;
        }
      };

  @JsonProperty public String name;

  @JsonProperty public String city;

  @JsonProperty public String state;

  @JsonProperty public long price;

  // For Avro only.
  @SuppressWarnings("unused")
  public NameCityStatePrice() {
    name = null;
    city = null;
    state = null;
    price = 0;
  }

  public NameCityStatePrice(String name, String city, String state, long price) {
    this.name = name;
    this.city = city;
    this.state = state;
    this.price = price;
  }

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (this == otherObject) {
      return true;
    }
    if (otherObject == null || getClass() != otherObject.getClass()) {
      return false;
    }

    NameCityStatePrice other = (NameCityStatePrice) otherObject;
    return Objects.equals(name, other.name)
        && Objects.equals(city, other.city)
        && Objects.equals(state, other.state)
        && Objects.equals(price, other.price);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, city, state, price);
  }

  @Override
  public long sizeInBytes() {
    return name.length() + 1L + city.length() + 1L + state.length() + 1L + 8L;
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
