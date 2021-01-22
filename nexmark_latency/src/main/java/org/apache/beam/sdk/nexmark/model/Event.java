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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.apache.beam.sdk.coders.InstantCoder;
import org.joda.time.Instant;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction}, or a
 * {@link Bid}.
 */
@DefaultSchema(JavaFieldSchema.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class Event implements KnownSize, Serializable {

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Event event = (Event) o;
    return Objects.equal(newPerson, event.newPerson)
        && Objects.equal(newAuction, event.newAuction)
        && Objects.equal(bid, event.bid);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(newPerson, newAuction, bid);
  }

  /** The type of object stored in this event. * */
  public enum Type {
    PERSON(0),
    AUCTION(1),
    BID(2);

    private final int value;

    Type(int value) {
      this.value = value;
    }
  }

  private static final Coder<Integer> INT_CODER = VarIntCoder.of();
  private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();

  public static final Coder<Event> CODER =
      new CustomCoder<Event>() {
        @Override
        public void encode(Event value, OutputStream outStream) throws IOException {
          if (value.newPerson != null) {
            INT_CODER.encode(Type.PERSON.value, outStream);
            Person.CODER.encode(value.newPerson, outStream);
          } else if (value.newAuction != null) {
            INT_CODER.encode(Type.AUCTION.value, outStream);
            Auction.CODER.encode(value.newAuction, outStream);
          } else if (value.bid != null) {
            INT_CODER.encode(Type.BID.value, outStream);
            Bid.CODER.encode(value.bid, outStream);
          } else {
            throw new RuntimeException("invalid event");
          }
          // TODO coder isn't working
          INSTANT_CODER.encode(value.systemTime, outStream);
        }

        @Override
        public Event decode(InputStream inStream) throws IOException {
          int tag = INT_CODER.decode(inStream);
          if (tag == Type.PERSON.value) {
            Person person = Person.CODER.decode(inStream);
            Instant timestamp = INSTANT_CODER.decode(inStream);
            return new Event(person, timestamp);
          } else if (tag == Type.AUCTION.value) {
            Auction auction = Auction.CODER.decode(inStream);
            Instant timestamp = INSTANT_CODER.decode(inStream);
            return new Event(auction, timestamp);
          } else if (tag == Type.BID.value) {
            Bid bid = Bid.CODER.decode(inStream);
            Instant timestamp = INSTANT_CODER.decode(inStream);
            return new Event(bid, timestamp);
          } else {
            throw new RuntimeException("invalid event encoding");
          }
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {}

        @Override
        public Object structuralValue(Event v) {
          return v;
        }
      };

  public @org.apache.avro.reflect.Nullable @Nullable Person newPerson;

  public @org.apache.avro.reflect.Nullable @Nullable Auction newAuction;

  public @org.apache.avro.reflect.Nullable @Nullable Bid bid;

  public Instant systemTime;

  @SuppressWarnings("unused")
  public Event() {
    newPerson = null;
    newAuction = null;
    bid = null;
    systemTime = Instant.now();
  }

  public Event(Person newPerson) {
    this.newPerson = newPerson;
    newAuction = null;
    bid = null;
    systemTime = Instant.now();
  }

  public Event(Person newPerson, Instant systemTime) {
    this.newPerson = newPerson;
    newAuction = null;
    bid = null;
    this.systemTime = systemTime;
  }

  public Event(Auction newAuction) {
    newPerson = null;
    this.newAuction = newAuction;
    bid = null;
    systemTime = Instant.now();
  }

  public Event(Auction newAuction, Instant systemTime) {
    newPerson = null;
    this.newAuction = newAuction;
    bid = null;
    this.systemTime = systemTime;
  }

  public Event(Bid bid) {
    newPerson = null;
    newAuction = null;
    this.bid = bid;
    systemTime = Instant.now();
  }

  public Event(Bid bid, Instant systemTime) {
    newPerson = null;
    newAuction = null;
    this.bid = bid;
    this.systemTime = systemTime;
  }

  /** Return a copy of event which captures {@code annotation}. (Used for debugging). */
  public Event withAnnotation(String annotation) {
    if (newPerson != null) {
      return new Event(newPerson.withAnnotation(annotation));
    } else if (newAuction != null) {
      return new Event(newAuction.withAnnotation(annotation));
    } else {
      return new Event(bid.withAnnotation(annotation));
    }
  }

  /** Does event have {@code annotation}? (Used for debugging.) */
  public boolean hasAnnotation(String annotation) {
    if (newPerson != null) {
      return newPerson.hasAnnotation(annotation);
    } else if (newAuction != null) {
      return newAuction.hasAnnotation(annotation);
    } else {
      return bid.hasAnnotation(annotation);
    }
  }

  @Override
  public long sizeInBytes() {
    if (newPerson != null) {
      return 1 + newPerson.sizeInBytes() + 8L;
    } else if (newAuction != null) {
      return 1 + newAuction.sizeInBytes() + 8L;
    } else if (bid != null) {
      return 1 + bid.sizeInBytes() + 8L;
    } else {
      throw new RuntimeException("invalid event");
    }
  }

  @Override
  public String toString() {
    if (newPerson != null) {
      return newPerson.toString();
    } else if (newAuction != null) {
      return newAuction.toString();
    } else if (bid != null) {
      return bid.toString();
    } else {
      throw new RuntimeException("invalid event");
    }
  }
}
