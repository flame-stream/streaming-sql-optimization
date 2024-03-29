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
package org.apache.beam.sdk.nexmark.sources.generator;

import static org.apache.beam.sdk.nexmark.sources.generator.model.AuctionGenerator.nextAuction;
import static org.apache.beam.sdk.nexmark.sources.generator.model.BidGenerator.nextBid;
import static org.apache.beam.sdk.nexmark.sources.generator.model.PersonGenerator.nextPerson;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.values.TimestampedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generator for synthetic events. We try to make the data vaguely reasonable. We also ensure most
 * primary key/foreign key relations are correct. Eg: a {@link Bid} event will usually have valid
 * auction and bidder ids which can be joined to already-generated Auction and Person events.
 *
 * <p>To help with testing, we generate timestamps relative to a given {@code baseTime}. Each new
 * event is given a timestamp advanced from the previous timestamp by {@code interEventDelayUs} (in
 * microseconds). The event stream is thus fully deterministic and does not depend on wallclock
 * time.
 *
 * <p>This class implements {@link org.apache.beam.sdk.io.UnboundedSource.CheckpointMark} so that we
 * can resume generating events from a saved snapshot.
 */
public class Generator implements Iterator<TimestampedValue<Event>>, Serializable {

    public static final Logger LOG = LoggerFactory.getLogger("generator");

    /**
     * The next event and its various timestamps. Ordered by increasing wallclock timestamp, then
     * (arbitrary but stable) event hash order.
     */
    public static class NextEvent implements Comparable<NextEvent> {
        /** When, in wallclock time, should this event be emitted? */
        public final long wallclockTimestamp;

        /** When, in event time, should this event be considered to have occured? */
        public final long eventTimestamp;

        /** The event itself. */
        public final Event event;

        /** The minimum of this and all future event timestamps. */
        public final long watermark;

        public NextEvent(long wallclockTimestamp, long eventTimestamp, Event event, long watermark) {
            this.wallclockTimestamp = wallclockTimestamp;
            this.eventTimestamp = eventTimestamp;
            this.event = event;
            this.watermark = watermark;
        }

        /**
         * Return a deep copy of next event with delay added to wallclock timestamp and event annotate
         * as 'LATE'.
         */
        public NextEvent withDelay(long delayMs) {
            return new NextEvent(
                    wallclockTimestamp + delayMs, eventTimestamp, event.withAnnotation("LATE"), watermark);
        }

        @Override
        public boolean equals(@Nullable Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            NextEvent nextEvent = (NextEvent) o;

            return (wallclockTimestamp == nextEvent.wallclockTimestamp
                    && eventTimestamp == nextEvent.eventTimestamp
                    && watermark == nextEvent.watermark
                    && event.equals(nextEvent.event));
        }

        @Override
        public int hashCode() {
            return Objects.hash(wallclockTimestamp, eventTimestamp, watermark, event);
        }

        @Override
        public int compareTo(NextEvent other) {
            int i = Long.compare(wallclockTimestamp, other.wallclockTimestamp);
            if (i != 0) {
                return i;
            }
            return Integer.compare(event.hashCode(), other.event.hashCode());
        }
    }

    /**
     * Configuration to generate events against. Note that it may be replaced by a call to {@link
     * #splitAtEventId}.
     */
    private GeneratorConfig config;

    private int personProportion;
    private int auctionProportion;
    private int bidProportion;

    /** Number of events generated by this generator. */
    private long eventsCountSoFar;
    private long personCountInCurrentWindow = 0;

    /** Wallclock time at which we emitted the first event (ms since epoch). Initially -1. */
    private long wallclockBaseTime;

    Generator(GeneratorConfig config, long eventsCountSoFar, long wallclockBaseTime) {
        this(config, eventsCountSoFar, wallclockBaseTime, config.personProportion, config.auctionProportion, config.bidProportion);
    }

    Generator(GeneratorConfig config, long eventsCountSoFar, long wallclockBaseTime, int personsProportion, int auctionsProportion, int bidsProportion) {
        checkNotNull(config);
        this.config = config;
        LOG.info("eventsCountSoFar " + eventsCountSoFar);
        LOG.info("max events " + config.maxEvents);
        this.eventsCountSoFar = eventsCountSoFar;
        this.wallclockBaseTime = wallclockBaseTime;
        this.personProportion = personsProportion;
        this.auctionProportion = auctionsProportion;
        this.bidProportion = bidsProportion;
    }


    /** Create a fresh generator according to {@code config}. */
    public Generator(GeneratorConfig config) {
        this(config, 0, -1);
    }

    /** Return a checkpoint for the current generator. */
    public GeneratorCheckpoint toCheckpoint() {
        return new GeneratorCheckpoint(eventsCountSoFar, wallclockBaseTime, personProportion, auctionProportion, bidProportion);
    }

    /** Return a deep copy of this generator. */
    public Generator copy() {
        checkNotNull(config);
        return new Generator(config, eventsCountSoFar, wallclockBaseTime, personProportion, auctionProportion, bidProportion);
    }

    /**
     * Return the current config for this generator. Note that configs may be replaced by {@link
     * #splitAtEventId}.
     */
    public GeneratorConfig getCurrentConfig() {
        return config;
    }

    /**
     * Mutate this generator so that it will only generate events up to but not including {@code
     * eventId}. Return a config to represent the events this generator will no longer yield. The
     * generators will run in on a serial timeline.
     */
    public GeneratorConfig splitAtEventId(long eventId) {
        long newMaxEvents = eventId - (config.firstEventId + config.firstEventNumber);
        GeneratorConfig remainConfig =
                config.copyWith(
                        config.firstEventId,
                        config.maxEvents - newMaxEvents,
                        config.firstEventNumber + newMaxEvents);
        config = config.copyWith(config.firstEventId, newMaxEvents, config.firstEventNumber);
        return remainConfig;
    }

    /**
     * Return the next 'event id'. Though events don't have ids we can simulate them to help with
     * bookkeeping.
     */
    public long getNextEventId() {
        return config.firstEventId + config.nextAdjustedEventNumber(eventsCountSoFar);
    }

    @Override
    public boolean hasNext() {
        return eventsCountSoFar < config.maxEvents;
    }

    /**
     * Return the next event. The outer timestamp is in wallclock time and corresponds to when the
     * event should fire. The inner timestamp is in event-time and represents the time the event is
     * purported to have taken place in the simulation.
     */
    public NextEvent nextEvent() {
        if (wallclockBaseTime < 0) {
            wallclockBaseTime = System.currentTimeMillis();
        }
        // When, in event time, we should generate the event. Monotonic.
        long eventTimestamp =
                config
                        .timestampAndInterEventDelayUsForEvent(config.nextEventNumber(eventsCountSoFar))
                        .getKey();
        // When, in event time, the event should say it was generated. Depending on outOfOrderGroupSize
        // may have local jitter.
        long adjustedEventTimestamp =
                config
                        .timestampAndInterEventDelayUsForEvent(config.nextAdjustedEventNumber(eventsCountSoFar))
                        .getKey();

//    System.out.println("generated " + new DateTime(adjustedEventTimestamp));

        // The minimum of this and all future adjusted event timestamps. Accounts for jitter in
        // the event timestamp.
        long watermark =
                config
                        .timestampAndInterEventDelayUsForEvent(
                                config.nextEventNumberForWatermark(eventsCountSoFar))
                        .getKey();
        // When, in wallclock time, we should emit the event.
        long wallclockTimestamp = wallclockBaseTime + (eventTimestamp - getCurrentConfig().baseTime);

        // Seed the random number generator with the next 'event id'.
        Random random = new Random(getNextEventId());

        long newEventId = getNextEventId();
        long rem = newEventId % config.proportionDenominator;

        long offsetInWindow = eventsCountSoFar % (config.getNexmarkConfiguration().nextEventRate * config.getNexmarkConfiguration().windowSizeSec);
        if (offsetInWindow == 0) {
            personCountInCurrentWindow = 0;
        }

        if (eventsCountSoFar == config.maxEvents / 4 || eventsCountSoFar == config.maxEvents * 3 / 5) {
            if (eventsCountSoFar == config.maxEvents / 4) {
                LOG.info("quarter reached");
            } else {
                LOG.info("three fifths reached");
            }
            LOG.info("old proportions " + personProportion + " " + auctionProportion + " " + bidProportion);
            int tmp = auctionProportion;
            auctionProportion = bidProportion;
            bidProportion = tmp;
            LOG.info("new proportions " + personProportion + " " + auctionProportion + " " + bidProportion);
        }
        Event event;
        // TODO this must skew the number of persons a bit, by how much?
        if (offsetInWindow < 20 || rem < personProportion) {
            personCountInCurrentWindow++;
//            LOG.info("generated [PERSON ] " + new DateTime(adjustedEventTimestamp));
            event =
                    new Event(nextPerson(newEventId, random, new DateTime(adjustedEventTimestamp), config));

            /*if (adjustedEventTimestamp >= 1637695440000L + 60000 && adjustedEventTimestamp < 1637695440000L + 80000) {
                LOG.info("generated [PERSON ] " + event.newPerson.id + " " + new DateTime(adjustedEventTimestamp));
            }*/
        } else if (rem < personProportion + auctionProportion) {
//            LOG.info("generated [AUCTION] " + new DateTime(adjustedEventTimestamp));
            event =
                    new Event(
                            nextAuction(eventsCountSoFar, newEventId, random, adjustedEventTimestamp, config, personCountInCurrentWindow));
            /*if (adjustedEventTimestamp >= 1637695440000L + 60000 && adjustedEventTimestamp < 1637695440000L + 80000) {
                LOG.info("generated [AUCTION] " + event.newAuction.seller + " " + new DateTime(adjustedEventTimestamp));
            }*/
        } else {
//            LOG.info("generated [Bid    ]  " + new DateTime(adjustedEventTimestamp));
            event = new Event(nextBid(newEventId, random, adjustedEventTimestamp, config, personCountInCurrentWindow));
            /*if (adjustedEventTimestamp >= 1637695440000L + 60000 && adjustedEventTimestamp < 1637695440000L + 80000) {
                LOG.info("generated [Bid    ]  " + event.bid.bidder + " " + new DateTime(adjustedEventTimestamp));
            }*/
        }

        eventsCountSoFar++;

        return new NextEvent(wallclockTimestamp, adjustedEventTimestamp, event, watermark);
    }

    @Override
    public TimestampedValue<Event> next() {
        NextEvent next = nextEvent();
        return TimestampedValue.of(next.event, new Instant(next.eventTimestamp));
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    /** Return how many microseconds till we emit the next event. */
    public long currentInterEventDelayUs() {
        return config
                .timestampAndInterEventDelayUsForEvent(config.nextEventNumber(eventsCountSoFar))
                .getValue();
    }

    /** Return an estimate of fraction of output consumed. */
    public double getFractionConsumed() {
        return (double) eventsCountSoFar / config.maxEvents;
    }

    @Override
    public String toString() {
        return String.format(
                "Generator{config:%s; eventsCountSoFar:%d; wallclockBaseTime:%d}",
                config, eventsCountSoFar, wallclockBaseTime);
    }
}
