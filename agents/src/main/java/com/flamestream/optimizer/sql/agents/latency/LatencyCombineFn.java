package com.flamestream.optimizer.sql.agents.latency;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Combines all the values in the window.
 * In this case, the values are rows of three timestamps (input time -- "systemTime" -- for each item that was 'joined')
 * and the arrival time of the resulting tuple ("arrivalTime").
 * Can be configured to select the max timestamp or the average of all latencies in the window.
 */
public class LatencyCombineFn extends Combine.CombineFn<Row, LatencyCombineFn.Accumulator, Latency> {
    private static final Logger LOG = LoggerFactory.getLogger("latency");

    // called once for each window
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator addInput(Accumulator mutableAccumulator, Row input) {
        if (mutableAccumulator != null) {
            // uncomment when calculating the mean value instead of max
            // mutableAccumulator.second++;
            Instant timestamp = Instant.EPOCH;
            if (input != null) {
                Instant timestamp1 = input.getValue("receiveTime");
                Instant timestamp2 = input.getValue("receiveTime0");
                Instant timestamp3 = input.getValue("receiveTime1");
                Instant arrivalTime = Instant.now();
                if (timestamp1 != null && timestamp1.getMillis() > timestamp.getMillis()) {
                    timestamp = timestamp1;
                }
                if (timestamp2 != null && timestamp2.getMillis() > timestamp.getMillis()) {
                    timestamp = timestamp2;
                }
                if (timestamp3 != null && timestamp3.getMillis() > timestamp.getMillis()) {
                    timestamp = timestamp3;
                }
                // uncomment to calculate the mean value
//                     mutableAccumulator.sum += arrivalTime.minus(timestamp.getMillis()).getMillis();
//                     mutableAccumulator.count++;

                // store the element with max arrival time
                if (timestamp.getMillis() > mutableAccumulator.first) {
                    mutableAccumulator.first = timestamp.getMillis();
//                        mutableAccumulator.second = arrivalTime.getMillis();
                }

                // store last element exit time as a second
                if (arrivalTime.getMillis() > mutableAccumulator.second) {
                    mutableAccumulator.second = arrivalTime.getMillis();
                }
            }
        }
        return mutableAccumulator;
    }

    @Override
    public Accumulator mergeAccumulators(@UnknownKeyFor @NonNull @Initialized Iterable<Accumulator> accumulators) {
        Accumulator merged = createAccumulator();
        for (Accumulator accum : accumulators) {
            if (accum.first > merged.first) {
                merged.first = accum.first;
                merged.second = accum.second;
            }
            // uncomment when calculating the mean value instead of the max value
//             merged.sum += accum.sum;
//             merged.count += accum.count;
        }
        return merged;
    }

    @Override
    public Latency extractOutput(Accumulator accumulator) {
        if (accumulator != null) {
            // uncomment when calculating the mean value instead of the max value
//             return new Latency(accumulator.sum / accumulator.count);
            return new Latency(accumulator.second - accumulator.first);
        }
        return new Latency(0);
    }

    public static class Accumulator implements Serializable {
        long first = 0; // you can use this as sum for mean value calculation. or as max input time
        long second = 0; // you can use this as count for mean value calculation. or as max arrival time

        public Accumulator() {
        }
    }
}
