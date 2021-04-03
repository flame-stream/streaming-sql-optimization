package org.apache.beam.sdk.nexmark.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Utility logging transform. Usage: .apply(ParDo.of(new LoggingDoFn())).
 */
public class LoggingDoFn<T> extends DoFn<T, T> {
    private String prefix = "";

    public LoggingDoFn() {
    }

    public LoggingDoFn(String prefix) {
        this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window, @Timestamp Instant timestamp) {
        T row = c.element();
        if (row != null) {
            System.out.println(row.toString());
        }
        c.output(row);
    }
}