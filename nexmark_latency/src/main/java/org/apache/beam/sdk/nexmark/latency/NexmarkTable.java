package org.apache.beam.sdk.nexmark.latency;

import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * Represents an unbounded Nexmark table
 */
public class NexmarkTable<InputT> extends BeamPCollectionTable<InputT> {
    private final double rate;

    public NexmarkTable(PCollection<InputT> upstream, double rate) {
        super(upstream);
        this.rate = rate;
    }

    @Override
    public BeamTableStatistics getTableStatistics(PipelineOptions options) {
        return BeamTableStatistics.createUnboundedTableStatistics(rate);
    }

}
