package com.flamestream.optimizer.testutils;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.UnboundedEventSource;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorCheckpoint;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class TestUnboundedRowSource extends UnboundedSource<Row, GeneratorCheckpoint> {
    private UnboundedEventSource source;
    private NexmarkConfiguration nexmarkConfig;
    private GeneratorConfig generatorConfig;
    
    public TestUnboundedRowSource() {
        final NexmarkConfiguration config = NexmarkConfiguration.DEFAULT;
        nexmarkConfig = config;
        generatorConfig = new GeneratorConfig(
                config,
                config.useWallclockEventTime ? System.currentTimeMillis() : 0,
                0,
                config.numEvents,
                0);
        source = new UnboundedEventSource(
                generatorConfig,
                config.numEventGenerators,
                config.watermarkHoldbackSec,
                config.isRateLimited);
    }


    @Override
    public List<? extends UnboundedSource<Row, GeneratorCheckpoint>> split(int desiredNumSplits, PipelineOptions options) {
        List<TestUnboundedRowSource> results = new ArrayList<>();
        // Ignore desiredNumSplits and use numEventGenerators instead.
        for (GeneratorConfig ignored : generatorConfig.split(nexmarkConfig.numEventGenerators)) {
            results.add(new TestUnboundedRowSource());
        }
        return results;
    }

    @Override
    public UnboundedReader<Row> createReader(PipelineOptions options, @Nullable GeneratorCheckpoint checkpointMark) {
        UnboundedReader<Event> eventReader = source.createReader(options, checkpointMark);
        return new RowReader(eventReader);
    }

    @Override
    public Coder<GeneratorCheckpoint> getCheckpointMarkCoder() {
        return source.getCheckpointMarkCoder();
    }

    @Override
    public  Coder<Row> getOutputCoder() {
        return new Coder<Row>() {
            @Override
            public void encode(Row value,  OutputStream outStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
                
            }

            @Override
            public Row decode(InputStream inStream) throws @UnknownKeyFor@NonNull@Initialized CoderException, @UnknownKeyFor@NonNull@Initialized IOException {
                return null;
            }

            @Override
            public  List<? extends  Coder< ?>> getCoderArguments() {
                return List.of();
            }

            @Override
            public void verifyDeterministic() {

            }
        };
    }

    private class RowReader extends UnboundedReader<Row> {
        UnboundedReader<Event> eventReader;

        public RowReader(UnboundedReader<Event> eventReader) {
            this.eventReader = eventReader;
        }

        @Override
        public boolean start() throws IOException {
            return eventReader.start();
        }

        @Override
        public boolean advance() throws IOException {
            return eventReader.advance();
        }

        @Override
        public Row getCurrent() throws NoSuchElementException {
            final Event currentEvent = eventReader.getCurrent();

            return null;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return eventReader.getCurrentTimestamp();
        }

        @Override
        public void close() throws IOException {
            eventReader.close();
        }

        @Override
        public Instant getWatermark() {
            return eventReader.getWatermark();
        }

        @Override
        public CheckpointMark getCheckpointMark() {
            return eventReader.getCheckpointMark();
        }

        @Override
        public UnboundedSource<Row, ?> getCurrentSource() {
            return TestUnboundedRowSource.this;
        }
    }
}
