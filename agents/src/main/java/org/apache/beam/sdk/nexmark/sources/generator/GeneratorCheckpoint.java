package org.apache.beam.sdk.nexmark.sources.generator;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.toStringHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.UnboundedSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorCheckpoint implements UnboundedSource.CheckpointMark {
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<Integer> INT_CODER = VarIntCoder.of();

    private static final Logger LOG = LoggerFactory.getLogger("generator");

    public static final Coder<GeneratorCheckpoint> CODER_INSTANCE =
            new CustomCoder<>() {
                @Override
                public void encode(GeneratorCheckpoint value, OutputStream outStream)
                        throws CoderException, IOException {
                    LONG_CODER.encode(value.numEvents, outStream);
                    LONG_CODER.encode(value.wallclockBaseTime, outStream);
                    INT_CODER.encode(value.personProroption, outStream);
                    INT_CODER.encode(value.auctionProportion, outStream);
                    INT_CODER.encode(value.bidProportion, outStream);
                }

                @Override
                public GeneratorCheckpoint decode(InputStream inStream) throws CoderException, IOException {
                    long numEvents = LONG_CODER.decode(inStream);
                    long wallclockBaseTime = LONG_CODER.decode(inStream);
                    int personProportion = INT_CODER.decode(inStream);
                    int auctionProportion = INT_CODER.decode(inStream);
                    int bidProportion = INT_CODER.decode(inStream);
                    return new GeneratorCheckpoint(numEvents, wallclockBaseTime, personProportion, auctionProportion, bidProportion);
                }

                @Override
                public void verifyDeterministic() throws NonDeterministicException {
                }
            };

    private final long numEvents;
    private final long wallclockBaseTime;
    private final int auctionProportion;
    private final int personProroption;
    private final int bidProportion;

    GeneratorCheckpoint(long numEvents, long wallclockBaseTime, int personProroption, int auctionProportion, int bidProportion) {
        this.numEvents = numEvents;
        this.wallclockBaseTime = wallclockBaseTime;
        this.auctionProportion = auctionProportion;
        this.personProroption = personProroption;
        this.bidProportion = bidProportion;
    }

    public Generator toGenerator(GeneratorConfig config) {
        LOG.info("to generator from proportions p:a:b: " + personProroption + " " + auctionProportion + " " + bidProportion);
        return new Generator(config, numEvents, wallclockBaseTime, personProroption, auctionProportion, bidProportion);
    }

    @Override
    public void finalizeCheckpoint() throws IOException {
        // Nothing to finalize.
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("numEvents", numEvents)
                .add("wallclockBaseTime", wallclockBaseTime)
                .add("personsProportion", personProroption)
                .add("auctionsProportion", auctionProportion)
                .add("bidsProportion", bidProportion)
                .toString();
    }
}
