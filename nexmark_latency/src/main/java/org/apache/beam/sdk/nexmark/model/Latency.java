package org.apache.beam.sdk.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class Latency implements KnownSize {


    private static final Coder<Long> LONG_CODER = VarLongCoder.of();

    public static final Coder<Latency> CODER =
            new CustomCoder<Latency>() {
                @Override
                public void encode(Latency value, OutputStream outStream)
                        throws CoderException, IOException {
                    if (value != null) {
                        LONG_CODER.encode(value.latency, outStream);
                    }

                }

                @Override
                public Latency decode(InputStream inStream) throws CoderException, IOException {
                    long latency = LONG_CODER.decode(inStream);
                    return new Latency(latency);
                }

                @Override
                public Object structuralValue(Latency v) {
                    return v;
                }
            };

    @JsonProperty public long latency;

    // For Avro only.
    @SuppressWarnings("unused")
    public Latency() {
        latency = 0;
    }

    public Latency(long latency) {
        this.latency = latency;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Latency latency1 = (Latency) o;
        return latency == latency1.latency;
    }

    @Override
    public int hashCode() {
        return Objects.hash(latency);
    }

    @Override
    public long sizeInBytes() {
        return 8L;
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
