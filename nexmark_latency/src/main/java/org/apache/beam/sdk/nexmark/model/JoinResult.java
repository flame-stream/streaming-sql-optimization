package org.apache.beam.sdk.nexmark.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;

@DefaultSchema(JavaFieldSchema.class)
public class JoinResult implements KnownSize, Serializable {
    private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();

    public static final Coder<JoinResult> CODER =
            new CustomCoder<JoinResult>() {
                @Override
                public void encode(JoinResult value, OutputStream outStream)
                        throws CoderException, IOException {
                    if (value != null) {
                        INSTANT_CODER.encode(value.timestamp1, outStream);
                        INSTANT_CODER.encode(value.timestamp2, outStream);
                        INSTANT_CODER.encode(value.timestamp3, outStream);
                    }

                }

                @Override
                public JoinResult decode(InputStream inStream) throws CoderException, IOException {
                    Instant timestamp1 = INSTANT_CODER.decode(inStream);
                    Instant timestamp2 = INSTANT_CODER.decode(inStream);
                    Instant timestamp3 = INSTANT_CODER.decode(inStream);
                    return new JoinResult(timestamp1, timestamp2, timestamp3);
                }

                @Override
                public Object structuralValue(JoinResult v) {
                    return v;
                }
            };

    @JsonProperty public Instant timestamp1;
    @JsonProperty public Instant timestamp2;
    @JsonProperty public Instant timestamp3;

    // For Avro only.
    @SuppressWarnings("unused")
    public JoinResult() {
        timestamp1 = Instant.now();
        timestamp2 = Instant.now();
        timestamp3 = Instant.now();
    }

    public JoinResult(Instant timestamp1, Instant timestamp2, Instant timestamp3) {
        this.timestamp1 = timestamp1;
        this.timestamp2 = timestamp2;
        this.timestamp3 = timestamp3;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinResult that = (JoinResult) o;
        return Objects.equals(timestamp1, that.timestamp1) && Objects.equals(timestamp2, that.timestamp2) && Objects.equals(timestamp3, that.timestamp3);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp1, timestamp2, timestamp3);
    }

    @Override
    public long sizeInBytes() {
        return 8L + 8L + 8L;
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
