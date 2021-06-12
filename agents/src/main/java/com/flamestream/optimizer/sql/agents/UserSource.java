package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

class UserSource {
    private final String tag;
    // TODO should there be any limitations on CheckpointT? should UserSource be a generic class?
    private final UnboundedSource<Row, @NonNull ?> source;

    public UserSource(String tag, UnboundedSource<Row, @NonNull ?> source) {
        this.tag = tag;
        this.source = source;
    }

    public String getTag() {
        return tag;
    }

    public UnboundedSource<Row, @NonNull ?> getSource() {
        return source;
    }
}
