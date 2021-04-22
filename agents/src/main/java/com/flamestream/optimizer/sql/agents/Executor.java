package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.Pipeline;

import javax.annotation.Nullable;

public interface Executor {
    // blocking, not async
    boolean startOrUpdate(Pipeline pipeline);

    @Nullable
    Pipeline current();
}
