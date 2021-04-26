package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.Pipeline;

import javax.annotation.Nullable;

public interface Executor {
    ChangingContext startOrUpdate(Pipeline pipeline);

    @Nullable
    Pipeline current();

    interface ChangingContext {
        ChangingStatus getStatus();
    }

    enum ChangingStatus {
        CHANGING_STARTED,
        NEW_GRAPH_DEPLOYING,
        NEW_GRAPH_DEPLOYED,
        NEW_GRAPH_STARTING,
        NEW_GRAPH_STARTED,
        DONE,
    }
}
