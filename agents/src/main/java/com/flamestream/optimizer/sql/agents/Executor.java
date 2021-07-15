package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.Pipeline;

import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * Second main agent, that is responsible for graph running and changing on the
 * specific stream processing system.
 */
public interface Executor {
    void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer);

    @Nullable
    Pipeline current();

    enum ChangingStatus {
        CHANGING_STARTED,
        NEW_GRAPH_DEPLOYING,
        NEW_GRAPH_DEPLOYED,
        NEW_GRAPH_STARTING,
        NEW_GRAPH_STARTED,
        DONE,
    }
}
