package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Collection;

public class CoordinatorExecutorPipeline {
    public static void fromUserQuery(
            final CostEstimator costEstimator,
            final @NonNull Collection<UserSource> inputs,
            final Coordinator.SqlQueryJob job,
            final PipelineOptions executorPipelineOptions) {

        final Executor executor = new ExecutorImpl(executorPipelineOptions);
        final Coordinator coordinator = new CoordinatorImpl(costEstimator, executor);
        for (UserSource input : inputs) {
            coordinator.registerInput(input.getTag(), input.getSource(), input.getSchema());
        }

        coordinator.start(job);
    }
}