package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.apache.beam.sdk.options.PipelineOptions;

import java.util.Collection;
import java.util.Map;

public class CoordinatorExecutorPipeline {
    public static void fromUserQuery(
            final CostEstimator costEstimator,
            final @NonNull Collection<UserSource> inputs,
            final Coordinator.SqlQueryJob job,
            final String optionsArguments) {

        final Executor executor = new ExecutorImpl(optionsArguments);
        final Coordinator coordinator = new CoordinatorImpl(costEstimator, executor);

        for (var input : inputs) {
            coordinator.registerInput(input.getSource(), input.getSchema(), input.getTableMapping());
        }

        coordinator.start(job);
        try {
            Thread.sleep(1000 * 60 * 5);
        } catch (Exception e) { }
    }
}