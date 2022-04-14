package com.flamestream.optimizer.sql.agents;

import com.flamestream.optimizer.sql.agents.impl.CoordinatorImpl;
import com.flamestream.optimizer.sql.agents.impl.ExecutorImpl;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

public class CoordinatorExecutorPipeline {
    public static void fromSqlQueryJob(
            final CostEstimator costEstimator,
            final @NonNull Collection<UserSource> inputs,
            final String optionsArguments,
            final Coordinator.SqlQueryJob job,
            final Coordinator.SqlQueryJob replacement) {

        final Executor executor = new ExecutorImpl(optionsArguments);
        final Coordinator coordinator = new CoordinatorImpl(costEstimator, executor);

        for (var input : inputs) {
            coordinator.registerInput(input.getSource(), input.getSchema(), input.getTableMapping(),input.getAdditionalTransforms() );
        }

        if (replacement != null) {
            coordinator.startAndReplaceLater(job, replacement);
        } else {
            coordinator.start(job);
        }
        try {
            Thread.sleep(1000 * 60 * 15);
        } catch (Exception e) { }
    }
}