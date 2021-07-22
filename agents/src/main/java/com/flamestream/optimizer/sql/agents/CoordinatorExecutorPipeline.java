package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.transforms.DoFn;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

public class CoordinatorExecutorPipeline {
    public static void fromUserQuery(
            final CostEstimator costEstimator,
            final @NonNull Collection<UserSource> inputs,
            final Coordinator.SqlQueryJob job) {

        final Executor executor = new ExecutorImpl();
        final Coordinator coordinator = new CoordinatorImpl(costEstimator, executor);
        for (UserSource input : inputs) {
            coordinator.registerInput(input.getTag(), input.getSource());
        }

        coordinator.start(job);
    }
}

// TODO should it return the result or simply send the pipeline to the cluster? I would expect the result
class CoordinatorExecutorDoFn extends DoFn<Coordinator.SqlQueryJob, Void> {
    private final Coordinator coordinator;

    public CoordinatorExecutorDoFn(@NonNull Coordinator coordinator) {
        this.coordinator = coordinator;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        final Coordinator.SqlQueryJob queryJob = c.element();
        if (queryJob == null) {
            return;
        }

        // submits the resulting pipeline to executor, which submits it to the cluster
        coordinator.start(queryJob);

        // TODO are we getting the results here? if so, how?
    }
}