package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.tools.Planner;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

public class CoordinatorExecutorPipeline {
    public static void fromUserQuerySource(
            final @NonNull Pipeline pipeline,
            final Planner planner, // could possibly be removed in the near future
            final QueryPlanner queryPlanner,
            final CostEstimator costEstimator,
            final @NonNull PTransform<@NonNull ? super PBegin, @NonNull PCollection<Coordinator.SqlQueryJob>> userQuerySource,
            final @NonNull Collection<UserSource> inputs) {

        final PCollection<Coordinator.SqlQueryJob> queries = pipeline.apply("ReadQuery", userQuerySource);

        final Executor executor = new ExecutorImpl();
        final Coordinator coordinator = new CoordinatorImpl(planner, queryPlanner, costEstimator, executor);
        for (UserSource input : inputs) {
            coordinator.registerInput(input.getTag(), input.getSource());
        }

        queries.apply("SubmitToCoordinator", ParDo.of(new CoordinatorExecutorDoFn(coordinator)));
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