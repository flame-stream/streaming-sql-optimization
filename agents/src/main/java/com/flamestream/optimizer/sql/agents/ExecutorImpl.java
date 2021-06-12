package com.flamestream.optimizer.sql.agents;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Consumer;

public class ExecutorImpl implements Executor, Serializable {
    private Pipeline currentPipeline;

    @Override
    public void startOrUpdate(Pipeline pipeline, Consumer<ChangingStatus> statusConsumer) {
        currentPipeline = pipeline;

        final PipelineRunner<@NonNull PipelineResult> runner = FlinkRunner.fromOptions(pipeline.getOptions());
        runner.run(pipeline);
    }

    @Nullable
    @Override
    public Pipeline current() {
        return currentPipeline;
    }
}
