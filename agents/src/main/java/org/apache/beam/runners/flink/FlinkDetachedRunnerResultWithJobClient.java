package org.apache.beam.runners.flink;

import org.apache.flink.core.execution.JobClient;

public class FlinkDetachedRunnerResultWithJobClient extends FlinkDetachedRunnerResult {
    private final JobClient jobClient;

    public FlinkDetachedRunnerResultWithJobClient(final JobClient jobClient) {
        this.jobClient = jobClient;
    }

    public JobClient getJobClient() {
        return jobClient;
    }
}
