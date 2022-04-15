package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;

public class JobExecutionResultWithClient {
    private final JobExecutionResult result;
    private final JobClient jobClient;

    public JobExecutionResultWithClient(final JobExecutionResult result, final JobClient jobClient) {
        this.result = result;
        this.jobClient = jobClient;
    }

    public JobExecutionResult getJobExecutionResult() {
        return result;
    }

    public JobClient getJobClient() {
        return jobClient;
    }
}
