package com.flamestream.optimizer.sql.agents;

public interface Executor {
    void startJob(String flinkJob);
    void changeJob(String newJob);
}
