package com.flamestream.optimizer.sql.agents;

import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

public interface Coordinator {
    void submitQuery(String query, POutput input, PInput output);
    void stop();
}
