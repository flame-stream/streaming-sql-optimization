package com.flamestream.optimizer.testutils;

import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.Map;

public class TestPipelineOptions implements PipelineOptions {
    @Override
    public <T extends PipelineOptions> T as(@UnknownKeyFor @NonNull @Initialized Class<T> kls) {
        return null;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Class<@UnknownKeyFor @NonNull @Initialized ? extends @UnknownKeyFor @NonNull @Initialized PipelineRunner<@UnknownKeyFor @NonNull @Initialized ?>> getRunner() {
        return null;
    }

    @Override
    public void setRunner(@UnknownKeyFor @NonNull @Initialized Class<@UnknownKeyFor @NonNull @Initialized ? extends @UnknownKeyFor @NonNull @Initialized PipelineRunner<@UnknownKeyFor @NonNull @Initialized ?>> kls) {

    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized CheckEnabled getStableUniqueNames() {
        return null;
    }

    @Override
    public void setStableUniqueNames(@UnknownKeyFor @NonNull @Initialized CheckEnabled enabled) {

    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getTempLocation() {
        return null;
    }

    @Override
    public void setTempLocation(@UnknownKeyFor @NonNull @Initialized String value) {

    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getJobName() {
        return null;
    }

    @Override
    public void setJobName(@UnknownKeyFor @NonNull @Initialized String jobName) {

    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Map<@UnknownKeyFor @NonNull @Initialized String, @UnknownKeyFor @NonNull @Initialized Map<@UnknownKeyFor @NonNull @Initialized String, @UnknownKeyFor @NonNull @Initialized Object>> outputRuntimeOptions() {
        return null;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized long getOptionsId() {
        return 0;
    }

    @Override
    public void setOptionsId(@UnknownKeyFor @NonNull @Initialized long id) {

    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getUserAgent() {
        return null;
    }

    @Override
    public void setUserAgent(@UnknownKeyFor @NonNull @Initialized String userAgent) {

    }

    @Override
    public void populateDisplayData(DisplayData.@UnknownKeyFor @NonNull @Initialized Builder builder) {

    }
}
