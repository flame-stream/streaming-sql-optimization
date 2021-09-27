package com.flamestream.optimizer.sql.agents.impl;

import java.time.Instant;

public interface StatisticsStreamsConsumer<K, V> {
    void put(K key, Instant timestamp, V value);
}
