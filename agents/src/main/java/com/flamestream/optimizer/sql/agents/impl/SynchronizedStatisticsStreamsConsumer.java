package com.flamestream.optimizer.sql.agents.impl;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

public class SynchronizedStatisticsStreamsConsumer<K, S> implements StatisticsStreamsConsumer<K, S> {
    private final BiConsumer<Instant, Map<K, S>> consumer;

    private final ConcurrentMap<K, Instant> taggedByKey;
    private final ConcurrentSkipListMap<Instant, ConcurrentMap<K, S>> timestampTagged =
            new ConcurrentSkipListMap<>();

    public SynchronizedStatisticsStreamsConsumer(BiConsumer<Instant, Map<K, S>> consumer) {
        this.consumer = consumer;
        taggedByKey = new ConcurrentHashMap<>();
    }

    @Override
    public synchronized void put(K key, Instant timestamp, S statistics) {
        final var tagged = taggedByKey.get(key);
        if (timestamp.isBefore(tagged)) {
            throw new IllegalStateException(timestamp + " < " + tagged);
        }
        final var keyStatistics = timestampTagged.computeIfAbsent(timestamp, __ -> new ConcurrentHashMap<>());
        keyStatistics.put(key, statistics);
        if (keyStatistics.size() == taggedByKey.size() && timestampTagged.remove(timestamp) != null) {
            consumer.accept(timestamp, keyStatistics);
        }
        if (tagged.equals(timestampTagged.firstKey())) {

        }
    }
}
