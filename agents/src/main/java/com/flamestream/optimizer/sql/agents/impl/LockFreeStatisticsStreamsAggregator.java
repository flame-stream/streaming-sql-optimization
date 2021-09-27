package com.flamestream.optimizer.sql.agents.impl;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LockFreeStatisticsStreamsAggregator<K, V> {
    public class Tagged {
        private final K key;
        private Instant timestamp;

        public Tagged(K key) {
            this.key = key;
        }

        public synchronized void put(Instant timestamp, V value) {
            if (timestamp.isBefore(timestamp)) {
                return;
            }
        }
    }

    private final ConcurrentMap<K, Tagged> taggedByKey;
    private final ConcurrentSkipListMap<Instant, ConcurrentMap<K, V>> timestampTagged =
            new ConcurrentSkipListMap<>();

    public LockFreeStatisticsStreamsAggregator(Set<K> keys) {
        taggedByKey = keys.stream().collect(Collectors.toConcurrentMap(Function.identity(), Tagged::new));
    }
}
