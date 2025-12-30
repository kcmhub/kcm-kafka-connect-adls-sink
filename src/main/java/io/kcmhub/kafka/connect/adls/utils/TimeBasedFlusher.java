package io.kcmhub.kafka.connect.adls.utils;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * Stateless helper to decide whether a buffer should be flushed based on time.
 */
public final class TimeBasedFlusher {

    private TimeBasedFlusher() {
    }

    public static boolean shouldFlush(long flushIntervalMs,
                                      long nowMs,
                                      TopicPartition tp,
                                      Map<TopicPartition, Long> lastFlushMsByTp) {
        if (flushIntervalMs <= 0) return false;

        long lastFlush = lastFlushMsByTp.getOrDefault(tp, nowMs);
        return (nowMs - lastFlush) >= flushIntervalMs;
    }
}

