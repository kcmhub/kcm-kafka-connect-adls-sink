package io.kcmhub.kafka.connect.adls;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdlsSinkTaskBufferTest {

    @Test
    void shouldFlushWhenFlushMaxRecordsReached() {
        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "acc");
        props.put("adls.filesystem", "fs");
        props.put("adls.sas.token", "token");
        props.put("flush.max.records", "2"); // petit pour le test

        TestableAdlsSinkTask task = new TestableAdlsSinkTask();
        task.start(props);

        // 3 records sur même topic/partition
        SinkRecord r1 = new SinkRecord("topicA", 0, null, null, null, "v1", 100L);
        SinkRecord r2 = new SinkRecord("topicA", 0, null, null, null, "v2", 101L);
        SinkRecord r3 = new SinkRecord("topicA", 0, null, null, null, "v3", 102L);

        task.put(List.of(r1, r2, r3));

        // flushMaxRecords = 2 => un flush pour v1+v2, un buffer restant pour v3
        assertEquals(1, task.flushes.size());
        TestableAdlsSinkTask.FlushCall first = task.flushes.get(0);
        assertEquals("topicA", first.topic);
        assertEquals(0, first.partition);
        assertEquals(100L, first.startOffset);
        assertTrue(first.content.contains("v1"));
        assertTrue(first.content.contains("v2"));

        // stop() doit flusher le reste (v3)
        task.stop();
        assertEquals(2, task.flushes.size());
    }

    @Test
    void shouldFlushWhenFlushIntervalReached() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "acc");
        props.put("adls.filesystem", "fs");
        props.put("adls.sas.token", "token");
        props.put("flush.max.records", "1000");
        props.put("flush.interval.ms", "1");

        TestableAdlsSinkTask task = new TestableAdlsSinkTask();
        task.start(props);

        // Prepare a partial buffer (no size-based flush)
        SinkRecord r1 = new SinkRecord("topicA", 0, null, null, null, "v1", 100L);
        task.put(List.of(r1));
        assertEquals(0, task.flushes.size());

        // Wait for the interval to pass
        Thread.sleep(5);

        // Next put triggers the time-based flush BEFORE appending new records
        SinkRecord r2 = new SinkRecord("topicA", 0, null, null, null, "v2", 101L);
        task.put(List.of(r2));

        assertEquals(1, task.flushes.size());
        assertTrue(task.flushes.get(0).content.contains("v1"));

        // stop() should flush the remaining buffer (v2)
        task.stop();
        assertEquals(2, task.flushes.size());
    }
}
