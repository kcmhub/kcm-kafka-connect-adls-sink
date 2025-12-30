package io.kcmhub.kafka.connect.adls;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kcmhub.kafka.connect.adls.utils.SimpleJsonFormatter.formatRecordValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdlsSinkTaskFormatterTest {

    @Test
    void shouldFormatStructAsJson() {
        // schema avec 2 champs
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("name", "Alice")
                .put("age", 30);

        SinkRecord record = new SinkRecord(
                "topic", 0, null, null, schema, struct, 42L
        );

        AdlsSinkTask task = new AdlsSinkTask();
        // on simule start() minimal pour éviter NPE (si nécessaire)
        task.start(Map.of(
                "adls.account.name", "acc",
                "adls.filesystem", "fs",
                "adls.sas.token", "token"
        ));

        String formatted = formatRecordValue(record); // soit méthode package-private, soit via reflection

        // format attendu : {"name":"Alice","age":30}
        assertTrue(formatted.contains("\"name\""));
        assertTrue(formatted.contains("\"Alice\""));
        assertTrue(formatted.contains("\"age\""));
        assertTrue(formatted.contains("30"));
    }
}

