package io.kcmhub.kafka.connect.adls.utils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON-like formatter used by the connector.
 *
 * <p>This is intentionally lightweight (no external JSON dependency) and is only meant
 * to serialize simple Kafka Connect data types (Struct/Map/List/primitives).</p>
 */
public final class SimpleJsonFormatter {

    private SimpleJsonFormatter() {
    }

    public static String formatRecordValue(SinkRecord record) {
        Schema schema = record.valueSchema();
        Object value = record.value();

        if (value == null) {
            return "null";
        }

        if (schema == null) {
            // Schemaless: String / Map / List...
            return value.toString();
        }

        if (schema.type() == Schema.Type.STRUCT && value instanceof Struct) {
            Struct struct = (Struct) value;
            Map<String, Object> map = new LinkedHashMap<>();
            for (Field field : schema.fields()) {
                map.put(field.name(), struct.get(field));
            }
            return toJson(map);
        }

        if (schema.type() == Schema.Type.MAP && value instanceof Map<?, ?>) {
            return toJson(value);
        }

        if (schema.type() == Schema.Type.ARRAY && value instanceof List<?>) {
            return toJson(value);
        }

        // Primitives and other types
        return value.toString();
    }

    /**
     * Recursively converts an object (Map, List, String, Number, Boolean or null) to a JSON string.
     * <li> Map -> JSON object (preserves iteration order when possible)
     * <li> List -> JSON array
     * <li> String -> escapes double quotes
     * <li> Number/Boolean -> toString()
     * <br/>
     * Limitations: this is a lightweight serializer (it does not fully escape control characters and does not handle
     * complex POJOs).
     */
    public static String toJson(Object obj) {
        if (obj == null) return "null";

        if (obj instanceof Map<?, ?>) {
            Map<?, ?> m = (Map<?, ?>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(e.getKey()).append("\":");
                sb.append(toJson(e.getValue()));
                first = false;
            }
            return sb.append("}").toString();
        }

        if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Object o : list) {
                if (!first) sb.append(",");
                sb.append(toJson(o));
                first = false;
            }
            return sb.append("]").toString();
        }

        if (obj instanceof String) {
            String s = (String) obj;
            return "\"" + s.replace("\"", "\\\"") + "\"";
        }

        return obj.toString();
    }
}
