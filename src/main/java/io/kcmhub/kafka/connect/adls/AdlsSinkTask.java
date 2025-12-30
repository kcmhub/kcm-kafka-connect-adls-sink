package io.kcmhub.kafka.connect.adls;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import io.kcmhub.kafka.connect.adls.dto.PartitionBuffer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static io.kcmhub.kafka.connect.adls.utils.CompressionUtils.gzip;

public class AdlsSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AdlsSinkTask.class);

    private String accountName;
    private String filesystem;
    private String basePath;
    private String sasToken;
    private int flushMaxRecords;
    private boolean compressGzip;
    private int retryMaxAttempts;

    private AdlsClientFactory clientFactory = new DefaultAdlsClientFactory();

    // For test purposes
    void setClientFactory(AdlsClientFactory factory) {
        this.clientFactory = factory;
    }

    private DataLakeFileClient buildFileClient(String filePath) {
        return clientFactory.createFileClient(accountName, filesystem, sasToken, filePath, retryMaxAttempts);
    }

    // Buffer par topic-partition

    private final Map<TopicPartition, PartitionBuffer> buffers = new HashMap<>();

    @Override
    public String version() {
        return "0.0.2";
    }

    @Override
    public void start(Map<String, String> props) {
        AdlsSinkConnectorConfig config = new AdlsSinkConnectorConfig(props);

        this.accountName = config.getString(AdlsSinkConnectorConfig.ACCOUNT_NAME_CONFIG);
        this.filesystem = config.getString(AdlsSinkConnectorConfig.FILESYSTEM_CONFIG);
        this.basePath = config.getString(AdlsSinkConnectorConfig.BASE_PATH_CONFIG);

        this.sasToken = config.getPassword(AdlsSinkConnectorConfig.SAS_TOKEN_CONFIG).value();
        if (sasToken.startsWith("?")) {
            sasToken = sasToken.substring(1);
        }

        this.flushMaxRecords = config.getInt(AdlsSinkConnectorConfig.FLUSH_MAX_RECORDS_CONFIG);
        this.compressGzip = config.getBoolean(AdlsSinkConnectorConfig.COMPRESS_GZIP_CONFIG);
        this.retryMaxAttempts = config.getInt(AdlsSinkConnectorConfig.RETRY_MAX_ATTEMPTS_CONFIG);

        log.info("AdlsSinkTask started. account={}, filesystem={}, basePath={}, flushMaxRecords={}, compressGzip={}, retryMaxAttempts={}",
                accountName, filesystem, basePath, flushMaxRecords, compressGzip, retryMaxAttempts);
    }

    // ----------------------------------------------------------------------
    //   FORMATTER AVRO / STRUCT / MAP / PRIMITIVES
    // ----------------------------------------------------------------------

    String formatRecordValue(SinkRecord record) {
        Schema schema = record.valueSchema();
        Object value = record.value();

        if (value == null) {
            return "null";
        }

        if (schema == null) {
            // schemaless : String / Map / List…
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
            Map<?, ?> m = (Map<?, ?>) value;
            return toJson(m);
        }

        if (schema.type() == Schema.Type.ARRAY && value instanceof List<?>) {
            List<?> l = (List<?>) value;
            return toJson(l);
        }

        // primitives & autres
        return value.toString();
    }

    /**
     * Convertit récursivement un objet (Map, List, String, Number, Boolean ou null) en JSON.
     * <li> Map → objet JSON (itération préservant l'ordre si possible)
     * <li> List → tableau JSON
     * <li> String → échappe les guillemets doubles
     * <li> Number/Boolean → toString()
     * <br/>
     * → Limites : ne gère pas l'échappement complet des caractères de contrôle ni les POJO complexes.
     *
     * @param obj objet à convertir (Map/List/String/Number/Boolean/null)
     * @return représentation JSON simple sous forme de String
     */
    private String toJson(Object obj) {
        if (obj == null) return "null";

        if (obj instanceof Map<?, ?>) {
            Map<?, ?> m = (Map<?, ?>) obj;
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (var e : m.entrySet()) {
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

    private static boolean isAuthFailure(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof DataLakeStorageException) {
                DataLakeStorageException ex = (DataLakeStorageException) cur;
                int statusCode = ex.getStatusCode();
                String errorCode = null;
                try {
                    if (ex.getServiceMessage() != null && ex.getErrorCode() != null) {
                        errorCode = String.valueOf(ex.getErrorCode());
                    }
                } catch (Exception ignored) {
                    // best-effort
                }

                // 401 Unauthorized ou 403 Forbidden
                if (statusCode == 401 || statusCode == 403) {
                    return true;
                }
                // fallback of previous check
                //  specific ADLS error code if status code is not enough
                if (errorCode != null && errorCode.toLowerCase(Locale.ROOT).contains("authenticationfailed")) {
                    return true;
                }
            }

            if (cur instanceof HttpResponseException) {
                HttpResponseException ex = (HttpResponseException) cur;
                int statusCode = ex.getResponse() != null ? ex.getResponse().getStatusCode() : -1;
                if (statusCode == 401 || statusCode == 403) {
                    return true;
                }
            }

            cur = cur.getCause();
        }
        return false;
    }

    protected void flushPartitionBuffer(PartitionBuffer buf) {
        if (buf.isEmpty()) return;

        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        String extension = compressGzip ? ".log.gz" : ".log";

        // nom de fichier basé sur topic / partition / start-offset
        String fileName = String.format(
                "%s-p%d-o%d%s",
                buf.getTopic(),
                buf.getPartition(),
                buf.getStartOffset(),
                extension
        );

        String filePath = String.format("%s/date=%s/%s", basePath, date, fileName);

        String content = buf.getBuffer().toString();
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (compressGzip) {
            bytes = gzip(bytes);
        }

        log.info("Writing {} records ({} bytes) to ADLS file {}",
                buf.getRecordCount(), bytes.length, filePath);

        try {
            DataLakeFileClient client = buildFileClient(filePath);
            client.create(true);

            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            client.append(input, 0, bytes.length);
            client.flush(bytes.length, true);

            buf.clear();
        } catch (Exception e) {
            if (isAuthFailure(e)) {
                throw new ConnectException("ADLS authentication/authorization failure while writing " + filePath + ". " +
                        "Check SAS token permissions/expiry.", e);
            }

            // Pour tous les autres cas, on laisse Kafka Connect retenter.
            throw new RetriableException("ADLS transient error while writing " + filePath, e);
        }
    }

    private PartitionBuffer getBuffer(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return buffers.computeIfAbsent(tp, k -> new PartitionBuffer(topic, partition));
    }

    // ----------------------------------------------------------------------
    //   TASK LOGIC
    // ----------------------------------------------------------------------

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;

        for (SinkRecord record : records) {
            String topic = record.topic();
            int partition = record.kafkaPartition();
            long offset = record.kafkaOffset();

            PartitionBuffer buf = getBuffer(topic, partition);
            String formatted = formatRecordValue(record);
            buf.append(offset, formatted);

            if (buf.getRecordCount() >= flushMaxRecords) {
                flushPartitionBuffer(buf);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Flushing remaining buffers before shutdown");
        for (PartitionBuffer buf : buffers.values()) {
            flushPartitionBuffer(buf);
        }
        buffers.clear();
    }
}
