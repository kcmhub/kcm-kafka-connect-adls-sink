package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import io.kcmhub.kafka.connect.adls.dto.PartitionBuffer;
import io.kcmhub.kafka.connect.adls.utils.AuthFailureDetector;
import io.kcmhub.kafka.connect.adls.utils.SimpleJsonFormatter;
import io.kcmhub.kafka.connect.adls.utils.TimeBasedFlusher;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
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
    private long flushIntervalMs;

    private AdlsClientFactory clientFactory = new DefaultAdlsClientFactory();

    // For test purposes
    void setClientFactory(AdlsClientFactory factory) {
        this.clientFactory = factory;
    }

    private DataLakeFileClient buildFileClient(String filePath) {
        return clientFactory.createFileClient(accountName, filesystem, sasToken, filePath, retryMaxAttempts);
    }

    // Buffers by topic-partition
    private final Map<TopicPartition, PartitionBuffer> buffers = new HashMap<>();

    // Last successful flush time per topic-partition (used for time-based flush)
    private final Map<TopicPartition, Long> lastFlushMsByTp = new HashMap<>();

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
        this.flushIntervalMs = config.getLong(AdlsSinkConnectorConfig.FLUSH_INTERVAL_MS_CONFIG);

        log.info("AdlsSinkTask started. account={}, filesystem={}, basePath={}, flushMaxRecords={}, compressGzip={}, retryMaxAttempts={}, flushIntervalMs={}",
                accountName, filesystem, basePath, flushMaxRecords, compressGzip, retryMaxAttempts, flushIntervalMs);
    }

    // ----------------------------------------------------------------------
    //   FORMATTER AVRO / STRUCT / MAP / PRIMITIVES
    // ----------------------------------------------------------------------

    String formatRecordValue(SinkRecord record) {
        return SimpleJsonFormatter.formatRecordValue(record);
    }

    protected void flushPartitionBuffer(PartitionBuffer buf) {
        if (buf.isEmpty()) return;

        String date = LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        String extension = compressGzip ? ".log.gz" : ".log";

        // File name based on topic / partition / start-offset
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

            // Update last flush after a successful write
            TopicPartition tp = new TopicPartition(buf.getTopic(), buf.getPartition());
            lastFlushMsByTp.put(tp, System.currentTimeMillis());
        } catch (Exception e) {
            if (AuthFailureDetector.isAuthFailure(e)) {
                throw new ConnectException("ADLS authentication/authorization failure while writing " + filePath + ". " +
                        "Check SAS token permissions/expiry.", e);
            }

            // For all other cases, let Kafka Connect retry.
            throw new RetriableException("ADLS transient error while writing " + filePath, e);
        }
    }

    private PartitionBuffer getBuffer(String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        return buffers.computeIfAbsent(tp, k -> {
            lastFlushMsByTp.putIfAbsent(tp, System.currentTimeMillis());
            return new PartitionBuffer(topic, partition);
        });
    }

    private void flushExpiredBuffersIfNeeded(long nowMs) {
        if (flushIntervalMs <= 0) return;

        for (Map.Entry<TopicPartition, PartitionBuffer> e : buffers.entrySet()) {
            TopicPartition tp = e.getKey();
            PartitionBuffer buf = e.getValue();

            if (buf.isEmpty()) continue;

            if (TimeBasedFlusher.shouldFlush(flushIntervalMs, nowMs, tp, lastFlushMsByTp)) {
                long lastFlush = lastFlushMsByTp.getOrDefault(tp, nowMs);
                long ageMs = nowMs - lastFlush;
                log.info("Flushing buffer for {} because flush.interval.ms={} (ageMs={})",
                        tp, flushIntervalMs, ageMs);
                flushPartitionBuffer(buf);
            }
        }
    }

    // ----------------------------------------------------------------------
    //   TASK LOGIC
    // ----------------------------------------------------------------------

    @Override
    public void put(Collection<SinkRecord> records) {

        long nowMs = System.currentTimeMillis();
        flushExpiredBuffersIfNeeded(nowMs);

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
        lastFlushMsByTp.clear();
    }
}
