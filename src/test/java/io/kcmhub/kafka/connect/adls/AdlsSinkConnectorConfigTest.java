package io.kcmhub.kafka.connect.adls;


import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class AdlsSinkConnectorConfigTest {

    @Test
    void shouldLoadConfigWithDefaults() {
        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "dxxxxxxxxxx1");
        props.put("adls.filesystem", "kafka-poc");
        props.put("adls.sas.token", "si=...");

        AdlsSinkConnectorConfig cfg = new AdlsSinkConnectorConfig(props);

        assertEquals("dxxxxxxxxxx1", cfg.getString(AdlsSinkConnectorConfig.ACCOUNT_NAME_CONFIG));
        assertEquals("kafka-poc", cfg.getString(AdlsSinkConnectorConfig.FILESYSTEM_CONFIG));
        assertEquals("kafka-export", cfg.getString(AdlsSinkConnectorConfig.BASE_PATH_CONFIG)); // default
        assertEquals(500, cfg.getInt(AdlsSinkConnectorConfig.FLUSH_MAX_RECORDS_CONFIG));       // default
        assertFalse(cfg.getBoolean(AdlsSinkConnectorConfig.COMPRESS_GZIP_CONFIG));             // default
        assertEquals(3, cfg.getInt(AdlsSinkConnectorConfig.RETRY_MAX_ATTEMPTS_CONFIG));        // default
    }

    @Test
    void shouldAllowRetryMaxAttemptsToBeZero() {
        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "acc");
        props.put("adls.filesystem", "fs");
        props.put("adls.sas.token", "token");
        props.put("adls.retry.max.attempts", "0");

        AdlsSinkConnectorConfig cfg = new AdlsSinkConnectorConfig(props);
        assertEquals(0, cfg.getInt(AdlsSinkConnectorConfig.RETRY_MAX_ATTEMPTS_CONFIG));
    }
}
