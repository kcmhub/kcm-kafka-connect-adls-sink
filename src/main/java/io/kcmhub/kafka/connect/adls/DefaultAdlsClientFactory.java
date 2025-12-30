package io.kcmhub.kafka.connect.adls;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;

import java.time.Duration;

public class DefaultAdlsClientFactory implements AdlsClientFactory {

    @Override
    public DataLakeFileClient createFileClient(String accountName,
                                               String filesystem,
                                               String sasToken,
                                               String path,
                                               int maxRetryAttempts) {
        String endpoint = String.format("https://%s.dfs.core.windows.net", accountName);

        int attempts = maxRetryAttempts <= 0 ? 1 : maxRetryAttempts;

        // Azure SDK: maxRetries = 0 => 1 tentative; maxRetries = 3 => 4 tentatives au total.
        int maxRetries = Math.max(0, attempts - 1);

        RequestRetryOptions retryOptions = new RequestRetryOptions(
                RetryPolicyType.EXPONENTIAL,
                maxRetries,
                null,
                Duration.ofSeconds(4),
                Duration.ofSeconds(60),
                null
        );

        return new DataLakePathClientBuilder()
                .endpoint(endpoint)
                .fileSystemName(filesystem)
                .pathName(path)
                .sasToken(sasToken)
                .retryOptions(retryOptions)
                .buildFileClient();
    }
}
