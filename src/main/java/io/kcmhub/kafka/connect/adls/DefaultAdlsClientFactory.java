package io.kcmhub.kafka.connect.adls;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;

public class DefaultAdlsClientFactory implements AdlsClientFactory {

    @Override
    public DataLakeFileClient createFileClient(String accountName,
                                               String filesystem,
                                               String sasToken,
                                               String path,
                                               int maxRetryAttempts) {
        String endpoint = String.format("https://%s.dfs.core.windows.net", accountName);

        // Config contract:
        // - maxRetryAttempts = number of retries (additional attempts)
        // - Azure SDK RequestRetryOptions uses maxTries = total number of tries and must be >= 1
        // Therefore: maxTries = maxRetryAttempts + 1
        int maxTries = Math.max(1, maxRetryAttempts + 1);

        // Azure SDK defaults: exponential backoff with 4s base delay and 120s max delay.
        // We keep explicit values close to our previous settings (base 4s, max 60s).
        RequestRetryOptions retryOptions = new RequestRetryOptions(
                RetryPolicyType.EXPONENTIAL,
                maxTries,
                null,
                4000L,
                60000L,
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
