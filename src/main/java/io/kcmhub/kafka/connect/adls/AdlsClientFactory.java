package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;

public interface AdlsClientFactory {

    DataLakeFileClient createFileClient(String accountName,
                                        String filesystem,
                                        String sasToken,
                                        String path,
                                        int maxRetryAttempts);
}
