package io.kcmhub.kafka.connect.adls;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

class AdlsSinkTaskAuthFailureTest {

    @Test
    void shouldFailTaskOnAuthFailure401() {
        DataLakeFileClient mockClient = Mockito.mock(DataLakeFileClient.class);

        DataLakeStorageException authEx = Mockito.mock(DataLakeStorageException.class);
        Mockito.when(authEx.getStatusCode()).thenReturn(401);

        Mockito.doThrow(authEx).when(mockClient).create(true);

        AdlsClientFactory factory = Mockito.mock(AdlsClientFactory.class);
        Mockito.when(factory.createFileClient(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyInt()
        )).thenReturn(mockClient);

        Map<String, String> props = new HashMap<>();
        props.put("adls.account.name", "acc");
        props.put("adls.filesystem", "fs");
        props.put("adls.sas.token", "token");
        props.put("flush.max.records", "10");

        AdlsSinkTask task = new AdlsSinkTask();
        task.start(props);
        task.setClientFactory(factory);

        SinkRecord r1 = new SinkRecord("topicA", 0, null, null, null, "v1", 100L);

        assertThrows(ConnectException.class, () -> {
            task.put(List.of(r1));
            task.stop();
        });

        Mockito.verify(mockClient, Mockito.never()).append(Mockito.any(InputStream.class), Mockito.anyLong(), Mockito.anyLong());
    }
}
