package io.kcmhub.kafka.connect.adls.utils;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.file.datalake.models.DataLakeStorageException;

import java.util.Locale;

/**
 * Centralizes the rules to detect authentication/authorization errors.
 */
public final class AuthFailureDetector {

    private AuthFailureDetector() {
    }

    public static boolean isAuthFailure(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof DataLakeStorageException) {
                DataLakeStorageException ex = (DataLakeStorageException) cur;
                int statusCode = ex.getStatusCode();

                if (statusCode == 401 || statusCode == 403) {
                    return true;
                }

                // Best-effort fallback for some ADLS errors.
                try {
                    String errorCode = ex.getErrorCode() != null ? String.valueOf(ex.getErrorCode()) : null;
                    if (errorCode != null && errorCode.toLowerCase(Locale.ROOT).contains("authenticationfailed")) {
                        return true;
                    }
                } catch (Exception ignored) {
                    // best-effort
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
}

