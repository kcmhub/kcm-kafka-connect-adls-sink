package io.kcmhub.kafka.connect.adls.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class CompressionUtils {
    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
                gos.write(data);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to gzip content", e);
        }
    }
}
