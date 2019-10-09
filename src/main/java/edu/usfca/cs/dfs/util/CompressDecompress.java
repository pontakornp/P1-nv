package edu.usfca.cs.dfs.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressDecompress {
    // Reference: https://dzone.com/articles/how-compress-and-uncompress
    public static byte[] compress(byte[] data) {
        byte[] output = null;
        try {
            Deflater deflater = new Deflater();
            deflater.setInput(data);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            output = outputStream.toByteArray();
        } catch (IOException e) {
            System.out.println("Fail to compress chunk.");
        }
        return output;

    }

    // Reference: https://dzone.com/articles/how-compress-and-uncompress
    public static byte[] decompress(byte[] data) {
        byte[] output = null;
        try {
            Inflater inflater = new Inflater();
            inflater.setInput(data);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            output = outputStream.toByteArray();
        } catch (IOException | DataFormatException e) {
            System.out.println("Fail to decompress chunk.");
        }
        return output;
    }
}
