package edu.usfca.cs.dfs.util;

public class Entropy {
	
	/*
	 * This calculates the shannon entropy of byte array
	 * Chunk bytearray is used as input returns entropy of chunk as response
	 */
    public static double calculateShannonEntropy(byte[] input) {
        if (input.length == 0) {
            return 0.0;
        }
        /* Total up the occurrences of each byte */
        int[] charCounts = new int[256];
        for (byte b : input) {
            charCounts[b & 0xFF]++;
        }
        double entropy = 0.0;
        for (int i = 0; i < 256; ++i) {
            if (charCounts[i] == 0.0) {
                continue;
            }
            double freq = (double) charCounts[i] / input.length;
            entropy -= freq * (Math.log(freq) / Math.log(2));
        }
        return entropy;
    }
    
    
    public static void main(String[] args) {
//        byte[] test = Files.readAllBytes(Paths.get(args[0]));
        byte[] test = {5,10,5};
        double entr = calculateShannonEntropy(test);
        System.out.println("Entropy (bits per byte): "
                + String.format("%.2f", entr));
        System.out.println("Optimal Size Reduction: "
                + String.format("%.0f%%", (1 - (entr / 8)) * 100));
        System.out.println("Optimal Compression Ratio: "
                + String.format("%.0f:1", 8 / entr));
    }
}
