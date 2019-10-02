package edu.usfca.cs.dfs;

import com.sangupta.murmur.Murmur3;

/**
 * BloomFilter - a way to test for set membership without actually storing all the data
 */
public class BloomFilter {

    public final int m = 10; // m: the number of bits in the filter
    public final int k = 3; // k: the number of hash functions
    public final long seed = 3;
    public long numberOfItems = 0;
    public byte[] bloomFilter = new byte[m];

    private long[] getBitLocations(byte[] data, int m, int k) {
        long[] results = new long[k];
        long hash1 = Murmur3.hash_x86_32(data, data.length, seed);
        long hash2 = Murmur3.hash_x86_32(data, data.length, hash1);
        for (int i = 0; i < k; i++) {
            results[i] = (hash1 + i * hash2) % m;
//            System.out.println(results[i]);
        }
        return results;
    }

    public boolean get(byte[] data) {
        /**
         * 1. Hash it!!!
         */
        long[] bitLocationArray = getBitLocations(data, m, k);

        /**
         * 2. Loop through bitLocation
         */
        for (int i = 0; i < bitLocationArray.length; i++) {
            if (bloomFilter[(int) bitLocationArray[i]] == 0) {
                return false;
            }
        }

        /**
         * 3. If any one location is 0 return false;
         * else return true!
         */
        return true;
    }

    public void put(byte[] data) {
        long[] bitLocations = getBitLocations(data, m, k);
        for (int i = 0; i < bitLocations.length; i++) {
            bloomFilter[(int) bitLocations[i]] = 1;
        }
        numberOfItems++;
    }
    // false positve formula: p = pow(1 - exp(-k / (m / numberOfItems)), k)
    public float falsePositive() {
        System.out.println("before:" + (double) -k / (m / numberOfItems));
        double exp = Math.exp((double) -k / (m / numberOfItems));
        System.out.println("Exp:" + exp);
        double p = Math.pow(1 - exp, k);
        System.out.println("p:" + p);
        return (float) p;
    }
}
