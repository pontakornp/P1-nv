package edu.usfca.cs.dfs;

import com.sangupta.murmur.Murmur3;

/**
 * BloomFilter - a way to test for set membership without actually storing all the data
 */
public class BloomFilter {
	private int bitCount;
	private int hashCount;
	private byte[] bloomFilterArray;
	private int elementCount;
	private static final long MURMUR_SEED = 0;
	
	public BloomFilter(Integer bitCount, Integer hashCount) {
		this.bitCount = bitCount;
		this.hashCount = hashCount;
		this.bloomFilterArray = new byte[bitCount];
	}

	public byte[] getBloomFilterArray() {
		return this.bloomFilterArray;
	}
	
	/**
     * 
     * 1. Generate the indexes of bloom filter using murmur function
     * 2. Uses optimised murmur hash which uses previous has to compute current hash
     * 3. Iterates for the number of hashCount
     * 
     */
    private int[] getBitLocations(byte[] data) {
    	int[] results = new int[this.hashCount];
    	
    	int prevHash = Math.abs((int) Murmur3.hash_x64_128(data, data.length, MURMUR_SEED)[0]%this.bitCount);
    	int nextHash = Math.abs((int) Murmur3.hash_x64_128(data, data.length, prevHash)[0]%this.bitCount);
    	
        for (int i = 0; i < this.hashCount; i++) {
            results[i] = (int) (prevHash + i*nextHash) % this.bitCount;
        }
        return results;
    }

    
    /**
     * 
     * 1. Generate the indexes of bloom filter using optimized murmur hash function
     * 2. Iterate through the indexes to check if the index is not zero in bloom filter
     * 3. If any one location is 0 return false else return true!	
     * 
     */
    public synchronized boolean getBloomKey(byte[] data) {
        int[] bitLocationArray = this.getBitLocations(data);

        for (int i = 0; i < bitLocationArray.length; i++) {
            if (this.bloomFilterArray[bitLocationArray[i]] == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * 1. Generate the indexes of bloom filter using optimized murmur hash function
     * 2. Updates the corresponding indexes of bloom filter
     * 3. Increments the element count
     * 
     */
    public synchronized void putBloomKey(byte[] data) {
        int[] bitLocations = this.getBitLocations(data);
        for (int i = 0; i < bitLocations.length; i++) {
            this.bloomFilterArray[bitLocations[i]] = 1;
        }
        this.elementCount++;
    }
    
    /**
     * 1. Generate the current false positive probability of bloom filter
     * false positve formula: p = pow(1 - exp(-k / (m / numberOfItems)), k)
     */
    public synchronized float falsePositive() {
        double exp = Math.exp((double) -this.hashCount / (this.bitCount / this.elementCount));
        double p = Math.pow(1 - exp, this.hashCount);
        return (float) p;
    }
}
