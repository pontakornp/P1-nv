package edu.usfca.cs.dfs.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class CheckSum {
    public static String checkSum(byte[] byteArray) {
        try {
        	byte[] copy = byteArray.clone();
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(copy);
            byte[] digest = md.digest();
            
            String checkSum = Base64.getEncoder().encodeToString(digest);
            return checkSum;
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Fail to do checksum.");
            return null;
        }
    }
    public static void main(String args[]) {
//        byte[] temp = {10,5,10};
//        String filename = "/Users/pontakornp/Documents/projects/bigdata/P1-nv/test_store.txt";
//        byte[] temp = new byte[0];
//        try {
//            temp = Files.readAllBytes(Paths.get(filename));
//            System.out.println(checkSum(temp));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }
}
