package edu.usfca.cs.dfs.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class CheckSum {
    public static String checkSum(byte[] byteArray) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(byteArray);
            byte[] digest = md.digest();
            
            String checkSum = Base64.getEncoder().encodeToString(digest);
            return checkSum;
        } catch (NoSuchAlgorithmException e) {
            System.out.println("Fail to do checksum.");
            return null;
        }
    }
    public static void main(String args[]) {
        byte[] temp = {10,5,10};
        System.out.println(checkSum(temp));
    }
}
