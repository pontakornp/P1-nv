package edu.usfca.cs.dfs.util;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CheckSum {
    public static String checkSum(byte[] byteArray) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(byteArray);
            byte[] digest = md.digest();
            String checkSum = DatatypeConverter.printHexBinary(digest).toUpperCase();
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
