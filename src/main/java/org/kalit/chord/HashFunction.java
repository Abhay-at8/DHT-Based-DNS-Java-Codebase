package org.kalit.chord;

import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashFunction {

    private static long hash(int i) {
        byte[] hashbytes = new byte[4];
        hashbytes[0] = (byte) (i >> 24);
        hashbytes[1] = (byte) (i >> 16);
        hashbytes[2] = (byte) (i >> 8);
        hashbytes[3] = (byte) (i /* >> 0 */);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (md != null) {
            md.reset();
            md.update(hashbytes);
            byte[] result = md.digest();

            byte[] compressed = new byte[4];
            for (int j = 0; j < 4; j++) {
                byte temp = result[j];
                for (int k = 1; k < 5; k++) {
                    temp = (byte) (temp ^ result[j + k]);
                }
                compressed[j] = temp;
            }

            long ret = (compressed[0] & 0xFF) << 24 | (compressed[1] & 0xFF) << 16 | (compressed[2] & 0xFF) << 8
                    | (compressed[3] & 0xFF);
            ret = ret & (long) 0xFFFFFFFFl;
            return ret;
        }
        return 0;
    }

    public static long hashSocketAddress(InetSocketAddress addr) {
        return hash(addr.hashCode());
    }

    public static long hashString(String str) {
        return hash(str.hashCode());
    }
}
