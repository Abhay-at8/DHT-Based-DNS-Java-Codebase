package org.kalit;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.kalit.chord.HashFunction;

public class Helper {

    private static HashMap<Integer, Long> powerOfTwo = null;
    public static boolean isOffline;

    public Helper() {
    	isOffline=true;
        powerOfTwo = new HashMap<Integer, Long>();
        long base = 1;
        for (int i = 0; i <= 32; i++) {
            powerOfTwo.put(i, base);
            base *= 2;
        }
    }

    public static long getPowerOfTwo(int k) {
        return powerOfTwo.get(k);
    }

    public static String hexIdAndPosition(InetSocketAddress addr) {
        long hash = HashFunction.hashSocketAddress(addr);
        return (longTo8DigitHex(hash) + " (" + hash * 100 / Helper.getPowerOfTwo(32) + "%)");
    }

    public static String longTo8DigitHex(long l) {
        String hex = Long.toHexString(l);
        int lack = 8 - hex.length();
        StringBuilder sb = new StringBuilder();
        for (int i = lack; i > 0; i--) {
            sb.append("0");
        }
        sb.append(hex);
        return sb.toString();
    }

    public static long ithStart(long nodeid, int i) {
        return (nodeid + powerOfTwo.get(i - 1)) % powerOfTwo.get(32);
    }

    public static long computeRelativeId(long universal, long local) {
        long ret = universal - local;
        if (ret < 0) {
            ret += powerOfTwo.get(32);
        }
        return ret;
    }

    public static InetSocketAddress requestAddress(InetSocketAddress server, String req) {

        if (server == null || req == null) {
            return null;
        }

        String response = sendRequest(server, req);

        if (response == null) {
            return null;
        }

        else if (response.startsWith("NOTHING"))
            return server;

        else {
            InetSocketAddress ret = Helper.createSocketAddress(response.split("_")[1]);
            return ret;
        }
    }
    public static HashMap<String,String[]> cacheFiletoMap(File file) throws IOException{
    	String line = null; 
    	HashMap<String, String[]> cache = new HashMap<String, String[]>(); 
        BufferedReader br = new BufferedReader(new FileReader(file)); 
        // read file line by line 
        while ((line = br.readLine()) != null) { 

            // split the line by : 
        	
            String[] parts = line.split(":"); 

            // first part is name, second is number 
            String name = parts[0].trim(); 
            String number = parts[1].trim(); 
            String time = parts[2].trim(); 

            // put name, number in HashMap if they are 
            // not empty 
            if (!name.equals("") && !number.equals("")) 
                cache.put(name, new String[] {number,time}); 
        }
        br.close();
        return cache;
    }
    public static String sendRequest(InetSocketAddress server, String req) {

        if (server == null || req == null)
            return null;

        Socket talkSocket = null;

        try {
            talkSocket = new Socket(server.getAddress(), server.getPort());
            PrintStream output = new PrintStream(talkSocket.getOutputStream());
            output.println(req);
        } catch (IOException e) {
            return null;
        }

        try {
            Thread.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        InputStream input = null;
        try {
            input = talkSocket.getInputStream();
        } catch (IOException e) {
            System.out.println("Cannot get input stream from " + server.toString() + "\nRequest is: " + req + "\n");
        }
        String response = Helper.inputStreamToString(input);

        try {
            talkSocket.close();
        } catch (IOException e) {
            throw new RuntimeException("Cannot close socket", e);
        }
        return response;
    }

    public static String inputStreamToString(InputStream in) {

        if (in == null) {
            return null;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            System.out.println("Cannot read line from input stream.");
            return null;
        }
        return line;
    }

    public static InetSocketAddress createSocketAddress(String addr) {
        if (addr == null) {
            return null;
        }
        String[] splitted = addr.split(":");
        if (splitted.length >= 2) {
            String ip = splitted[0];
            if (ip.startsWith("/")) {
                ip = ip.substring(1);
            }
            InetAddress m_ip = null;
            try {
                m_ip = InetAddress.getByName(ip);
            } catch (UnknownHostException e) {
                System.out.println("Cannot create ip address: " + ip);
                return null;
            }
            String port = splitted[1];
            int m_port = Integer.parseInt(port);
            return new InetSocketAddress(m_ip, m_port);
        } else {
            return null;
        }

    }
}