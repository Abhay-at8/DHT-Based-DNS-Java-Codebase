package org.kalit.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.kalit.Helper;
import org.kalit.chord.HashFunction;

public class LoadParallel {
    private static InetSocketAddress address;
    private static Helper helper;

    public static void main(String args[]) throws IOException, ClassNotFoundException {
        helper = new Helper();
        

        if (args.length == 3 || args.length == 2) {
            address = Helper.createSocketAddress(args[0] + ":" + args[1]);
            
            if (address == null) {
                System.out.println("Cannot find address you are trying to contact. Now exit.");
                System.exit(0);
            }
            float avgTime=0;
            int no=0;
            long sumTime=0;
            
            File file = new File("cache.txt"); 
            
           // BufferedWriter bf = new BufferedWriter(new FileWriter(file,true));
            
            HashMap<String, String[]> cache= new HashMap<String, String[]>(); 

            long hrs=1;
 


            // successfully constructed socket address of the node we are
            // trying to contact, check if it's alive
            String response = Helper.sendRequest(address, "KEEP");

            // if it's dead, exit
            if (response == null || !response.equals("ALIVE")) {
                System.out.println("\nCannot find node you are trying to contact. Now exit.\n");
                System.exit(0);
            }

            // it's alive, print connection info
            System.out.println("Connection to node " + address.getAddress().toString() + ", port " + address.getPort()
                    + ", position " + Helper.hexIdAndPosition(address) + ".");

            // check if system is stable
            boolean pred = false;
            boolean succ = false;
            InetSocketAddress pred_addr = Helper.requestAddress(address, "YOURPRE");
            InetSocketAddress succ_addr = Helper.requestAddress(address, "YOURSUCC");
            if (pred_addr == null || succ_addr == null) {
                System.out.println("The node your are contacting is disconnected. Now exit.");
                System.exit(0);
            }
            if (pred_addr.equals(address))
                pred = true;
            if (succ_addr.equals(address))
                succ = true;

            // we suppose the system is stable if (1) this node has both valid
            // predecessor and successor or (2) none of them
            while (pred ^ succ) {
                System.out.println("Waiting for the system to be stable...");
                pred_addr = Helper.requestAddress(address, "YOURPRE");
                succ_addr = Helper.requestAddress(address, "YOURSUCC");
                if (pred_addr == null || succ_addr == null) {
                    System.out.println("The node your are contacting is disconnected. Now exit.");
                    System.exit(0);
                }
                if (pred_addr.equals(address))
                    pred = true;
                else
                    pred = false;
                if (succ_addr.equals(address))
                    succ = true;
                else
                    succ = false;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                }
            }

            // begin to take user input
            Scanner userinput = new Scanner(System.in);

            File folder = new File(args[2]);
            File[] listOfFiles = folder.listFiles();
            	
            Arrays.asList(listOfFiles).parallelStream()
            .forEach(
                s -> {
                if (s.isFile()) {
                	String fp=args[2]+"\\"+s.getName();
                    System.out.println("File name is "+fp);
                    Scanner in = null;
                
            
            try {
                in = new Scanner(new FileInputStream(
                		fp));
            } catch (Exception e) {
                e.printStackTrace();
            }
           
            
            System.out.println("\n\n\n\nFeeding dataset. Please wait....\n\n");
            
            while (in.hasNextLine()) {
            	String input = in.nextLine();
                long startTime = System.nanoTime();

                long hash = HashFunction.hashString(input);
                System.out.println("\nHash value is " + Long.toHexString(hash));
                
                InetSocketAddress result = Helper.requestAddress(address, "FINDSUCC_" + hash);

                if (result == null) {
                    System.out.println("The node your are contacting is disconnected. Now exit.");
                    userinput.close();
                    System.exit(0);
                }
                
                try {
             
                    Socket querySocket = new Socket(result.getAddress(), result.getPort() + 1);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
                    ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
                    objectOutputStream.writeObject(input);
                    String resolvedIP = (String) objectInputStream.readObject();

                    long totalTime = System.nanoTime() - startTime;

                    System.out.println(input + " : " + resolvedIP);
                    System.out.println("DNS RESOLUTION TIME: " +TimeUnit.NANOSECONDS.toMillis(totalTime)+ "ms "+fp);

                    
                    querySocket.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            in.close();
            

            System.out.println("\nCompleted feeding dataset.\n");
            
           }
                
          });
            
   
        } else {
            System.out.println("\nInvalid input. Now exit.\n");
        }
    }
}