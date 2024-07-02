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
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.kalit.Helper;
import org.kalit.chord.HashFunction;

public class Query {
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
            
            BufferedWriter bf = new BufferedWriter(new FileWriter(file,true));
            
            HashMap<String, String[]> cache= new HashMap<String, String[]>(); 
           // HashMap<String, String[]> cache = helper.cacheFiletoMap(file); 
            if(file.isFile()) {
            	try {
	            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
	            cache=(HashMap<String, String[]>)ois.readObject();
	            ois.close();
            	}
            	catch(EOFException e ){
            		System.out.println("EOF exception");
            	}
            }
            long hrs=1;
            
//            String line = null; 
//            BufferedReader br = new BufferedReader(new FileReader(file)); 
//            // read file line by line 
//            while ((line = br.readLine()) != null) { 
//  
//                // split the line by : 
//            	
//                String[] parts = line.split(":"); 
//  
//                // first part is name, second is number 
//                String name = parts[0].trim(); 
//                String number = parts[1].trim(); 
//  
//                // put name, number in HashMap if they are 
//                // not empty 
//                if (!name.equals("") && !number.equals("")) 
//                    cache.put(name, number); 
//            }
//            br.close();


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
            Scanner in = null;
            String input = "";
            FileWriter myWriter = new FileWriter("report.csv");
            

            
            
            try {
                in = new Scanner(new FileInputStream(
                        "./dataset"));
            } catch (Exception e) {
                e.printStackTrace(); 
            }
           
            if (args.length == 3) {
            System.out.println("\nFeeding dataset. Please wait....");
            
            while (in.hasNextLine()) {
                input = in.nextLine();
                long startTime = System.nanoTime();
                if(cache.containsKey(input)) {
                	String [] op=cache.get(input);
	                long timeDiff=System.currentTimeMillis()-Long.parseLong(op[1]);
	            	System.out.println(op[1]+"time diff is "+TimeUnit.MILLISECONDS.toHours(timeDiff));
	            	if(TimeUnit.MILLISECONDS.toHours(timeDiff)>=hrs) {
	            		cache.remove(input);
	            		
	            		System.out.println("Cache refreshed");
	            	}
            	else {
                	
                	//System.out.println("Cached"+input+":"+cache.get(input));
                	long totalTime = System.nanoTime() - startTime;
                	
                	// myWriter.write(input+","+cache.get(input)+","+TimeUnit.NANOSECONDS.toMillis(totalTime)+",cache\n");
                	System.out.println(input+","+cache.get(input)[0]+","+TimeUnit.NANOSECONDS.toMillis(totalTime)+",cache\n");
                	myWriter.write(input+","+cache.get(input)[0]+","+TimeUnit.NANOSECONDS.toMillis(totalTime)+","+address.getAddress().toString()+","+totalTime+"\n");
                    
                	
                     sumTime=sumTime+totalTime;
                     no+=1;
                	continue;
            	}
                }
                long hash = HashFunction.hashString(input);
                System.out.println("\nHash value is " + Long.toHexString(hash));
                
                InetSocketAddress result = Helper.requestAddress(address, "FINDSUCC_" + hash);

                if (result == null) {
                    System.out.println("The node your are contacting is disconnected. Now exit.");
                    userinput.close();
                    System.exit(0);
                }
                
                try {
//                	int porttest=result.getPort() + 1;
//                	System.out.println("abhay "+porttest);
                    /*Socket querySocket = new Socket(result.getAddress(), result.getPort() + 1);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
                    ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
                    objectOutputStream.writeObject(input);
                    String resolvedIP = (String) objectInputStream.readObject();*/
                	DatagramSocket querySocket = new DatagramSocket();
                	byte[] inp = input.getBytes();
                	
                    DatagramPacket request = new DatagramPacket(inp, inp.length, result.getAddress(), result.getPort() + 1);
                   // System.out.println("sent buffer "+ input);
                    querySocket.send(request);
     
                    byte[] buffer = new byte[65000];
                    
                    DatagramPacket responseUDP = new DatagramPacket(buffer, buffer.length);
                    querySocket.receive(responseUDP);
     
                    String resolvedIP = new String(buffer, 0, responseUDP.getLength());
                    if(resolvedIP!=null && !resolvedIP.equals("null")) {
                    	cache.put(input,new String[] {resolvedIP,Long.toString(System.currentTimeMillis())});
                    	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
                		oos.writeObject(cache);
                		oos.close();
//                    	bf.write(input + ":"+ resolvedIP+":"+System.nanoTime());
//                        bf.newLine(); 
//                        bf.flush(); 
                    }
                    long totalTime = System.nanoTime() - startTime;
                    System.out.println("\nResponse from node " + address.getAddress().toString() + ", port "
                            + address.getPort() + ", position " + Helper.hexIdAndPosition(address) + ":");
                    System.out.println("Node " + result.getAddress().toString() + ", port " + result.getPort()
                            + ", position " + Helper.hexIdAndPosition(result));
                    System.out.println(input + " : " + resolvedIP);
                    System.out.println("DNS RESOLUTION TIME: " +TimeUnit.NANOSECONDS.toMillis(totalTime)+ "ms");
                    myWriter.write(input+","+resolvedIP+","+TimeUnit.NANOSECONDS.toMillis(totalTime)+","+address.getAddress().toString()+","+totalTime+"\n");
          
                    sumTime=sumTime+totalTime;
                    no+=1;
                    
                    querySocket.close();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            in.close();
            

            System.out.println("\nCompleted feeding dataset.\n");
            
            if(no!=0) {
            	avgTime=sumTime/no;
            	//myWriter.close();
            	System.out.println("Avg TIME: " + TimeUnit.NANOSECONDS.toMillis((long) avgTime)+ "ms");
            }
        //    System.exit(0);
            }
            
            while (true) {
            	
                System.out.println("\nPlease enter your domainName (or type \"quit\" to leave): ");
                String command = userinput.nextLine();
                
                
                
                if (command.startsWith("quit")) {
                	if(no!=0) {
	                	avgTime=sumTime/no;
	                	myWriter.close();
	                	System.out.println("Avg TIME: " + TimeUnit.NANOSECONDS.toMillis((long) avgTime)+ "ms");
	                }
                	
                	bf.close();
                    System.exit(0);
                } else if (command.length() > 0) {
                    long hash = HashFunction.hashString(command);
                    System.out.println("\nHash value is " + Long.toHexString(hash));
                    long startTime = System.nanoTime();
                    
                    if(cache.containsKey(command)) {
                    	String [] op=cache.get(command);
                    	long timeDiff=System.currentTimeMillis()-Long.parseLong(op[1]);
                    	System.out.println(op[1]+"time diff is "+TimeUnit.MILLISECONDS.toHours(timeDiff));
                    	if(TimeUnit.MILLISECONDS.toHours(timeDiff)>=hrs) {
                    		cache.remove(command);
                    		
                    		System.out.println("Cache refreshed");
                    	}
                    	else {
                    	//System.out.println("Cached "+command+":"+cache.get(command));
                    	long totalTime = System.nanoTime() - startTime;
                    	System.out.println(command+","+cache.get(command)[0]+","+(totalTime)+ " ns,cache\n");
                         
                         sumTime=sumTime+totalTime;
                         no+=1;
                    	continue;
                    	}
                    }
                    InetSocketAddress result = Helper.requestAddress(address, "FINDSUCC_" + hash);

                    // if fail to send request, local node is disconnected, exit
                    if (result == null) {
                        System.out.println("The node your are contacting is disconnected. Now exit.");
                        userinput.close();
                        System.exit(0);
                    }

                    try {/*
                        Socket querySocket = new Socket(result.getAddress(), result.getPort() + 1);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
                        ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
                        objectOutputStream.writeObject(command);
                        String resolvedIP = (String) objectInputStream.readObject();
                        if(resolvedIP!=null && !resolvedIP.equals("null")) {
                        	cache.put(command,new String[] {resolvedIP,Long.toString(System.currentTimeMillis())});
//                        	bf.write(command + ":"+ resolvedIP);
//                            bf.newLine(); 
//                            bf.flush();
                        	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
                    		oos.writeObject(cache);
                    		oos.close();
                        }*/
                    	
                    	DatagramSocket querySocket = new DatagramSocket();
                    	byte[] inp = command.getBytes();
                    	
                        DatagramPacket request = new DatagramPacket(inp, inp.length, result.getAddress(), result.getPort() + 1);
                        System.out.println("sent buffer "+ command);
                        querySocket.send(request);
         
                        byte[] buffer = new byte[65000];
                        
                        DatagramPacket responseUDP = new DatagramPacket(buffer, buffer.length);
                        querySocket.receive(responseUDP);
         
                        String resolvedIP = new String(buffer, 0, responseUDP.getLength());
                        
                        if(resolvedIP!=null && !resolvedIP.equals("null")) {
                        	cache.put(command,new String[] {resolvedIP,Long.toString(System.currentTimeMillis())});
//                        	bf.write(command + ":"+ resolvedIP);
//                            bf.newLine(); 
//                            bf.flush();
                        	ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
                    		oos.writeObject(cache);
                    		oos.close();
                        }
                        long totalTime = System.nanoTime() - startTime;

                        System.out.println("\nResponse from node " + address.getAddress().toString() + ", port "
                                + address.getPort() + ", position " + Helper.hexIdAndPosition(address) + ":");
                        System.out.println("Node " + result.getAddress().toString() + ", port " + result.getPort()
                                + ", position " + Helper.hexIdAndPosition(result));
                        System.out.println("RESOLVED IP: " + resolvedIP);
                        System.out.println("DNS RESOLUTION TIME: " +TimeUnit.NANOSECONDS.toMillis(totalTime)+ "ms");
                        sumTime=sumTime+totalTime;
                        no+=1;
                        myWriter.write(command+","+resolvedIP+","+TimeUnit.NANOSECONDS.toMillis(totalTime)+","+address.getAddress().toString()+","+totalTime+"\n");
                         
                        

                        querySocket.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }   
        } else {
            System.out.println("\nInvalid input. Now exit.\n");
        }
    }
}