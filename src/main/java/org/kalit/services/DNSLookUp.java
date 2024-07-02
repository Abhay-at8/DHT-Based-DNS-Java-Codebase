package org.kalit.services;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.List;
import java.util.Scanner;

import org.kalit.Helper;
import org.kalit.chord.ChordNode;
import org.xbill.DNS.*;
import org.xbill.DNS.Record;

public class DNSLookUp implements Runnable {
    private ChordNode node;
    private boolean running;
    private int count;
    private Connection c = null;
    private Statement stmt = null;
    private static Helper helper =new Helper();
    private DatagramSocket socket;

    public DNSLookUp(ChordNode node) {
        this.node = node;
        this.running = true;
        this.count = 0;
        
        try {
        	socket = new DatagramSocket(node.getNodeAddress().getPort() + 1);
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:" + node.toString() + ".db");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            String sql = "CREATE TABLE IF NOT EXISTS DOMAINNAMEMAP " + "( DOMAINNAME TEXT PRIMARY KEY NOT NULL, "
                    + "IP TEXT )";
            stmt.executeUpdate(sql);
            System.out.println("Table created successfully");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

    }

    @Override
    public void run() {
        while (running) {
            try {
            	Scanner userinput = new Scanner(System.in);
                
                
                	Scanner in = new Scanner(new FileInputStream("./dns.properties"));
                	String var = in.nextLine();
                	System.out.println(var);
                	if(var.equals("true"))
            			helper.isOffline=true;
            		if(var.equals("false"))
            			helper.isOffline=false;
            		in.close();
            	
            	/*
                ServerSocket connectionSocket = new ServerSocket(node.getNodeAddress().getPort() + 1);
                Socket socket = connectionSocket.accept();
                count++;
                System.out.println("Connection Established! Count : " + count);
                ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                String domainName = (String) objectInputStream.readObject();
                String resolvedIP = getDomainIP(domainName);
                objectOutputStream.writeObject(resolvedIP);
                socket.close();
                connectionSocket.close();*/
            		
            		byte[] commandByte = new byte[65000];
                    DatagramPacket request = new DatagramPacket(commandByte, commandByte.length);
                    socket.receive(request);
                    
                    String msg =new String(request.getData(), request.getOffset(), request.getLength());
                   // System.out.println("recieved buffer "+ request+" and\n"+msg+" over");
                    String domainName = new String(commandByte, 0, request.getLength());
                    String resolvedIP = getDomainIP(domainName);
                    if(resolvedIP==null) resolvedIP="null";
                    byte[] buffer = resolvedIP.getBytes();
           		 
                    InetAddress clientAddress = request.getAddress();
                    int clientPort = request.getPort();
                    
                    DatagramPacket response = new DatagramPacket(buffer, buffer.length, clientAddress, clientPort);
                    socket.send(response);
            } catch (IOException e) {
               // e.printStackTrace();
            	System.out.println("port in use");
            }
        }
    }

    public synchronized String getDomainIP(String domainName) {
        String findIP = null;
        try {
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT IP FROM DOMAINNAMEMAP WHERE DOMAINNAME='" + domainName + "' ;");
            
            while (rs.next()) {
                findIP = rs.getString("ip");
            }
            rs.close();
            
            if(helper.isOffline) {
        		System.out.println("Offline");
        	}
            else {
        		findIP = addDomainName(domainName,findIP);
        	}
            if (findIP == null) {
            	System.out.println(domainName+ " NOT IN DB and cannot resolve");
            	
            }
            else 
            {	//findIP=findIP.split(",")[0];
            	findIP=findIP.replaceAll(",","_");
            	System.out.println(domainName+ " IN DB" );
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(findIP);
        
        return findIP;
    }

    public synchronized String addDomainName(String domainName, String prevIP) {
        Lookup lookup;
        String googleDNS = "8.8.8.8";
        String ip = null;
        try {
            lookup = new Lookup(domainName, Type.A);
            SimpleResolver resolver = new SimpleResolver(googleDNS);

            Record[] result = null;

            int run = 3;
            while (run > 0) {
                resolver.setTimeout(5);
                lookup.setResolver(resolver);
                result = lookup.run();
                if (result == null) {
                    run--;
                    continue;
                } else
                    break;
            }

            if (result == null) {
                System.out.println("Could not resolve: " + domainName);
            } else {
                List<Record> records = java.util.Arrays.asList(result);
                java.util.Collections.shuffle(records);
                String resolvedIP;
                for (Record record : records) {
                    resolvedIP = ((ARecord) record).getAddress().getHostAddress();
                    ip = resolvedIP;
                }
            }
            
            String sql;
            if(prevIP==null) {
	            sql = "INSERT INTO DOMAINNAMEMAP (DOMAINNAME,IP) " + "VALUES ('" + domainName + "'," + "'" + ip
	                    + "' );";
	            
	            stmt = c.createStatement();
	            stmt.executeUpdate(sql);
	            stmt.close();
	            c.commit();
	            prevIP=ip;
            }
            else {
            	boolean isPresent=false;
            	String[] ipList=prevIP.split(",");
            	for (String ips : ipList) {
            		if(ips.equals(ip)) {
            			isPresent=true;
            			break;
            		}
            	}
            	
            	if(!isPresent) {
            		System.out.println("Different Ip queried this time " + ip +" "+prevIP);
            		prevIP=ip+",";
            		int cnt=1;
            		
            		
            		for (String ips : ipList) {
            			if(cnt==5) break;
            			prevIP+=ips+",";
            			cnt++;
            			//System.out.println(cnt);
						
					}
            		sql = "UPDATE DOMAINNAMEMAP SET IP = '" +prevIP+ "' WHERE DOMAINNAME = '" + domainName+"'";
            		stmt = c.createStatement();
                    stmt.executeUpdate(sql);
                    stmt.close();
                    c.commit();
            	}
            	else {
            		System.out.println("Same Ip got queried");
            	}
            }
            
        } catch (TextParseException e) {
            System.out.println("LookUp Exception");
            e.printStackTrace();
        } catch (UnknownHostException e) {
            System.out.println("Resolver Exception");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("SQL Exception");
            e.printStackTrace();
        }
        return prevIP;
    }

    public void stop() {
        running = false;
        try {
            stmt.close();
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}