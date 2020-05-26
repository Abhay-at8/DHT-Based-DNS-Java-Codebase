package org.kalit.services;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.List;

import org.kalit.chord.ChordNode;
import org.xbill.DNS.*;

public class DNSLookUp implements Runnable {
    private ChordNode node;
    private boolean running;
    private int count;
    private Connection c = null;
    private Statement stmt = null;

    public DNSLookUp(ChordNode node) {
        this.node = node;
        this.running = true;
        this.count = 0;
        try {
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
                connectionSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
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
            if (findIP == null)
                findIP = addDomainName(domainName);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return findIP;
    }

    public synchronized String addDomainName(String domainName) {
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
            stmt = c.createStatement();
            String sql = "INSERT INTO DOMAINNAMEMAP (DOMAINNAME,IP) " + "VALUES ('" + domainName + "'," + "'" + ip
                    + "' );";
            stmt.executeUpdate(sql);
            stmt.close();
            c.commit();
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
        return ip;
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