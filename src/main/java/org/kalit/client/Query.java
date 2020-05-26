package org.kalit.client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import org.kalit.Helper;
import org.kalit.chord.HashFunction;

public class Query {
    private static InetSocketAddress address;
    private static Helper helper;

    public static void main(String args[]) {
        helper = new Helper();

        if (args.length == 2) {
            address = Helper.createSocketAddress(args[0] + ":" + args[1]);
            if (address == null) {
                System.out.println("Cannot find address you are trying to contact. Now exit.");
                System.exit(0);
            }

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
            try {
                in = new Scanner(new FileInputStream(
                        "/home/kalit/Desktop/DNSServerWithCaching/src/main/java/org/kalit/client/dataset"));
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("\nFeeding dataset. Please wait....");
            while (in.hasNextLine()) {
                input = in.nextLine();
                long hash = HashFunction.hashString(input);
                System.out.println("\nHash value is " + Long.toHexString(hash));
                InetSocketAddress result = Helper.requestAddress(address, "FINDSUCC_" + hash);

                if (result == null) {
                    System.out.println("The node your are contacting is disconnected. Now exit.");
                    userinput.close();
                    System.exit(0);
                }

                try {
                    Socket querySocket = new Socket("127.0.0.1", result.getPort() + 1);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
                    ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
                    objectOutputStream.writeObject(input);
                    String resolvedIP = (String) objectInputStream.readObject();
                    System.out.println("\nResponse from node " + address.getAddress().toString() + ", port "
                            + address.getPort() + ", position " + Helper.hexIdAndPosition(address) + ":");
                    System.out.println("Node " + result.getAddress().toString() + ", port " + result.getPort()
                            + ", position " + Helper.hexIdAndPosition(result));
                    System.out.println(input + " : " + resolvedIP);
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

            while (true) {
                System.out.println("\nPlease enter your domainName (or type \"quit\" to leave): ");
                String command = userinput.nextLine();

                if (command.startsWith("quit")) {
                    System.exit(0);
                } else if (command.length() > 0) {
                    long hash = HashFunction.hashString(command);
                    System.out.println("\nHash value is " + Long.toHexString(hash));
                    InetSocketAddress result = Helper.requestAddress(address, "FINDSUCC_" + hash);

                    // if fail to send request, local node is disconnected, exit
                    if (result == null) {
                        System.out.println("The node your are contacting is disconnected. Now exit.");
                        userinput.close();
                        System.exit(0);
                    }

                    try {
                        Socket querySocket = new Socket("127.0.0.1", result.getPort() + 1);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(querySocket.getOutputStream());
                        ObjectInputStream objectInputStream = new ObjectInputStream(querySocket.getInputStream());
                        objectOutputStream.writeObject(command);
                        String resolvedIP = (String) objectInputStream.readObject();

                        System.out.println("\nResponse from node " + address.getAddress().toString() + ", port "
                                + address.getPort() + ", position " + Helper.hexIdAndPosition(address) + ":");
                        System.out.println("Node " + result.getAddress().toString() + ", port " + result.getPort()
                                + ", position " + Helper.hexIdAndPosition(result));
                        System.out.println("RESOLVED IP: " + resolvedIP);

                        querySocket.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            System.out.println("\nInvalid input. Now exit.\n");
        }
    }
}