package org.kalit.chord;

import org.kalit.Helper;

import java.net.InetSocketAddress;
import java.util.Scanner;

public class Chord {
    private static ChordNode node;
    private static InetSocketAddress contact;
    private static Helper helper;

    public static void main(String args[]) {
        helper = new Helper();

        String IP = args[0];
        String port = args[1];

        node = new ChordNode(Helper.createSocketAddress(IP + ":" + port));

        if (args.length == 2) {
            contact = node.getNodeAddress();
        } else if (args.length == 4) {
            String newIP = args[2];
            String newPort = args[3];
            contact = Helper.createSocketAddress(newIP + ":" + newPort);
            if (contact == null) {
                System.out.println("Cannot find node you are trying to contact. Now exit.");
                return;
            }
        } else {
            System.out.println("Wrong input. Now exit.");
            System.exit(0);
        }

        boolean successfulJoin = node.join(contact);

        if (!successfulJoin) {
            System.out.println("Cannot connect with node you are trying to contact. Now exit.");
            System.exit(0);
        }

        System.out.println("Joining the Chord ring.");
        System.out.println("Local IP: " + IP);
        node.printNeighbors();

        Scanner userinput = new Scanner(System.in);
        while (true) {
            System.out.println("\nType \"info\" to check this node's data or \n type \"quit\"to leave ring: ");
            String command = null;
            command = userinput.next();
            if (command.startsWith("quit")) {
                node.stopAllThreads();
                System.out.println("Leaving the ring...");
                userinput.close();
                System.exit(0);
            } else if (command.startsWith("info")) {
                node.printDataStructure();
            }
        }
    }
}