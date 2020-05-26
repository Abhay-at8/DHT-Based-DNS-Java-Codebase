package org.kalit.services;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.kalit.chord.ChordNode;

public class Listener implements Runnable {

    private ChordNode node;
    private boolean running;
    private ServerSocket serverSocket;

    public Listener(ChordNode node) {
        this.node = node;
        this.running = true;
        InetSocketAddress address = node.getNodeAddress();
        int port = address.getPort();
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            throw new RuntimeException("\nCannot open listener port " + port + ". Now exit.\n", e);
        }
    }

    @Override
    public void run() {
        while (running) {
            Socket talkSocket = null;
            try {
                talkSocket = serverSocket.accept();
            } catch (IOException e) {
                throw new RuntimeException("Cannot accepting connection", e);
            }
            new Thread(new Talker(talkSocket, node)).start();
        }
    }

    public void stop() {
        running = false;
    }
}